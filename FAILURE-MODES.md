# Medelanden: Failure Modes

This document catalogs every failure mode in Medelanden, what happens when it
occurs, and how the system recovers. Each failure mode references the relevant
source code and, where applicable, the test that exercises it.

## Overview

Medelanden is designed so that failures degrade the system gracefully rather
than catastrophically. The general principle: **writes always succeed on the
local node; everything else is best-effort.**

---

## 1. Write Path Failures

The write path is intentionally simple: validate, assign sequence, append to
WAL, ACK. Failures here are local and immediate.

### 1.1 No Matching Stream

**What happens:** A message is published with a subject that does not match any
stream's subject filter.

**Behavior:** The node returns an error: `no stream matches subject "<subject>"`.
The message is not stored. The producer receives `-ERR` over the wire protocol.

**Recovery:** The producer must publish to a subject that matches a configured
stream, or the operator must create a stream with the appropriate subject filter.

**Code:** `broker/node.go:282` (Publish), `broker/stream.go:112` (MatchSubject
check)

**Test:** `TestNodeNoMatchingStream` in `broker/node_test.go`

### 1.2 WAL Write Failure

**What happens:** The underlying filesystem returns an error when the WAL tries
to append (disk full, I/O error, permission denied).

**Behavior:** The WAL's `Append` method rolls back the sequence counter and
returns the error. The node surfaces this as a publish error. The message is not
acknowledged. Metrics counter `medelanden_publish_errors_total` is incremented.

**Recovery:** The operator must resolve the filesystem issue (free space, fix
permissions, replace disk). Once resolved, the node resumes accepting writes
without restart.

**Code:** `broker/wal.go:115-151` (Append with rollback)

**Test:** `TestWALWriteFailure` in `broker/failure_test.go`

### 1.3 Message Encode Failure

**What happens:** A message cannot be serialized to JSON (e.g., it contains
fields that cannot be marshaled).

**Behavior:** The WAL rolls back the sequence counter. An error is returned to
the caller. In practice this is extremely unlikely with the current `Message`
struct since all fields are primitive types.

**Recovery:** Fix the message content. This is a programming error in the
producer.

**Code:** `broker/wal.go:122-125`

### 1.4 Fsync Failure

**What happens:** The WAL is configured with `fsync_policy: every` and the
`file.Sync()` call fails after a successful write.

**Behavior:** The message is written to the OS page cache but may not be durable
on disk. The WAL returns an error wrapping the fsync failure. The sequence
counter is NOT rolled back (the data is in the file, just not synced).

**Impact:** If the node crashes before the OS flushes the page cache, the message
may be lost. With `fsync_policy: interval`, the background fsync goroutine
silently ignores sync errors (it will retry on the next tick). With
`fsync_policy: none`, fsync is never called and this failure mode does not apply.

**Code:** `broker/wal.go:143-148` (FsyncEvery), `broker/wal.go:97-111`
(background fsync)

### 1.5 Duplicate Stream Creation

**What happens:** A stream is created with a name that already exists.

**Behavior:** `CreateStream` returns an error: `stream <name> already exists`.
The existing stream is not affected.

**Test:** `TestNodeDuplicateStreamCreation` in `broker/node_test.go`

---

## 2. Read Path Failures

The read path is where complexity lives. Failures here affect consumers but
never affect the write path.

### 2.1 Consumer on Nonexistent Stream

**What happens:** A consumer is created referencing a stream name that does not
exist.

**Behavior:** `CreateConsumer` returns an error: `stream <name> not found`.

**Code:** `broker/node.go:326-329`

**Test:** `TestConsumerOnNonexistentStream` in `broker/failure_test.go`

### 2.2 Late Message Arrival (Drop Policy)

**What happens:** A message arrives at a consumer with `producer_ts` below the
current watermark, and the consumer is configured with `late_policy: drop`.

**Behavior:** The message is counted (`medelanden_consumer_late_messages_total`
increments) but silently dropped. It never appears in the consumer's output.

**Impact:** Data loss for this specific message on this specific consumer. The
message still exists in the WAL and can be read by other consumers or replayed.

**Code:** `broker/consumer.go:285-292`

**Test:** `TestConsumerLateMessageDrop` in `broker/consumer_test.go`

### 2.3 Late Message Arrival (Emit Unordered Policy)

**What happens:** Same as above, but with `late_policy: emit_unordered`.

**Behavior:** The message is emitted immediately as a single-message batch with
the `Late` flag set to `true`. It is NOT inserted into the normal window
pipeline, so it arrives out of order relative to previously emitted batches.

**Impact:** The consumer sees all data but ordering guarantees are relaxed for
late arrivals.

**Code:** `broker/consumer.go:294-308`

**Test:** `TestConsumerLateMessageEmitUnordered` in `broker/consumer_test.go`

### 2.4 Output Channel Full

**What happens:** The consumer's output channel (buffered at 100 batches) is
full when a window batch is ready to emit.

**Behavior:** The batch is silently dropped (`select/default` pattern). Late
messages emitted via `emit_unordered` are also dropped if the channel is full.

**Impact:** Data loss on the consumer. The messages still exist in the WAL. A
slow consumer that cannot keep up with the emit rate will miss batches.

**Recovery:** The consumer should be consuming from the `Output()` channel fast
enough to avoid backpressure. Increasing the channel buffer or adding consumer
concurrency can help.

**Code:** `broker/consumer.go:302-304` (late emit), `broker/consumer.go:433-435`
(window emit)

**Test:** `TestConsumerOutputChannelBackpressure` in `broker/failure_test.go`

### 2.5 Peer Fetch Failure During Read

**What happens:** The consumer has a `fetchPeerData` function configured, and it
returns an error when trying to pull data from a peer.

**Behavior:** The error is silently ignored for that peer in that read cycle. The
consumer continues with data from the local WAL and any other responsive peers.

**Impact:** The consumer may have an incomplete view of the data for this cycle.
On the next cycle, the fetch will be retried. If the peer is permanently down,
the watermark timeout mechanism will eventually exclude it.

**Code:** `broker/consumer.go:262-265`

**Test:** `TestConsumerPeerFetchFailure` in `broker/failure_test.go`

### 2.6 Watermark Stall from Dead Peer

**What happens:** A consumer is tracking multiple sources. One source stops
sending updates (the node is dead or partitioned).

**Behavior:** The watermark stalls at the dead source's last known progress.
After `watermark_timeout` elapses without an update, the source is marked dead
and excluded from the watermark calculation. The watermark then jumps forward.

**Impact:** During the stall, no new windows are emitted even though live sources
have advanced. After the timeout, windows resume. Messages from the dead peer
that arrive after the jump are treated as late arrivals.

**Code:** `broker/consumer.go:344-388`

**Test:** `TestConsumerWatermarkStallOnNodeDeath` in `broker/consumer_test.go`

### 2.7 Consumer Position Load Failure

**What happens:** A consumer cannot load its saved position from disk (file
missing, corrupt JSON, permission error).

**Behavior:** The error is silently ignored. The consumer starts fresh with
empty positions and zero watermark.

**Impact:** The consumer will re-read and re-process messages it may have already
seen. Deduplication at the application level may be needed if exactly-once
processing is required.

**Code:** `broker/consumer.go:102-103`

**Test:** `TestConsumerPositionCorruption` in `broker/failure_test.go`

---

## 3. Replication Failures

Replication is best-effort. All failures in the replication path are non-fatal
and self-healing.

### 3.1 Peer Unreachable During Anti-Entropy

**What happens:** The replicator tries to exchange digests or pull messages from
a peer, but the gRPC call fails (connection refused, timeout, DNS failure).

**Behavior:** The error is silently ignored. The replicator moves on to the next
peer. The next anti-entropy cycle (default: 1 second) will try again.

**Impact:** Replication to/from that peer is delayed until connectivity is
restored. Data on the local node remains safe in its WAL.

**Code:** `broker/peer_grpc.go:321-325` (syncWithPeer), `broker/peer_grpc.go:346-348`
(ExchangeDigest failure)

### 3.2 Partial Replication (Pull Limit)

**What happens:** A peer has thousands of messages that the local node is
missing. The pull request is limited to 1000 messages per request.

**Behavior:** The replicator pulls at most 1000 messages per cycle per peer.
Remaining gaps are filled in subsequent cycles.

**Impact:** Large backlogs take multiple anti-entropy cycles to fully replicate.
This is by design -- it prevents a single replication pull from overwhelming
the network or memory.

**Code:** `broker/peer_grpc.go:127-129` (limit enforcement),
`broker/replication.go:136-139`

### 3.3 Replicated Message Already Exists

**What happens:** The replicator pulls a message from a peer that the local node
already has (same sequence number).

**Behavior:** `AppendReplicated` returns `(false, nil)` -- the message is
silently skipped. No duplicate is created.

**Code:** `broker/wal.go:165-167`

**Test:** `TestWALAppendReplicated` in `broker/wal_test.go`

### 3.4 Replicated Message Without Sequence

**What happens:** A replicated message arrives with `NodeSeq == 0`.

**Behavior:** `AppendReplicated` returns an error: `replicated message has no
sequence number`. The message is not stored.

**Code:** `broker/wal.go:161-163`

**Test:** `TestWALAppendReplicatedNoSeq` in `broker/failure_test.go`

---

## 4. Cluster / Network Failures

### 4.1 Network Partition (Split Brain)

**What happens:** The cluster is split into two or more partitions that cannot
communicate.

**Behavior:** Each partition continues operating independently:

- All nodes accept writes on their local WAL.
- Replication only occurs within each partition.
- Gossip only propagates within each partition.
- Consumers on each side see only their partition's data.

After the partition heals:

- Gossip rediscovers peers across partitions.
- Anti-entropy replication fills in the gaps.
- Consumers merge data from all sources and deduplicate.

**Impact:** During the partition, each side has incomplete data. After healing,
the data converges. There is no split-brain conflict resolution needed because
the design is append-only and dedup-on-read.

**Test:** `TestIntegrationWriteDuringPartition` and
`TestIntegrationSplitBrainMerge` in `broker/integration_test.go`

### 4.2 All Peers Down (Single Survivor)

**What happens:** All nodes except one are down.

**Behavior:** The surviving node continues accepting writes and serving reads
from its local data. Replication stops (no live peers). Gossip rounds find no
peers to contact.

**Impact:** No data redundancy. If the surviving node also fails, unreplicated
data is lost. But the node itself is fully functional.

**Test:** `TestIntegrationSingleNodeSurvival` in `broker/integration_test.go`

### 4.3 Seed Node Down at Startup

**What happens:** A node starts and tries to join the cluster, but all seed
nodes are unreachable.

**Behavior:** The `Join` method adds seed entries to the peer table even if they
are not reachable. The gossip loop will periodically attempt to contact them. The
node operates standalone until seeds become reachable.

**Impact:** The node works in isolation until cluster connectivity is established.
No error is returned from Join because seeds are treated as hints, not
requirements.

**Code:** `broker/cluster.go:81-104`

**Test:** `TestClusterSeedNodeDown` in `broker/failure_test.go`

### 4.4 Peer Failure Detection

**What happens:** A peer stops responding to gossip.

**Behavior:** The failure detection loop checks `LastSeen` for each peer on
every tick (default: 1 second). If a peer's `LastSeen` exceeds
`failure_timeout` (default: 5 seconds), its state transitions from `PeerAlive`
to `PeerDead`. The `onLeave` callback fires if registered.

**Impact:** Dead peers are excluded from replication targets and watermark
calculations. They remain in the peer table (they are not removed, just marked
dead). If they come back, gossip will mark them alive again.

**Code:** `broker/cluster.go:232-254`

**Test:** `TestClusterPeerFailureDetection` in `broker/failure_test.go`

---

## 5. WAL / Storage Failures

### 5.1 WAL Corruption (Partial Write)

**What happens:** The WAL file contains a partial or corrupt message at the end
(e.g., due to a crash during write).

**Behavior:** During WAL rebuild (on startup), the `DecodeMessageFromBytes`
function will fail at the corruption point. The rebuild loop breaks, and all
messages up to the corruption point are indexed. The corrupt tail is effectively
ignored (it remains in the file but is never indexed).

**Impact:** Only the partially-written message is lost. All preceding messages
are recovered. Future writes append after the indexed portion (though the
corrupt bytes remain in the file as dead space).

**Code:** `broker/wal.go:66-94` (rebuild with truncation)

**Test:** `TestWALCorruptionRecovery` in `broker/failure_test.go`

### 5.2 WAL Directory Creation Failure

**What happens:** The WAL cannot create its data directory (permission denied,
read-only filesystem).

**Behavior:** `NewWAL` returns an error: `create WAL dir: <underlying error>`.
The stream cannot be created.

**Impact:** The node cannot store data for this stream. The operator must fix
filesystem permissions or provide a writable path.

**Code:** `broker/wal.go:32-34`

### 5.3 WAL File Open Failure

**What happens:** The WAL data file cannot be opened (permission denied, too
many open files).

**Behavior:** `NewWAL` returns an error: `open WAL file: <underlying error>`.

**Code:** `broker/wal.go:36-39`

### 5.4 Node Crash with `fsync_policy: none`

**What happens:** A node crashes and it was configured with `fsync_policy: none`.

**Behavior:** Any messages in the OS page cache that have not been flushed to
disk are lost. On restart, the WAL rebuild recovers only what was durably
written.

**Impact:** The loss window depends on the OS's flush interval (typically
seconds). This is the trade-off: maximum write speed in exchange for a larger
data loss window.

### 5.5 Node Crash with `fsync_policy: interval`

**What happens:** A node crashes between fsync intervals.

**Behavior:** Messages written since the last fsync may be lost. The loss
window is bounded by the configured `fsync_interval` (default: 100ms).

**Impact:** At most `fsync_interval` worth of messages are lost. This is the
recommended default for most workloads.

### 5.6 Node Crash with `fsync_policy: every`

**What happens:** A node crashes after a successful fsync.

**Behavior:** All acknowledged messages are durable on disk. The WAL rebuild
recovers everything.

**Impact:** No data loss (assuming the disk itself is intact). This is the
slowest write mode.

**Test:** `TestWALRecovery` in `broker/wal_test.go`

---

## 6. TCP / Protocol Failures

### 6.1 Client Connection Timeout

**What happens:** A TCP client connection is idle for more than 30 seconds
without sending a command.

**Behavior:** The server sends a `PONG` keepalive. If the client does not
respond, the connection is eventually closed by the read deadline.

**Code:** `broker/server.go:142-152`

### 6.2 Malformed Protocol Command

**What happens:** A client sends a command that the protocol parser cannot parse.

**Behavior:** The parser returns an error, and the connection is closed.

**Code:** `broker/server.go:144-152`

### 6.3 Server Shutdown During Active Connections

**What happens:** The server is stopped while clients are connected.

**Behavior:** The `stopCh` channel is closed, which causes all handler
goroutines to exit their select loops. The listener is closed (rejecting new
connections), and all active connections are explicitly closed.

**Code:** `broker/server.go:460-485`

---

## 7. Failure Mode Summary Table

| Failure | Impact | Self-Healing? | Data Loss? |
|---|---|---|---|
| No matching stream | Write rejected | No (config error) | No |
| WAL write failure | Write rejected | Yes (when disk recovers) | No |
| WAL corruption | Partial recovery | Yes (on restart) | Last partial message |
| Node crash (fsync=none) | OS cache lost | Yes (on restart) | Seconds of data |
| Node crash (fsync=interval) | Interval data lost | Yes (on restart) | Up to fsync_interval |
| Node crash (fsync=every) | Full recovery | Yes (on restart) | None |
| Peer unreachable | Replication delayed | Yes (next cycle) | No |
| Network partition | Data diverges | Yes (after heal) | No |
| All peers down | No redundancy | Yes (when peers return) | No (unless solo node also fails) |
| Late message (drop) | Message skipped | No (by design) | Yes (for that consumer) |
| Late message (emit) | Out-of-order delivery | No (by design) | No |
| Consumer channel full | Batch dropped | No (consumer too slow) | Yes (for that consumer) |
| Dead peer stalls watermark | Window emission paused | Yes (after timeout) | No |
| Consumer position corrupt | Fresh start | Yes (re-reads data) | No (may re-process) |
