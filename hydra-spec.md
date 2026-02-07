# Hydra: AP-Durable Message Broker Specification

**Version:** 0.1 (Draft)
**Status:** Proposal
**Authors:** Micke (Wingbits)

---

## Abstract

Hydra is a message broker designed for **availability-first durable messaging**. It fills a gap in the current messaging ecosystem: existing systems optimize for either throughput without durability (NATS Core, Redis Pub/Sub) or durability with consensus-based availability trade-offs (Kafka, JetStream, Pulsar). Hydra provides durable buffering that **never rejects writes**, replicates asynchronously, and reconstructs ordered streams on read using server-side windowing and deduplication.

The target domain is high-throughput telemetry, IoT, and sensor data — workloads where brief data loss is acceptable but stream unavailability is not.

---

## 1. Design Principles

1. **Every node accepts writes independently, always.** There is no leader, no quorum, and no write-path coordination. A node that is running accepts messages. Period.
2. **Replication is asynchronous and best-effort.** Nodes replicate data to peers when they can. Replication lag is expected and tolerated.
3. **Ordering is reconstructed on read, not enforced on write.** Messages carry producer timestamps. The serving node merges data from itself and available peers, sorts by timestamp within configurable windows, deduplicates, and emits an ordered stream.
4. **The client is dumb.** All merge, windowing, deduplication, and watermark logic lives server-side. Consumers see a simple, ordered subscription.
5. **Tunable trade-offs.** Operators configure window duration, late-arrival policy, replication targets, and dedup keys per stream. The broker provides knobs, not opinions.

---

## 2. Core Concepts

### 2.1 Stream

A named, append-only log of messages. Each stream has a **subject filter** (e.g., `aircraft.>`) that determines which messages are captured. Streams are configured with retention policies (max bytes, max age, max messages).

A stream exists independently on every node that is configured to capture it. There is no "owner" of a stream — every node has its own local copy.

### 2.2 Message

A message consists of:

| Field | Type | Required | Description |
|---|---|---|---|
| `subject` | string | yes | Routing subject (e.g., `aircraft.A1B2C3`) |
| `payload` | bytes | yes | Arbitrary message body |
| `producer_ts` | uint64 | yes | Producer-assigned timestamp (Unix nanos). Used for ordering. |
| `dedup_key` | string | no | Application-defined deduplication key. If not provided, `subject + producer_ts` is used. |
| `msg_id` | string | no | Globally unique message ID (UUID). Auto-generated if not provided. Used for exact dedup across nodes. |
| `node_seq` | uint64 | internal | Per-node monotonic sequence number, assigned at write time. |
| `node_id` | string | internal | ID of the node that first accepted the message. |

### 2.3 Node

A single broker instance with local persistent storage (WAL). Each node:

- Accepts writes to any stream it is configured to capture
- Maintains a local WAL per stream
- Participates in peer-to-peer anti-entropy replication
- Serves read requests by merging local and peer data

### 2.4 Cluster

A set of nodes that are aware of each other. Cluster membership is managed via gossip protocol (SWIM or similar). There is **no leader election** and **no consensus protocol**. The cluster is used for:

- Peer discovery
- Replication target selection
- Failure detection (for watermark advancement)

### 2.5 Consumer

A named subscription to a stream with server-side merge semantics. Consumers are durable — the broker tracks the consumer's position per node.

### 2.6 Window

A time-bounded buffer used during read-side merge. The serving node collects messages from all available sources within the window, sorts them by `producer_ts`, deduplicates, and emits them as a batch when the window closes.

### 2.7 Watermark

A timestamp `W` such that the serving node believes it has seen all messages with `producer_ts <= W`. The watermark advances based on progress from all **live** peers. Dead peers are excluded from the watermark calculation after a configurable timeout (`watermark_timeout`).

---

## 3. Architecture

```
                    Producers
                   /    |    \
                  v     v     v
            ┌────────┐ ┌────────┐ ┌────────┐
            │ Node A │ │ Node B │ │ Node C │
            │        │ │        │ │        │
            │ ┌────┐ │ │ ┌────┐ │ │ ┌────┐ │
            │ │WAL │ │ │ │WAL │ │ │ │WAL │ │
            │ └────┘ │ │ └────┘ │ │ └────┘ │
            └───┬────┘ └───┬────┘ └───┬────┘
                │          │          │
                └──── Gossip + ───────┘
                   Anti-Entropy
                        │
               Consumer connects to
                   ANY node
                        │
                        v
                ┌──────────────┐
                │  Read Path   │
                │              │
                │  local WAL   │──┐
                │  + peer pull │  │ merge
                │  + window    │  │ buffer
                │  + dedup     │──┘
                │  + watermark │
                │  + emit      │
                └──────┬───────┘
                       │
                       v
                Consumer receives
                ordered, deduped
                    stream
```

---

## 4. Write Path

### 4.1 Producer Publish

1. Producer sends a message to any node (random, round-robin, or nearest).
2. The receiving node validates the message (subject matches a stream, required fields present).
3. The node assigns `node_seq` (monotonic counter) and `node_id`.
4. The message is appended to the local WAL.
5. The node acknowledges the write to the producer.

**There is no coordination with other nodes on the write path.** The write completes when the local WAL append is durable (fsync policy is configurable per stream).

### 4.2 Fsync Policies

| Policy | Behavior | Trade-off |
|---|---|---|
| `none` | No fsync, OS decides when to flush | Fastest. Data loss on node crash (last ~seconds). |
| `interval` | Fsync every N milliseconds | Bounded loss window. Good default. |
| `every` | Fsync every write | Slowest. Minimal loss on crash. |

Default: `interval` at 100ms.

### 4.3 Write Failures

A write can only fail if:

- The node is down (producer retries on another node)
- The local disk is full (stream retention should prevent this)
- The message is malformed

A write **never** fails due to replication state, peer availability, or cluster health.

---

## 5. Replication

### 5.1 Anti-Entropy Protocol

Each node periodically exchanges **stream digests** with its peers. A digest is a compact summary of what a node has for a given stream:

```
StreamDigest {
  stream:    string       // stream name
  node_id:   string       // the node this digest describes
  max_seq:   uint64       // highest node_seq in local WAL
  max_ts:    uint64       // highest producer_ts seen
  checkpoints: []Checkpoint  // sparse checkpoints for gap detection
}

Checkpoint {
  seq:  uint64
  ts:   uint64
  hash: uint64  // hash of messages in range
}
```

When a node detects that a peer has messages it doesn't, it pulls the missing range. Pull requests are bounded to prevent overwhelming a peer.

### 5.2 Replication Targets

Streams are configured with a `replication_target` (e.g., 2 or 3). This is a **soft goal**, not a hard requirement. The broker tries to replicate each message to at least `replication_target` nodes, but never blocks the write path waiting for this.

Replication status is tracked and exposed as a metric:

- `replication_lag_seconds` — how far behind the least-replicated peer is
- `messages_below_target` — count of messages replicated to fewer than `replication_target` nodes

### 5.3 Replication Topology

Default: **full mesh** (every node replicates to every other node for the same stream). For large clusters, this can be configured to use a **replication factor** where each node replicates to a subset of peers.

### 5.4 Conflict Resolution

There are no conflicts in the traditional sense. Each node's WAL is append-only and authoritative for messages it originally received. Replication only adds messages from other nodes — it never overwrites local data.

Deduplication occurs on read, not during replication. A message may exist on multiple nodes (which is desired for durability), and the read path handles dedup.

---

## 6. Read Path

This is where Hydra's core innovation lies. The read path is a **server-side stream processing pipeline** that merges multiple independent WALs into a single ordered, deduplicated stream.

### 6.1 Consumer Configuration

```json
{
  "name": "my-consumer",
  "stream": "aircraft",
  "ordering": "producer_ts",
  "window_duration": "2s",
  "watermark_timeout": "5s",
  "dedup_key": ["subject", "producer_ts"],
  "late_policy": "drop",
  "deliver_policy": "new"
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Durable consumer name. Position is tracked. |
| `stream` | string | required | Stream to consume from. |
| `ordering` | string | `producer_ts` | Field to sort by within windows. |
| `window_duration` | duration | `2s` | How long to hold a window open before emitting. |
| `watermark_timeout` | duration | `5s` | How long to wait for a dead peer before excluding it from the watermark. |
| `dedup_key` | string[] | `["msg_id"]` | Fields used for deduplication. |
| `late_policy` | enum | `drop` | What to do with messages that arrive after their window closed: `drop` or `emit_unordered`. |
| `deliver_policy` | enum | `new` | Starting position: `new`, `all`, `by_time`. |
| `subject_filter` | string | `>` | Optional further subject filtering within the stream. |

### 6.2 Read Pipeline

When a consumer subscribes, the serving node (the node the consumer is connected to) executes the following pipeline:

```
Step 1: Source Collection
   ├── Read from local WAL (always available)
   ├── Pull from Peer A (if alive)
   ├── Pull from Peer B (if alive)
   └── Pull from Peer C (if alive)
              │
Step 2: Merge Buffer
   │   Incoming messages are placed into a time-bucketed
   │   buffer, keyed by window start time.
              │
Step 3: Watermark Advancement
   │   For each source (local + peers), track the latest
   │   producer_ts seen. The watermark is:
   │     W = min(latest_ts for each LIVE source) - window_duration
   │   A source is considered dead after watermark_timeout with
   │   no progress → excluded from min() calculation.
              │
Step 4: Window Emission
   │   When W advances past a window's end time:
   │   1. Sort messages in window by producer_ts
   │   2. Deduplicate by dedup_key
   │   3. Emit to consumer
   │   4. Record consumer position
              │
Step 5: Late Arrivals
      Messages with producer_ts < W are late:
      - drop: silently discard
      - emit_unordered: deliver with a late flag
```

### 6.3 Watermark Detail

The watermark is the central mechanism for deciding when it's safe to emit a window. It answers: "have I waited long enough to be confident I've seen everything?"

```
Given:
  sources = [local, peer_a, peer_b, peer_c]
  live_sources = sources where last_seen < watermark_timeout ago

For each source s in live_sources:
  progress[s] = max producer_ts received from s

watermark = min(progress[s] for s in live_sources) - window_duration
```

**When a node dies:**

1. For `watermark_timeout` seconds, the watermark stalls (waiting for the dead node).
2. After `watermark_timeout`, the dead node is excluded from the calculation.
3. The watermark jumps forward. Messages that were only on the dead node and within the skipped range are lost.
4. When the node comes back, its messages are replicated. Messages with `producer_ts > current watermark` are processed normally. Messages with `producer_ts < watermark` are handled per `late_policy`.

### 6.4 Consumer Position Tracking

Consumer positions are tracked per-source:

```json
{
  "consumer": "my-consumer",
  "positions": {
    "node_a": { "last_seq": 48291, "last_ts": 1706900000000000 },
    "node_b": { "last_seq": 51002, "last_ts": 1706900000100000 },
    "node_c": { "last_seq": 49875, "last_ts": 1706900000050000 }
  },
  "watermark": 1706899998000000
}
```

This allows seamless failover: if the consumer reconnects to a different node, it provides its position map and the new serving node resumes from the correct point on each source.

### 6.5 Fan-Out Optimization

Multiple consumers on the same stream with the same (or similar) window configuration can share the merge pipeline. The serving node maintains a single merge buffer per stream and fans out to all consumers after dedup/ordering.

---

## 7. Failure Modes and Behavior

### 7.1 Single Node Failure

| Scenario | Write Path Impact | Read Path Impact |
|---|---|---|
| Node A dies | Producers routed to B, C. No writes lost (except in-flight to A). | Consumers on A reconnect to B or C. Watermark stalls for `watermark_timeout`, then advances. Messages only on A's unreplicated WAL are lost until A recovers. |
| Node A recovers | A starts accepting writes again. Anti-entropy fills replication gaps. | A's recovered messages flow into read pipeline. Those within open windows are merged normally. Those behind watermark follow `late_policy`. |

### 7.2 Majority Failure

| Scenario | Write Path Impact | Read Path Impact |
|---|---|---|
| Nodes A and B die (3-node cluster) | Node C continues accepting ALL writes. Replication is unavailable. | Consumers on C see only C's data. Watermark advances based on C alone after `watermark_timeout`. When A, B recover, anti-entropy fills gaps. |

**This is the key differentiator from Raft-based systems.** In Kafka or JetStream R3, losing 2 of 3 nodes means the stream stops accepting writes. In Hydra, the remaining node continues operating with reduced durability but full availability.

### 7.3 Network Partition

| Scenario | Behavior |
|---|---|
| [A, B] — [C] partition | Both sides continue accepting writes independently. Both sides serve consumers with their available data. When partition heals, anti-entropy reconciles. Consumers may see duplicates during reconciliation (handled by dedup). |

### 7.4 Full Cluster Failure

All nodes down simultaneously. Writes fail (no node to accept them). On recovery, each node's WAL is intact. Anti-entropy reconciles. Consumer positions are durably stored and resume correctly.

### 7.5 Slow Node

A node that is alive but slow (disk I/O issues, GC pauses) will cause its `progress[s]` to lag. The watermark is pulled back, increasing end-to-end latency. If the lag exceeds `watermark_timeout`, the slow node is excluded from watermark calculation (same as dead node).

---

## 8. Cluster Management

### 8.1 Membership

Nodes discover each other via:

- **Seed nodes** (static configuration)
- **DNS discovery** (SRV records)
- **Gossip protocol** (SWIM-based) for health and metadata propagation

There is no leader election. All nodes are equal.

### 8.2 Adding a Node

1. New node joins the gossip cluster.
2. New node is configured with stream definitions.
3. Anti-entropy begins pulling existing data from peers.
4. Once caught up (or immediately, if the operator prefers), the new node starts accepting writes and serving reads.

### 8.3 Removing a Node

1. Node is marked as draining.
2. Anti-entropy prioritizes replicating the draining node's unique data to other peers.
3. Once replication target is met for all messages, the node leaves the cluster.
4. If the node is removed abruptly, data that was only on that node is lost. Anti-entropy from other nodes fills what they have.

### 8.4 Stream Configuration

Stream definitions are propagated via gossip. All nodes that match a stream's **placement policy** will capture that stream.

```json
{
  "name": "aircraft",
  "subjects": ["aircraft.>"],
  "retention": {
    "max_bytes": "10GB",
    "max_age": "24h",
    "max_msgs": 0
  },
  "replication_target": 2,
  "fsync_policy": "interval",
  "fsync_interval": "100ms",
  "placement": {
    "tags": [],
    "count": 0
  }
}
```

| Placement Field | Description |
|---|---|
| `tags` | Only nodes with matching tags capture this stream. Empty = all nodes. |
| `count` | Max number of nodes that capture this stream. 0 = all matching nodes. |

---

## 9. Wire Protocol

Hydra uses a simple text-based protocol inspired by NATS, with binary payloads.

### 9.1 Publish

```
PUB <subject> <dedup_key> <producer_ts> <size>\r\n
<payload>\r\n
```

Response:
```
+OK <node_seq>\r\n
```

Or with msg_id:
```
MPUB <subject> <msg_id> <dedup_key> <producer_ts> <size>\r\n
<payload>\r\n
```

### 9.2 Subscribe (Simple — No Windowing)

For use cases that don't need ordering/dedup (equivalent to NATS Core):

```
SUB <subject> <consumer_name>\r\n
```

Messages delivered as:
```
MSG <subject> <consumer_name> <producer_ts> <msg_id> <size>\r\n
<payload>\r\n
```

### 9.3 Subscribe (Merged — Windowed)

```
MSUB <subject> <consumer_name> <window_duration> <watermark_timeout>\r\n
```

Messages delivered in window batches:
```
WBATCH <consumer_name> <window_start_ts> <window_end_ts> <msg_count>\r\n
MSG <subject> <producer_ts> <msg_id> <size>\r\n
<payload>\r\n
MSG <subject> <producer_ts> <msg_id> <size>\r\n
<payload>\r\n
WEND\r\n
```

### 9.4 Acknowledge

```
ACK <consumer_name> <msg_id>\r\n
```

Or batch:
```
ACKW <consumer_name> <window_end_ts>\r\n
```

### 9.5 Consumer Position Resume

```
RESUME <consumer_name>\r\n
```

Response:
```
+POSITIONS <json_position_map>\r\n
```

---

## 10. Observability

### 10.1 Metrics

| Metric | Type | Description |
|---|---|---|
| `hydra_writes_total` | counter | Total writes accepted by this node |
| `hydra_write_latency_seconds` | histogram | Local write latency (WAL append) |
| `hydra_replication_lag_seconds` | gauge | Per-peer replication lag |
| `hydra_messages_below_target` | gauge | Messages replicated to fewer than target nodes |
| `hydra_watermark_ts` | gauge | Current watermark per consumer |
| `hydra_watermark_stall_seconds` | gauge | Time the watermark has been stalled (waiting for a dead peer) |
| `hydra_window_emit_latency_seconds` | histogram | Time from window close to consumer delivery |
| `hydra_late_messages_total` | counter | Messages that arrived after their window closed |
| `hydra_dedup_total` | counter | Messages deduplicated on read |
| `hydra_peer_status` | gauge | Per-peer: 0=dead, 1=alive, 2=slow |

### 10.2 Health Endpoint

```
GET /healthz

{
  "status": "ok",
  "node_id": "node-a",
  "peers": {
    "node-b": { "status": "alive", "lag_ms": 150 },
    "node-c": { "status": "alive", "lag_ms": 80 }
  },
  "streams": {
    "aircraft": {
      "local_messages": 1482901,
      "replication_status": "target_met"
    }
  }
}
```

---

## 11. Test Cases

### 11.1 Write Availability

**TC-W1: Write succeeds with all peers down**
```
Given: 3-node cluster [A, B, C], stream "test" on all nodes
When:  B and C are killed
And:   Producer publishes 1000 messages to A
Then:  All 1000 writes succeed
And:   All 1000 messages are in A's local WAL
```

**TC-W2: Write succeeds during network partition**
```
Given: 3-node cluster, partition [A] — [B, C]
When:  Producer publishes to A, another producer publishes to B
Then:  Both writes succeed independently
And:   After partition heals, anti-entropy reconciles both WALs
```

**TC-W3: Write latency is independent of replication**
```
Given: 3-node cluster, B has artificially high latency (500ms)
When:  Producer publishes to A
Then:  Write ack latency is <= local WAL latency (not affected by B's slowness)
```

### 11.2 Replication

**TC-R1: Messages replicate to all peers**
```
Given: 3-node cluster, stream with replication_target=2
When:  1000 messages are published to A
Then:  Within 5 seconds, at least 2 nodes have all 1000 messages
And:   Within 30 seconds, all 3 nodes have all 1000 messages
```

**TC-R2: Anti-entropy fills gaps after recovery**
```
Given: 3-node cluster, B is down
When:  1000 messages are published to A (replicated to C)
And:   B comes back up
Then:  B receives all 1000 messages via anti-entropy within 30 seconds
```

**TC-R3: Replication does not duplicate on the same node**
```
Given: 3-node cluster
When:  1000 messages published to A, replicated to B and C
Then:  Each node's WAL contains exactly 1000 messages (no duplicates in storage)
```

### 11.3 Read Path — Ordering

**TC-O1: Messages are emitted in producer_ts order**
```
Given: Consumer with window_duration=2s
When:  Messages arrive: ts=5, ts=2, ts=8, ts=1, ts=3
And:   Window [0-2s] closes
Then:  Consumer receives: ts=1, ts=2, ts=3 (sorted)
```

**TC-O2: Cross-node merge produces correct order**
```
Given: 3-node cluster, consumer connected to A
When:  Node A has: ts=1, ts=4, ts=7
And:   Node B has: ts=2, ts=5, ts=8
And:   Node C has: ts=3, ts=6, ts=9
Then:  Consumer receives: ts=1, ts=2, ts=3, ts=4, ts=5, ts=6, ts=7, ts=8, ts=9
```

**TC-O3: Messages within same timestamp are stable-sorted by msg_id**
```
Given: Two messages with identical producer_ts but different msg_id
When:  Window closes
Then:  Messages are emitted in msg_id lexicographic order (deterministic)
```

### 11.4 Read Path — Deduplication

**TC-D1: Duplicate messages are deduplicated**
```
Given: Message M exists on nodes A, B, and C (via replication)
When:  Consumer reads merged stream
Then:  Message M appears exactly once
```

**TC-D2: Custom dedup_key works**
```
Given: Consumer with dedup_key=["subject", "producer_ts"]
When:  Two messages with same subject and producer_ts but different payloads arrive
Then:  Only one is emitted (first seen wins)
```

**TC-D3: Different messages are not falsely deduplicated**
```
Given: Two messages with same producer_ts but different subjects
When:  Consumer with default dedup reads
Then:  Both messages are emitted
```

### 11.5 Read Path — Watermark

**TC-WM1: Watermark advances with all nodes alive**
```
Given: 3-node cluster, window=2s, watermark_timeout=5s
When:  All nodes are producing messages at steady rate
Then:  Watermark advances continuously
And:   End-to-end latency is approximately window_duration
```

**TC-WM2: Watermark stalls when a node dies, then advances**
```
Given: 3-node cluster, watermark_timeout=5s
When:  Node C dies at time T
Then:  Watermark stalls (stops advancing)
And:   After 5 seconds, C is excluded from watermark calculation
And:   Watermark jumps forward based on A and B's progress
And:   Latency spike = watermark_timeout duration
```

**TC-WM3: Late messages after watermark jump are dropped**
```
Given: Node C dies, watermark advances past its last message
When:  Node C recovers and replicates its messages
Then:  Messages with producer_ts < current watermark are dropped (late_policy=drop)
And:   hydra_late_messages_total is incremented
```

**TC-WM4: Late messages can be emitted unordered**
```
Given: Same as TC-WM3 but late_policy=emit_unordered
Then:  Late messages are delivered to consumer with a `late=true` flag
And:   They are NOT inserted into the ordered window stream
```

### 11.6 Consumer Durability

**TC-CD1: Consumer resumes after disconnect**
```
Given: Consumer "C1" has processed messages up to watermark W1
When:  Consumer disconnects and reconnects to the same node
Then:  Consumer receives messages starting from W1 (no redelivery, no gap)
```

**TC-CD2: Consumer resumes on a different node**
```
Given: Consumer "C1" was connected to Node A
When:  Node A dies, consumer reconnects to Node B
And:   Consumer sends RESUME with its position map
Then:  Node B continues serving from the correct position per source
```

**TC-CD3: Consumer position survives node restart**
```
Given: Consumer "C1" has acknowledged up to window W5
When:  Serving node restarts
Then:  Consumer position is loaded from disk and resumable
```

### 11.7 Failure Resilience

**TC-F1: Cluster survives losing all but one node**
```
Given: 5-node cluster
When:  4 nodes die simultaneously
Then:  Remaining node continues accepting writes and serving reads
And:   Consumers see only the surviving node's data (reduced coverage, not outage)
```

**TC-F2: Split-brain produces correct results after heal**
```
Given: 3-node cluster, partition [A] — [B, C]
When:  Both sides accept 1000 unique messages (2000 total)
And:   Partition heals
And:   Anti-entropy completes
Then:  Consumer reading merged stream sees all 2000 messages, ordered, deduplicated
```

**TC-F3: Rapid node flapping doesn't corrupt data**
```
Given: Node C restarts 10 times in 60 seconds
Then:  No data corruption on any node
And:   Consumer output is correct (may have latency spikes from watermark stalls)
And:   Replication eventually converges
```

### 11.8 Performance

**TC-P1: Write throughput scales linearly with nodes**
```
Given: N-node cluster
When:  Producers distribute writes evenly across nodes
Then:  Total write throughput ≈ N × single-node throughput
```

**TC-P2: Read merge overhead is bounded**
```
Given: 3-node cluster, 100K messages/sec aggregate write rate
When:  Consumer subscribes with window=2s
Then:  Read-side merge adds < 10ms latency beyond the window duration
And:   CPU overhead of merge is < 15% of a single core
```

**TC-P3: Replication does not affect write latency**
```
Given: 3-node cluster under heavy replication load
When:  Measuring p99 write latency
Then:  p99 write latency is within 2x of single-node (no-replication) p99
```

---

## 12. Comparison with Existing Systems

| Property | Kafka | NATS JetStream | Redis Streams | Hydra |
|---|---|---|---|---|
| Write availability | Requires ISR quorum | Requires Raft quorum | Primary must be up | Any single node suffices |
| Write-path coordination | Yes (ISR ack) | Yes (Raft consensus) | No (async replication) | No |
| Durability | Strong (replicated + fsync) | Strong (Raft) | Weak (async replication) | Tunable (soft replication target) |
| Ordering guarantee | Total order per partition | Total order per stream | Arrival order | Timestamp order within windows |
| Consumer buffering | Yes (committed offsets) | Yes (durable consumers) | Yes (consumer groups) | Yes (position maps + windows) |
| Survives majority failure | No (ISR < min.insync) | No (Raft quorum) | Yes (if new primary elected) | Yes (every node independent) |
| Deduplication | No (application-level) | msg_id based | No | Built-in, configurable |
| Ordering on read | No (write-time ordering) | No (write-time ordering) | No | Yes (server-side merge) |

---

## 13. Limitations and Non-Goals

1. **No exactly-once semantics.** Hydra provides at-least-once delivery with application-level dedup. This is intentional — exactly-once requires consensus.
2. **No total ordering across partitions.** Ordering is per-window, based on producer timestamps. If producers have clock skew, ordering within that skew range is undefined.
3. **No transactions.** There is no multi-message atomic publish.
4. **Not a database.** Hydra is a message broker with retention. It does not support queries, indexes, or random reads.
5. **Clock skew sensitivity.** Producer timestamps must be reasonably synchronized (within the window duration). NTP is sufficient for most use cases. The window duration should be set larger than expected clock skew.
6. **No back-pressure to producers.** Hydra always accepts writes. If consumers can't keep up, the stream fills up and old messages are discarded per retention policy. This is a feature, not a bug.
7. **Data loss is possible.** If a message is only on one node and that node's disk fails before replication, the message is lost. This is the explicit trade-off for write availability.

---

## 14. Future Considerations

- **Tiered storage**: Offload cold data to object storage (S3) while keeping hot data on local disk.
- **Consumer-side filtering**: Server-side filtering within the merge pipeline to reduce data transfer.
- **Schema registry integration**: Enforce schemas on publish.
- **Multi-region**: Geo-aware replication with configurable per-region windows.
- **Adaptive windowing**: Automatically adjust window duration based on observed replication lag.

---

## Appendix A: Glossary

| Term | Definition |
|---|---|
| WAL | Write-Ahead Log. Local append-only persistent storage on each node. |
| Anti-entropy | Background process that detects and fills replication gaps between nodes. |
| Watermark | Timestamp boundary: all messages before it are considered "seen" from all live sources. |
| Window | Time-bounded buffer used to collect, sort, and deduplicate messages before emission. |
| Dedup key | Application-defined field(s) used to identify duplicate messages across nodes. |
| Late arrival | A message that arrives after its window has already closed. |
| Producer timestamp | Application-assigned timestamp indicating when the event occurred. |
| Position map | Per-consumer, per-source tracking of the last processed sequence number. |

---

## Appendix B: Example — Aviation Telemetry

This is the motivating use case. ~6000 ADS-B ground stations report aircraft positions.

**Stream configuration:**
```json
{
  "name": "aircraft",
  "subjects": ["aircraft.>"],
  "retention": { "max_age": "4h", "max_bytes": "50GB" },
  "replication_target": 2,
  "fsync_policy": "interval",
  "fsync_interval": "200ms"
}
```

**Consumer configuration:**
```json
{
  "name": "position-processor",
  "stream": "aircraft",
  "ordering": "producer_ts",
  "window_duration": "3s",
  "watermark_timeout": "10s",
  "dedup_key": ["subject", "producer_ts"],
  "late_policy": "drop"
}
```

**Why Hydra fits:**

- 5-6TB/day ingest. Write availability is paramount — stations don't buffer.
- Natural dedup key: ICAO address + timestamp. The same position report from the same aircraft at the same time is the same data.
- Brief data loss (seconds) is acceptable. Extended stream unavailability is not.
- Consumers (analytics pipeline, customer APIs) restart frequently. They need buffering but not at the cost of write availability.
