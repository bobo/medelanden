# Medelanden: Design Philosophy

## Why Medelanden Exists

The messaging ecosystem has a gap. Systems like NATS Core and Redis Pub/Sub
optimize for throughput but provide no durability. Systems like Kafka, JetStream,
and Pulsar provide durability through consensus protocols but sacrifice
availability during leader elections, quorum failures, or network partitions.

Medelanden fills this gap: **durable buffering that never rejects writes**. It is
purpose-built for workloads where brief data loss is tolerable but stream
unavailability is not -- high-throughput telemetry, IoT, sensor data, and similar
fire-and-forget-but-please-try-to-keep-it scenarios.

## The Five Principles

### 1. Every Node Accepts Writes, Always

There is no leader. There is no quorum. There is no write-path coordination. A
node that is running accepts messages. Period.

This is the foundational decision that everything else follows from. In a
consensus-based system, a write must be acknowledged by a majority before it is
considered committed. That means if your 3-node cluster loses 2 nodes, writes
stop. In Medelanden, if your 3-node cluster loses 2 nodes, the surviving node
continues accepting writes at full speed.

The trade-off: two nodes can accept the same logical message independently, and
replication is not guaranteed at write time. We accept this. Deduplication
happens on the read side.

### 2. Replication Is Asynchronous and Best-Effort

Nodes replicate data to peers when they can using anti-entropy protocols. Each
replication cycle, a node exchanges stream digests (compact summaries of what
data it holds) with its peers. When a gap is found, the node pulls the missing
messages.

This means:

- **Writes never block on replication.** Write latency is always local I/O only.
- **Replication lag is expected.** A message written to Node A may not appear on
  Node B for seconds (or longer during a partition).
- **Replication targets are soft goals.** Configuring `replication_target: 2`
  means "try to put this on 2 nodes," not "guarantee this is on 2 nodes."

### 3. Ordering Is Reconstructed on Read, Not Enforced on Write

Messages carry producer timestamps (`producer_ts`). The write path does not care
about ordering -- it appends to a local WAL in arrival order. Ordering is
reconstructed by the consumer's read pipeline:

1. The server collects messages from the local WAL and available peers.
2. Messages are placed into time-bounded **windows**.
3. Within each window, messages are sorted by `producer_ts` (with `msg_id` as
   tiebreaker for stability).
4. Duplicates are removed using a configurable dedup key.
5. The sorted, deduplicated batch is emitted to the consumer.

This design means the write path stays simple and fast (append-only), while the
read path does the heavy lifting of imposing order.

### 4. The Client Is Dumb

All merge, windowing, deduplication, and watermark logic lives server-side. A
producer publishes and gets an ACK. A consumer subscribes and receives ordered
batches. The client library is thin.

This is a deliberate choice. Pushing complexity to the server means:

- Clients are easy to implement in any language.
- Operational knobs (window size, late policy, dedup key) are changed server-side
  without redeploying clients.
- The server can optimize merge and dedup with full knowledge of all sources.

### 5. Tunable Trade-Offs

Medelanden provides knobs, not opinions. Every trade-off is configurable per
stream or per consumer:

| Knob | What It Controls | Trade-Off |
|---|---|---|
| `fsync_policy` | When WAL data is flushed to disk | Durability vs. write speed |
| `replication_target` | How many copies of data to maintain | Data safety vs. replication load |
| `window_duration` | Size of the merge/sort window | Ordering accuracy vs. latency |
| `watermark_timeout` | How long to wait for a slow peer | Completeness vs. stall duration |
| `late_policy` | What to do with messages arriving after their window | Data completeness vs. ordering guarantees |
| `dedup_key` | Which fields define "same message" | Dedup correctness vs. flexibility |
| `max_age` / `max_bytes` / `max_msgs` | Retention limits | Storage cost vs. replay depth |

## What Medelanden Is NOT

- **Not a consensus system.** There is no Raft, no Paxos, no leader election. If
  you need exactly-once delivery with total ordering, use Kafka or Pulsar.
- **Not a queue.** Messages are not removed after consumption. They are retained
  according to stream policy. Multiple consumers can read the same stream.
- **Not a database.** Medelanden is an append-only log with time-based retrieval.
  It does not support queries, indexes, or transactions.

## The CAP Position

Medelanden is an **AP system** (Availability + Partition tolerance) with tunable
eventual consistency:

- **During normal operation:** All nodes accept writes, replication keeps them
  roughly in sync, consumers get ordered reads.
- **During a network partition:** Both sides continue accepting writes. Data
  diverges. After the partition heals, anti-entropy replication converges the
  data. Consumers on the read side merge and deduplicate.
- **After node failure:** Surviving nodes continue without interruption. When the
  failed node recovers, it catches up via anti-entropy. If it never recovers,
  any data that was not yet replicated is lost (bounded by the fsync policy and
  replication lag at the time of failure).

## Design Consequences

These principles lead to specific engineering consequences throughout the system:

**Write path is trivially simple.** Validate subject, assign sequence, append to
WAL, ACK. No coordination, no locks beyond the local WAL mutex.

**Read path is where complexity lives.** Windowing, sorting, deduplication,
watermark tracking, peer data fetching -- all happens at read time. This is
intentional: writes are hot-path, reads are warm-path.

**Failure modes are predictable.** Because every node is independent, failure
of any component degrades the system gracefully rather than catastrophically.
See [FAILURE-MODES.md](FAILURE-MODES.md) for a detailed catalog.

**Cluster state is eventually consistent.** Gossip takes time to propagate.
During convergence, nodes may have stale views of peer health. This is acceptable
because no operation depends on a globally consistent cluster view.
