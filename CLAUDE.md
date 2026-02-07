# CLAUDE.md

Guide for AI assistants working on the Medelanden codebase.

## Project Overview

Medelanden is an **AP-durable distributed message broker** written in Go. It provides availability-first durable messaging where every node accepts writes independently with no leader, no quorum, and no write-path coordination. Replication is asynchronous via anti-entropy protocols. Ordering is reconstructed on read using windowing and deduplication.

Target use cases: high-throughput telemetry, IoT, sensor data.

## Repository Structure

```
main.go                         # Broker entry point (CLI flags, node startup)
broker/                         # Core broker package (~7,700 LOC)
  node.go                       # Central node orchestration
  server.go                     # TCP server & HTTP endpoints
  protocol.go                   # Text-based wire protocol parsing
  stream.go                     # Stream management
  consumer.go                   # Consumer read pipeline (windowing, dedup, ordering)
  wal.go                        # Write-ahead log (per-stream persistence)
  cluster.go                    # Gossip-based cluster membership
  replication.go                # Anti-entropy replication logic
  peer_grpc.go                  # gRPC peer communication
  config.go                     # All config types and defaults
  metrics.go                    # Prometheus metrics
  message.go                    # Message types
  *_test.go                     # Unit, integration, and failure tests
client/                         # Go client library (TCP wire protocol)
  client.go                     # Client implementation
  client_test.go
  example_test.go
proto/                          # Protobuf/gRPC definitions
  medelanden.proto              # PeerService: Gossip, ExchangeDigest, PullMessages, Ping
  pb/                           # Generated Go code (do not edit)
api/v1alpha1/                   # Kubernetes CRD types
  medelandencluster_types.go
  medelandenstream_types.go
  medelandenconsumer_types.go
  zz_generated.deepcopy.go      # Generated (do not edit)
internal/controller/            # Kubernetes operator controllers
  medelandencluster_controller.go
  medelandenstream_controller.go
  medelandenconsumer_controller.go
cmd/operator/main.go            # Kubernetes operator entry point
config/                         # Kubernetes manifests (CRDs, RBAC, samples)
e2e/                            # Kind-based end-to-end tests
  kind_test.go
  manifests/                    # Kind cluster config, StatefulSet deployment
```

## Language and Dependencies

- **Go 1.24** (module: `medelanden`)
- gRPC/Protobuf: `google.golang.org/grpc`, `google.golang.org/protobuf`
- Kubernetes: `k8s.io/api`, `k8s.io/apimachinery`, `k8s.io/client-go`, `sigs.k8s.io/controller-runtime`
- Monitoring: `github.com/prometheus/client_golang`
- Logging: `go.uber.org/zap`

## Build and Test Commands

```bash
# Build
make build            # go build -v ./...

# Lint
make vet              # go vet ./...

# Unit tests (broker package, 60s timeout)
make test             # go test -v -short -count=1 -timeout 60s ./broker/...

# Integration tests (multi-node replication, failure scenarios)
make test-integration # go test -v -count=1 -timeout 180s -run 'TestIntegration' ./broker/...

# All tests including operator controller tests (requires envtest binaries)
go test -v -count=1 -timeout 180s ./...

# End-to-end tests (requires Kind, Docker)
make test-e2e-kind    # Creates Kind cluster, builds/loads image, deploys, runs e2e
```

## CI Pipeline

Two GitHub Actions workflows run on push to `main` and PRs:

1. **CI** (`.github/workflows/ci.yml`): Build -> Vet -> Unit Tests -> Integration Tests -> All Tests (with envtest)
2. **Kind E2E** (`.github/workflows/kind.yml`): Kind cluster -> Docker build -> Deploy 3-node StatefulSet -> e2e tests

## Code Generation

```bash
# Regenerate protobuf/gRPC code (requires protoc + Go plugins)
# Source: proto/medelanden.proto -> proto/pb/

# Regenerate Kubernetes deepcopy methods
make generate         # controller-gen object paths="./api/..."

# Regenerate CRD and RBAC manifests
make manifests        # controller-gen crd rbac paths="./..."
```

Generated files (do not edit manually):
- `proto/pb/*.go`
- `api/v1alpha1/zz_generated.deepcopy.go`
- `config/crd/bases/*.yaml`
- `config/rbac/*.yaml`

## Architecture Key Concepts

### Write Path (fast, simple)
Validate subject -> Assign sequence -> Append to WAL -> ACK. No coordination, no cross-node locks.

### Read Path (complex, windowed)
Consumer collects messages from local WAL and peers into time-bounded windows, applies dedup by `msg_id`, sorts by `producer_ts`, and emits ordered batches. Late arrivals are handled by configurable `LatePolicy` (`drop` or `emit_unordered`).

### Replication
Asynchronous anti-entropy: nodes periodically exchange stream digests via gRPC, then pull missing messages. No guaranteed replication at write time.

### Cluster Membership
Gossip-based, no leader election. Failure detection via configurable timeout (default 5s).

### Wire Protocol
Text-based (similar to NATS). Commands: `PUB`, `MPUB`, `SUB`, `MSUB`, `ACK`, `ACKW`, `RESUME`, `PING`, `INFO`. Parsed in `broker/protocol.go`.

## Configuration Types

Key types in `broker/config.go`:
- `StreamConfig`: name, subjects, retention (max_bytes/max_age/max_msgs), replication_target, fsync_policy
- `ConsumerConfig`: stream, window_duration, watermark_timeout, dedup_key, late_policy, deliver_policy
- `NodeConfig`: id, data_dir, bind_addr, peer_addr, seeds
- `ClusterConfig`: gossip_interval, failure_timeout

Policy enums: `FsyncPolicy` (none/interval/every), `LatePolicy` (drop/emit_unordered), `DeliverPolicy` (new/all/by_time)

## Ports

- **4222**: Client TCP connections (wire protocol)
- **4223**: Peer-to-peer gRPC
- **8080**: HTTP health and Prometheus metrics

## Testing Conventions

- Unit tests use `-short` flag and live in `broker/*_test.go`
- Integration tests are prefixed with `TestIntegration` and test multi-node behavior
- Failure tests (`broker/failure_test.go`) inject failures and verify recovery
- E2E tests use build tag `e2e` and require a running Kind cluster
- Operator controller tests require Kubebuilder envtest binaries

## Design Documentation

- `PHILOSOPHY.md`: Five core design principles and CAP analysis
- `FAILURE-MODES.md`: Catalog of every failure mode with recovery paths
- `hydra-spec.md`: Complete technical specification (original name: Hydra)
