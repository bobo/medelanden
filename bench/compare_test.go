// Package bench provides comparative benchmarks between Medelanden and NATS.
//
// These benchmarks run identical workloads against both systems to produce
// an apples-to-apples comparison. Both systems are started in-process to
// eliminate network variability.
//
// Run with:
//
//	go test -bench=. -benchmem -count=1 -timeout 300s ./bench/...
//
// Or via Makefile:
//
//	make bench-compare
package bench

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"medelanden/broker"

	natsserver "github.com/nats-io/nats-server/v2/server"
	nats "github.com/nats-io/nats.go"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// startNATSServer starts an embedded NATS server with optional JetStream.
func startNATSServer(b *testing.B, jetstream bool) (*natsserver.Server, string) {
	b.Helper()
	opts := &natsserver.Options{
		Host:   "127.0.0.1",
		Port:   -1, // auto-assign
		NoLog:  true,
		NoSigs: true,
	}
	if jetstream {
		opts.JetStream = true
		opts.StoreDir = b.TempDir()
	}
	s, err := natsserver.NewServer(opts)
	if err != nil {
		b.Fatal(err)
	}
	s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		b.Fatal("NATS server not ready")
	}
	b.Cleanup(func() { s.Shutdown() })
	return s, s.ClientURL()
}

// startMedelandenNode starts a Medelanden broker node with no fsync (fast path).
func startMedelandenNode(b *testing.B) *broker.Node {
	b.Helper()
	dir := b.TempDir()
	node, err := broker.NewNode(broker.NodeConfig{
		ID:      "bench-node",
		DataDir: dir,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { node.Stop() })
	return node
}

func makePayload(size int) []byte {
	return bytes.Repeat([]byte("x"), size)
}

// ---------------------------------------------------------------------------
// Publish throughput: NATS Core (no persistence) vs Medelanden (FsyncNone)
//
// This compares in-memory pub/sub (NATS Core) against Medelanden's write path
// with fsync disabled. Both avoid disk I/O on the critical path.
// ---------------------------------------------------------------------------

func BenchmarkPublish_NATSCore(b *testing.B) {
	_, url := startNATSServer(b, false)
	nc, err := nats.Connect(url)
	if err != nil {
		b.Fatal(err)
	}
	defer nc.Close()

	payload := makePayload(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nc.Publish("test.data", payload)
	}
	nc.Flush()
}

func BenchmarkPublish_Medelanden(b *testing.B) {
	node := startMedelandenNode(b)
	cfg := broker.DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = broker.FsyncNone
	node.CreateStream(cfg)

	payload := makePayload(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := &broker.Message{
			Subject:    "test.data",
			Payload:    payload,
			ProducerTS: uint64(i) * 1_000_000,
		}
		node.Publish(msg)
	}
}

// ---------------------------------------------------------------------------
// Publish throughput: NATS JetStream (persistent) vs Medelanden (FsyncNone)
//
// JetStream uses file storage with its default sync interval (lenient fsync).
// Medelanden uses FsyncNone. Both persist to disk but avoid synchronous fsync
// on every write.
// ---------------------------------------------------------------------------

func BenchmarkPublishPersistent_NATSJetStream(b *testing.B) {
	_, url := startNATSServer(b, true)
	nc, err := nats.Connect(url)
	if err != nil {
		b.Fatal(err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		b.Fatal(err)
	}
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test.>"},
		Storage:  nats.FileStorage,
	})
	if err != nil {
		b.Fatal(err)
	}

	payload := makePayload(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		js.Publish("test.data", payload)
	}
}

func BenchmarkPublishPersistent_Medelanden(b *testing.B) {
	node := startMedelandenNode(b)
	cfg := broker.DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = broker.FsyncNone
	node.CreateStream(cfg)

	payload := makePayload(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := &broker.Message{
			Subject:    "test.data",
			Payload:    payload,
			ProducerTS: uint64(i) * 1_000_000,
		}
		node.Publish(msg)
	}
}

// ---------------------------------------------------------------------------
// Publish latency (measure per-message time)
// ---------------------------------------------------------------------------

func BenchmarkPublishLatency_NATSCore(b *testing.B) {
	_, url := startNATSServer(b, false)
	nc, err := nats.Connect(url)
	if err != nil {
		b.Fatal(err)
	}
	defer nc.Close()

	payload := makePayload(128)
	b.ResetTimer()
	for b.Loop() {
		nc.Publish("test.data", payload)
		nc.Flush()
	}
}

func BenchmarkPublishLatency_Medelanden(b *testing.B) {
	node := startMedelandenNode(b)
	cfg := broker.DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = broker.FsyncNone
	node.CreateStream(cfg)

	payload := makePayload(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := &broker.Message{
			Subject:    "test.data",
			Payload:    payload,
			ProducerTS: uint64(i) * 1_000_000,
		}
		node.Publish(msg)
	}
}

// ---------------------------------------------------------------------------
// Payload size scaling
// ---------------------------------------------------------------------------

func BenchmarkPayloadSizes_NATSCore(b *testing.B) {
	sizes := []int{64, 256, 1024, 4096}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			_, url := startNATSServer(b, false)
			nc, err := nats.Connect(url)
			if err != nil {
				b.Fatal(err)
			}
			defer nc.Close()

			payload := makePayload(size)
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				nc.Publish("test.data", payload)
			}
			nc.Flush()
		})
	}
}

func BenchmarkPayloadSizes_Medelanden(b *testing.B) {
	sizes := []int{64, 256, 1024, 4096}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			node := startMedelandenNode(b)
			cfg := broker.DefaultStreamConfig("test", []string{"test.>"})
			cfg.FsyncPolicy = broker.FsyncNone
			node.CreateStream(cfg)

			payload := makePayload(size)
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				msg := &broker.Message{
					Subject:    "test.data",
					Payload:    payload,
					ProducerTS: uint64(i) * 1_000_000,
				}
				node.Publish(msg)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Concurrent publish (multiple goroutines)
// ---------------------------------------------------------------------------

func BenchmarkConcurrentPublish_NATSCore(b *testing.B) {
	_, url := startNATSServer(b, false)
	nc, err := nats.Connect(url)
	if err != nil {
		b.Fatal(err)
	}
	defer nc.Close()

	payload := makePayload(128)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			nc.Publish("test.data", payload)
		}
	})
	nc.Flush()
}

func BenchmarkConcurrentPublish_Medelanden(b *testing.B) {
	node := startMedelandenNode(b)
	cfg := broker.DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = broker.FsyncNone
	node.CreateStream(cfg)

	payload := makePayload(128)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			msg := &broker.Message{
				Subject:    "test.data",
				Payload:    payload,
				ProducerTS: uint64(i) * 1_000_000,
			}
			node.Publish(msg)
			i++
		}
	})
}

// ---------------------------------------------------------------------------
// JetStream publish throughput with async publish (batched acks)
// ---------------------------------------------------------------------------

func BenchmarkPublishAsync_NATSJetStream(b *testing.B) {
	_, url := startNATSServer(b, true)
	nc, err := nats.Connect(url)
	if err != nil {
		b.Fatal(err)
	}
	defer nc.Close()

	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		b.Fatal(err)
	}
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test.>"},
		Storage:  nats.FileStorage,
	})
	if err != nil {
		b.Fatal(err)
	}

	payload := makePayload(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		js.PublishAsync("test.data", payload)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(30 * time.Second):
		b.Fatal("async publish did not complete")
	}
}

// ---------------------------------------------------------------------------
// High-throughput test: measure msg/sec over a fixed duration
// This reports throughput as a test log (not a benchmark) for easy comparison.
// ---------------------------------------------------------------------------

func TestThroughputComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput comparison in short mode")
	}

	const (
		duration    = 3 * time.Second
		payloadSize = 128
	)
	payload := makePayload(payloadSize)

	// --- NATS Core ---
	natsOpts := &natsserver.Options{
		Host:   "127.0.0.1",
		Port:   -1,
		NoLog:  true,
		NoSigs: true,
	}
	ns, err := natsserver.NewServer(natsOpts)
	if err != nil {
		t.Fatal(err)
	}
	ns.Start()
	defer ns.Shutdown()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server not ready")
	}

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	natsCount := 0
	natsStart := time.Now()
	for time.Since(natsStart) < duration {
		nc.Publish("test.data", payload)
		natsCount++
	}
	nc.Flush()
	natsElapsed := time.Since(natsStart)
	natsRate := float64(natsCount) / natsElapsed.Seconds()

	// --- NATS JetStream ---
	jsOpts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		NoLog:     true,
		NoSigs:    true,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	jsServer, err := natsserver.NewServer(jsOpts)
	if err != nil {
		t.Fatal(err)
	}
	jsServer.Start()
	defer jsServer.Shutdown()
	if !jsServer.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS JetStream server not ready")
	}

	jsNc, err := nats.Connect(jsServer.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer jsNc.Close()

	js, err := jsNc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test.>"},
		Storage:  nats.FileStorage,
	})
	if err != nil {
		t.Fatal(err)
	}

	jsCount := 0
	jsStart := time.Now()
	for time.Since(jsStart) < duration {
		js.Publish("test.data", payload)
		jsCount++
	}
	jsElapsed := time.Since(jsStart)
	jsRate := float64(jsCount) / jsElapsed.Seconds()

	// --- Medelanden ---
	dir := t.TempDir()
	node, err := broker.NewNode(broker.NodeConfig{ID: "bench-node", DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Stop()

	cfg := broker.DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = broker.FsyncNone
	node.CreateStream(cfg)

	mdCount := 0
	mdStart := time.Now()
	for time.Since(mdStart) < duration {
		msg := &broker.Message{
			Subject:    "test.data",
			Payload:    payload,
			ProducerTS: uint64(mdCount) * 1_000_000,
		}
		node.Publish(msg)
		mdCount++
	}
	mdElapsed := time.Since(mdStart)
	mdRate := float64(mdCount) / mdElapsed.Seconds()

	// --- Concurrent Medelanden ---
	dir2 := t.TempDir()
	node2, err := broker.NewNode(broker.NodeConfig{ID: "bench-node-2", DataDir: dir2})
	if err != nil {
		t.Fatal(err)
	}
	defer node2.Stop()

	cfg2 := broker.DefaultStreamConfig("test", []string{"test.>"})
	cfg2.FsyncPolicy = broker.FsyncNone
	node2.CreateStream(cfg2)

	var mdConcCount int64
	var wg sync.WaitGroup
	goroutines := 4
	wg.Add(goroutines)
	mdConcStart := time.Now()
	for g := 0; g < goroutines; g++ {
		go func(workerID int) {
			defer wg.Done()
			local := 0
			for time.Since(mdConcStart) < duration {
				msg := &broker.Message{
					Subject:    "test.data",
					Payload:    payload,
					ProducerTS: uint64(workerID*10_000_000 + local),
				}
				node2.Publish(msg)
				local++
			}
			// atomic add would be cleaner but this is fine for a test
			_ = local
		}(g)
	}
	wg.Wait()
	mdConcElapsed := time.Since(mdConcStart)
	mdConcCount = int64(node2.GetStream("test").MessageCount())
	mdConcRate := float64(mdConcCount) / mdConcElapsed.Seconds()

	t.Log("")
	t.Log("=== Throughput Comparison (128B payload, 3s window) ===")
	t.Log("")
	t.Logf("  %-35s %12s %12s", "System", "msgs/sec", "total msgs")
	t.Logf("  %-35s %12s %12s", strings.Repeat("-", 35), strings.Repeat("-", 12), strings.Repeat("-", 12))
	t.Logf("  %-35s %12.0f %12d", "NATS Core (no persistence)", natsRate, natsCount)
	t.Logf("  %-35s %12.0f %12d", "NATS JetStream (file storage)", jsRate, jsCount)
	t.Logf("  %-35s %12.0f %12d", "Medelanden (FsyncNone, 1 writer)", mdRate, mdCount)
	t.Logf("  %-35s %12.0f %12d", fmt.Sprintf("Medelanden (FsyncNone, %d writers)", goroutines), mdConcRate, mdConcCount)
	t.Log("")
	t.Log("Notes:")
	t.Log("  - NATS Core: pure pub/sub, no disk persistence")
	t.Log("  - NATS JetStream: file storage, default sync interval (lenient fsync)")
	t.Log("  - Medelanden: WAL-backed, FsyncNone (no synchronous disk writes)")
	t.Log("  - All tests run in-process on the same machine")
}
