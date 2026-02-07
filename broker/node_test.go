package broker

import (
	"testing"
	"time"
)

func makeTestNode(t *testing.T, id string) (*Node, string) {
	t.Helper()
	dir := tempDir(t)

	cfg := NodeConfig{
		ID:      id,
		DataDir: dir,
	}

	node, err := NewNode(cfg)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { node.Stop() })
	return node, dir
}

// TC-W1: Write succeeds with all peers down (single node, no peers)
func TestNodeWriteSucceeds(t *testing.T) {
	node, _ := makeTestNode(t, "node-a")

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	streamCfg.FsyncPolicy = FsyncNone
	if err := node.CreateStream(streamCfg); err != nil {
		t.Fatal(err)
	}

	// Publish 1000 messages
	for i := uint64(0); i < 1000; i++ {
		msg := &Message{
			Subject:    "test.data",
			Payload:    []byte("payload"),
			ProducerTS: i * 1_000_000,
		}
		seq, err := node.Publish(msg)
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		if seq != i+1 {
			t.Fatalf("expected seq %d, got %d", i+1, seq)
		}
	}

	// Verify all in WAL
	stream := node.GetStream("test")
	if stream.MessageCount() != 1000 {
		t.Fatalf("expected 1000 messages, got %d", stream.MessageCount())
	}
}

// TC-W3: Write latency is independent of replication
func TestNodeWriteLatencyIsLocal(t *testing.T) {
	node, _ := makeTestNode(t, "node-a")

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	streamCfg.FsyncPolicy = FsyncNone
	if err := node.CreateStream(streamCfg); err != nil {
		t.Fatal(err)
	}

	// Measure write latency (should be very fast, purely local)
	start := time.Now()
	for i := uint64(0); i < 100; i++ {
		msg := &Message{
			Subject:    "test.data",
			Payload:    []byte("payload"),
			ProducerTS: i * 1_000_000,
		}
		if _, err := node.Publish(msg); err != nil {
			t.Fatal(err)
		}
	}
	elapsed := time.Since(start)

	// 100 messages should complete well within 1 second
	if elapsed > 1*time.Second {
		t.Errorf("100 writes took %v, expected < 1s", elapsed)
	}
}

func TestNodePublishAutoPopulatesFields(t *testing.T) {
	node, _ := makeTestNode(t, "node-a")

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	streamCfg.FsyncPolicy = FsyncNone
	node.CreateStream(streamCfg)

	msg := &Message{
		Subject:    "test.data",
		Payload:    []byte("payload"),
		ProducerTS: 12345,
	}
	seq, err := node.Publish(msg)
	if err != nil {
		t.Fatal(err)
	}

	// Message should have auto-populated fields
	if msg.MsgID == "" {
		t.Error("MsgID should be auto-populated")
	}
	if msg.NodeID != "node-a" {
		t.Errorf("NodeID: expected 'node-a', got %q", msg.NodeID)
	}
	if msg.DedupKey == "" {
		t.Error("DedupKey should be auto-populated")
	}
	if seq != 1 {
		t.Errorf("expected seq 1, got %d", seq)
	}
}

func TestNodeNoMatchingStream(t *testing.T) {
	node, _ := makeTestNode(t, "node-a")

	streamCfg := DefaultStreamConfig("aircraft", []string{"aircraft.>"})
	streamCfg.FsyncPolicy = FsyncNone
	node.CreateStream(streamCfg)

	msg := &Message{
		Subject:    "vehicle.car1",
		Payload:    []byte("data"),
		ProducerTS: 1000,
	}

	_, err := node.Publish(msg)
	if err == nil {
		t.Fatal("expected error for non-matching subject")
	}
}

func TestNodeDuplicateStreamCreation(t *testing.T) {
	node, _ := makeTestNode(t, "node-a")

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	streamCfg.FsyncPolicy = FsyncNone

	if err := node.CreateStream(streamCfg); err != nil {
		t.Fatal(err)
	}
	if err := node.CreateStream(streamCfg); err == nil {
		t.Fatal("expected error for duplicate stream")
	}
}

func TestNodeCreateConsumer(t *testing.T) {
	node, _ := makeTestNode(t, "node-a")

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	streamCfg.FsyncPolicy = FsyncNone
	node.CreateStream(streamCfg)

	consumerCfg := DefaultConsumerConfig("my-consumer", "test")
	consumer, err := node.CreateConsumer(consumerCfg)
	if err != nil {
		t.Fatal(err)
	}
	if consumer.Name() != "my-consumer" {
		t.Errorf("expected 'my-consumer', got %q", consumer.Name())
	}

	// Getting same consumer again should return existing
	consumer2, err := node.CreateConsumer(consumerCfg)
	if err != nil {
		t.Fatal(err)
	}
	if consumer != consumer2 {
		t.Error("expected same consumer instance")
	}
}

func TestNodeInfo(t *testing.T) {
	node, _ := makeTestNode(t, "node-a")

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	streamCfg.FsyncPolicy = FsyncNone
	node.CreateStream(streamCfg)

	info := node.Info()
	if info.Status != "ok" {
		t.Errorf("expected status 'ok', got %q", info.Status)
	}
	if info.NodeID != "node-a" {
		t.Errorf("expected node_id 'node-a', got %q", info.NodeID)
	}
	if _, ok := info.Streams["test"]; !ok {
		t.Error("expected 'test' stream in info")
	}
}

func TestNodeMetrics(t *testing.T) {
	node, _ := makeTestNode(t, "node-a")

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	streamCfg.FsyncPolicy = FsyncNone
	node.CreateStream(streamCfg)

	for i := 0; i < 10; i++ {
		msg := &Message{
			Subject:    "test.data",
			Payload:    []byte("payload"),
			ProducerTS: uint64(i) * 1000,
		}
		node.Publish(msg)
	}

	metrics := node.Metrics()
	if metrics.WritesTotal != 10 {
		t.Errorf("expected 10 writes, got %d", metrics.WritesTotal)
	}
	if metrics.Streams["test"] != 10 {
		t.Errorf("expected 10 messages in stream, got %d", metrics.Streams["test"])
	}
}

func TestNodeFindStreamForSubject(t *testing.T) {
	node, _ := makeTestNode(t, "node-a")

	node.CreateStream(DefaultStreamConfig("aircraft", []string{"aircraft.>"}))
	node.CreateStream(DefaultStreamConfig("vehicle", []string{"vehicle.>"}))

	if name := node.FindStreamForSubject("aircraft.A1"); name != "aircraft" {
		t.Errorf("expected 'aircraft', got %q", name)
	}
	if name := node.FindStreamForSubject("vehicle.car1"); name != "vehicle" {
		t.Errorf("expected 'vehicle', got %q", name)
	}
	if name := node.FindStreamForSubject("unknown.thing"); name != "" {
		t.Errorf("expected empty, got %q", name)
	}
}
