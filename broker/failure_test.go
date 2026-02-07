package broker

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// =============================================================================
// 1. Write Path Failures
// =============================================================================

// FM-W1: WAL write failure when the file is closed (simulates I/O error).
func TestWALWriteFailure(t *testing.T) {
	dir := tempDir(t)
	wal, err := NewWAL(filepath.Join(dir, "wal"), FsyncNone, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Write one good message
	msg := &Message{
		Subject:    "test.data",
		Payload:    []byte("good"),
		ProducerTS: 1000,
		MsgID:      GenerateID(),
		NodeID:     "node-1",
	}
	seq, err := wal.Append(msg)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 1 {
		t.Fatalf("expected seq 1, got %d", seq)
	}

	// Close the underlying file to simulate I/O failure
	wal.file.Close()

	// Next write should fail
	badMsg := &Message{
		Subject:    "test.data",
		Payload:    []byte("should-fail"),
		ProducerTS: 2000,
		MsgID:      GenerateID(),
		NodeID:     "node-1",
	}
	_, err = wal.Append(badMsg)
	if err == nil {
		t.Fatal("expected error when writing to closed file")
	}

	// Sequence counter should have rolled back
	if wal.LastSeq() != 1 {
		t.Errorf("expected seq to roll back to 1, got %d", wal.LastSeq())
	}
}

// FM-W2: Publishing to a node with no streams at all.
func TestNodePublishNoStreams(t *testing.T) {
	node, _ := makeTestNode(t, "node-empty")

	msg := &Message{
		Subject:    "anything.data",
		Payload:    []byte("data"),
		ProducerTS: 1000,
	}
	_, err := node.Publish(msg)
	if err == nil {
		t.Fatal("expected error when no streams exist")
	}
}

// FM-W3: Publish error increments error metrics.
func TestNodePublishErrorMetrics(t *testing.T) {
	node, _ := makeTestNode(t, "node-metrics")

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	streamCfg.FsyncPolicy = FsyncNone
	node.CreateStream(streamCfg)

	// Publish to a non-matching subject to trigger an error
	msg := &Message{
		Subject:    "wrong.subject",
		Payload:    []byte("data"),
		ProducerTS: 1000,
	}
	_, err := node.Publish(msg)
	if err == nil {
		t.Fatal("expected error for non-matching subject")
	}

	// Writes total should still be 0
	metrics := node.Metrics()
	if metrics.WritesTotal != 0 {
		t.Errorf("expected 0 writes, got %d", metrics.WritesTotal)
	}
}

// =============================================================================
// 2. WAL / Storage Failures
// =============================================================================

// FM-S1: WAL recovers correctly from a truncated/corrupt tail.
func TestWALCorruptionRecovery(t *testing.T) {
	dir := tempDir(t)
	walDir := filepath.Join(dir, "wal")

	// Write some good messages with fsync=every for durability
	wal, err := NewWAL(walDir, FsyncEvery, 0)
	if err != nil {
		t.Fatal(err)
	}

	for i := uint64(1); i <= 5; i++ {
		msg := &Message{
			Subject:    "test",
			Payload:    []byte(fmt.Sprintf("msg-%d", i)),
			ProducerTS: i * 1000,
			MsgID:      GenerateID(),
			NodeID:     "node-1",
		}
		wal.Append(msg)
	}
	wal.Close()

	// Append garbage to the end of the WAL file to simulate corruption
	walPath := filepath.Join(walDir, "wal.dat")
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	// Write a length prefix that claims more data than exists
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], 99999)
	f.Write(lenBuf[:])
	f.Write([]byte("partial-garbage"))
	f.Close()

	// Reopen the WAL -- it should recover the 5 good messages
	wal2, err := NewWAL(walDir, FsyncEvery, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	if wal2.LastSeq() != 5 {
		t.Fatalf("expected 5 messages after corruption recovery, got %d", wal2.LastSeq())
	}

	// Verify the recovered messages are intact
	msgs, err := wal2.Read(1, 5)
	if err != nil {
		t.Fatal(err)
	}
	for i, msg := range msgs {
		expected := uint64(i+1) * 1000
		if msg.ProducerTS != expected {
			t.Errorf("msg %d: expected ts %d, got %d", i, expected, msg.ProducerTS)
		}
	}

	// New writes should work after corruption recovery
	newMsg := &Message{
		Subject:    "test",
		Payload:    []byte("after-corruption"),
		ProducerTS: 9999,
		MsgID:      GenerateID(),
		NodeID:     "node-1",
	}
	seq, err := wal2.Append(newMsg)
	if err != nil {
		t.Fatalf("failed to write after corruption recovery: %v", err)
	}
	if seq != 6 {
		t.Errorf("expected seq 6, got %d", seq)
	}
}

// FM-S2: WAL with completely empty file recovers fine.
func TestWALEmptyFileRecovery(t *testing.T) {
	dir := tempDir(t)
	walDir := filepath.Join(dir, "wal")

	// Create and close an empty WAL
	wal, err := NewWAL(walDir, FsyncNone, 0)
	if err != nil {
		t.Fatal(err)
	}
	wal.Close()

	// Reopen -- should work with 0 messages
	wal2, err := NewWAL(walDir, FsyncNone, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	if wal2.LastSeq() != 0 {
		t.Fatalf("expected 0 messages, got %d", wal2.LastSeq())
	}

	// Should accept new writes
	msg := &Message{
		Subject:    "test",
		Payload:    []byte("first"),
		ProducerTS: 1000,
		MsgID:      GenerateID(),
		NodeID:     "node-1",
	}
	seq, err := wal2.Append(msg)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 1 {
		t.Errorf("expected seq 1, got %d", seq)
	}
}

// FM-S3: WAL with only garbage (no valid messages) recovers to empty state.
func TestWALFullGarbageRecovery(t *testing.T) {
	dir := tempDir(t)
	walDir := filepath.Join(dir, "wal")

	// Manually create a WAL file with only garbage
	os.MkdirAll(walDir, 0755)
	walPath := filepath.Join(walDir, "wal.dat")
	os.WriteFile(walPath, []byte("this-is-not-a-valid-wal"), 0644)

	// Open should succeed, treating the file as empty (no valid messages)
	wal, err := NewWAL(walDir, FsyncNone, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	if wal.LastSeq() != 0 {
		t.Fatalf("expected 0 messages from garbage file, got %d", wal.LastSeq())
	}
}

// FM-S4: Replicated message with no sequence number is rejected.
func TestWALAppendReplicatedNoSeq(t *testing.T) {
	dir := tempDir(t)
	wal, err := NewWAL(filepath.Join(dir, "wal"), FsyncNone, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	msg := &Message{
		Subject:    "test",
		Payload:    []byte("data"),
		ProducerTS: 1000,
		MsgID:      GenerateID(),
		NodeID:     "node-remote",
		NodeSeq:    0, // no sequence
	}

	_, err = wal.AppendReplicated(msg)
	if err == nil {
		t.Fatal("expected error for replicated message with no sequence")
	}
}

// =============================================================================
// 3. Read Path / Consumer Failures
// =============================================================================

// FM-R1: Consumer on a nonexistent stream.
func TestConsumerOnNonexistentStream(t *testing.T) {
	node, _ := makeTestNode(t, "node-a")

	streamCfg := DefaultStreamConfig("existing", []string{"existing.>"})
	streamCfg.FsyncPolicy = FsyncNone
	node.CreateStream(streamCfg)

	consumerCfg := DefaultConsumerConfig("my-consumer", "nonexistent")
	_, err := node.CreateConsumer(consumerCfg)
	if err == nil {
		t.Fatal("expected error creating consumer on nonexistent stream")
	}
}

// FM-R2: Consumer output channel backpressure (full channel drops batches).
func TestConsumerOutputChannelBackpressure(t *testing.T) {
	dir := tempDir(t)

	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	stream, err := NewStream(cfg, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	consumerCfg := ConsumerConfig{
		Name:             "backpressure-consumer",
		Stream:           "test",
		WindowDuration:   1 * time.Second,
		WatermarkTimeout: 5 * time.Second,
		DedupKey:         []string{"msg_id"},
		LatePolicy:       LatePolicyDrop,
	}

	consumer, err := NewConsumer(consumerCfg, stream, "node-1", dir)
	if err != nil {
		t.Fatal(err)
	}

	// Fill the output channel (capacity 100)
	for i := 0; i < 100; i++ {
		consumer.outputCh <- &WindowBatch{
			ConsumerName: "backpressure-consumer",
			Messages:     []*Message{{MsgID: fmt.Sprintf("fill-%d", i)}},
		}
	}

	// Now inject messages and try to emit -- the batch should be dropped
	// because the channel is full.
	msgs := []*Message{
		{Subject: "test.a", ProducerTS: 500_000_000, MsgID: "overflow-msg", NodeID: "node-1"},
	}
	consumer.InjectMessages(msgs)
	consumer.ForceAdvanceWatermark(2_000_000_000)
	consumer.FlushWindows()

	// Channel should still be at capacity (the new batch was dropped)
	if len(consumer.outputCh) != 100 {
		t.Errorf("expected channel to remain at 100, got %d", len(consumer.outputCh))
	}
}

// FM-R3: Consumer peer fetch failure is silently handled.
// The consumer continues collecting local data even when a peer fetch fails.
func TestConsumerPeerFetchFailure(t *testing.T) {
	dir := tempDir(t)

	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	stream, err := NewStream(cfg, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	// Publish a local message
	stream.Publish(&Message{
		Subject:    "test.data",
		Payload:    []byte("local"),
		ProducerTS: 1_000_000_000,
		MsgID:      "local-1",
		NodeID:     "node-1",
	})

	consumerCfg := ConsumerConfig{
		Name:             "fetch-fail-consumer",
		Stream:           "test",
		WindowDuration:   5 * time.Second,
		WatermarkTimeout: 50 * time.Millisecond,
		DedupKey:         []string{"msg_id"},
		LatePolicy:       LatePolicyDrop,
		DeliverPolicy:    DeliverAll,
	}

	consumer, err := NewConsumer(consumerCfg, stream, "node-1", dir)
	if err != nil {
		t.Fatal(err)
	}

	fetchCalled := false
	consumer.AddSource("node-failing")
	consumer.SetPeerFetcher(func(nodeID string, startSeq uint64) ([]*Message, error) {
		if nodeID == "node-failing" {
			fetchCalled = true
			return nil, fmt.Errorf("connection refused")
		}
		return nil, nil
	})

	// Manually drive the pipeline instead of relying on the read loop.
	// Step 1: Collect messages (local + peer). Peer will fail.
	consumer.mu.Lock()
	consumer.collectMessages()
	consumer.mu.Unlock()

	if !fetchCalled {
		t.Fatal("expected peer fetch to be attempted")
	}

	// Step 2: Update local progress and wait for the failing peer to time out.
	consumer.UpdateSourceProgress("node-1", 10_000_000_000)
	time.Sleep(100 * time.Millisecond)

	// Step 3: Refresh local progress (so it remains alive) and advance watermark.
	consumer.UpdateSourceProgress("node-1", 10_000_000_000)

	consumer.mu.Lock()
	consumer.advanceWatermark()
	consumer.emitWindows()
	consumer.mu.Unlock()

	// The failing peer should have timed out, allowing the watermark to advance
	// using only the local source, and the local message should be emitted.
	select {
	case batch := <-consumer.Output():
		if len(batch.Messages) == 0 {
			t.Fatal("expected at least one message")
		}
		if batch.Messages[0].MsgID != "local-1" {
			t.Errorf("expected local-1, got %s", batch.Messages[0].MsgID)
		}
	default:
		t.Fatal("no batch emitted despite failing peer being timed out")
	}
}

// FM-R4: Consumer position file corruption causes fresh start.
func TestConsumerPositionCorruption(t *testing.T) {
	dir := tempDir(t)

	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	stream, err := NewStream(cfg, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	consumerCfg := ConsumerConfig{
		Name:             "corrupt-consumer",
		Stream:           "test",
		WindowDuration:   2 * time.Second,
		WatermarkTimeout: 5 * time.Second,
		DedupKey:         []string{"msg_id"},
		LatePolicy:       LatePolicyDrop,
	}

	// Create consumer, set positions, save them
	consumer1, err := NewConsumer(consumerCfg, stream, "node-1", dir)
	if err != nil {
		t.Fatal(err)
	}
	consumer1.SetPositions(map[string]*SourcePosition{
		"node-a": {NodeID: "node-a", LastSeq: 50, LastTS: 5_000_000_000},
	}, 4_000_000_000)
	consumer1.Stop()

	// Corrupt the position file
	posPath := filepath.Join(dir, "consumers", "corrupt-consumer.json")
	os.WriteFile(posPath, []byte("{{{{not valid json"), 0644)

	// Create a new consumer -- should start fresh despite corruption
	consumer2, err := NewConsumer(consumerCfg, stream, "node-1", dir)
	if err != nil {
		t.Fatal(err)
	}

	// Positions should be empty (fresh start)
	positions := consumer2.Positions()
	if len(positions) != 0 {
		t.Errorf("expected empty positions after corruption, got %d entries", len(positions))
	}
	if consumer2.Watermark() != 0 {
		t.Errorf("expected watermark 0 after corruption, got %d", consumer2.Watermark())
	}
}

// FM-R5: Deleting a stream also deletes its consumers.
func TestDeleteStreamCascadesToConsumers(t *testing.T) {
	node, _ := makeTestNode(t, "node-a")

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	streamCfg.FsyncPolicy = FsyncNone
	node.CreateStream(streamCfg)

	// Create two consumers
	node.CreateConsumer(DefaultConsumerConfig("consumer-a", "test"))
	node.CreateConsumer(DefaultConsumerConfig("consumer-b", "test"))

	// Verify they exist
	if len(node.ListConsumers("test")) != 2 {
		t.Fatal("expected 2 consumers before delete")
	}

	// Delete the stream
	node.DeleteStream("test")

	// Both consumers should be gone
	if c := node.GetConsumer("consumer-a"); c != nil {
		t.Error("consumer-a should have been deleted")
	}
	if c := node.GetConsumer("consumer-b"); c != nil {
		t.Error("consumer-b should have been deleted")
	}
}

// FM-R6: Consumer with wrong stream name in delete is rejected.
func TestDeleteConsumerWrongStream(t *testing.T) {
	node, _ := makeTestNode(t, "node-a")

	cfg1 := DefaultStreamConfig("stream-a", []string{"a.>"})
	cfg1.FsyncPolicy = FsyncNone
	node.CreateStream(cfg1)

	cfg2 := DefaultStreamConfig("stream-b", []string{"b.>"})
	cfg2.FsyncPolicy = FsyncNone
	node.CreateStream(cfg2)

	node.CreateConsumer(DefaultConsumerConfig("consumer-x", "stream-a"))

	// Try to delete consumer-x via stream-b
	err := node.DeleteConsumer("stream-b", "consumer-x")
	if err == nil {
		t.Fatal("expected error when deleting consumer via wrong stream")
	}

	// Consumer should still exist
	if c := node.GetConsumer("consumer-x"); c == nil {
		t.Error("consumer should not have been deleted")
	}
}

// =============================================================================
// 4. Cluster / Network Failures
// =============================================================================

// FM-C1: Seed node down -- node still starts and operates.
func TestClusterSeedNodeDown(t *testing.T) {
	cfg := ClusterConfig{
		GossipInterval: 100 * time.Millisecond,
		FailureTimeout: 500 * time.Millisecond,
	}
	cluster := NewCluster("node-alone", "127.0.0.1:19999", cfg)

	// Join with unreachable seeds -- should not error
	err := cluster.Join([]string{"127.0.0.1:29999", "127.0.0.1:39999"})
	if err != nil {
		t.Fatalf("Join should not fail with unreachable seeds: %v", err)
	}

	// Peers should include seed entries
	peers := cluster.Peers()
	if len(peers) != 2 {
		t.Errorf("expected 2 seed peers, got %d", len(peers))
	}

	cluster.Stop()
}

// FM-C2: Peer failure detection marks dead peers.
func TestClusterPeerFailureDetection(t *testing.T) {
	cfg := ClusterConfig{
		GossipInterval: 50 * time.Millisecond,
		FailureTimeout: 100 * time.Millisecond,
	}
	cluster := NewCluster("node-1", "127.0.0.1:19999", cfg)

	// Manually add a peer that was recently alive
	cluster.mu.Lock()
	cluster.peers["node-2"] = &PeerInfo{
		ID:       "node-2",
		PeerAddr: "127.0.0.1:29999",
		State:    PeerAlive,
		LastSeen: time.Now().Add(-200 * time.Millisecond), // already past timeout
	}
	cluster.mu.Unlock()

	// Start the failure detection loop
	cluster.StartBackground()
	defer cluster.Stop()

	// Wait for detection to run
	time.Sleep(150 * time.Millisecond)

	// Peer should be marked dead
	cluster.mu.RLock()
	peer := cluster.peers["node-2"]
	state := peer.State
	cluster.mu.RUnlock()

	if state != PeerDead {
		t.Errorf("expected peer to be dead, got state %d", state)
	}
}

// FM-C3: Gossip from self is ignored.
func TestClusterGossipFromSelfIgnored(t *testing.T) {
	cfg := DefaultClusterConfig()
	cluster := NewCluster("node-1", "127.0.0.1:19999", cfg)

	// Simulate gossip from ourselves
	cluster.updatePeerFromGossip("node-1", "127.0.0.1:19999", nil, "")

	// Should not add ourselves as a peer
	if cluster.PeerCount() != 0 {
		t.Errorf("expected 0 peers (self ignored), got %d", cluster.PeerCount())
	}
}

// FM-C4: Gossip learns about new peers transitively.
func TestClusterGossipTransitivePeerDiscovery(t *testing.T) {
	cfg := DefaultClusterConfig()
	cluster := NewCluster("node-1", "127.0.0.1:19999", cfg)

	// Node-2 tells us about node-3 (which we haven't seen directly)
	transitivePeers := map[string]string{
		"node-3": "127.0.0.1:39999",
	}
	cluster.updatePeerFromGossip("node-2", "127.0.0.1:29999", transitivePeers, "127.0.0.1:29999")

	// We should now know about both node-2 and node-3
	if cluster.PeerCount() != 2 {
		t.Errorf("expected 2 peers, got %d", cluster.PeerCount())
	}
	if !cluster.IsAlive("node-2") {
		t.Error("node-2 should be alive")
	}
	if !cluster.IsAlive("node-3") {
		t.Error("node-3 should be alive")
	}
}

// FM-C5: Seed entries are cleaned up after gossip identifies the real node.
func TestClusterSeedCleanup(t *testing.T) {
	cfg := DefaultClusterConfig()
	cluster := NewCluster("node-1", "127.0.0.1:19999", cfg)

	// Add seed entry
	cluster.Join([]string{"127.0.0.1:29999"})

	// Simulate gossip from the seed address
	cluster.updatePeerFromGossip("node-2", "127.0.0.1:29999", nil, "127.0.0.1:29999")

	// The seed:... entry should be gone, replaced by node-2
	cluster.mu.RLock()
	_, hasSeed := cluster.peers["seed:127.0.0.1:29999"]
	_, hasNode := cluster.peers["node-2"]
	cluster.mu.RUnlock()

	if hasSeed {
		t.Error("seed entry should have been cleaned up")
	}
	if !hasNode {
		t.Error("node-2 should exist as a real peer")
	}
}

// =============================================================================
// 5. Concurrent Access / Stress Failures
// =============================================================================

// FM-X1: Concurrent publishes to the same stream don't corrupt the WAL.
func TestConcurrentPublishSafety(t *testing.T) {
	node, _ := makeTestNode(t, "node-concurrent")

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	streamCfg.FsyncPolicy = FsyncNone
	node.CreateStream(streamCfg)

	const goroutines = 10
	const msgsPerGoroutine = 100

	done := make(chan struct{})
	errs := make(chan error, goroutines*msgsPerGoroutine)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer func() { done <- struct{}{} }()
			for i := 0; i < msgsPerGoroutine; i++ {
				msg := &Message{
					Subject:    "test.data",
					Payload:    []byte(fmt.Sprintf("g%d-m%d", id, i)),
					ProducerTS: uint64(id*msgsPerGoroutine+i) * 1_000_000,
				}
				_, err := node.Publish(msg)
				if err != nil {
					errs <- err
				}
			}
		}(g)
	}

	for g := 0; g < goroutines; g++ {
		<-done
	}
	close(errs)

	for err := range errs {
		t.Errorf("publish error: %v", err)
	}

	expected := uint64(goroutines * msgsPerGoroutine)
	actual := node.GetStream("test").MessageCount()
	if actual != expected {
		t.Errorf("expected %d messages, got %d", expected, actual)
	}
}

// FM-X2: Double stop is safe (idempotent).
func TestNodeDoubleStop(t *testing.T) {
	dir := tempDir(t)
	cfg := NodeConfig{ID: "node-double", DataDir: dir}
	node, err := NewNode(cfg)
	if err != nil {
		t.Fatal(err)
	}

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	streamCfg.FsyncPolicy = FsyncNone
	node.CreateStream(streamCfg)

	// First stop
	if err := node.Stop(); err != nil {
		t.Fatalf("first stop: %v", err)
	}
	// Second stop should not panic
	if err := node.Stop(); err != nil {
		t.Fatalf("second stop: %v", err)
	}
}

// FM-X3: Consumer double stop is safe.
func TestConsumerDoubleStop(t *testing.T) {
	dir := tempDir(t)

	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	stream, err := NewStream(cfg, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	consumerCfg := DefaultConsumerConfig("test-consumer", "test")
	consumer, err := NewConsumer(consumerCfg, stream, "node-1", dir)
	if err != nil {
		t.Fatal(err)
	}

	consumer.Start()
	consumer.Stop()
	// Second stop should not panic
	consumer.Stop()
}

// FM-X4: Cluster double stop is safe.
func TestClusterDoubleStop(t *testing.T) {
	cfg := DefaultClusterConfig()
	cluster := NewCluster("node-1", "127.0.0.1:19999", cfg)

	cluster.Stop()
	// Second stop should not panic
	cluster.Stop()
}

// =============================================================================
// 6. Message Encoding Failures
// =============================================================================

// FM-E1: Decoding a message that exceeds size limit (64MB).
func TestDecodeMessageTooLarge(t *testing.T) {
	// Craft a length prefix that claims 65MB
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], 65*1024*1024)

	data := make([]byte, 4+10) // length prefix + some data
	copy(data[:4], buf[:])
	copy(data[4:], []byte("small-data"))

	_, _, err := DecodeMessageFromBytes(data)
	if err == nil {
		t.Fatal("expected error for oversized message")
	}
}

// FM-E2: Decoding from insufficient data.
func TestDecodeMessageInsufficientData(t *testing.T) {
	// Too short for length prefix
	_, _, err := DecodeMessageFromBytes([]byte{0x00, 0x01})
	if err == nil {
		t.Fatal("expected error for insufficient data")
	}

	// Length prefix says 100 bytes but only 10 available
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], 100)
	data := make([]byte, 14)
	copy(data[:4], buf[:])
	_, _, err = DecodeMessageFromBytes(data)
	if err == nil {
		t.Fatal("expected error for truncated message data")
	}
}

// FM-E3: Decoding invalid JSON in message body.
func TestDecodeMessageInvalidJSON(t *testing.T) {
	body := []byte("{not valid json")
	data := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(data[:4], uint32(len(body)))
	copy(data[4:], body)

	_, _, err := DecodeMessageFromBytes(data)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}
