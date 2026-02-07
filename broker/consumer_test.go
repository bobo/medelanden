package broker

import (
	"testing"
	"time"
)

// TC-O1: Messages are emitted in producer_ts order
func TestConsumerOrderingWithinWindow(t *testing.T) {
	dir := tempDir(t)

	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	stream, err := NewStream(cfg, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	consumerCfg := ConsumerConfig{
		Name:             "test-consumer",
		Stream:           "test",
		Ordering:         "producer_ts",
		WindowDuration:   2 * time.Second,
		WatermarkTimeout: 5 * time.Second,
		DedupKey:         []string{"msg_id"},
		LatePolicy:       LatePolicyDrop,
		DeliverPolicy:    DeliverAll,
	}

	consumer, err := NewConsumer(consumerCfg, stream, "node-1", dir)
	if err != nil {
		t.Fatal(err)
	}

	// Inject messages in non-sorted order:
	// Window [0-2s] should contain ts=1s, 1.5s, 0.5s
	// (Using nanoseconds: 2s = 2_000_000_000)
	msgs := []*Message{
		{Subject: "test.a", ProducerTS: 1_500_000_000, MsgID: "msg-3", NodeID: "node-1"},
		{Subject: "test.a", ProducerTS: 500_000_000, MsgID: "msg-1", NodeID: "node-1"},
		{Subject: "test.a", ProducerTS: 1_000_000_000, MsgID: "msg-2", NodeID: "node-1"},
	}

	consumer.InjectMessages(msgs)

	// Advance watermark past the window end (2s)
	consumer.ForceAdvanceWatermark(3_000_000_000)
	consumer.FlushWindows()

	select {
	case batch := <-consumer.Output():
		if len(batch.Messages) != 3 {
			t.Fatalf("expected 3 messages, got %d", len(batch.Messages))
		}
		// Verify sorted order
		if batch.Messages[0].ProducerTS != 500_000_000 {
			t.Errorf("first message ts: expected 500000000, got %d", batch.Messages[0].ProducerTS)
		}
		if batch.Messages[1].ProducerTS != 1_000_000_000 {
			t.Errorf("second message ts: expected 1000000000, got %d", batch.Messages[1].ProducerTS)
		}
		if batch.Messages[2].ProducerTS != 1_500_000_000 {
			t.Errorf("third message ts: expected 1500000000, got %d", batch.Messages[2].ProducerTS)
		}
	default:
		t.Fatal("no batch emitted")
	}
}

// TC-O3: Messages within same timestamp are stable-sorted by msg_id
func TestConsumerStableSortByMsgID(t *testing.T) {
	dir := tempDir(t)

	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	stream, err := NewStream(cfg, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	consumerCfg := ConsumerConfig{
		Name:             "test-consumer",
		Stream:           "test",
		WindowDuration:   2 * time.Second,
		WatermarkTimeout: 5 * time.Second,
		DedupKey:         []string{"msg_id"},
		LatePolicy:       LatePolicyDrop,
	}

	consumer, err := NewConsumer(consumerCfg, stream, "node-1", dir)
	if err != nil {
		t.Fatal(err)
	}

	// Two messages with identical producer_ts
	msgs := []*Message{
		{Subject: "test.a", ProducerTS: 1_000_000_000, MsgID: "zzz", NodeID: "node-1"},
		{Subject: "test.b", ProducerTS: 1_000_000_000, MsgID: "aaa", NodeID: "node-1"},
	}

	consumer.InjectMessages(msgs)
	consumer.ForceAdvanceWatermark(3_000_000_000)
	consumer.FlushWindows()

	select {
	case batch := <-consumer.Output():
		if len(batch.Messages) != 2 {
			t.Fatalf("expected 2 messages, got %d", len(batch.Messages))
		}
		// Should be sorted by msg_id: "aaa" before "zzz"
		if batch.Messages[0].MsgID != "aaa" {
			t.Errorf("first message: expected 'aaa', got %q", batch.Messages[0].MsgID)
		}
		if batch.Messages[1].MsgID != "zzz" {
			t.Errorf("second message: expected 'zzz', got %q", batch.Messages[1].MsgID)
		}
	default:
		t.Fatal("no batch emitted")
	}
}

// TC-D1: Duplicate messages are deduplicated
func TestConsumerDeduplication(t *testing.T) {
	dir := tempDir(t)

	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	stream, err := NewStream(cfg, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	consumerCfg := ConsumerConfig{
		Name:             "test-consumer",
		Stream:           "test",
		WindowDuration:   2 * time.Second,
		WatermarkTimeout: 5 * time.Second,
		DedupKey:         []string{"msg_id"},
		LatePolicy:       LatePolicyDrop,
	}

	consumer, err := NewConsumer(consumerCfg, stream, "node-1", dir)
	if err != nil {
		t.Fatal(err)
	}

	// Same message replicated from 3 nodes (same msg_id)
	msgs := []*Message{
		{Subject: "test.a", ProducerTS: 1_000_000_000, MsgID: "same-id", NodeID: "node-a", Payload: []byte("data")},
		{Subject: "test.a", ProducerTS: 1_000_000_000, MsgID: "same-id", NodeID: "node-b", Payload: []byte("data")},
		{Subject: "test.a", ProducerTS: 1_000_000_000, MsgID: "same-id", NodeID: "node-c", Payload: []byte("data")},
	}

	consumer.InjectMessages(msgs)
	consumer.ForceAdvanceWatermark(3_000_000_000)
	consumer.FlushWindows()

	select {
	case batch := <-consumer.Output():
		if len(batch.Messages) != 1 {
			t.Fatalf("expected 1 message after dedup, got %d", len(batch.Messages))
		}
		if consumer.DedupCount() != 2 {
			t.Errorf("expected dedup count 2, got %d", consumer.DedupCount())
		}
	default:
		t.Fatal("no batch emitted")
	}
}

// TC-D2: Custom dedup_key works
func TestConsumerCustomDedupKey(t *testing.T) {
	dir := tempDir(t)

	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	stream, err := NewStream(cfg, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	consumerCfg := ConsumerConfig{
		Name:             "test-consumer",
		Stream:           "test",
		WindowDuration:   2 * time.Second,
		WatermarkTimeout: 5 * time.Second,
		DedupKey:         []string{"subject", "producer_ts"},
		LatePolicy:       LatePolicyDrop,
	}

	consumer, err := NewConsumer(consumerCfg, stream, "node-1", dir)
	if err != nil {
		t.Fatal(err)
	}

	// Two messages with same subject and producer_ts but different payloads
	msgs := []*Message{
		{Subject: "test.a", ProducerTS: 1_000_000_000, MsgID: "id-1", Payload: []byte("first"), NodeID: "node-1"},
		{Subject: "test.a", ProducerTS: 1_000_000_000, MsgID: "id-2", Payload: []byte("second"), NodeID: "node-1"},
	}

	consumer.InjectMessages(msgs)
	consumer.ForceAdvanceWatermark(3_000_000_000)
	consumer.FlushWindows()

	select {
	case batch := <-consumer.Output():
		if len(batch.Messages) != 1 {
			t.Fatalf("expected 1 message after custom dedup, got %d", len(batch.Messages))
		}
		// First seen wins
		if string(batch.Messages[0].Payload) != "first" {
			t.Errorf("expected 'first' payload, got %q", batch.Messages[0].Payload)
		}
	default:
		t.Fatal("no batch emitted")
	}
}

// TC-D3: Different messages are not falsely deduplicated
func TestConsumerNonFalseDedup(t *testing.T) {
	dir := tempDir(t)

	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	stream, err := NewStream(cfg, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	consumerCfg := ConsumerConfig{
		Name:             "test-consumer",
		Stream:           "test",
		WindowDuration:   2 * time.Second,
		WatermarkTimeout: 5 * time.Second,
		DedupKey:         []string{"msg_id"},
		LatePolicy:       LatePolicyDrop,
	}

	consumer, err := NewConsumer(consumerCfg, stream, "node-1", dir)
	if err != nil {
		t.Fatal(err)
	}

	// Two messages with same producer_ts but different msg_ids
	msgs := []*Message{
		{Subject: "test.a", ProducerTS: 1_000_000_000, MsgID: "id-1", NodeID: "node-1"},
		{Subject: "test.b", ProducerTS: 1_000_000_000, MsgID: "id-2", NodeID: "node-1"},
	}

	consumer.InjectMessages(msgs)
	consumer.ForceAdvanceWatermark(3_000_000_000)
	consumer.FlushWindows()

	select {
	case batch := <-consumer.Output():
		if len(batch.Messages) != 2 {
			t.Fatalf("expected 2 messages (no false dedup), got %d", len(batch.Messages))
		}
	default:
		t.Fatal("no batch emitted")
	}
}

// TC-WM3: Late messages after watermark jump are dropped
func TestConsumerLateMessageDrop(t *testing.T) {
	dir := tempDir(t)

	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	stream, err := NewStream(cfg, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	consumerCfg := ConsumerConfig{
		Name:             "test-consumer",
		Stream:           "test",
		WindowDuration:   2 * time.Second,
		WatermarkTimeout: 5 * time.Second,
		DedupKey:         []string{"msg_id"},
		LatePolicy:       LatePolicyDrop,
	}

	consumer, err := NewConsumer(consumerCfg, stream, "node-1", dir)
	if err != nil {
		t.Fatal(err)
	}

	// Set watermark to 5s
	consumer.ForceAdvanceWatermark(5_000_000_000)

	// Inject a late message (ts < watermark)
	msgs := []*Message{
		{Subject: "test.a", ProducerTS: 1_000_000_000, MsgID: "late-msg", NodeID: "node-1"},
	}
	consumer.InjectMessages(msgs)

	// No window should emit for the late message
	consumer.FlushWindows()

	select {
	case <-consumer.Output():
		t.Fatal("should not emit late message with drop policy")
	default:
		// good
	}

	if consumer.LateMessages() != 1 {
		t.Errorf("expected 1 late message, got %d", consumer.LateMessages())
	}
}

// TC-WM4: Late messages can be emitted unordered
func TestConsumerLateMessageEmitUnordered(t *testing.T) {
	dir := tempDir(t)

	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	stream, err := NewStream(cfg, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	consumerCfg := ConsumerConfig{
		Name:             "test-consumer",
		Stream:           "test",
		WindowDuration:   2 * time.Second,
		WatermarkTimeout: 5 * time.Second,
		DedupKey:         []string{"msg_id"},
		LatePolicy:       LatePolicyEmitUnordered,
	}

	consumer, err := NewConsumer(consumerCfg, stream, "node-1", dir)
	if err != nil {
		t.Fatal(err)
	}

	// Set watermark to 5s
	consumer.ForceAdvanceWatermark(5_000_000_000)

	// Inject a late message
	msgs := []*Message{
		{Subject: "test.a", ProducerTS: 1_000_000_000, MsgID: "late-msg", NodeID: "node-1"},
	}
	consumer.InjectMessages(msgs)

	select {
	case batch := <-consumer.Output():
		if len(batch.Messages) != 1 {
			t.Fatalf("expected 1 late message, got %d", len(batch.Messages))
		}
		if !batch.Messages[0].Late {
			t.Error("expected late flag to be set")
		}
	default:
		t.Fatal("expected late message to be emitted with emit_unordered policy")
	}

	if consumer.LateMessages() != 1 {
		t.Errorf("expected 1 late message, got %d", consumer.LateMessages())
	}
}

// TC-O2: Cross-node merge produces correct order
func TestConsumerCrossNodeMerge(t *testing.T) {
	dir := tempDir(t)

	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	stream, err := NewStream(cfg, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	consumerCfg := ConsumerConfig{
		Name:             "test-consumer",
		Stream:           "test",
		WindowDuration:   10 * time.Second, // large window to capture all
		WatermarkTimeout: 5 * time.Second,
		DedupKey:         []string{"msg_id"},
		LatePolicy:       LatePolicyDrop,
	}

	consumer, err := NewConsumer(consumerCfg, stream, "node-a", dir)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate messages from 3 different nodes
	msgs := []*Message{
		// Node A
		{Subject: "test.a", ProducerTS: 1_000_000_000, MsgID: "a-1", NodeID: "node-a"},
		{Subject: "test.a", ProducerTS: 4_000_000_000, MsgID: "a-2", NodeID: "node-a"},
		{Subject: "test.a", ProducerTS: 7_000_000_000, MsgID: "a-3", NodeID: "node-a"},
		// Node B
		{Subject: "test.a", ProducerTS: 2_000_000_000, MsgID: "b-1", NodeID: "node-b"},
		{Subject: "test.a", ProducerTS: 5_000_000_000, MsgID: "b-2", NodeID: "node-b"},
		{Subject: "test.a", ProducerTS: 8_000_000_000, MsgID: "b-3", NodeID: "node-b"},
		// Node C
		{Subject: "test.a", ProducerTS: 3_000_000_000, MsgID: "c-1", NodeID: "node-c"},
		{Subject: "test.a", ProducerTS: 6_000_000_000, MsgID: "c-2", NodeID: "node-c"},
		{Subject: "test.a", ProducerTS: 9_000_000_000, MsgID: "c-3", NodeID: "node-c"},
	}

	consumer.InjectMessages(msgs)
	consumer.ForceAdvanceWatermark(11_000_000_000)
	consumer.FlushWindows()

	select {
	case batch := <-consumer.Output():
		if len(batch.Messages) != 9 {
			t.Fatalf("expected 9 messages, got %d", len(batch.Messages))
		}
		// Verify strict ordering by producer_ts
		for i := 0; i < len(batch.Messages)-1; i++ {
			if batch.Messages[i].ProducerTS > batch.Messages[i+1].ProducerTS {
				t.Errorf("ordering violation at index %d: ts=%d > ts=%d",
					i, batch.Messages[i].ProducerTS, batch.Messages[i+1].ProducerTS)
			}
		}
		// Verify sequence: 1, 2, 3, 4, 5, 6, 7, 8, 9 (seconds)
		for i, msg := range batch.Messages {
			expected := uint64(i+1) * 1_000_000_000
			if msg.ProducerTS != expected {
				t.Errorf("msg %d: expected ts %d, got %d", i, expected, msg.ProducerTS)
			}
		}
	default:
		t.Fatal("no batch emitted")
	}
}

func TestConsumerWatermarkAdvancement(t *testing.T) {
	dir := tempDir(t)

	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	stream, err := NewStream(cfg, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	consumerCfg := ConsumerConfig{
		Name:             "test-consumer",
		Stream:           "test",
		WindowDuration:   2 * time.Second,
		WatermarkTimeout: 100 * time.Millisecond,
		DedupKey:         []string{"msg_id"},
		LatePolicy:       LatePolicyDrop,
	}

	consumer, err := NewConsumer(consumerCfg, stream, "node-1", dir)
	if err != nil {
		t.Fatal(err)
	}

	// Update progress for the local source
	consumer.UpdateSourceProgress("node-1", 10_000_000_000) // 10s

	// Watermark should be: min(10s) - 2s = 8s
	consumer.mu.Lock()
	consumer.advanceWatermark()
	wm := consumer.watermark
	consumer.mu.Unlock()

	if wm != 8_000_000_000 {
		t.Errorf("expected watermark 8s, got %d ns", wm)
	}
}

func TestConsumerMultiSourceWatermark(t *testing.T) {
	dir := tempDir(t)

	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	stream, err := NewStream(cfg, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	consumerCfg := ConsumerConfig{
		Name:             "test-consumer",
		Stream:           "test",
		WindowDuration:   2 * time.Second,
		WatermarkTimeout: 5 * time.Second,
		DedupKey:         []string{"msg_id"},
		LatePolicy:       LatePolicyDrop,
	}

	consumer, err := NewConsumer(consumerCfg, stream, "node-a", dir)
	if err != nil {
		t.Fatal(err)
	}

	// Add two more sources
	consumer.AddSource("node-b")
	consumer.AddSource("node-c")

	// Update progress: A=10s, B=8s, C=12s
	consumer.UpdateSourceProgress("node-a", 10_000_000_000)
	consumer.UpdateSourceProgress("node-b", 8_000_000_000)
	consumer.UpdateSourceProgress("node-c", 12_000_000_000)

	// Watermark should be: min(10s, 8s, 12s) - 2s = 6s
	consumer.mu.Lock()
	consumer.advanceWatermark()
	wm := consumer.watermark
	consumer.mu.Unlock()

	if wm != 6_000_000_000 {
		t.Errorf("expected watermark 6s, got %d ns", wm)
	}
}

// TC-WM2: Watermark stalls when a node dies, then advances
func TestConsumerWatermarkStallOnNodeDeath(t *testing.T) {
	dir := tempDir(t)

	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	stream, err := NewStream(cfg, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	consumerCfg := ConsumerConfig{
		Name:             "test-consumer",
		Stream:           "test",
		WindowDuration:   2 * time.Second,
		WatermarkTimeout: 100 * time.Millisecond, // Short timeout for test
		DedupKey:         []string{"msg_id"},
		LatePolicy:       LatePolicyDrop,
	}

	consumer, err := NewConsumer(consumerCfg, stream, "node-a", dir)
	if err != nil {
		t.Fatal(err)
	}

	consumer.AddSource("node-b")

	// Both sources at 10s
	consumer.UpdateSourceProgress("node-a", 10_000_000_000)
	consumer.UpdateSourceProgress("node-b", 10_000_000_000)

	// Advance watermark: min(10s, 10s) - 2s = 8s
	consumer.mu.Lock()
	consumer.advanceWatermark()
	wm1 := consumer.watermark
	consumer.mu.Unlock()

	if wm1 != 8_000_000_000 {
		t.Errorf("expected watermark 8s, got %d ns", wm1)
	}

	// Node A advances to 20s, Node B is dead (no update)
	consumer.UpdateSourceProgress("node-a", 20_000_000_000)
	// Don't update node-b - simulate death

	// Watermark should stall because node-b hasn't advanced
	consumer.mu.Lock()
	consumer.advanceWatermark()
	wm2 := consumer.watermark
	consumer.mu.Unlock()

	// Watermark is still 8s because node-b is at 10s
	if wm2 != 8_000_000_000 {
		t.Errorf("expected watermark to stall at 8s, got %d ns", wm2)
	}

	// Wait for watermark timeout to expire
	time.Sleep(150 * time.Millisecond)

	// Refresh node-a's progress so it's still considered alive
	consumer.UpdateSourceProgress("node-a", 20_000_000_000)

	// Now node-b should be excluded (timed out), watermark jumps
	consumer.mu.Lock()
	consumer.advanceWatermark()
	wm3 := consumer.watermark
	consumer.mu.Unlock()

	if wm3 != 18_000_000_000 {
		t.Errorf("expected watermark to jump to 18s after timeout, got %d ns", wm3)
	}
}

func TestConsumerPositionPersistence(t *testing.T) {
	dir := tempDir(t)

	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	stream, err := NewStream(cfg, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	consumerCfg := ConsumerConfig{
		Name:             "test-consumer",
		Stream:           "test",
		WindowDuration:   2 * time.Second,
		WatermarkTimeout: 5 * time.Second,
		DedupKey:         []string{"msg_id"},
		LatePolicy:       LatePolicyDrop,
	}

	// Create consumer, set positions, save
	consumer1, err := NewConsumer(consumerCfg, stream, "node-1", dir)
	if err != nil {
		t.Fatal(err)
	}

	consumer1.SetPositions(map[string]*SourcePosition{
		"node-a": {NodeID: "node-a", LastSeq: 100, LastTS: 5_000_000_000},
		"node-b": {NodeID: "node-b", LastSeq: 200, LastTS: 6_000_000_000},
	}, 4_000_000_000)

	consumer1.Stop()

	// Create new consumer, load positions
	consumer2, err := NewConsumer(consumerCfg, stream, "node-1", dir)
	if err != nil {
		t.Fatal(err)
	}

	positions := consumer2.Positions()
	if positions["node-a"].LastSeq != 100 {
		t.Errorf("node-a last_seq: expected 100, got %d", positions["node-a"].LastSeq)
	}
	if positions["node-b"].LastSeq != 200 {
		t.Errorf("node-b last_seq: expected 200, got %d", positions["node-b"].LastSeq)
	}
	if consumer2.Watermark() != 4_000_000_000 {
		t.Errorf("watermark: expected 4s, got %d", consumer2.Watermark())
	}
}
