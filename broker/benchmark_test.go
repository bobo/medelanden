package broker

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Message encoding / decoding
// ---------------------------------------------------------------------------

func BenchmarkGenerateID(b *testing.B) {
	for b.Loop() {
		GenerateID()
	}
}

func BenchmarkMessageEncode(b *testing.B) {
	msg := &Message{
		Subject:    "aircraft.A1B2C3.position",
		Payload:    bytes.Repeat([]byte("x"), 256),
		ProducerTS: 1706900000000000000,
		DedupKey:   "aircraft.A1B2C3.position|1706900000000000000",
		MsgID:      "abcdef01-2345-6789-abcd-ef0123456789",
		NodeSeq:    42,
		NodeID:     "node-0",
	}
	b.ResetTimer()
	for b.Loop() {
		msg.Encode()
	}
}

func BenchmarkMessageDecode(b *testing.B) {
	msg := &Message{
		Subject:    "aircraft.A1B2C3.position",
		Payload:    bytes.Repeat([]byte("x"), 256),
		ProducerTS: 1706900000000000000,
		DedupKey:   "aircraft.A1B2C3.position|1706900000000000000",
		MsgID:      "abcdef01-2345-6789-abcd-ef0123456789",
		NodeSeq:    42,
		NodeID:     "node-0",
	}
	encoded, _ := msg.Encode()
	b.ResetTimer()
	for b.Loop() {
		DecodeMessageFromBytes(encoded)
	}
}

func BenchmarkEffectiveDedupKey_MsgID(b *testing.B) {
	msg := &Message{
		MsgID:   "abcdef01-2345-6789-abcd-ef0123456789",
		Subject: "aircraft.A1.position",
	}
	fields := []string{"msg_id"}
	b.ResetTimer()
	for b.Loop() {
		msg.EffectiveDedupKey(fields)
	}
}

func BenchmarkEffectiveDedupKey_Composite(b *testing.B) {
	msg := &Message{
		MsgID:      "abcdef01-2345-6789-abcd-ef0123456789",
		Subject:    "aircraft.A1.position",
		ProducerTS: 1706900000000000000,
		NodeID:     "node-0",
	}
	fields := []string{"subject", "producer_ts", "node_id"}
	b.ResetTimer()
	for b.Loop() {
		msg.EffectiveDedupKey(fields)
	}
}

// ---------------------------------------------------------------------------
// WAL operations
// ---------------------------------------------------------------------------

func BenchmarkWALAppend_FsyncNone(b *testing.B) {
	dir := b.TempDir()
	wal, err := NewWAL(filepath.Join(dir, "wal"), FsyncNone, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer wal.Close()

	msg := &Message{
		Subject:    "test.data",
		Payload:    bytes.Repeat([]byte("x"), 128),
		ProducerTS: 1000,
		MsgID:      "bench-msg",
		NodeID:     "node-bench",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg.ProducerTS = uint64(i) * 1_000_000
		msg.NodeSeq = 0 // reset so Append assigns
		wal.Append(msg)
	}
}

func BenchmarkWALAppend_FsyncEvery(b *testing.B) {
	dir := b.TempDir()
	wal, err := NewWAL(filepath.Join(dir, "wal"), FsyncEvery, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer wal.Close()

	msg := &Message{
		Subject:    "test.data",
		Payload:    bytes.Repeat([]byte("x"), 128),
		ProducerTS: 1000,
		MsgID:      "bench-msg",
		NodeID:     "node-bench",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg.ProducerTS = uint64(i) * 1_000_000
		msg.NodeSeq = 0
		wal.Append(msg)
	}
}

func BenchmarkWALRead(b *testing.B) {
	dir := b.TempDir()
	wal, err := NewWAL(filepath.Join(dir, "wal"), FsyncNone, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer wal.Close()

	// Pre-populate with 10k messages
	for i := 0; i < 10000; i++ {
		msg := &Message{
			Subject:    "test.data",
			Payload:    bytes.Repeat([]byte("x"), 128),
			ProducerTS: uint64(i) * 1_000_000,
			MsgID:      fmt.Sprintf("msg-%d", i),
			NodeID:     "node-bench",
		}
		wal.Append(msg)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Read a 100-message range from the middle
		wal.Read(5000, 5100)
	}
}

func BenchmarkWALReadFrom(b *testing.B) {
	dir := b.TempDir()
	wal, err := NewWAL(filepath.Join(dir, "wal"), FsyncNone, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer wal.Close()

	for i := 0; i < 1000; i++ {
		msg := &Message{
			Subject:    "test.data",
			Payload:    bytes.Repeat([]byte("x"), 128),
			ProducerTS: uint64(i) * 1_000_000,
			MsgID:      fmt.Sprintf("msg-%d", i),
			NodeID:     "node-bench",
		}
		wal.Append(msg)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Read last 50 messages (common case: consumer catching up)
		wal.ReadFrom(951)
	}
}

func BenchmarkWALAppendReplicated(b *testing.B) {
	dir := b.TempDir()
	wal, err := NewWAL(filepath.Join(dir, "wal"), FsyncNone, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer wal.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := &Message{
			Subject:    "test.data",
			Payload:    bytes.Repeat([]byte("x"), 128),
			ProducerTS: uint64(i) * 1_000_000,
			MsgID:      fmt.Sprintf("repl-%d", i),
			NodeID:     "node-remote",
			NodeSeq:    uint64(i + 1),
		}
		wal.AppendReplicated(msg)
	}
}

// ---------------------------------------------------------------------------
// Protocol parsing
// ---------------------------------------------------------------------------

func BenchmarkProtocolParsePUB(b *testing.B) {
	payload := strings.Repeat("x", 256)
	line := fmt.Sprintf("PUB aircraft.A1.position mykey 1706900000000000000 %d\r\n%s\r\n", len(payload), payload)
	data := []byte(line)
	b.ResetTimer()
	for b.Loop() {
		r := bytes.NewReader(data)
		p := NewProtocolParser(r)
		p.ParseCommand()
	}
}

func BenchmarkProtocolParseMPUB(b *testing.B) {
	payload := strings.Repeat("x", 256)
	line := fmt.Sprintf("MPUB aircraft.A1.position msg-123 mykey 1706900000000000000 %d\r\n%s\r\n", len(payload), payload)
	data := []byte(line)
	b.ResetTimer()
	for b.Loop() {
		r := bytes.NewReader(data)
		p := NewProtocolParser(r)
		p.ParseCommand()
	}
}

func BenchmarkProtocolParsePING(b *testing.B) {
	data := []byte("PING\r\n")
	b.ResetTimer()
	for b.Loop() {
		r := bytes.NewReader(data)
		p := NewProtocolParser(r)
		p.ParseCommand()
	}
}

func BenchmarkProtocolParseSUB(b *testing.B) {
	data := []byte("SUB aircraft.> my-consumer\r\n")
	b.ResetTimer()
	for b.Loop() {
		r := bytes.NewReader(data)
		p := NewProtocolParser(r)
		p.ParseCommand()
	}
}

func BenchmarkProtocolParseMSUB(b *testing.B) {
	data := []byte("MSUB aircraft.> my-consumer 2s 5s\r\n")
	b.ResetTimer()
	for b.Loop() {
		r := bytes.NewReader(data)
		p := NewProtocolParser(r)
		p.ParseCommand()
	}
}

func BenchmarkProtocolParseStreamCreate(b *testing.B) {
	data := []byte(`STREAM CREATE {"name":"events","subjects":["events.>"],"replication_target":3}` + "\r\n")
	b.ResetTimer()
	for b.Loop() {
		r := bytes.NewReader(data)
		p := NewProtocolParser(r)
		p.ParseCommand()
	}
}

// ---------------------------------------------------------------------------
// Subject matching
// ---------------------------------------------------------------------------

func BenchmarkStreamMatchSubject_Wildcard(b *testing.B) {
	cfg := DefaultStreamConfig("test", []string{"aircraft.>"})
	cfg.FsyncPolicy = FsyncNone
	dir := b.TempDir()
	stream, err := NewStream(cfg, dir)
	if err != nil {
		b.Fatal(err)
	}
	defer stream.Close()

	b.ResetTimer()
	for b.Loop() {
		stream.MatchSubject("aircraft.A1B2C3.position.lat")
	}
}

func BenchmarkStreamMatchSubject_ExactTokens(b *testing.B) {
	cfg := DefaultStreamConfig("test", []string{"aircraft.*.position"})
	cfg.FsyncPolicy = FsyncNone
	dir := b.TempDir()
	stream, err := NewStream(cfg, dir)
	if err != nil {
		b.Fatal(err)
	}
	defer stream.Close()

	b.ResetTimer()
	for b.Loop() {
		stream.MatchSubject("aircraft.A1B2C3.position")
	}
}

func BenchmarkStreamMatchSubject_MultiplePatterns(b *testing.B) {
	cfg := DefaultStreamConfig("test", []string{
		"aircraft.>",
		"vehicle.>",
		"sensor.>",
		"telemetry.>",
	})
	cfg.FsyncPolicy = FsyncNone
	dir := b.TempDir()
	stream, err := NewStream(cfg, dir)
	if err != nil {
		b.Fatal(err)
	}
	defer stream.Close()

	b.ResetTimer()
	for b.Loop() {
		// Match the last pattern (worst case)
		stream.MatchSubject("telemetry.temp.node-5")
	}
}

func BenchmarkStreamMatchSubject_NoMatch(b *testing.B) {
	cfg := DefaultStreamConfig("test", []string{
		"aircraft.>",
		"vehicle.>",
		"sensor.>",
	})
	cfg.FsyncPolicy = FsyncNone
	dir := b.TempDir()
	stream, err := NewStream(cfg, dir)
	if err != nil {
		b.Fatal(err)
	}
	defer stream.Close()

	b.ResetTimer()
	for b.Loop() {
		stream.MatchSubject("unknown.topic.data")
	}
}

// ---------------------------------------------------------------------------
// Node.Publish (full write path)
// ---------------------------------------------------------------------------

func BenchmarkNodePublish(b *testing.B) {
	dir := b.TempDir()
	node, err := NewNode(NodeConfig{ID: "bench-node", DataDir: dir})
	if err != nil {
		b.Fatal(err)
	}
	defer node.Stop()

	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	node.CreateStream(cfg)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := &Message{
			Subject:    "test.data",
			Payload:    bytes.Repeat([]byte("x"), 128),
			ProducerTS: uint64(i) * 1_000_000,
		}
		node.Publish(msg)
	}
}

func BenchmarkNodePublish_LargePayload(b *testing.B) {
	dir := b.TempDir()
	node, err := NewNode(NodeConfig{ID: "bench-node", DataDir: dir})
	if err != nil {
		b.Fatal(err)
	}
	defer node.Stop()

	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	node.CreateStream(cfg)

	payload := bytes.Repeat([]byte("x"), 4096) // 4KB payloads
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := &Message{
			Subject:    "test.data",
			Payload:    payload,
			ProducerTS: uint64(i) * 1_000_000,
		}
		node.Publish(msg)
	}
}

func BenchmarkNodePublish_PrePopulatedID(b *testing.B) {
	dir := b.TempDir()
	node, err := NewNode(NodeConfig{ID: "bench-node", DataDir: dir})
	if err != nil {
		b.Fatal(err)
	}
	defer node.Stop()

	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	node.CreateStream(cfg)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := &Message{
			Subject:    "test.data",
			Payload:    bytes.Repeat([]byte("x"), 128),
			ProducerTS: uint64(i) * 1_000_000,
			MsgID:      fmt.Sprintf("msg-%d", i),
			DedupKey:   fmt.Sprintf("test.data|%d", i*1_000_000),
		}
		node.Publish(msg)
	}
}

func BenchmarkNodePublish_Concurrent(b *testing.B) {
	dir := b.TempDir()
	node, err := NewNode(NodeConfig{ID: "bench-node", DataDir: dir})
	if err != nil {
		b.Fatal(err)
	}
	defer node.Stop()

	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	node.CreateStream(cfg)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			msg := &Message{
				Subject:    "test.data",
				Payload:    bytes.Repeat([]byte("x"), 128),
				ProducerTS: uint64(i) * 1_000_000,
			}
			node.Publish(msg)
			i++
		}
	})
}

// ---------------------------------------------------------------------------
// Consumer pipeline: windowing, dedup, emit
// ---------------------------------------------------------------------------

func BenchmarkConsumerBufferMessage(b *testing.B) {
	dir := b.TempDir()
	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	stream, err := NewStream(cfg, dir)
	if err != nil {
		b.Fatal(err)
	}
	defer stream.Close()

	consumerCfg := ConsumerConfig{
		Name:             "bench-consumer",
		Stream:           "test",
		WindowDuration:   2 * time.Second,
		WatermarkTimeout: 5 * time.Second,
		DedupKey:         []string{"msg_id"},
		LatePolicy:       LatePolicyDrop,
	}
	consumer, err := NewConsumer(consumerCfg, stream, "node-1", dir)
	if err != nil {
		b.Fatal(err)
	}

	msgs := make([]*Message, b.N)
	for i := range msgs {
		msgs[i] = &Message{
			Subject:    "test.data",
			ProducerTS: uint64(i) * 1_000_000,
			MsgID:      fmt.Sprintf("msg-%d", i),
			NodeID:     "node-1",
		}
	}

	b.ResetTimer()
	consumer.mu.Lock()
	for i := 0; i < b.N; i++ {
		consumer.bufferMessage(msgs[i])
	}
	consumer.mu.Unlock()
}

func BenchmarkConsumerDedup(b *testing.B) {
	dir := b.TempDir()
	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	stream, err := NewStream(cfg, dir)
	if err != nil {
		b.Fatal(err)
	}
	defer stream.Close()

	consumerCfg := ConsumerConfig{
		Name:             "bench-consumer",
		Stream:           "test",
		WindowDuration:   2 * time.Second,
		WatermarkTimeout: 5 * time.Second,
		DedupKey:         []string{"msg_id"},
		LatePolicy:       LatePolicyDrop,
	}
	consumer, err := NewConsumer(consumerCfg, stream, "node-1", dir)
	if err != nil {
		b.Fatal(err)
	}

	// Build a slice with 50% duplicates
	msgs := make([]*Message, 1000)
	for i := range msgs {
		msgs[i] = &Message{
			Subject:    "test.data",
			ProducerTS: uint64(i%500) * 1_000_000,
			MsgID:      fmt.Sprintf("msg-%d", i%500), // 50% duplicates
			NodeID:     "node-1",
		}
	}

	b.ResetTimer()
	for b.Loop() {
		consumer.dedup(msgs)
	}
}

func BenchmarkConsumerDedup_NoDuplicates(b *testing.B) {
	dir := b.TempDir()
	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	stream, err := NewStream(cfg, dir)
	if err != nil {
		b.Fatal(err)
	}
	defer stream.Close()

	consumerCfg := ConsumerConfig{
		Name:             "bench-consumer",
		Stream:           "test",
		WindowDuration:   2 * time.Second,
		WatermarkTimeout: 5 * time.Second,
		DedupKey:         []string{"msg_id"},
		LatePolicy:       LatePolicyDrop,
	}
	consumer, err := NewConsumer(consumerCfg, stream, "node-1", dir)
	if err != nil {
		b.Fatal(err)
	}

	msgs := make([]*Message, 1000)
	for i := range msgs {
		msgs[i] = &Message{
			Subject:    "test.data",
			ProducerTS: uint64(i) * 1_000_000,
			MsgID:      fmt.Sprintf("msg-%d", i),
			NodeID:     "node-1",
		}
	}

	b.ResetTimer()
	for b.Loop() {
		consumer.dedup(msgs)
	}
}

func BenchmarkConsumerEmitWindows(b *testing.B) {
	dir := b.TempDir()
	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	stream, err := NewStream(cfg, dir)
	if err != nil {
		b.Fatal(err)
	}
	defer stream.Close()

	consumerCfg := ConsumerConfig{
		Name:             "bench-consumer",
		Stream:           "test",
		WindowDuration:   2 * time.Second,
		WatermarkTimeout: 5 * time.Second,
		DedupKey:         []string{"msg_id"},
		LatePolicy:       LatePolicyDrop,
	}
	consumer, err := NewConsumer(consumerCfg, stream, "node-1", dir)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Inject 100 messages into a single window
		consumer.mu.Lock()
		windowBase := uint64(i) * 10_000_000_000 // each iteration gets fresh window space
		for j := 0; j < 100; j++ {
			consumer.bufferMessage(&Message{
				Subject:    "test.data",
				ProducerTS: windowBase + uint64(j)*1_000_000, // all within same 2s window
				MsgID:      fmt.Sprintf("msg-%d-%d", i, j),
				NodeID:     "node-1",
			})
		}
		consumer.watermark = windowBase + 3_000_000_000 // advance past window
		consumer.emitWindows()
		consumer.mu.Unlock()

		// Drain the output channel
		for {
			select {
			case <-consumer.Output():
			default:
				goto drained
			}
		}
	drained:
	}
}

// BenchmarkConsumerPipeline measures the full consumer pipeline:
// inject -> buffer -> advance watermark -> emit windows
func BenchmarkConsumerPipeline(b *testing.B) {
	dir := b.TempDir()
	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	stream, err := NewStream(cfg, dir)
	if err != nil {
		b.Fatal(err)
	}
	defer stream.Close()

	consumerCfg := ConsumerConfig{
		Name:             "bench-consumer",
		Stream:           "test",
		WindowDuration:   2 * time.Second,
		WatermarkTimeout: 5 * time.Second,
		DedupKey:         []string{"msg_id"},
		LatePolicy:       LatePolicyDrop,
	}
	consumer, err := NewConsumer(consumerCfg, stream, "node-1", dir)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		windowBase := uint64(i) * 10_000_000_000

		// Inject messages from 3 simulated nodes
		msgs := make([]*Message, 0, 30)
		for n := 0; n < 3; n++ {
			for j := 0; j < 10; j++ {
				msgs = append(msgs, &Message{
					Subject:    "test.data",
					ProducerTS: windowBase + uint64(j)*100_000_000,
					MsgID:      fmt.Sprintf("msg-%d-n%d-j%d", i, n, j),
					NodeID:     fmt.Sprintf("node-%d", n),
				})
			}
		}

		consumer.InjectMessages(msgs)
		consumer.UpdateSourceProgress("node-1", windowBase+3_000_000_000)
		consumer.mu.Lock()
		consumer.advanceWatermark()
		consumer.emitWindows()
		consumer.mu.Unlock()

		// Drain output
		for {
			select {
			case <-consumer.Output():
			default:
				goto drained2
			}
		}
	drained2:
	}
}

// ---------------------------------------------------------------------------
// Protocol formatting (response path)
// ---------------------------------------------------------------------------

func BenchmarkFormatOK(b *testing.B) {
	for b.Loop() {
		FormatOK(42)
	}
}

func BenchmarkFormatMsg(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 256)
	b.ResetTimer()
	for b.Loop() {
		FormatMsg("aircraft.A1.position", "my-consumer", 1706900000000000000, "msg-id-123", payload)
	}
}

func BenchmarkFormatWindowBatch(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 128)
	b.ResetTimer()
	for b.Loop() {
		FormatWindowBatchStart("my-consumer", 1000000000, 3000000000, 10)
		for j := 0; j < 10; j++ {
			FormatWindowBatchMsg("test.data", uint64(j)*100_000_000, fmt.Sprintf("msg-%d", j), payload)
		}
		FormatWindowBatchEnd()
	}
}

// ---------------------------------------------------------------------------
// End-to-end write throughput tests
// ---------------------------------------------------------------------------

func BenchmarkEndToEnd_PublishAndRead(b *testing.B) {
	dir := b.TempDir()
	node, err := NewNode(NodeConfig{ID: "bench-node", DataDir: dir})
	if err != nil {
		b.Fatal(err)
	}
	defer node.Stop()

	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	node.CreateStream(cfg)

	// Publish b.N messages
	for i := 0; i < b.N; i++ {
		msg := &Message{
			Subject:    "test.data",
			Payload:    bytes.Repeat([]byte("x"), 128),
			ProducerTS: uint64(i) * 1_000_000,
		}
		node.Publish(msg)
	}

	b.ResetTimer()
	// Read all messages back
	stream := node.GetStream("test")
	for b.Loop() {
		stream.ReadFrom(1)
	}
}

// ---------------------------------------------------------------------------
// Payload size scaling
// ---------------------------------------------------------------------------

func BenchmarkNodePublish_PayloadSizes(b *testing.B) {
	sizes := []int{64, 256, 1024, 4096, 16384}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			dir := b.TempDir()
			node, err := NewNode(NodeConfig{ID: "bench-node", DataDir: dir})
			if err != nil {
				b.Fatal(err)
			}
			defer node.Stop()

			cfg := DefaultStreamConfig("test", []string{"test.>"})
			cfg.FsyncPolicy = FsyncNone
			node.CreateStream(cfg)

			payload := bytes.Repeat([]byte("x"), size)
			b.ResetTimer()
			b.SetBytes(int64(size))
			for i := 0; i < b.N; i++ {
				msg := &Message{
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
// Concurrent publish with multiple goroutines
// ---------------------------------------------------------------------------

func BenchmarkNodePublish_Goroutines(b *testing.B) {
	goroutines := []int{1, 2, 4, 8}
	for _, g := range goroutines {
		b.Run(fmt.Sprintf("%d-goroutines", g), func(b *testing.B) {
			dir := b.TempDir()
			node, err := NewNode(NodeConfig{ID: "bench-node", DataDir: dir})
			if err != nil {
				b.Fatal(err)
			}
			defer node.Stop()

			cfg := DefaultStreamConfig("test", []string{"test.>"})
			cfg.FsyncPolicy = FsyncNone
			node.CreateStream(cfg)

			perGoroutine := b.N / g
			if perGoroutine == 0 {
				perGoroutine = 1
			}

			b.ResetTimer()
			var wg sync.WaitGroup
			wg.Add(g)
			for w := 0; w < g; w++ {
				go func(workerID int) {
					defer wg.Done()
					for i := 0; i < perGoroutine; i++ {
						msg := &Message{
							Subject:    "test.data",
							Payload:    bytes.Repeat([]byte("x"), 128),
							ProducerTS: uint64(workerID*perGoroutine+i) * 1_000_000,
						}
						node.Publish(msg)
					}
				}(w)
			}
			wg.Wait()
		})
	}
}
