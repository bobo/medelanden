package broker

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// Helper to find a free TCP port.
func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

// Helper to create a cluster of N nodes with replication.
func makeCluster(t *testing.T, n int, streamCfg StreamConfig) ([]*Node, []string) {
	t.Helper()

	peerAddrs := make([]string, n)
	for i := 0; i < n; i++ {
		peerAddrs[i] = freePort(t)
	}

	nodes := make([]*Node, n)
	for i := 0; i < n; i++ {
		dir := tempDir(t)
		cfg := NodeConfig{
			ID:       fmt.Sprintf("node-%d", i),
			DataDir:  dir,
			PeerAddr: peerAddrs[i],
			Seeds:    peerAddrs,
		}

		node, err := NewNode(cfg)
		if err != nil {
			t.Fatal(err)
		}

		streamCfg.FsyncPolicy = FsyncNone
		if err := node.CreateStream(streamCfg); err != nil {
			t.Fatal(err)
		}

		if err := node.Start(); err != nil {
			t.Fatal(err)
		}

		nodes[i] = node
	}

	t.Cleanup(func() {
		for _, n := range nodes {
			n.Stop()
		}
	})

	// Give gossip time to discover peers
	time.Sleep(2 * time.Second)

	return nodes, peerAddrs
}

// TC-W1: Write succeeds with all peers down
func TestIntegrationWriteWithPeersDown(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	dir := tempDir(t)
	cfg := NodeConfig{
		ID:      "node-a",
		DataDir: dir,
	}

	node, err := NewNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Stop()

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	streamCfg.FsyncPolicy = FsyncNone
	node.CreateStream(streamCfg)

	// No peers at all - write must still succeed
	for i := uint64(0); i < 1000; i++ {
		msg := &Message{
			Subject:    "test.data",
			Payload:    []byte("payload"),
			ProducerTS: i * 1_000_000,
		}
		if _, err := node.Publish(msg); err != nil {
			t.Fatalf("publish %d failed: %v", i, err)
		}
	}

	stream := node.GetStream("test")
	if stream.MessageCount() != 1000 {
		t.Fatalf("expected 1000 messages, got %d", stream.MessageCount())
	}
}

// TC-R1: Messages replicate to all peers
func TestIntegrationReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	streamCfg.ReplicationTarget = 2

	nodes, _ := makeCluster(t, 3, streamCfg)

	// Publish 100 messages to node 0
	for i := uint64(0); i < 100; i++ {
		msg := &Message{
			Subject:    "test.data",
			Payload:    []byte(fmt.Sprintf("msg-%d", i)),
			ProducerTS: i * 1_000_000,
		}
		if _, err := nodes[0].Publish(msg); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Verify local write
	if nodes[0].GetStream("test").MessageCount() != 100 {
		t.Fatalf("node-0: expected 100 messages, got %d", nodes[0].GetStream("test").MessageCount())
	}

	// Wait for replication (with timeout)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		count1 := nodes[1].GetStream("test").MessageCount()
		count2 := nodes[2].GetStream("test").MessageCount()
		if count1 >= 100 && count2 >= 100 {
			return // success
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Report what we got
	for i, n := range nodes {
		t.Logf("node-%d: %d messages", i, n.GetStream("test").MessageCount())
	}
	t.Fatal("replication did not complete within 30 seconds")
}

// TC-R3: Replication does not duplicate on the same node
func TestIntegrationReplicationNoDuplicates(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	nodes, _ := makeCluster(t, 3, streamCfg)

	// Publish 50 messages to node 0
	for i := uint64(0); i < 50; i++ {
		msg := &Message{
			Subject:    "test.data",
			Payload:    []byte(fmt.Sprintf("msg-%d", i)),
			ProducerTS: i * 1_000_000,
		}
		nodes[0].Publish(msg)
	}

	// Wait for replication
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		allDone := true
		for _, n := range nodes {
			if n.GetStream("test").MessageCount() < 50 {
				allDone = false
				break
			}
		}
		if allDone {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Verify no duplicates: each node should have exactly 50
	for i, n := range nodes {
		count := n.GetStream("test").MessageCount()
		if count != 50 {
			t.Errorf("node-%d: expected 50 messages, got %d", i, count)
		}
	}
}

// TC-W2: Write succeeds during network partition (simulated)
func TestIntegrationWriteDuringPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Two independent nodes (simulating partition: no seeds)
	dir1 := tempDir(t)
	dir2 := tempDir(t)

	node1, _ := NewNode(NodeConfig{ID: "node-a", DataDir: dir1})
	node2, _ := NewNode(NodeConfig{ID: "node-b", DataDir: dir2})
	defer node1.Stop()
	defer node2.Stop()

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	streamCfg.FsyncPolicy = FsyncNone
	node1.CreateStream(streamCfg)
	node2.CreateStream(streamCfg)

	// Both nodes accept writes independently
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := uint64(0); i < 500; i++ {
			msg := &Message{
				Subject:    "test.data",
				Payload:    []byte("from-a"),
				ProducerTS: i * 1_000_000,
			}
			if _, err := node1.Publish(msg); err != nil {
				t.Errorf("node-a publish %d: %v", i, err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := uint64(0); i < 500; i++ {
			msg := &Message{
				Subject:    "test.data",
				Payload:    []byte("from-b"),
				ProducerTS: (i + 500) * 1_000_000,
			}
			if _, err := node2.Publish(msg); err != nil {
				t.Errorf("node-b publish %d: %v", i, err)
				return
			}
		}
	}()

	wg.Wait()

	// Both nodes should have their own 500 messages
	if node1.GetStream("test").MessageCount() != 500 {
		t.Errorf("node-a: expected 500, got %d", node1.GetStream("test").MessageCount())
	}
	if node2.GetStream("test").MessageCount() != 500 {
		t.Errorf("node-b: expected 500, got %d", node2.GetStream("test").MessageCount())
	}
}

// TC-F1: Cluster survives losing all but one node
func TestIntegrationSingleNodeSurvival(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Create a single node (simulating last survivor of 5-node cluster)
	dir := tempDir(t)
	node, _ := NewNode(NodeConfig{ID: "node-survivor", DataDir: dir})
	defer node.Stop()

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	streamCfg.FsyncPolicy = FsyncNone
	node.CreateStream(streamCfg)

	// Must accept writes
	for i := uint64(0); i < 100; i++ {
		msg := &Message{
			Subject:    "test.data",
			Payload:    []byte("survivor-data"),
			ProducerTS: i * 1_000_000,
		}
		if _, err := node.Publish(msg); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Must serve reads
	stream := node.GetStream("test")
	msgs, err := stream.ReadFrom(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 100 {
		t.Fatalf("expected 100 messages, got %d", len(msgs))
	}
}

// TC-P1: Write throughput test
func TestIntegrationWriteThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	node, _ := makeTestNode(t, "node-bench")

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	streamCfg.FsyncPolicy = FsyncNone
	node.CreateStream(streamCfg)

	count := 10000
	start := time.Now()

	for i := 0; i < count; i++ {
		msg := &Message{
			Subject:    "test.data",
			Payload:    []byte("benchmark-payload-with-some-data"),
			ProducerTS: uint64(i) * 1_000_000,
		}
		if _, err := node.Publish(msg); err != nil {
			t.Fatal(err)
		}
	}

	elapsed := time.Since(start)
	rate := float64(count) / elapsed.Seconds()
	t.Logf("Write throughput: %.0f msg/sec (%d messages in %v)", rate, count, elapsed)

	// Should be able to do at least 10k msg/sec on a single node
	if rate < 10000 {
		t.Errorf("write throughput too low: %.0f msg/sec", rate)
	}
}

// TCP server test
func TestIntegrationTCPServer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	node, _ := makeTestNode(t, "node-tcp")

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	streamCfg.FsyncPolicy = FsyncNone
	node.CreateStream(streamCfg)

	addr := freePort(t)
	server := NewServer(addr, node)
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()

	// Connect and send PUB command
	conn, err := net.DialTimeout("tcp", server.Addr(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Send a PUB
	msg := fmt.Sprintf("PUB test.data mykey 1000000000 5\r\nhello\r\n")
	_, err = conn.Write([]byte(msg))
	if err != nil {
		t.Fatal(err)
	}

	// Read response
	buf := make([]byte, 256)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	response := string(buf[:n])
	if response[:3] != "+OK" {
		t.Errorf("expected +OK, got %q", response)
	}

	// Verify message in stream
	if node.GetStream("test").MessageCount() != 1 {
		t.Errorf("expected 1 message, got %d", node.GetStream("test").MessageCount())
	}
}

// HTTP health endpoint test
func TestIntegrationHealthEndpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	node, _ := makeTestNode(t, "node-http")

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	streamCfg.FsyncPolicy = FsyncNone
	node.CreateStream(streamCfg)

	tcpAddr := freePort(t)
	httpAddr := freePort(t)

	server := NewServer(tcpAddr, node)
	server.Start()
	defer server.Stop()
	server.StartHTTP(httpAddr)

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Check health endpoint using raw TCP
	conn, err := net.DialTimeout("tcp", httpAddr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	req := "GET /healthz HTTP/1.0\r\nHost: localhost\r\n\r\n"
	conn.Write([]byte(req))

	buf := make([]byte, 4096)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := conn.Read(buf)
	response := string(buf[:n])

	if len(response) == 0 {
		t.Fatal("empty response from health endpoint")
	}
	// Check for HTTP 200 in the response line
	if !strings.Contains(response, "200") {
		t.Errorf("expected 200 in response, got: %s", response[:min(len(response), 100)])
	}
	if !strings.Contains(response, "node-http") {
		t.Errorf("expected node-http in response body, got: %s", response[:min(len(response), 200)])
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// End-to-end test: publish -> read pipeline -> consumer output
func TestIntegrationEndToEnd(t *testing.T) {
	dir := tempDir(t)
	node, _ := NewNode(NodeConfig{ID: "node-e2e", DataDir: dir})
	defer node.Stop()

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	streamCfg.FsyncPolicy = FsyncNone
	node.CreateStream(streamCfg)

	// Publish messages
	for i := uint64(1); i <= 10; i++ {
		msg := &Message{
			Subject:    "test.data",
			Payload:    []byte(fmt.Sprintf("msg-%d", i)),
			ProducerTS: i * 1_000_000_000, // i seconds
		}
		node.Publish(msg)
	}

	// Create consumer
	consumerCfg := ConsumerConfig{
		Name:             "e2e-consumer",
		Stream:           "test",
		Ordering:         "producer_ts",
		WindowDuration:   5 * time.Second,
		WatermarkTimeout: 1 * time.Second,
		DedupKey:         []string{"msg_id"},
		LatePolicy:       LatePolicyDrop,
		DeliverPolicy:    DeliverAll,
	}

	consumer, err := node.CreateConsumer(consumerCfg)
	if err != nil {
		t.Fatal(err)
	}

	// The consumer's read loop goroutine is running. Set source progress
	// high enough to advance the watermark past the first windows.
	// The read loop will collect messages and emit batches.
	consumer.UpdateSourceProgress("node-e2e", 12_000_000_000) // 12s

	// Read from output channel
	var batches []*WindowBatch
	timeout := time.After(1 * time.Second)
	for {
		select {
		case batch := <-consumer.Output():
			batches = append(batches, batch)
		case <-timeout:
			goto done
		}
	}
done:

	// Verify we got messages
	totalMsgs := 0
	for _, b := range batches {
		totalMsgs += len(b.Messages)
	}

	if totalMsgs == 0 {
		t.Fatal("no messages received from consumer")
	}

	// Verify ordering within each batch
	for _, b := range batches {
		for i := 1; i < len(b.Messages); i++ {
			if b.Messages[i].ProducerTS < b.Messages[i-1].ProducerTS {
				t.Errorf("ordering violation in batch: ts=%d < ts=%d",
					b.Messages[i].ProducerTS, b.Messages[i-1].ProducerTS)
			}
		}
	}
}

// TC-F2: Split-brain produces correct results after heal (simulated)
func TestIntegrationSplitBrainMerge(t *testing.T) {
	dir := tempDir(t)

	// Simulate: two partitions each produce unique messages
	// After "heal", a consumer merges both sets using the read pipeline.
	// We create the consumer directly (not via node.CreateConsumer) to avoid
	// the read loop goroutine and control the pipeline manually.

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	streamCfg.FsyncPolicy = FsyncNone
	stream, err := NewStream(streamCfg, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	// Partition A messages (odd timestamps)
	for i := uint64(1); i <= 10; i += 2 {
		msg := &Message{
			Subject:    "test.data",
			Payload:    []byte(fmt.Sprintf("partition-a-%d", i)),
			ProducerTS: i * 1_000_000_000,
			MsgID:      fmt.Sprintf("a-%d", i),
			NodeID:     "merger",
		}
		stream.Publish(msg)
	}

	// Partition B messages (even timestamps)
	for i := uint64(2); i <= 10; i += 2 {
		msg := &Message{
			Subject:    "test.data",
			Payload:    []byte(fmt.Sprintf("partition-b-%d", i)),
			ProducerTS: i * 1_000_000_000,
			MsgID:      fmt.Sprintf("b-%d", i),
			NodeID:     "merger",
		}
		stream.Publish(msg)
	}

	// Create consumer directly (don't start the read loop)
	consumerCfg := ConsumerConfig{
		Name:             "merge-consumer",
		Stream:           "test",
		WindowDuration:   15 * time.Second,
		WatermarkTimeout: 1 * time.Second,
		DedupKey:         []string{"msg_id"},
		LatePolicy:       LatePolicyDrop,
	}

	consumer, err := NewConsumer(consumerCfg, stream, "merger", dir)
	if err != nil {
		t.Fatal(err)
	}

	// Manually run the pipeline: collect messages, then set progress, advance, emit
	// Note: collectMessages updates source progress from actual data, so we set
	// the higher progress AFTER collection to simulate time passing.
	consumer.mu.Lock()
	consumer.collectMessages()
	consumer.mu.Unlock()

	consumer.UpdateSourceProgress("merger", 30_000_000_000)

	consumer.mu.Lock()
	consumer.advanceWatermark()
	consumer.emitWindows()
	consumer.mu.Unlock()

	select {
	case batch := <-consumer.Output():
		if len(batch.Messages) != 10 {
			t.Fatalf("expected 10 messages from merged partitions, got %d", len(batch.Messages))
		}
		// Verify order: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 seconds
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
