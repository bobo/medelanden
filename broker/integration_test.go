package broker

import (
	"bufio"
	"context"
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
			BindAddr: "127.0.0.1:0",
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
			n.Stop(context.Background())
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
		ID:       "node-a",
		DataDir:  dir,
		BindAddr: "127.0.0.1:0",
	}

	node, err := NewNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Stop(context.Background())

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

	node1, _ := NewNode(NodeConfig{ID: "node-a", DataDir: dir1, BindAddr: "127.0.0.1:0"})
	node2, _ := NewNode(NodeConfig{ID: "node-b", DataDir: dir2, BindAddr: "127.0.0.1:0"})
	defer node1.Stop(context.Background())
	defer node2.Stop(context.Background())

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
	node, _ := NewNode(NodeConfig{ID: "node-survivor", DataDir: dir, BindAddr: "127.0.0.1:0"})
	defer node.Stop(context.Background())

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

	// Threshold is kept low enough for shared CI runners (e.g. GitHub Actions)
	// while still catching major regressions. Dedicated hardware typically
	// achieves 30k+ msg/sec.
	if rate < 2000 {
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
	defer server.Stop(context.Background())

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
	defer server.Stop(context.Background())
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

// End-to-end test: publish -> read pipeline -> consumer output
func TestIntegrationEndToEnd(t *testing.T) {
	dir := tempDir(t)
	node, _ := NewNode(NodeConfig{ID: "node-e2e", DataDir: dir, BindAddr: "127.0.0.1:0"})
	defer node.Stop(context.Background())

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

// Stream and consumer management over TCP
func TestIntegrationStreamConsumerManagement(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	node, _ := makeTestNode(t, "node-mgmt")

	addr := freePort(t)
	server := NewServer(addr, node)
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	defer server.Stop(context.Background())

	conn, err := net.DialTimeout("tcp", server.Addr(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	send := func(cmd string) string {
		t.Helper()
		conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
		conn.Write([]byte(cmd))
		buf := make([]byte, 4096)
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("read response for %q: %v", cmd, err)
		}
		return strings.TrimRight(string(buf[:n]), "\r\n")
	}

	// Create a stream via protocol
	resp := send("STREAM CREATE {\"name\":\"events\",\"subjects\":[\"events.>\"]}\r\n")
	if !strings.HasPrefix(resp, "+OK") {
		t.Fatalf("stream create: %q", resp)
	}

	// List streams
	resp = send("STREAM LIST\r\n")
	if !strings.Contains(resp, "events") {
		t.Fatalf("stream list should contain 'events': %q", resp)
	}

	// Stream info
	resp = send("STREAM INFO events\r\n")
	if !strings.Contains(resp, "+STREAM.INFO") {
		t.Fatalf("expected +STREAM.INFO, got: %q", resp)
	}
	if !strings.Contains(resp, "\"name\":\"events\"") {
		t.Fatalf("stream info should contain name: %q", resp)
	}

	// Publish a message to confirm the stream is functional
	resp = send("PUB events.click mykey 1000000000 5\r\nhello\r\n")
	if !strings.HasPrefix(resp, "+OK") {
		t.Fatalf("publish: %q", resp)
	}

	// Create a consumer via protocol
	resp = send("CONSUMER CREATE {\"name\":\"my-consumer\",\"stream\":\"events\",\"window_duration\":\"2s\",\"watermark_timeout\":\"5s\"}\r\n")
	if !strings.HasPrefix(resp, "+OK") {
		t.Fatalf("consumer create: %q", resp)
	}

	// List consumers
	resp = send("CONSUMER LIST events\r\n")
	if !strings.Contains(resp, "my-consumer") {
		t.Fatalf("consumer list should contain 'my-consumer': %q", resp)
	}

	// Consumer info
	resp = send("CONSUMER INFO events my-consumer\r\n")
	if !strings.Contains(resp, "+CONSUMER.INFO") {
		t.Fatalf("expected +CONSUMER.INFO, got: %q", resp)
	}
	if !strings.Contains(resp, "\"name\":\"my-consumer\"") {
		t.Fatalf("consumer info should contain name: %q", resp)
	}

	// Delete consumer
	resp = send("CONSUMER DELETE events my-consumer\r\n")
	if !strings.HasPrefix(resp, "+OK") {
		t.Fatalf("consumer delete: %q", resp)
	}

	// Consumer should be gone
	resp = send("CONSUMER LIST events\r\n")
	if strings.Contains(resp, "my-consumer") {
		t.Fatalf("consumer list should not contain deleted consumer: %q", resp)
	}

	// Delete stream
	resp = send("STREAM DELETE events\r\n")
	if !strings.HasPrefix(resp, "+OK") {
		t.Fatalf("stream delete: %q", resp)
	}

	// Stream should be gone
	resp = send("STREAM INFO events\r\n")
	if !strings.Contains(resp, "-ERR") {
		t.Fatalf("expected error for deleted stream: %q", resp)
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

// TC-SUB: Consumer delivery over TCP wire protocol.
// Publishes messages, creates SUB via TCP, verifies MSG frames arrive.
func TestIntegrationTCPSubscribeDelivery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	node, _ := makeTestNode(t, "node-sub")

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	streamCfg.FsyncPolicy = FsyncNone
	node.CreateStream(streamCfg)

	server := NewServer("127.0.0.1:0", node)
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	defer server.Stop(context.Background())

	conn, err := net.DialTimeout("tcp", server.Addr(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	// Publish a few messages first so the consumer has data to deliver
	for i := 0; i < 5; i++ {
		cmd := fmt.Sprintf("PUB test.data key %d 5\r\nhello\r\n", (i+1)*1_000_000_000)
		writer.WriteString(cmd)
		writer.Flush()
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		line, _ := reader.ReadString('\n')
		if !strings.HasPrefix(strings.TrimRight(line, "\r\n"), "+OK") {
			t.Fatalf("publish %d failed: %s", i, line)
		}
	}

	// Subscribe
	writer.WriteString("SUB test.> tcp-consumer\r\n")
	writer.Flush()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, _ := reader.ReadString('\n')
	if !strings.HasPrefix(strings.TrimRight(line, "\r\n"), "+OK") {
		t.Fatalf("SUB failed: %s", line)
	}

	// Read delivered MSG frames
	var received int
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		line = strings.TrimRight(line, "\r\n")

		if line == "PONG" {
			continue
		}
		if !strings.HasPrefix(line, "MSG ") {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) < 6 {
			t.Fatalf("malformed MSG: %s", line)
		}

		// Read payload
		size := 0
		fmt.Sscanf(parts[5], "%d", &size)
		payload := make([]byte, size+2) // +2 for \r\n
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		if _, err := reader.Read(payload); err != nil {
			t.Fatalf("read payload: %v", err)
		}

		received++
		if received >= 5 {
			break
		}
	}

	if received == 0 {
		t.Fatal("no MSG frames received from SUB")
	}
	t.Logf("received %d MSG frames via TCP SUB", received)
}

// TC-MC: Consumer on one node reads replicated data from another node.
func TestIntegrationMultiNodeConsumer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	nodes, _ := makeCluster(t, 2, streamCfg)

	// Publish to node-0
	for i := uint64(1); i <= 20; i++ {
		msg := &Message{
			Subject:    "test.data",
			Payload:    []byte(fmt.Sprintf("msg-%d", i)),
			ProducerTS: i * 1_000_000_000,
		}
		nodes[0].Publish(msg)
	}

	// Wait for replication to node-1
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if nodes[1].GetStream("test").MessageCount() >= 20 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if nodes[1].GetStream("test").MessageCount() < 20 {
		t.Fatalf("replication incomplete: node-1 has %d messages",
			nodes[1].GetStream("test").MessageCount())
	}

	// Create consumer on node-1 that reads the replicated data.
	// WindowDuration must be small relative to message timestamps so
	// watermark (= minSourceTS - windowDuration) advances past the windows.
	consumerCfg := ConsumerConfig{
		Name:             "multi-node-consumer",
		Stream:           "test",
		Ordering:         "producer_ts",
		WindowDuration:   2 * time.Second,
		WatermarkTimeout: 500 * time.Millisecond,
		DedupKey:         []string{"msg_id"},
		LatePolicy:       LatePolicyDrop,
		DeliverPolicy:    DeliverAll,
	}

	consumer, err := nodes[1].CreateConsumer(consumerCfg)
	if err != nil {
		t.Fatal(err)
	}

	// The readLoop (100ms tick) collects messages from local WAL and
	// advances the watermark based on source progress.  With messages
	// at timestamps 1-20s and WindowDuration=2s, watermark reaches 18s
	// after the first collection cycle, emitting most windows.
	var totalMsgs int
	timeout := time.After(5 * time.Second)
	for {
		select {
		case batch := <-consumer.Output():
			for i := 1; i < len(batch.Messages); i++ {
				if batch.Messages[i].ProducerTS < batch.Messages[i-1].ProducerTS {
					t.Errorf("ordering violation: %d < %d",
						batch.Messages[i].ProducerTS, batch.Messages[i-1].ProducerTS)
				}
			}
			totalMsgs += len(batch.Messages)
		case <-timeout:
			goto done
		}
	}
done:
	if totalMsgs == 0 {
		t.Fatal("consumer on node-1 received no messages")
	}
	t.Logf("consumer on node-1 received %d/%d messages from replicated data", totalMsgs, 20)
}

// TC-RJ: A node stops, more data is published, node restarts and catches up.
func TestIntegrationNodeRejoin(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Set up 3 nodes manually to control lifecycle
	peerAddrs := make([]string, 3)
	for i := range peerAddrs {
		peerAddrs[i] = freePort(t)
	}

	dirs := make([]string, 3)
	nodes := make([]*Node, 3)
	for i := range nodes {
		dirs[i] = tempDir(t)
		cfg := NodeConfig{
			ID:       fmt.Sprintf("rejoin-%d", i),
			DataDir:  dirs[i],
			BindAddr: "127.0.0.1:0",
			PeerAddr: peerAddrs[i],
			Seeds:    peerAddrs,
		}
		node, err := NewNode(cfg)
		if err != nil {
			t.Fatal(err)
		}

		streamCfg := DefaultStreamConfig("test", []string{"test.>"})
		streamCfg.FsyncPolicy = FsyncNone
		node.CreateStream(streamCfg)
		node.Start()
		nodes[i] = node
	}
	defer func() {
		for _, n := range nodes {
			if n != nil {
				n.Stop(context.Background())
			}
		}
	}()

	// Wait for gossip
	time.Sleep(3 * time.Second)

	// Publish 50 messages to node-0
	for i := uint64(0); i < 50; i++ {
		msg := &Message{
			Subject:    "test.data",
			Payload:    []byte(fmt.Sprintf("phase1-%d", i)),
			ProducerTS: i * 1_000_000,
		}
		nodes[0].Publish(msg)
	}

	// Wait for replication to node-2
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if nodes[2].GetStream("test").MessageCount() >= 50 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	countBefore := nodes[2].GetStream("test").MessageCount()
	t.Logf("node-2 has %d messages before stop", countBefore)

	// Stop node-2
	nodes[2].Stop(context.Background())
	nodes[2] = nil

	// Publish 50 more messages while node-2 is down
	for i := uint64(50); i < 100; i++ {
		msg := &Message{
			Subject:    "test.data",
			Payload:    []byte(fmt.Sprintf("phase2-%d", i)),
			ProducerTS: i * 1_000_000,
		}
		nodes[0].Publish(msg)
	}

	// Wait for replication to node-1
	deadline = time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if nodes[1].GetStream("test").MessageCount() >= 100 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Restart node-2 with the same data dir and peer addr
	cfg2 := NodeConfig{
		ID:       "rejoin-2",
		DataDir:  dirs[2],
		BindAddr: "127.0.0.1:0",
		PeerAddr: peerAddrs[2],
		Seeds:    peerAddrs,
	}
	node2, err := NewNode(cfg2)
	if err != nil {
		t.Fatal(err)
	}

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	streamCfg.FsyncPolicy = FsyncNone
	node2.CreateStream(streamCfg)
	node2.Start()
	nodes[2] = node2

	// Wait for node-2 to catch up via anti-entropy
	deadline = time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if nodes[2].GetStream("test").MessageCount() >= 100 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	count := nodes[2].GetStream("test").MessageCount()
	if count < 100 {
		t.Fatalf("node-2 did not catch up: expected >= 100, got %d", count)
	}
	t.Logf("node-2 caught up to %d messages after rejoin", count)
}

// TC-WAL: Node restarts from persisted WAL and continues serving.
func TestIntegrationWALRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	dir := tempDir(t)

	// Phase 1: create node, publish, stop
	node1, err := NewNode(NodeConfig{
		ID:       "wal-recovery",
		DataDir:  dir,
		BindAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	streamCfg.FsyncPolicy = FsyncEvery
	node1.CreateStream(streamCfg)

	for i := uint64(0); i < 100; i++ {
		msg := &Message{
			Subject:    "test.data",
			Payload:    []byte(fmt.Sprintf("persisted-%d", i)),
			ProducerTS: i * 1_000_000,
		}
		if _, err := node1.Publish(msg); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	if node1.GetStream("test").MessageCount() != 100 {
		t.Fatalf("expected 100 messages before stop, got %d",
			node1.GetStream("test").MessageCount())
	}

	node1.Stop(context.Background())

	// Phase 2: new node, same data dir, recover from WAL
	node2, err := NewNode(NodeConfig{
		ID:       "wal-recovery",
		DataDir:  dir,
		BindAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node2.Stop(context.Background())

	// Re-create the stream (loads existing WAL from disk)
	node2.CreateStream(streamCfg)

	recovered := node2.GetStream("test").MessageCount()
	if recovered != 100 {
		t.Fatalf("expected 100 recovered messages, got %d", recovered)
	}

	// Verify we can read the recovered data
	stream := node2.GetStream("test")
	msgs, err := stream.ReadFrom(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 100 {
		t.Fatalf("expected 100 messages from ReadFrom, got %d", len(msgs))
	}

	// Verify we can continue publishing
	for i := uint64(100); i < 110; i++ {
		msg := &Message{
			Subject:    "test.data",
			Payload:    []byte(fmt.Sprintf("new-%d", i)),
			ProducerTS: i * 1_000_000,
		}
		if _, err := node2.Publish(msg); err != nil {
			t.Fatalf("publish after recovery %d: %v", i, err)
		}
	}

	if node2.GetStream("test").MessageCount() != 110 {
		t.Fatalf("expected 110 total messages, got %d",
			node2.GetStream("test").MessageCount())
	}

	t.Logf("recovered %d messages, published 10 more = %d total",
		recovered, node2.GetStream("test").MessageCount())
}

// TC-SD: Graceful shutdown force-closes connections that don't drain in time.
func TestIntegrationGracefulShutdownDrain(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	node, _ := makeTestNode(t, "node-drain")

	streamCfg := DefaultStreamConfig("test", []string{"test.>"})
	streamCfg.FsyncPolicy = FsyncNone
	node.CreateStream(streamCfg)

	server := NewServer("127.0.0.1:0", node)
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}

	// Open connections that will be idle (blocked on read with 30s deadline)
	conns := make([]net.Conn, 3)
	for i := range conns {
		conn, err := net.DialTimeout("tcp", server.Addr(), 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		conns[i] = conn

		// Send one PUB to confirm connection is working
		cmd := fmt.Sprintf("PUB test.data key %d 3\r\nfoo\r\n", (i+1)*1_000_000)
		conn.Write([]byte(cmd))
		buf := make([]byte, 64)
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, _ := conn.Read(buf)
		if !strings.HasPrefix(string(buf[:n]), "+OK") {
			t.Fatalf("conn %d: expected +OK, got %q", i, string(buf[:n]))
		}
	}

	// Stop with a short timeout — handlers are blocked on 30s read deadline,
	// so they won't drain in time. Stop must force-close them.
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	server.Stop(ctx)
	elapsed := time.Since(start)

	// Stop should return in ~500ms (the timeout), not 30s (the read deadline)
	if elapsed > 3*time.Second {
		t.Fatalf("Stop took %v, expected ~500ms (force-close timeout)", elapsed)
	}
	t.Logf("server.Stop completed in %v with 3 idle connections", elapsed)

	// All connections should be closed
	for i, conn := range conns {
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		_, err := conn.Read(make([]byte, 1))
		if err == nil {
			t.Errorf("conn %d: expected closed connection, but read succeeded", i)
		}
		conn.Close()
	}
}
