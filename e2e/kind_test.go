//go:build e2e

package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	kindContext = flag.String("kind-context", "kind-medelanden-test", "kubectl context for the Kind cluster")
	namespace   = flag.String("namespace", "medelanden", "Kubernetes namespace")
	replicas    = flag.Int("replicas", 3, "Number of StatefulSet replicas")
)

// portForward starts kubectl port-forward to a specific pod and returns the
// local port and a cancel function. It retries until the port is reachable.
func portForward(t *testing.T, pod string, remotePort int) (int, context.CancelFunc) {
	t.Helper()

	// Find a free local port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	localPort := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	ctx, cancel := context.WithCancel(context.Background())

	cmd := exec.CommandContext(ctx, "kubectl",
		"--context", *kindContext,
		"-n", *namespace,
		"port-forward",
		fmt.Sprintf("pod/%s", pod),
		fmt.Sprintf("%d:%d", localPort, remotePort),
	)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard

	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start port-forward to %s: %v", pod, err)
	}

	// Wait for port to become reachable
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", localPort), 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return localPort, cancel
		}
		time.Sleep(200 * time.Millisecond)
	}

	cancel()
	t.Fatalf("port-forward to %s:%d did not become reachable", pod, remotePort)
	return 0, nil
}

// kubectl runs a kubectl command and returns stdout.
func kubectl(t *testing.T, args ...string) string {
	t.Helper()
	fullArgs := append([]string{"--context", *kindContext, "-n", *namespace}, args...)
	cmd := exec.Command("kubectl", fullArgs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl %v failed: %v\n%s", args, err, string(out))
	}
	return string(out)
}

// nodeInfo is the JSON structure returned by /healthz.
type nodeInfo struct {
	Status  string                    `json:"status"`
	NodeID  string                    `json:"node_id"`
	Peers   map[string]peerStatus     `json:"peers"`
	Streams map[string]streamInfo     `json:"streams"`
}

type peerStatus struct {
	Status string `json:"status"`
	LagMs  int64  `json:"lag_ms"`
}

type streamInfo struct {
	LocalMessages     uint64 `json:"local_messages"`
	ReplicationStatus string `json:"replication_status"`
}

// nodeMetrics is the JSON structure returned by /metrics.
type nodeMetrics struct {
	WritesTotal   uint64            `json:"writes_total"`
	AvgWriteLatUs int64             `json:"avg_write_latency_us"`
	Streams       map[string]uint64 `json:"stream_message_counts"`
}

// podName returns the StatefulSet pod name for a given index.
func podName(index int) string {
	return fmt.Sprintf("medelanden-%d", index)
}

// TestKindPodsReady verifies all StatefulSet pods are running and ready.
func TestKindPodsReady(t *testing.T) {
	out := kubectl(t, "get", "pods", "-l", "app=medelanden",
		"-o", "jsonpath={range .items[*]}{.metadata.name} {.status.phase}{'\\n'}{end}")

	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) < *replicas {
		t.Fatalf("expected %d pods, got %d:\n%s", *replicas, len(lines), out)
	}

	for _, line := range lines {
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		if parts[1] != "Running" {
			t.Errorf("pod %s is %s, expected Running", parts[0], parts[1])
		}
	}
}

// TestKindHealthEndpoints verifies /healthz returns valid JSON for each pod.
func TestKindHealthEndpoints(t *testing.T) {
	for i := 0; i < *replicas; i++ {
		i := i
		t.Run(podName(i), func(t *testing.T) {
			t.Parallel()
			port, cancel := portForward(t, podName(i), 8080)
			defer cancel()

			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", port))
			if err != nil {
				t.Fatalf("GET /healthz: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 {
				t.Fatalf("expected 200, got %d", resp.StatusCode)
			}

			var info nodeInfo
			if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
				t.Fatalf("decode health: %v", err)
			}

			if info.Status != "ok" {
				t.Errorf("node %s status = %q, want ok", podName(i), info.Status)
			}

			if info.NodeID == "" {
				t.Error("node_id is empty")
			}

			// Verify the "test" stream exists
			if _, ok := info.Streams["test"]; !ok {
				t.Error("stream 'test' not found in health response")
			}
		})
	}
}

// TestKindPeerDiscovery verifies all nodes have discovered each other via gossip.
func TestKindPeerDiscovery(t *testing.T) {
	// Give gossip time to fully converge
	time.Sleep(3 * time.Second)

	port, cancel := portForward(t, podName(0), 8080)
	defer cancel()

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", port))
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()

	var info nodeInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// Node-0 should see at least replicas-1 peers
	alivePeers := 0
	for _, p := range info.Peers {
		if p.Status == "alive" {
			alivePeers++
		}
	}

	if alivePeers < *replicas-1 {
		t.Errorf("node-0 sees %d alive peers, expected at least %d; peers: %+v",
			alivePeers, *replicas-1, info.Peers)
	}
}

// TestKindPublish publishes messages to a pod via the TCP wire protocol and
// verifies they are accepted.
func TestKindPublish(t *testing.T) {
	port, cancel := portForward(t, podName(0), 4222)
	defer cancel()

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	msgCount := 50

	for i := 0; i < msgCount; i++ {
		payload := fmt.Sprintf("kind-msg-%d", i)
		ts := uint64(i+1) * 1_000_000_000
		cmd := fmt.Sprintf("PUB test.data key-%d %d %d\r\n%s\r\n",
			i, ts, len(payload), payload)

		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write([]byte(cmd)); err != nil {
			t.Fatalf("write msg %d: %v", i, err)
		}

		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read response for msg %d: %v", i, err)
		}

		if !strings.HasPrefix(line, "+OK") {
			t.Fatalf("msg %d: expected +OK, got %q", i, strings.TrimSpace(line))
		}
	}

	// Verify via /metrics that the messages were stored
	httpPort, httpCancel := portForward(t, podName(0), 8080)
	defer httpCancel()

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", httpPort))
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()

	var metrics nodeMetrics
	if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
		t.Fatalf("decode metrics: %v", err)
	}

	if count, ok := metrics.Streams["test"]; !ok || count < uint64(msgCount) {
		t.Errorf("expected at least %d messages in stream 'test', got %d", msgCount, count)
	}
}

// TestKindReplication publishes to one pod and verifies messages replicate to others.
func TestKindReplication(t *testing.T) {
	// Publish messages to pod-0
	port, cancel := portForward(t, podName(0), 4222)
	defer cancel()

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	msgCount := 30

	for i := 0; i < msgCount; i++ {
		payload := fmt.Sprintf("repl-msg-%d", i)
		ts := uint64(i+1) * 2_000_000_000 // use different timestamps to avoid dedup with other tests
		cmd := fmt.Sprintf("PUB test.repl rkey-%d %d %d\r\n%s\r\n",
			i, ts, len(payload), payload)

		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write([]byte(cmd)); err != nil {
			t.Fatalf("write: %v", err)
		}

		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if !strings.HasPrefix(line, "+OK") {
			t.Fatalf("expected +OK, got %q", strings.TrimSpace(line))
		}
	}

	// Wait for replication to propagate
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		allReplicated := true

		for i := 1; i < *replicas; i++ {
			httpPort, httpCancel := portForward(t, podName(i), 8080)
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", httpPort))
			httpCancel()
			if err != nil {
				allReplicated = false
				break
			}

			var metrics nodeMetrics
			json.NewDecoder(resp.Body).Decode(&metrics)
			resp.Body.Close()

			count := metrics.Streams["test"]
			if count < uint64(msgCount) {
				allReplicated = false
				break
			}
		}

		if allReplicated {
			return
		}
		time.Sleep(2 * time.Second)
	}

	// Report final state
	for i := 0; i < *replicas; i++ {
		httpPort, httpCancel := portForward(t, podName(i), 8080)
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", httpPort))
		httpCancel()
		if err != nil {
			t.Logf("pod-%d: error getting metrics: %v", i, err)
			continue
		}
		var metrics nodeMetrics
		json.NewDecoder(resp.Body).Decode(&metrics)
		resp.Body.Close()
		t.Logf("pod-%d: stream 'test' has %d messages", i, metrics.Streams["test"])
	}
	t.Fatal("replication did not complete within timeout")
}

// TestKindConcurrentWrites publishes concurrently to different pods and verifies
// all messages are accepted (AP guarantee: never reject writes).
func TestKindConcurrentWrites(t *testing.T) {
	var wg sync.WaitGroup
	errors := make(chan error, *replicas)

	msgsPerPod := 20

	for i := 0; i < *replicas; i++ {
		wg.Add(1)
		go func(podIdx int) {
			defer wg.Done()

			port, cancel := portForward(t, podName(podIdx), 4222)
			defer cancel()

			conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)
			if err != nil {
				errors <- fmt.Errorf("pod-%d dial: %v", podIdx, err)
				return
			}
			defer conn.Close()

			reader := bufio.NewReader(conn)

			for j := 0; j < msgsPerPod; j++ {
				payload := fmt.Sprintf("concurrent-pod%d-msg%d", podIdx, j)
				ts := uint64((podIdx*1000)+j+1) * 3_000_000_000
				cmd := fmt.Sprintf("PUB test.concurrent ckey-%d-%d %d %d\r\n%s\r\n",
					podIdx, j, ts, len(payload), payload)

				conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if _, err := conn.Write([]byte(cmd)); err != nil {
					errors <- fmt.Errorf("pod-%d write %d: %v", podIdx, j, err)
					return
				}

				conn.SetReadDeadline(time.Now().Add(5 * time.Second))
				line, err := reader.ReadString('\n')
				if err != nil {
					errors <- fmt.Errorf("pod-%d read %d: %v", podIdx, j, err)
					return
				}
				if !strings.HasPrefix(line, "+OK") {
					errors <- fmt.Errorf("pod-%d msg %d: expected +OK, got %q", podIdx, j, strings.TrimSpace(line))
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

// TestKindPodRestart deletes a pod and verifies the cluster continues to serve
// writes, and that the restarted pod rejoins.
func TestKindPodRestart(t *testing.T) {
	// Delete pod-2
	kubectl(t, "delete", "pod", podName(2), "--grace-period=0", "--force")

	// Verify pod-0 still accepts writes
	port, cancel := portForward(t, podName(0), 4222)
	defer cancel()

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)
	if err != nil {
		t.Fatalf("dial pod-0 after deleting pod-2: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	payload := "after-restart"
	ts := uint64(999) * 4_000_000_000
	cmd := fmt.Sprintf("PUB test.restart rstart %d %d\r\n%s\r\n",
		ts, len(payload), payload)

	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte(cmd)); err != nil {
		t.Fatalf("write after pod delete: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read after pod delete: %v", err)
	}
	if !strings.HasPrefix(line, "+OK") {
		t.Fatalf("expected +OK after pod delete, got %q", strings.TrimSpace(line))
	}

	// Wait for pod-2 to come back
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		out := kubectl(t, "get", "pod", podName(2),
			"-o", "jsonpath={.status.phase}")
		if strings.TrimSpace(out) == "Running" {
			// Check readiness
			ready := kubectl(t, "get", "pod", podName(2),
				"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
			if strings.TrimSpace(ready) == "True" {
				return // pod is back and ready
			}
		}
		time.Sleep(3 * time.Second)
	}

	t.Fatal("pod-2 did not rejoin the cluster within timeout")
}

// TestKindInfoEndpoint verifies the INFO command works via TCP.
func TestKindInfoEndpoint(t *testing.T) {
	port, cancel := portForward(t, podName(0), 4222)
	defer cancel()

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte("INFO\r\n")); err != nil {
		t.Fatalf("write INFO: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read INFO response: %v", err)
	}

	if !strings.HasPrefix(line, "+INFO") {
		t.Fatalf("expected +INFO, got %q", strings.TrimSpace(line))
	}

	// Extract the JSON part
	jsonStr := strings.TrimPrefix(strings.TrimSpace(line), "+INFO ")
	var info nodeInfo
	if err := json.Unmarshal([]byte(jsonStr), &info); err != nil {
		t.Fatalf("decode INFO JSON: %v (raw: %q)", err, jsonStr)
	}

	if info.NodeID == "" {
		t.Error("INFO returned empty node_id")
	}
}

// TestKindPingPong verifies the PING/PONG protocol works.
func TestKindPingPong(t *testing.T) {
	port, cancel := portForward(t, podName(0), 4222)
	defer cancel()

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte("PING\r\n")); err != nil {
		t.Fatalf("write PING: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read PONG: %v", err)
	}

	if strings.TrimSpace(line) != "PONG" {
		t.Fatalf("expected PONG, got %q", strings.TrimSpace(line))
	}
}

// TestKindMetricsEndpoint verifies /metrics returns valid JSON with expected fields.
func TestKindMetricsEndpoint(t *testing.T) {
	port, cancel := portForward(t, podName(0), 8080)
	defer cancel()

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", port))
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var metrics nodeMetrics
	if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
		t.Fatalf("decode metrics: %v", err)
	}

	if _, ok := metrics.Streams["test"]; !ok {
		t.Error("stream 'test' not found in metrics")
	}
}

// TestKindPublishSubscribe publishes messages to pod-0 and subscribes on pod-1,
// verifying the full distributed path: write → replication → consumer delivery.
func TestKindPublishSubscribe(t *testing.T) {
	// Publish to pod-0
	pubPort, pubCancel := portForward(t, podName(0), 4222)
	defer pubCancel()

	pubConn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", pubPort), 5*time.Second)
	if err != nil {
		t.Fatalf("dial pod-0: %v", err)
	}
	defer pubConn.Close()

	pubReader := bufio.NewReader(pubConn)
	msgCount := 10

	for i := 0; i < msgCount; i++ {
		payload := fmt.Sprintf("e2e-sub-%d", i)
		ts := uint64(i+1) * 6_000_000_000
		cmd := fmt.Sprintf("PUB test.sub skey-%d %d %d\r\n%s\r\n",
			i, ts, len(payload), payload)

		pubConn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := pubConn.Write([]byte(cmd)); err != nil {
			t.Fatalf("write msg %d: %v", i, err)
		}

		pubConn.SetReadDeadline(time.Now().Add(5 * time.Second))
		line, err := pubReader.ReadString('\n')
		if err != nil {
			t.Fatalf("read response %d: %v", i, err)
		}
		if !strings.HasPrefix(line, "+OK") {
			t.Fatalf("msg %d: expected +OK, got %q", i, strings.TrimSpace(line))
		}
	}

	// Wait for replication to pod-1
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		httpPort, httpCancel := portForward(t, podName(1), 8080)
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", httpPort))
		httpCancel()
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}

		var metrics nodeMetrics
		json.NewDecoder(resp.Body).Decode(&metrics)
		resp.Body.Close()

		if metrics.Streams["test"] >= uint64(msgCount) {
			break
		}
		time.Sleep(2 * time.Second)
	}

	// Subscribe on pod-1
	subPort, subCancel := portForward(t, podName(1), 4222)
	defer subCancel()

	subConn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", subPort), 5*time.Second)
	if err != nil {
		t.Fatalf("dial pod-1: %v", err)
	}
	defer subConn.Close()

	subReader := bufio.NewReader(subConn)

	subConn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := subConn.Write([]byte("SUB test.> e2e-consumer\r\n")); err != nil {
		t.Fatalf("write SUB: %v", err)
	}

	subConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := subReader.ReadString('\n')
	if err != nil {
		t.Fatalf("read SUB response: %v", err)
	}
	if !strings.HasPrefix(line, "+OK") {
		t.Fatalf("SUB: expected +OK, got %q", strings.TrimSpace(line))
	}

	// Read MSG frames
	var received int
	readDeadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(readDeadline) {
		subConn.SetReadDeadline(time.Now().Add(3 * time.Second))
		line, err := subReader.ReadString('\n')
		if err != nil {
			break
		}
		line = strings.TrimRight(line, "\r\n")

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
		payload := make([]byte, size+2)
		subConn.SetReadDeadline(time.Now().Add(3 * time.Second))
		io.ReadFull(subReader, payload)

		received++
	}

	if received == 0 {
		t.Fatal("no MSG frames received on pod-1 — cross-node subscribe failed")
	}
	t.Logf("received %d MSG frames on pod-1 via cross-node subscribe", received)
}

