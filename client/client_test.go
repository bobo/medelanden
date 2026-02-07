package client

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// mockServer simulates the Medelanden wire protocol for testing.
type mockServer struct {
	listener net.Listener
	handler  func(conn net.Conn)
	done     chan struct{}
}

func newMockServer(t *testing.T, handler func(conn net.Conn)) *mockServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &mockServer{listener: ln, handler: handler, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handler(conn)
		}
	}()
	return s
}

func (s *mockServer) addr() string {
	return s.listener.Addr().String()
}

func (s *mockServer) close() {
	s.listener.Close()
	<-s.done
}

func TestDial(t *testing.T) {
	srv := newMockServer(t, func(conn net.Conn) {
		defer conn.Close()
		// Just accept and hold the connection
		buf := make([]byte, 1)
		conn.Read(buf)
	})
	defer srv.close()

	c, err := Dial(srv.addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()
}

func TestDialError(t *testing.T) {
	_, err := Dial("127.0.0.1:1") // port 1 should be unreachable
	if err == nil {
		t.Fatal("expected error from Dial to unreachable address")
	}
}

func TestPublishPUB(t *testing.T) {
	var received struct {
		mu      sync.Mutex
		subject string
		dedup   string
		ts      uint64
		payload string
	}

	srv := newMockServer(t, func(conn net.Conn) {
		defer conn.Close()
		reader := bufio.NewReader(conn)

		line, _ := reader.ReadString('\n')
		line = strings.TrimRight(line, "\r\n")
		parts := strings.Fields(line)
		// PUB <subject> <dedup_key> <producer_ts> <size>
		if len(parts) >= 5 && parts[0] == "PUB" {
			received.mu.Lock()
			received.subject = parts[1]
			received.dedup = parts[2]
			received.ts, _ = strconv.ParseUint(parts[3], 10, 64)
			size, _ := strconv.Atoi(parts[4])
			payload := make([]byte, size)
			io.ReadFull(reader, payload)
			received.payload = string(payload)
			reader.ReadString('\n') // trailing \r\n
			received.mu.Unlock()

			conn.Write([]byte("+OK 42\r\n"))
		}
	})
	defer srv.close()

	c, err := Dial(srv.addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	ts := uint64(1700000000000000000)
	seq, err := c.Publish("aircraft.N123AB", []byte(`{"alt":35000}`),
		WithDedupKey("reading-1"),
		WithProducerTS(ts))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if seq != 42 {
		t.Errorf("seq = %d, want 42", seq)
	}

	received.mu.Lock()
	defer received.mu.Unlock()

	if received.subject != "aircraft.N123AB" {
		t.Errorf("subject = %q, want %q", received.subject, "aircraft.N123AB")
	}
	if received.dedup != "reading-1" {
		t.Errorf("dedup = %q, want %q", received.dedup, "reading-1")
	}
	if received.ts != ts {
		t.Errorf("ts = %d, want %d", received.ts, ts)
	}
	if received.payload != `{"alt":35000}` {
		t.Errorf("payload = %q, want %q", received.payload, `{"alt":35000}`)
	}
}

func TestPublishMPUB(t *testing.T) {
	var gotMsgID string

	srv := newMockServer(t, func(conn net.Conn) {
		defer conn.Close()
		reader := bufio.NewReader(conn)

		line, _ := reader.ReadString('\n')
		line = strings.TrimRight(line, "\r\n")
		parts := strings.Fields(line)
		// MPUB <subject> <msg_id> <dedup_key> <producer_ts> <size>
		if len(parts) >= 6 && parts[0] == "MPUB" {
			gotMsgID = parts[2]
			size, _ := strconv.Atoi(parts[5])
			payload := make([]byte, size)
			io.ReadFull(reader, payload)
			reader.ReadString('\n')
			conn.Write([]byte("+OK 7\r\n"))
		}
	})
	defer srv.close()

	c, err := Dial(srv.addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	seq, err := c.Publish("sensors.temp", []byte("22.5"),
		WithMsgID("custom-id-99"),
		WithProducerTS(1700000000000000000))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if seq != 7 {
		t.Errorf("seq = %d, want 7", seq)
	}
	if gotMsgID != "custom-id-99" {
		t.Errorf("msg_id = %q, want %q", gotMsgID, "custom-id-99")
	}
}

func TestPublishServerError(t *testing.T) {
	srv := newMockServer(t, func(conn net.Conn) {
		defer conn.Close()
		reader := bufio.NewReader(conn)
		line, _ := reader.ReadString('\n')
		parts := strings.Fields(strings.TrimRight(line, "\r\n"))
		if len(parts) >= 5 {
			size, _ := strconv.Atoi(parts[4])
			payload := make([]byte, size)
			io.ReadFull(reader, payload)
			reader.ReadString('\n')
		}
		conn.Write([]byte("-ERR no stream matches subject\r\n"))
	})
	defer srv.close()

	c, err := Dial(srv.addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	_, err = c.Publish("unknown.subject", []byte("data"))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "no stream matches subject") {
		t.Errorf("error = %q, want to contain 'no stream matches subject'", err)
	}
}

func TestPing(t *testing.T) {
	srv := newMockServer(t, func(conn net.Conn) {
		defer conn.Close()
		reader := bufio.NewReader(conn)

		line, _ := reader.ReadString('\n')
		line = strings.TrimRight(line, "\r\n")
		if line == "PING" {
			conn.Write([]byte("PONG\r\n"))
		}
	})
	defer srv.close()

	c, err := Dial(srv.addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	if err := c.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestInfo(t *testing.T) {
	srv := newMockServer(t, func(conn net.Conn) {
		defer conn.Close()
		reader := bufio.NewReader(conn)

		line, _ := reader.ReadString('\n')
		line = strings.TrimRight(line, "\r\n")
		if line == "INFO" {
			info := `{"status":"ok","node_id":"node-1","peers":{},"streams":{"aircraft":{"local_messages":100,"replication_status":"ok"}}}`
			conn.Write([]byte(fmt.Sprintf("+INFO %s\r\n", info)))
		}
	})
	defer srv.close()

	c, err := Dial(srv.addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	info, err := c.Info()
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.NodeID != "node-1" {
		t.Errorf("NodeID = %q, want %q", info.NodeID, "node-1")
	}
	if info.Status != "ok" {
		t.Errorf("Status = %q, want %q", info.Status, "ok")
	}
	if s, ok := info.Streams["aircraft"]; !ok || s.LocalMessages != 100 {
		t.Errorf("Streams[aircraft] = %+v, want LocalMessages=100", info.Streams["aircraft"])
	}
}

func TestSubscribeSimple(t *testing.T) {
	srv := newMockServer(t, func(conn net.Conn) {
		defer conn.Close()
		reader := bufio.NewReader(conn)

		line, _ := reader.ReadString('\n')
		line = strings.TrimRight(line, "\r\n")
		parts := strings.Fields(line)
		if parts[0] != "SUB" {
			return
		}

		conn.Write([]byte("+OK\r\n"))

		// Deliver 3 messages
		for i := 0; i < 3; i++ {
			payload := fmt.Sprintf("msg-%d", i)
			ts := uint64(1700000000000000000 + i)
			msgID := fmt.Sprintf("id-%d", i)
			frame := fmt.Sprintf("MSG aircraft.N123 my-consumer %d %s %d\r\n%s\r\n",
				ts, msgID, len(payload), payload)
			conn.Write([]byte(frame))
		}

		// Hold connection open until client disconnects
		buf := make([]byte, 1)
		conn.Read(buf)
	})
	defer srv.close()

	c, err := Dial(srv.addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	sub, err := c.Subscribe("aircraft.>", "my-consumer")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	for i := 0; i < 3; i++ {
		select {
		case msg := <-sub.Messages():
			wantPayload := fmt.Sprintf("msg-%d", i)
			if string(msg.Payload) != wantPayload {
				t.Errorf("msg[%d].Payload = %q, want %q", i, msg.Payload, wantPayload)
			}
			if msg.Subject != "aircraft.N123" {
				t.Errorf("msg[%d].Subject = %q, want %q", i, msg.Subject, "aircraft.N123")
			}
			if msg.Consumer != "my-consumer" {
				t.Errorf("msg[%d].Consumer = %q, want %q", i, msg.Consumer, "my-consumer")
			}
			wantID := fmt.Sprintf("id-%d", i)
			if msg.MsgID != wantID {
				t.Errorf("msg[%d].MsgID = %q, want %q", i, msg.MsgID, wantID)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for msg[%d]", i)
		}
	}

	sub.Unsubscribe()
}

func TestSubscribeWindowed(t *testing.T) {
	srv := newMockServer(t, func(conn net.Conn) {
		defer conn.Close()
		reader := bufio.NewReader(conn)

		line, _ := reader.ReadString('\n')
		line = strings.TrimRight(line, "\r\n")
		parts := strings.Fields(line)
		if parts[0] != "MSUB" {
			return
		}

		// Verify window params were sent
		if parts[3] != "2s" || parts[4] != "5s" {
			conn.Write([]byte("-ERR bad params\r\n"))
			return
		}

		conn.Write([]byte("+OK\r\n"))

		// Send one window batch with 2 messages
		conn.Write([]byte("WBATCH my-consumer 1700000000000000000 1700000002000000000 2\r\n"))

		p1 := "payload-1"
		conn.Write([]byte(fmt.Sprintf("MSG aircraft.N1 1700000000500000000 id-1 %d\r\n%s\r\n", len(p1), p1)))

		p2 := "payload-2"
		conn.Write([]byte(fmt.Sprintf("MSG aircraft.N2 1700000001000000000 id-2 %d\r\n%s\r\n", len(p2), p2)))

		conn.Write([]byte("WEND\r\n"))

		// Hold connection open
		buf := make([]byte, 1)
		conn.Read(buf)
	})
	defer srv.close()

	c, err := Dial(srv.addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	sub, err := c.SubscribeWindowed("aircraft.>", "my-consumer", 2*time.Second, 5*time.Second)
	if err != nil {
		t.Fatalf("SubscribeWindowed: %v", err)
	}

	select {
	case batch := <-sub.Batches():
		if batch.Consumer != "my-consumer" {
			t.Errorf("batch.Consumer = %q, want %q", batch.Consumer, "my-consumer")
		}
		if batch.WindowStart != 1700000000000000000 {
			t.Errorf("batch.WindowStart = %d, want 1700000000000000000", batch.WindowStart)
		}
		if batch.WindowEnd != 1700000002000000000 {
			t.Errorf("batch.WindowEnd = %d, want 1700000002000000000", batch.WindowEnd)
		}
		if len(batch.Messages) != 2 {
			t.Fatalf("len(batch.Messages) = %d, want 2", len(batch.Messages))
		}
		if string(batch.Messages[0].Payload) != "payload-1" {
			t.Errorf("batch.Messages[0].Payload = %q, want %q", batch.Messages[0].Payload, "payload-1")
		}
		if batch.Messages[0].MsgID != "id-1" {
			t.Errorf("batch.Messages[0].MsgID = %q, want %q", batch.Messages[0].MsgID, "id-1")
		}
		if string(batch.Messages[1].Payload) != "payload-2" {
			t.Errorf("batch.Messages[1].Payload = %q, want %q", batch.Messages[1].Payload, "payload-2")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for window batch")
	}

	sub.Unsubscribe()
}

func TestAck(t *testing.T) {
	srv := newMockServer(t, func(conn net.Conn) {
		defer conn.Close()
		reader := bufio.NewReader(conn)

		line, _ := reader.ReadString('\n')
		line = strings.TrimRight(line, "\r\n")
		parts := strings.Fields(line)
		if parts[0] == "ACK" && parts[1] == "my-consumer" && parts[2] == "msg-42" {
			conn.Write([]byte("+OK\r\n"))
		} else {
			conn.Write([]byte("-ERR unexpected\r\n"))
		}
	})
	defer srv.close()

	c, err := Dial(srv.addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	if err := c.Ack("my-consumer", "msg-42"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

func TestAckWindow(t *testing.T) {
	srv := newMockServer(t, func(conn net.Conn) {
		defer conn.Close()
		reader := bufio.NewReader(conn)

		line, _ := reader.ReadString('\n')
		line = strings.TrimRight(line, "\r\n")
		parts := strings.Fields(line)
		if parts[0] == "ACKW" && parts[1] == "my-consumer" && parts[2] == "1700000002000000000" {
			conn.Write([]byte("+OK\r\n"))
		} else {
			conn.Write([]byte("-ERR unexpected\r\n"))
		}
	})
	defer srv.close()

	c, err := Dial(srv.addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	if err := c.AckWindow("my-consumer", 1700000002000000000); err != nil {
		t.Fatalf("AckWindow: %v", err)
	}
}

func TestResume(t *testing.T) {
	srv := newMockServer(t, func(conn net.Conn) {
		defer conn.Close()
		reader := bufio.NewReader(conn)

		line, _ := reader.ReadString('\n')
		line = strings.TrimRight(line, "\r\n")
		parts := strings.Fields(line)
		if parts[0] == "RESUME" && parts[1] == "my-consumer" {
			posJSON := `{"consumer":"my-consumer","positions":{"node-1":{"node_id":"node-1","last_seq":100,"last_ts":1700000000000000000}},"watermark":1699999999000000000}`
			conn.Write([]byte(fmt.Sprintf("+POSITIONS %s\r\n", posJSON)))
		} else {
			conn.Write([]byte("-ERR consumer not found\r\n"))
		}
	})
	defer srv.close()

	c, err := Dial(srv.addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	pos, err := c.Resume("my-consumer")
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if pos.Consumer != "my-consumer" {
		t.Errorf("Consumer = %q, want %q", pos.Consumer, "my-consumer")
	}
	if pos.Watermark != 1699999999000000000 {
		t.Errorf("Watermark = %d, want 1699999999000000000", pos.Watermark)
	}
	src, ok := pos.Positions["node-1"]
	if !ok {
		t.Fatal("missing position for node-1")
	}
	if src.LastSeq != 100 {
		t.Errorf("LastSeq = %d, want 100", src.LastSeq)
	}
}

func TestResumeNotFound(t *testing.T) {
	srv := newMockServer(t, func(conn net.Conn) {
		defer conn.Close()
		reader := bufio.NewReader(conn)
		reader.ReadString('\n')
		conn.Write([]byte("-ERR consumer not found\r\n"))
	})
	defer srv.close()

	c, err := Dial(srv.addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	_, err = c.Resume("nonexistent")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "consumer not found") {
		t.Errorf("error = %q, want to contain 'consumer not found'", err)
	}
}

func TestCloseIdempotent(t *testing.T) {
	srv := newMockServer(t, func(conn net.Conn) {
		defer conn.Close()
		buf := make([]byte, 1)
		conn.Read(buf)
	})
	defer srv.close()

	c, err := Dial(srv.addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestPublishAfterClose(t *testing.T) {
	srv := newMockServer(t, func(conn net.Conn) {
		defer conn.Close()
		buf := make([]byte, 1)
		conn.Read(buf)
	})
	defer srv.close()

	c, err := Dial(srv.addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	c.Close()

	_, err = c.Publish("test", []byte("data"))
	if err == nil {
		t.Fatal("expected error after Close")
	}
}

func TestDefaultDedupKey(t *testing.T) {
	var gotDedup string

	srv := newMockServer(t, func(conn net.Conn) {
		defer conn.Close()
		reader := bufio.NewReader(conn)

		line, _ := reader.ReadString('\n')
		parts := strings.Fields(strings.TrimRight(line, "\r\n"))
		if len(parts) >= 5 && parts[0] == "PUB" {
			gotDedup = parts[2]
			size, _ := strconv.Atoi(parts[4])
			payload := make([]byte, size)
			io.ReadFull(reader, payload)
			reader.ReadString('\n')
			conn.Write([]byte("+OK 1\r\n"))
		}
	})
	defer srv.close()

	c, err := Dial(srv.addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	// Publish with no DedupKey option — should use default "-"
	_, err = c.Publish("test.subject", []byte("hello"))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if gotDedup != "-" {
		t.Errorf("dedup = %q, want %q", gotDedup, "-")
	}
}

func TestMultiplePublishes(t *testing.T) {
	var mu sync.Mutex
	var seqs []uint64

	srv := newMockServer(t, func(conn net.Conn) {
		defer conn.Close()
		reader := bufio.NewReader(conn)

		for i := uint64(1); i <= 5; i++ {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			parts := strings.Fields(strings.TrimRight(line, "\r\n"))
			if len(parts) >= 5 && parts[0] == "PUB" {
				size, _ := strconv.Atoi(parts[4])
				payload := make([]byte, size)
				io.ReadFull(reader, payload)
				reader.ReadString('\n')
				conn.Write([]byte(fmt.Sprintf("+OK %d\r\n", i)))
				mu.Lock()
				seqs = append(seqs, i)
				mu.Unlock()
			}
		}
	})
	defer srv.close()

	c, err := Dial(srv.addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	for i := uint64(1); i <= 5; i++ {
		seq, err := c.Publish("test", []byte(fmt.Sprintf("msg-%d", i)))
		if err != nil {
			t.Fatalf("Publish[%d]: %v", i, err)
		}
		if seq != i {
			t.Errorf("Publish[%d] seq = %d, want %d", i, seq, i)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seqs) != 5 {
		t.Errorf("server received %d publishes, want 5", len(seqs))
	}
}
