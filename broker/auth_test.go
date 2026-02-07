package broker

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

func TestAuthNKeySuccess(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	pubB64 := base64.StdEncoding.EncodeToString(pub)

	dir := t.TempDir()
	node, err := NewNode(NodeConfig{
		ID:       "auth-test",
		DataDir:  dir,
		BindAddr: "127.0.0.1:0",
		Auth: AuthConfig{
			Enabled:        true,
			AuthorizedKeys: []string{pubB64},
			ConnectTimeout: 5 * time.Second,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Stop(context.Background())

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

	// Read server INFO with nonce
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "INFO ") {
		t.Fatalf("expected INFO, got %q", line)
	}

	var serverInfo struct {
		NodeID string `json:"node_id"`
		Nonce  string `json:"nonce"`
	}
	if err := json.Unmarshal([]byte(line[5:]), &serverInfo); err != nil {
		t.Fatal(err)
	}
	if serverInfo.NodeID != "auth-test" {
		t.Fatalf("expected node_id auth-test, got %q", serverInfo.NodeID)
	}

	nonce, err := base64.StdEncoding.DecodeString(serverInfo.Nonce)
	if err != nil {
		t.Fatal(err)
	}

	// Sign nonce with private key
	sig := ed25519.Sign(priv, nonce)
	sigB64 := base64.StdEncoding.EncodeToString(sig)

	// Send CONNECT
	fmt.Fprintf(writer, "CONNECT %s %s\r\n", pubB64, sigB64)
	writer.Flush()

	// Should get +OK
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err = reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "+OK") {
		t.Fatalf("expected +OK, got %q", line)
	}
}

func TestAuthNKeyWrongKey(t *testing.T) {
	// Server's authorized key
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	pubB64 := base64.StdEncoding.EncodeToString(pub)

	// Client uses a different key
	wrongPub, wrongPriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	wrongPubB64 := base64.StdEncoding.EncodeToString(wrongPub)

	dir := t.TempDir()
	node, err := NewNode(NodeConfig{
		ID:       "auth-test",
		DataDir:  dir,
		BindAddr: "127.0.0.1:0",
		Auth: AuthConfig{
			Enabled:        true,
			AuthorizedKeys: []string{pubB64},
			ConnectTimeout: 5 * time.Second,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Stop(context.Background())

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

	// Read INFO
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	line = strings.TrimRight(line, "\r\n")

	var serverInfo struct {
		Nonce string `json:"nonce"`
	}
	json.Unmarshal([]byte(line[5:]), &serverInfo)
	nonce, _ := base64.StdEncoding.DecodeString(serverInfo.Nonce)

	// Sign with wrong key
	sig := ed25519.Sign(wrongPriv, nonce)
	sigB64 := base64.StdEncoding.EncodeToString(sig)

	fmt.Fprintf(writer, "CONNECT %s %s\r\n", wrongPubB64, sigB64)
	writer.Flush()

	// Should get -ERR unauthorized
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err = reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "-ERR") {
		t.Fatalf("expected -ERR, got %q", line)
	}
	if !strings.Contains(line, "unauthorized") {
		t.Fatalf("expected unauthorized error, got %q", line)
	}
}

func TestAuthNKeyBadSignature(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	pubB64 := base64.StdEncoding.EncodeToString(pub)

	dir := t.TempDir()
	node, err := NewNode(NodeConfig{
		ID:       "auth-test",
		DataDir:  dir,
		BindAddr: "127.0.0.1:0",
		Auth: AuthConfig{
			Enabled:        true,
			AuthorizedKeys: []string{pubB64},
			ConnectTimeout: 5 * time.Second,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Stop(context.Background())

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

	// Read INFO
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	reader.ReadString('\n')

	// Send CONNECT with correct key but fabricated signature
	badSig := base64.StdEncoding.EncodeToString(make([]byte, 64))
	fmt.Fprintf(writer, "CONNECT %s %s\r\n", pubB64, badSig)
	writer.Flush()

	// Should get -ERR
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "-ERR") {
		t.Fatalf("expected -ERR, got %q", line)
	}
	if !strings.Contains(line, "signature") {
		t.Fatalf("expected signature error, got %q", line)
	}
}

func TestAuthNKeyTimeout(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	pubB64 := base64.StdEncoding.EncodeToString(pub)

	dir := t.TempDir()
	node, err := NewNode(NodeConfig{
		ID:       "auth-test",
		DataDir:  dir,
		BindAddr: "127.0.0.1:0",
		Auth: AuthConfig{
			Enabled:        true,
			AuthorizedKeys: []string{pubB64},
			ConnectTimeout: 500 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Stop(context.Background())

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

	// Read INFO but don't send CONNECT
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(strings.TrimRight(line, "\r\n"), "INFO ") {
		t.Fatalf("expected INFO, got %q", line)
	}

	// Wait for timeout - should get -ERR or connection close
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err = reader.ReadString('\n')
	if err != nil {
		// Connection closed by server after timeout
		return
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "-ERR") {
		t.Fatalf("expected -ERR or connection close, got %q", line)
	}
}

func TestAuthDisabledNoHandshake(t *testing.T) {
	dir := t.TempDir()
	node, err := NewNode(NodeConfig{
		ID:       "noauth-test",
		DataDir:  dir,
		BindAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Stop(context.Background())

	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone
	node.CreateStream(cfg)

	server := NewServer("127.0.0.1:0", node)
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	defer server.Stop(context.Background())

	// Connect without auth and publish - should work
	conn, err := net.DialTimeout("tcp", server.Addr(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	writer := bufio.NewWriter(conn)
	reader := bufio.NewReader(conn)

	// Send PING directly (no auth handshake)
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	writer.WriteString("PING\r\n")
	writer.Flush()

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	line = strings.TrimRight(line, "\r\n")
	if line != "PONG" {
		t.Fatalf("expected PONG, got %q", line)
	}
}
