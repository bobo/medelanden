package broker

import (
	"os"
	"path/filepath"
	"testing"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "medelanden-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func TestWALAppendAndRead(t *testing.T) {
	dir := tempDir(t)
	wal, err := NewWAL(filepath.Join(dir, "wal"), FsyncNone, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	// Append 100 messages
	for i := uint64(1); i <= 100; i++ {
		msg := &Message{
			Subject:    "test.subject",
			Payload:    []byte("hello"),
			ProducerTS: i * 1000,
			MsgID:      GenerateID(),
			NodeID:     "node-1",
		}
		seq, err := wal.Append(msg)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if seq != i {
			t.Fatalf("expected seq %d, got %d", i, seq)
		}
	}

	if wal.LastSeq() != 100 {
		t.Fatalf("expected last seq 100, got %d", wal.LastSeq())
	}

	// Read all
	msgs, err := wal.Read(1, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 100 {
		t.Fatalf("expected 100 messages, got %d", len(msgs))
	}

	// Verify ordering
	for i, msg := range msgs {
		expected := uint64(i+1) * 1000
		if msg.ProducerTS != expected {
			t.Fatalf("msg %d: expected ts %d, got %d", i, expected, msg.ProducerTS)
		}
	}

	// Read range
	msgs, err = wal.Read(50, 60)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 11 {
		t.Fatalf("expected 11 messages, got %d", len(msgs))
	}
	if msgs[0].ProducerTS != 50000 {
		t.Fatalf("expected first ts 50000, got %d", msgs[0].ProducerTS)
	}
}

func TestWALReadFrom(t *testing.T) {
	dir := tempDir(t)
	wal, err := NewWAL(filepath.Join(dir, "wal"), FsyncNone, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	for i := uint64(1); i <= 50; i++ {
		msg := &Message{
			Subject:    "test",
			Payload:    []byte("data"),
			ProducerTS: i,
			MsgID:      GenerateID(),
			NodeID:     "node-1",
		}
		wal.Append(msg)
	}

	msgs, err := wal.ReadFrom(45)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 6 {
		t.Fatalf("expected 6 messages, got %d", len(msgs))
	}
}

func TestWALRecovery(t *testing.T) {
	dir := tempDir(t)
	walDir := filepath.Join(dir, "wal")

	// Write some messages
	wal, err := NewWAL(walDir, FsyncEvery, 0)
	if err != nil {
		t.Fatal(err)
	}

	for i := uint64(1); i <= 10; i++ {
		msg := &Message{
			Subject:    "test",
			Payload:    []byte("persistent"),
			ProducerTS: i * 100,
			MsgID:      GenerateID(),
			NodeID:     "node-1",
		}
		wal.Append(msg)
	}
	wal.Close()

	// Reopen and verify
	wal2, err := NewWAL(walDir, FsyncEvery, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	if wal2.LastSeq() != 10 {
		t.Fatalf("after recovery: expected last seq 10, got %d", wal2.LastSeq())
	}

	msgs, err := wal2.Read(1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 10 {
		t.Fatalf("after recovery: expected 10 messages, got %d", len(msgs))
	}

	for i, msg := range msgs {
		if msg.ProducerTS != uint64(i+1)*100 {
			t.Fatalf("msg %d: expected ts %d, got %d", i, (i+1)*100, msg.ProducerTS)
		}
	}
}

func TestWALAppendReplicated(t *testing.T) {
	dir := tempDir(t)
	wal, err := NewWAL(filepath.Join(dir, "wal"), FsyncNone, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	msg := &Message{
		Subject:    "test",
		Payload:    []byte("replicated"),
		ProducerTS: 1000,
		MsgID:      GenerateID(),
		NodeID:     "node-remote",
		NodeSeq:    1,
	}

	added, err := wal.AppendReplicated(msg)
	if err != nil {
		t.Fatal(err)
	}
	if !added {
		t.Fatal("expected message to be added")
	}

	// Try to add the same sequence again
	added, err = wal.AppendReplicated(msg)
	if err != nil {
		t.Fatal(err)
	}
	if added {
		t.Fatal("expected duplicate to be rejected")
	}

	if wal.MessageCount() != 1 {
		t.Fatalf("expected 1 message, got %d", wal.MessageCount())
	}
}

func TestWALEmptyRead(t *testing.T) {
	dir := tempDir(t)
	wal, err := NewWAL(filepath.Join(dir, "wal"), FsyncNone, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	msgs, err := wal.Read(1, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 0 {
		t.Fatalf("expected 0 messages, got %d", len(msgs))
	}
}

func TestWALMaxProducerTS(t *testing.T) {
	dir := tempDir(t)
	wal, err := NewWAL(filepath.Join(dir, "wal"), FsyncNone, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	ts, err := wal.MaxProducerTS()
	if err != nil {
		t.Fatal(err)
	}
	if ts != 0 {
		t.Fatalf("expected 0 for empty WAL, got %d", ts)
	}

	for _, ts := range []uint64{500, 100, 300, 900, 200} {
		msg := &Message{
			Subject:    "test",
			Payload:    []byte("data"),
			ProducerTS: ts,
			MsgID:      GenerateID(),
			NodeID:     "node-1",
		}
		wal.Append(msg)
	}

	maxTS, err := wal.MaxProducerTS()
	if err != nil {
		t.Fatal(err)
	}
	// MaxProducerTS returns the ts of the LAST message in the WAL (by seq order)
	// which is 200 (the last appended)
	if maxTS != 200 {
		t.Fatalf("expected max ts 200 (last appended), got %d", maxTS)
	}
}
