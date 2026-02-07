package broker

import (
	"testing"
)

func TestSubjectMatching(t *testing.T) {
	tests := []struct {
		pattern string
		subject string
		match   bool
	}{
		// Exact match
		{"aircraft.position", "aircraft.position", true},
		{"aircraft.position", "aircraft.speed", false},

		// Single token wildcard
		{"aircraft.*", "aircraft.A1B2C3", true},
		{"aircraft.*", "aircraft.A1B2C3.position", false},

		// Multi-token wildcard (>)
		{"aircraft.>", "aircraft.A1B2C3", true},
		{"aircraft.>", "aircraft.A1B2C3.position", true},
		{"aircraft.>", "aircraft.A1B2C3.position.lat", true},
		{"aircraft.>", "vehicle.car", false},

		// Root wildcard
		{">", "anything", true},
		{">", "multi.level.subject", true},

		// Mixed
		{"*.position", "aircraft.position", true},
		{"*.position", "vehicle.position", true},
		{"*.position", "aircraft.speed", false},
	}

	for _, tt := range tests {
		m := compileSubject(tt.pattern)
		got := matchSubject(m, tt.subject)
		if got != tt.match {
			t.Errorf("pattern=%q subject=%q: expected %v, got %v", tt.pattern, tt.subject, tt.match, got)
		}
	}
}

func TestStreamPublishAndRead(t *testing.T) {
	dir := tempDir(t)

	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone

	stream, err := NewStream(cfg, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	// Publish messages
	for i := uint64(1); i <= 50; i++ {
		msg := &Message{
			Subject:    "test.data",
			Payload:    []byte("payload"),
			ProducerTS: i * 1000,
			MsgID:      GenerateID(),
			NodeID:     "node-1",
		}
		seq, err := stream.Publish(msg)
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		if seq != i {
			t.Fatalf("expected seq %d, got %d", i, seq)
		}
	}

	if stream.MessageCount() != 50 {
		t.Fatalf("expected 50 messages, got %d", stream.MessageCount())
	}

	// Read
	msgs, err := stream.Read(1, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 50 {
		t.Fatalf("expected 50 messages, got %d", len(msgs))
	}
}

func TestStreamSubjectFilter(t *testing.T) {
	dir := tempDir(t)

	cfg := DefaultStreamConfig("aircraft", []string{"aircraft.>"})
	cfg.FsyncPolicy = FsyncNone

	stream, err := NewStream(cfg, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	// Matching subject
	msg1 := &Message{
		Subject:    "aircraft.A1B2C3",
		Payload:    []byte("position"),
		ProducerTS: 1000,
		MsgID:      GenerateID(),
		NodeID:     "node-1",
	}
	if _, err := stream.Publish(msg1); err != nil {
		t.Fatal(err)
	}

	// Non-matching subject
	msg2 := &Message{
		Subject:    "vehicle.car1",
		Payload:    []byte("position"),
		ProducerTS: 2000,
		MsgID:      GenerateID(),
		NodeID:     "node-1",
	}
	if _, err := stream.Publish(msg2); err == nil {
		t.Fatal("expected error for non-matching subject")
	}

	if stream.MessageCount() != 1 {
		t.Fatalf("expected 1 message, got %d", stream.MessageCount())
	}
}

func TestStreamDigest(t *testing.T) {
	dir := tempDir(t)

	cfg := DefaultStreamConfig("test", []string{"test.>"})
	cfg.FsyncPolicy = FsyncNone

	stream, err := NewStream(cfg, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	for i := uint64(1); i <= 5; i++ {
		msg := &Message{
			Subject:    "test.data",
			Payload:    []byte("payload"),
			ProducerTS: i * 1000,
			MsgID:      GenerateID(),
			NodeID:     "node-1",
		}
		stream.Publish(msg)
	}

	digest := stream.Digest()
	if digest.Stream != "test" {
		t.Fatalf("expected stream 'test', got %q", digest.Stream)
	}
	if digest.MaxSeq != 5 {
		t.Fatalf("expected max seq 5, got %d", digest.MaxSeq)
	}
}
