package broker

import (
	"bytes"
	"testing"
)

func TestMessageEncodeDecode(t *testing.T) {
	original := &Message{
		Subject:    "aircraft.A1B2C3",
		Payload:    []byte("position data here"),
		ProducerTS: 1706900000000000,
		DedupKey:   "aircraft.A1B2C3|1706900000000000",
		MsgID:      "test-msg-id-123",
		NodeSeq:    42,
		NodeID:     "node-a",
	}

	encoded, err := original.Encode()
	if err != nil {
		t.Fatal(err)
	}

	decoded, n, err := DecodeMessageFromBytes(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(encoded) {
		t.Fatalf("expected consumed %d bytes, got %d", len(encoded), n)
	}

	if decoded.Subject != original.Subject {
		t.Errorf("subject: %q != %q", decoded.Subject, original.Subject)
	}
	if !bytes.Equal(decoded.Payload, original.Payload) {
		t.Errorf("payload mismatch")
	}
	if decoded.ProducerTS != original.ProducerTS {
		t.Errorf("producer_ts: %d != %d", decoded.ProducerTS, original.ProducerTS)
	}
	if decoded.DedupKey != original.DedupKey {
		t.Errorf("dedup_key: %q != %q", decoded.DedupKey, original.DedupKey)
	}
	if decoded.MsgID != original.MsgID {
		t.Errorf("msg_id: %q != %q", decoded.MsgID, original.MsgID)
	}
	if decoded.NodeSeq != original.NodeSeq {
		t.Errorf("node_seq: %d != %d", decoded.NodeSeq, original.NodeSeq)
	}
	if decoded.NodeID != original.NodeID {
		t.Errorf("node_id: %q != %q", decoded.NodeID, original.NodeID)
	}
}

func TestMessageDecodeFromReader(t *testing.T) {
	msg := &Message{
		Subject:    "test",
		Payload:    []byte("hello"),
		ProducerTS: 12345,
		MsgID:      "id-1",
		NodeID:     "n1",
	}

	encoded, err := msg.Encode()
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := DecodeMessage(bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}

	if decoded.Subject != msg.Subject {
		t.Errorf("subject: %q != %q", decoded.Subject, msg.Subject)
	}
	if decoded.MsgID != msg.MsgID {
		t.Errorf("msg_id: %q != %q", decoded.MsgID, msg.MsgID)
	}
}

func TestEffectiveDedupKey(t *testing.T) {
	msg := &Message{
		Subject:    "aircraft.A1",
		ProducerTS: 1000,
		MsgID:      "unique-id",
		DedupKey:   "custom-key",
		NodeID:     "node-1",
	}

	// Default: msg_id
	key := msg.EffectiveDedupKey([]string{"msg_id"})
	if key != "unique-id" {
		t.Errorf("expected 'unique-id', got %q", key)
	}

	// Subject + producer_ts
	key = msg.EffectiveDedupKey([]string{"subject", "producer_ts"})
	if key != "aircraft.A1|1000" {
		t.Errorf("expected 'aircraft.A1|1000', got %q", key)
	}

	// Empty fields defaults to msg_id
	key = msg.EffectiveDedupKey(nil)
	if key != "unique-id" {
		t.Errorf("expected 'unique-id', got %q", key)
	}
}

func TestGenerateID(t *testing.T) {
	ids := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id := GenerateID()
		if ids[id] {
			t.Fatalf("duplicate ID generated: %s", id)
		}
		ids[id] = true
	}
}
