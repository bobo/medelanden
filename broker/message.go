package broker

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

var MaxMessageSize int64 = 64 * 1024 * 1024

// Message represents a single message in the broker.
type Message struct {
	Subject    string `json:"subject"`
	Payload    []byte `json:"payload"`
	ProducerTS uint64 `json:"producer_ts"` // Unix nanoseconds
	DedupKey   string `json:"dedup_key,omitempty"`
	MsgID      string `json:"msg_id"`
	NodeSeq    uint64 `json:"node_seq"` // Per-node monotonic sequence
	NodeID     string `json:"node_id"`  // ID of the node that first accepted this
	Late       bool   `json:"late,omitempty"` // Set when delivered as a late arrival
}

// GenerateID creates a new unique message ID.
func GenerateID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// EffectiveDedupKey returns the deduplication key for the message given
// the consumer's dedup key fields.
func (m *Message) EffectiveDedupKey(fields []string) string {
	if len(fields) == 0 {
		return m.MsgID
	}
	parts := make([]string, 0, len(fields))
	for _, f := range fields {
		switch f {
		case "msg_id":
			parts = append(parts, m.MsgID)
		case "subject":
			parts = append(parts, m.Subject)
		case "producer_ts":
			parts = append(parts, fmt.Sprintf("%d", m.ProducerTS))
		case "dedup_key":
			parts = append(parts, m.DedupKey)
		case "node_id":
			parts = append(parts, m.NodeID)
		}
	}
	return strings.Join(parts, "|")
}

// Encode serializes a message to bytes (length-prefixed JSON).
func (m *Message) Encode() ([]byte, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshal message: %w", err)
	}
	buf := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(data)))
	copy(buf[4:], data)
	return buf, nil
}

// DecodeMessage reads a length-prefixed JSON message from a reader.
func DecodeMessage(r io.Reader) (*Message, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(lenBuf[:])
	if size > uint32(MaxMessageSize) {
		return nil, fmt.Errorf("message too large: %d bytes", size)
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal message: %w", err)
	}
	return &msg, nil
}

// DecodeMessageFromBytes decodes a message from a byte slice containing
// the length-prefixed encoding.
func DecodeMessageFromBytes(data []byte) (*Message, int, error) {
	if len(data) < 4 {
		return nil, 0, fmt.Errorf("insufficient data for length prefix")
	}
	size := binary.BigEndian.Uint32(data[:4])
	total := 4 + int(size)
	if len(data) < total {
		return nil, 0, fmt.Errorf("insufficient data: need %d, have %d", total, len(data))
	}
	var msg Message
	if err := json.Unmarshal(data[4:total], &msg); err != nil {
		return nil, 0, fmt.Errorf("unmarshal message: %w", err)
	}
	return &msg, total, nil
}
