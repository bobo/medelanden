package broker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Command represents a parsed wire protocol command.
type Command interface {
	Type() string
}

// PubCommand represents a PUB message.
type PubCommand struct {
	Subject    string
	DedupKey   string
	ProducerTS uint64
	Size       int
	Payload    []byte
}

func (c *PubCommand) Type() string { return "PUB" }

// MPubCommand represents an MPUB message (with msg_id).
type MPubCommand struct {
	Subject    string
	MsgID      string
	DedupKey   string
	ProducerTS uint64
	Size       int
	Payload    []byte
}

func (c *MPubCommand) Type() string { return "MPUB" }

// SubCommand represents a SUB (simple subscribe) command.
type SubCommand struct {
	Subject      string
	ConsumerName string
}

func (c *SubCommand) Type() string { return "SUB" }

// MSubCommand represents an MSUB (merged/windowed subscribe) command.
type MSubCommand struct {
	Subject          string
	ConsumerName     string
	WindowDuration   string
	WatermarkTimeout string
}

func (c *MSubCommand) Type() string { return "MSUB" }

// AckCommand represents an ACK command.
type AckCommand struct {
	ConsumerName string
	MsgID        string
}

func (c *AckCommand) Type() string { return "ACK" }

// AckWindowCommand represents an ACKW (batch acknowledge) command.
type AckWindowCommand struct {
	ConsumerName string
	WindowEndTS  uint64
}

func (c *AckWindowCommand) Type() string { return "ACKW" }

// ResumeCommand represents a RESUME command.
type ResumeCommand struct {
	ConsumerName string
}

func (c *ResumeCommand) Type() string { return "RESUME" }

// PingCommand represents a PING command.
type PingCommand struct{}

func (c *PingCommand) Type() string { return "PING" }

// InfoCommand represents an INFO request.
type InfoCommand struct{}

func (c *InfoCommand) Type() string { return "INFO" }

// ProtocolParser parses the wire protocol from a reader.
type ProtocolParser struct {
	reader *bufio.Reader
}

// NewProtocolParser creates a new parser.
func NewProtocolParser(r io.Reader) *ProtocolParser {
	return &ProtocolParser{
		reader: bufio.NewReaderSize(r, 64*1024),
	}
}

// ParseCommand reads and parses the next command from the wire.
func (p *ProtocolParser) ParseCommand() (Command, error) {
	line, err := p.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")

	parts := strings.Fields(line)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty command")
	}

	switch strings.ToUpper(parts[0]) {
	case "PUB":
		return p.parsePub(parts)
	case "MPUB":
		return p.parseMPub(parts)
	case "SUB":
		return p.parseSub(parts)
	case "MSUB":
		return p.parseMSub(parts)
	case "ACK":
		return p.parseAck(parts)
	case "ACKW":
		return p.parseAckWindow(parts)
	case "RESUME":
		return p.parseResume(parts)
	case "PING":
		return &PingCommand{}, nil
	case "INFO":
		return &InfoCommand{}, nil
	default:
		return nil, fmt.Errorf("unknown command: %s", parts[0])
	}
}

func (p *ProtocolParser) parsePub(parts []string) (*PubCommand, error) {
	// PUB <subject> <dedup_key> <producer_ts> <size>\r\n
	// <payload>\r\n
	if len(parts) < 5 {
		return nil, fmt.Errorf("PUB requires 4 arguments: subject, dedup_key, producer_ts, size")
	}

	ts, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid producer_ts: %w", err)
	}

	size, err := strconv.Atoi(parts[4])
	if err != nil {
		return nil, fmt.Errorf("invalid size: %w", err)
	}

	payload := make([]byte, size)
	if _, err := io.ReadFull(p.reader, payload); err != nil {
		return nil, fmt.Errorf("read payload: %w", err)
	}

	// Read trailing \r\n
	trail := make([]byte, 2)
	if _, err := io.ReadFull(p.reader, trail); err != nil {
		return nil, fmt.Errorf("read trailing CRLF: %w", err)
	}

	return &PubCommand{
		Subject:    parts[1],
		DedupKey:   parts[2],
		ProducerTS: ts,
		Size:       size,
		Payload:    payload,
	}, nil
}

func (p *ProtocolParser) parseMPub(parts []string) (*MPubCommand, error) {
	// MPUB <subject> <msg_id> <dedup_key> <producer_ts> <size>\r\n
	// <payload>\r\n
	if len(parts) < 6 {
		return nil, fmt.Errorf("MPUB requires 5 arguments: subject, msg_id, dedup_key, producer_ts, size")
	}

	ts, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid producer_ts: %w", err)
	}

	size, err := strconv.Atoi(parts[5])
	if err != nil {
		return nil, fmt.Errorf("invalid size: %w", err)
	}

	payload := make([]byte, size)
	if _, err := io.ReadFull(p.reader, payload); err != nil {
		return nil, fmt.Errorf("read payload: %w", err)
	}

	// Read trailing \r\n
	trail := make([]byte, 2)
	if _, err := io.ReadFull(p.reader, trail); err != nil {
		return nil, fmt.Errorf("read trailing CRLF: %w", err)
	}

	return &MPubCommand{
		Subject:    parts[1],
		MsgID:      parts[2],
		DedupKey:   parts[3],
		ProducerTS: ts,
		Size:       size,
		Payload:    payload,
	}, nil
}

func (p *ProtocolParser) parseSub(parts []string) (*SubCommand, error) {
	// SUB <subject> <consumer_name>\r\n
	if len(parts) < 3 {
		return nil, fmt.Errorf("SUB requires 2 arguments: subject, consumer_name")
	}
	return &SubCommand{
		Subject:      parts[1],
		ConsumerName: parts[2],
	}, nil
}

func (p *ProtocolParser) parseMSub(parts []string) (*MSubCommand, error) {
	// MSUB <subject> <consumer_name> <window_duration> <watermark_timeout>\r\n
	if len(parts) < 5 {
		return nil, fmt.Errorf("MSUB requires 4 arguments: subject, consumer_name, window_duration, watermark_timeout")
	}
	return &MSubCommand{
		Subject:          parts[1],
		ConsumerName:     parts[2],
		WindowDuration:   parts[3],
		WatermarkTimeout: parts[4],
	}, nil
}

func (p *ProtocolParser) parseAck(parts []string) (*AckCommand, error) {
	// ACK <consumer_name> <msg_id>\r\n
	if len(parts) < 3 {
		return nil, fmt.Errorf("ACK requires 2 arguments: consumer_name, msg_id")
	}
	return &AckCommand{
		ConsumerName: parts[1],
		MsgID:        parts[2],
	}, nil
}

func (p *ProtocolParser) parseAckWindow(parts []string) (*AckWindowCommand, error) {
	// ACKW <consumer_name> <window_end_ts>\r\n
	if len(parts) < 3 {
		return nil, fmt.Errorf("ACKW requires 2 arguments: consumer_name, window_end_ts")
	}
	ts, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid window_end_ts: %w", err)
	}
	return &AckWindowCommand{
		ConsumerName: parts[1],
		WindowEndTS:  ts,
	}, nil
}

func (p *ProtocolParser) parseResume(parts []string) (*ResumeCommand, error) {
	// RESUME <consumer_name>\r\n
	if len(parts) < 2 {
		return nil, fmt.Errorf("RESUME requires 1 argument: consumer_name")
	}
	return &ResumeCommand{
		ConsumerName: parts[1],
	}, nil
}

// FormatOK formats a +OK response.
func FormatOK(seq uint64) string {
	return fmt.Sprintf("+OK %d\r\n", seq)
}

// FormatError formats an -ERR response.
func FormatError(msg string) string {
	return fmt.Sprintf("-ERR %s\r\n", msg)
}

// FormatMsg formats a MSG delivery.
func FormatMsg(subject, consumerName string, producerTS uint64, msgID string, payload []byte) string {
	return fmt.Sprintf("MSG %s %s %d %s %d\r\n%s\r\n",
		subject, consumerName, producerTS, msgID, len(payload), payload)
}

// FormatWindowBatchStart formats a WBATCH header.
func FormatWindowBatchStart(consumerName string, windowStart, windowEnd uint64, msgCount int) string {
	return fmt.Sprintf("WBATCH %s %d %d %d\r\n",
		consumerName, windowStart, windowEnd, msgCount)
}

// FormatWindowBatchMsg formats a message within a window batch.
func FormatWindowBatchMsg(subject string, producerTS uint64, msgID string, payload []byte) string {
	return fmt.Sprintf("MSG %s %d %s %d\r\n%s\r\n",
		subject, producerTS, msgID, len(payload), payload)
}

// FormatWindowBatchEnd formats a WEND marker.
func FormatWindowBatchEnd() string {
	return "WEND\r\n"
}

// FormatPong formats a PONG response.
func FormatPong() string {
	return "PONG\r\n"
}

// FormatPositions formats a +POSITIONS response.
func FormatPositions(positions map[string]*SourcePosition, watermark uint64) string {
	posMap := ConsumerPositionMap{
		Positions: positions,
		Watermark: watermark,
	}
	data, _ := marshalJSON(posMap)
	return fmt.Sprintf("+POSITIONS %s\r\n", data)
}

func marshalJSON(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}
