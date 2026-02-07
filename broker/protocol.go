package broker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
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

// StreamCreateCommand creates a new stream.
// STREAM.CREATE <json_config>\r\n
type StreamCreateCommand struct {
	Config StreamConfig
}

func (c *StreamCreateCommand) Type() string { return "STREAM.CREATE" }

// StreamDeleteCommand deletes a stream.
// STREAM.DELETE <name>\r\n
type StreamDeleteCommand struct {
	Name string
}

func (c *StreamDeleteCommand) Type() string { return "STREAM.DELETE" }

// StreamListCommand lists all streams.
// STREAM.LIST\r\n
type StreamListCommand struct{}

func (c *StreamListCommand) Type() string { return "STREAM.LIST" }

// StreamInfoCommand returns info about a stream.
// STREAM.INFO <name>\r\n
type StreamInfoCommand struct {
	Name string
}

func (c *StreamInfoCommand) Type() string { return "STREAM.INFO" }

// ConsumerCreateCommand creates a new consumer.
// CONSUMER.CREATE <json_config>\r\n
type ConsumerCreateCommand struct {
	Config ConsumerConfig
}

func (c *ConsumerCreateCommand) Type() string { return "CONSUMER.CREATE" }

// ConsumerDeleteCommand deletes a consumer.
// CONSUMER.DELETE <stream> <name>\r\n
type ConsumerDeleteCommand struct {
	Stream string
	Name   string
}

func (c *ConsumerDeleteCommand) Type() string { return "CONSUMER.DELETE" }

// ConsumerListCommand lists consumers for a stream.
// CONSUMER.LIST <stream>\r\n
type ConsumerListCommand struct {
	Stream string
}

func (c *ConsumerListCommand) Type() string { return "CONSUMER.LIST" }

// ConsumerInfoCommand returns info about a consumer.
// CONSUMER.INFO <stream> <name>\r\n
type ConsumerInfoCommand struct {
	Stream string
	Name   string
}

func (c *ConsumerInfoCommand) Type() string { return "CONSUMER.INFO" }

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

	cmd := strings.ToUpper(parts[0])
	// Handle dotted commands (e.g. STREAM.CREATE)
	if len(parts) >= 2 {
		compound := strings.ToUpper(parts[0] + "." + parts[1])
		switch compound {
		case "STREAM.CREATE":
			return p.parseStreamCreate(parts)
		case "STREAM.DELETE":
			return p.parseStreamDelete(parts)
		case "STREAM.LIST":
			return &StreamListCommand{}, nil
		case "STREAM.INFO":
			return p.parseStreamInfo(parts)
		case "CONSUMER.CREATE":
			return p.parseConsumerCreate(parts)
		case "CONSUMER.DELETE":
			return p.parseConsumerDelete(parts)
		case "CONSUMER.LIST":
			return p.parseConsumerList(parts)
		case "CONSUMER.INFO":
			return p.parseConsumerInfo(parts)
		}
	}

	switch cmd {
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

// wireStreamConfig is used for JSON parsing of stream configs over the wire,
// where durations are sent as human-readable strings like "24h" instead of nanosecond integers.
type wireStreamConfig struct {
	Name              string   `json:"name"`
	Subjects          []string `json:"subjects"`
	MaxBytes          int64    `json:"max_bytes,omitempty"`
	MaxAge            string   `json:"max_age,omitempty"`
	MaxMsgs           int64    `json:"max_msgs,omitempty"`
	ReplicationTarget int      `json:"replication_target,omitempty"`
	FsyncPolicy       string   `json:"fsync_policy,omitempty"`
	FsyncInterval     string   `json:"fsync_interval,omitempty"`
	PlacementTags     []string `json:"placement_tags,omitempty"`
	PlacementCount    int      `json:"placement_count,omitempty"`
}

func (p *ProtocolParser) parseStreamCreate(parts []string) (*StreamCreateCommand, error) {
	// STREAM CREATE <json_config>\r\n
	if len(parts) < 3 {
		return nil, fmt.Errorf("STREAM CREATE requires JSON config argument")
	}
	jsonStr := strings.Join(parts[2:], " ")
	var wire wireStreamConfig
	if err := json.Unmarshal([]byte(jsonStr), &wire); err != nil {
		return nil, fmt.Errorf("invalid stream config: %w", err)
	}

	cfg := StreamConfig{
		Name:              wire.Name,
		Subjects:          wire.Subjects,
		MaxBytes:          wire.MaxBytes,
		MaxMsgs:           wire.MaxMsgs,
		ReplicationTarget: wire.ReplicationTarget,
		PlacementTags:     wire.PlacementTags,
		PlacementCount:    wire.PlacementCount,
	}

	if wire.FsyncPolicy != "" {
		cfg.FsyncPolicy = FsyncPolicy(wire.FsyncPolicy)
	}
	if wire.MaxAge != "" {
		d, err := time.ParseDuration(wire.MaxAge)
		if err != nil {
			return nil, fmt.Errorf("invalid max_age: %w", err)
		}
		cfg.MaxAge = d
	}
	if wire.FsyncInterval != "" {
		d, err := time.ParseDuration(wire.FsyncInterval)
		if err != nil {
			return nil, fmt.Errorf("invalid fsync_interval: %w", err)
		}
		cfg.FsyncInterval = d
	}

	return &StreamCreateCommand{Config: cfg}, nil
}

func (p *ProtocolParser) parseStreamDelete(parts []string) (*StreamDeleteCommand, error) {
	// STREAM DELETE <name>\r\n
	if len(parts) < 3 {
		return nil, fmt.Errorf("STREAM DELETE requires stream name")
	}
	return &StreamDeleteCommand{Name: parts[2]}, nil
}

func (p *ProtocolParser) parseStreamInfo(parts []string) (*StreamInfoCommand, error) {
	// STREAM INFO <name>\r\n
	if len(parts) < 3 {
		return nil, fmt.Errorf("STREAM INFO requires stream name")
	}
	return &StreamInfoCommand{Name: parts[2]}, nil
}

// wireConsumerConfig is used for JSON parsing of consumer configs over the wire,
// where durations are sent as human-readable strings like "2s" instead of nanosecond integers.
type wireConsumerConfig struct {
	Name             string   `json:"name"`
	Stream           string   `json:"stream"`
	Ordering         string   `json:"ordering,omitempty"`
	WindowDuration   string   `json:"window_duration,omitempty"`
	WatermarkTimeout string   `json:"watermark_timeout,omitempty"`
	DedupKey         []string `json:"dedup_key,omitempty"`
	LatePolicy       string   `json:"late_policy,omitempty"`
	DeliverPolicy    string   `json:"deliver_policy,omitempty"`
	DeliverFromTime  uint64   `json:"deliver_from_time,omitempty"`
	SubjectFilter    string   `json:"subject_filter,omitempty"`
}

func (p *ProtocolParser) parseConsumerCreate(parts []string) (*ConsumerCreateCommand, error) {
	// CONSUMER CREATE <json_config>\r\n
	if len(parts) < 3 {
		return nil, fmt.Errorf("CONSUMER CREATE requires JSON config argument")
	}
	jsonStr := strings.Join(parts[2:], " ")
	var wire wireConsumerConfig
	if err := json.Unmarshal([]byte(jsonStr), &wire); err != nil {
		return nil, fmt.Errorf("invalid consumer config: %w", err)
	}

	cfg := ConsumerConfig{
		Name:            wire.Name,
		Stream:          wire.Stream,
		Ordering:        wire.Ordering,
		DedupKey:        wire.DedupKey,
		DeliverFromTime: wire.DeliverFromTime,
		SubjectFilter:   wire.SubjectFilter,
	}

	if wire.LatePolicy != "" {
		cfg.LatePolicy = LatePolicy(wire.LatePolicy)
	}
	if wire.DeliverPolicy != "" {
		cfg.DeliverPolicy = DeliverPolicy(wire.DeliverPolicy)
	}
	if wire.WindowDuration != "" {
		d, err := time.ParseDuration(wire.WindowDuration)
		if err != nil {
			return nil, fmt.Errorf("invalid window_duration: %w", err)
		}
		cfg.WindowDuration = d
	}
	if wire.WatermarkTimeout != "" {
		d, err := time.ParseDuration(wire.WatermarkTimeout)
		if err != nil {
			return nil, fmt.Errorf("invalid watermark_timeout: %w", err)
		}
		cfg.WatermarkTimeout = d
	}

	return &ConsumerCreateCommand{Config: cfg}, nil
}

func (p *ProtocolParser) parseConsumerDelete(parts []string) (*ConsumerDeleteCommand, error) {
	// CONSUMER DELETE <stream> <name>\r\n
	if len(parts) < 4 {
		return nil, fmt.Errorf("CONSUMER DELETE requires stream and consumer name")
	}
	return &ConsumerDeleteCommand{Stream: parts[2], Name: parts[3]}, nil
}

func (p *ProtocolParser) parseConsumerList(parts []string) (*ConsumerListCommand, error) {
	// CONSUMER LIST <stream>\r\n
	if len(parts) < 3 {
		return nil, fmt.Errorf("CONSUMER LIST requires stream name")
	}
	return &ConsumerListCommand{Stream: parts[2]}, nil
}

func (p *ProtocolParser) parseConsumerInfo(parts []string) (*ConsumerInfoCommand, error) {
	// CONSUMER INFO <stream> <name>\r\n
	if len(parts) < 4 {
		return nil, fmt.Errorf("CONSUMER INFO requires stream and consumer name")
	}
	return &ConsumerInfoCommand{Stream: parts[2], Name: parts[3]}, nil
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

// FormatStreamInfo formats a +STREAM.INFO response.
func FormatStreamInfo(state *StreamState) string {
	data, _ := json.Marshal(state)
	return fmt.Sprintf("+STREAM.INFO %s\r\n", data)
}

// FormatStreamList formats a +STREAM.LIST response.
func FormatStreamList(names []string) string {
	data, _ := json.Marshal(names)
	return fmt.Sprintf("+STREAM.LIST %s\r\n", data)
}

// FormatConsumerInfo formats a +CONSUMER.INFO response.
func FormatConsumerInfo(state *ConsumerState) string {
	data, _ := json.Marshal(state)
	return fmt.Sprintf("+CONSUMER.INFO %s\r\n", data)
}

// FormatConsumerList formats a +CONSUMER.LIST response.
func FormatConsumerList(names []string) string {
	data, _ := json.Marshal(names)
	return fmt.Sprintf("+CONSUMER.LIST %s\r\n", data)
}

func marshalJSON(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}
