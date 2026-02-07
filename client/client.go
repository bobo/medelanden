// Package client provides a Go client for the Medelanden message broker.
//
// The client speaks the Medelanden text-based wire protocol over TCP, supporting
// publish (PUB/MPUB), subscribe (SUB/MSUB), acknowledgment (ACK/ACKW),
// consumer resume (RESUME), and health queries (PING, INFO).
//
// Basic usage:
//
//	c, err := client.Dial("localhost:4222")
//	if err != nil { ... }
//	defer c.Close()
//
//	// Publish a message
//	seq, err := c.Publish("sensors.temp", []byte(`{"value":22.5}`),
//	    client.WithDedupKey("sensor-1|reading-42"))
//
//	// Subscribe and receive messages
//	sub, err := c.Subscribe("sensors.>", "my-consumer")
//	for msg := range sub.Messages() {
//	    fmt.Println(msg.Subject, string(msg.Payload))
//	}
package client

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Msg represents a message received from the broker.
type Msg struct {
	Subject    string
	Payload    []byte
	ProducerTS uint64
	MsgID      string
	Consumer   string
	Late       bool
}

// WindowBatch represents a batch of messages from a windowed subscription.
type WindowBatch struct {
	Consumer    string
	WindowStart uint64
	WindowEnd   uint64
	Messages    []*Msg
}

// SourcePosition tracks a consumer's position on a specific source node.
type SourcePosition struct {
	NodeID  string `json:"node_id"`
	LastSeq uint64 `json:"last_seq"`
	LastTS  uint64 `json:"last_ts"`
}

// ConsumerPositionMap is the position state returned by RESUME.
type ConsumerPositionMap struct {
	Consumer  string                     `json:"consumer"`
	Positions map[string]*SourcePosition `json:"positions"`
	Watermark uint64                     `json:"watermark"`
}

// NodeInfo is the server info returned by INFO.
type NodeInfo struct {
	Status  string                   `json:"status"`
	NodeID  string                   `json:"node_id"`
	Peers   map[string]PeerStatus    `json:"peers"`
	Streams map[string]StreamInfo    `json:"streams"`
}

// PeerStatus is the health status of a peer node.
type PeerStatus struct {
	Status string `json:"status"`
	LagMs  int64  `json:"lag_ms"`
}

// StreamInfo describes a stream on a node.
type StreamInfo struct {
	LocalMessages     uint64 `json:"local_messages"`
	ReplicationStatus string `json:"replication_status"`
}

// StreamConfig configures a named message stream (client-side mirror of broker.StreamConfig).
type StreamConfig struct {
	Name              string   `json:"name"`
	Subjects          []string `json:"subjects"`
	MaxBytes          int64    `json:"max_bytes,omitempty"`
	MaxAge            string   `json:"max_age,omitempty"`
	MaxMsgs           int64    `json:"max_msgs,omitempty"`
	ReplicationTarget int      `json:"replication_target,omitempty"`
	FsyncPolicy       string   `json:"fsync_policy,omitempty"`
}

// StreamState is a detailed snapshot of a stream's state.
type StreamState struct {
	Name          string     `json:"name"`
	Config        StreamConfig `json:"config"`
	Messages      uint64     `json:"messages"`
	FirstSeq      uint64     `json:"first_seq"`
	LastSeq       uint64     `json:"last_seq"`
	ConsumerCount int        `json:"consumer_count"`
}

// ConsumerConfig configures a durable consumer (client-side mirror of broker.ConsumerConfig).
type ConsumerConfig struct {
	Name             string   `json:"name"`
	Stream           string   `json:"stream"`
	Ordering         string   `json:"ordering,omitempty"`
	WindowDuration   string   `json:"window_duration,omitempty"`
	WatermarkTimeout string   `json:"watermark_timeout,omitempty"`
	DedupKey         []string `json:"dedup_key,omitempty"`
	LatePolicy       string   `json:"late_policy,omitempty"`
	DeliverPolicy    string   `json:"deliver_policy,omitempty"`
	SubjectFilter    string   `json:"subject_filter,omitempty"`
}

// ConsumerState is a detailed snapshot of a consumer's state.
type ConsumerState struct {
	Name         string                     `json:"name"`
	Stream       string                     `json:"stream"`
	Config       ConsumerConfig             `json:"config"`
	Watermark    uint64                     `json:"watermark"`
	LateMessages uint64                     `json:"late_messages"`
	DedupCount   uint64                     `json:"dedup_count"`
	Positions    map[string]*SourcePosition `json:"positions"`
}

// Subscription represents an active subscription to a subject.
type Subscription struct {
	consumer string
	msgCh    chan *Msg
	batchCh  chan *WindowBatch
	windowed bool
	done     chan struct{}
	once     sync.Once
	closeCh  sync.Once // guards channel close
}

// Messages returns a channel that delivers individual messages. Use this for
// simple (SUB) subscriptions.
func (s *Subscription) Messages() <-chan *Msg {
	return s.msgCh
}

// Batches returns a channel that delivers window batches. Use this for
// windowed (MSUB) subscriptions.
func (s *Subscription) Batches() <-chan *WindowBatch {
	return s.batchCh
}

// Unsubscribe stops the subscription and closes its channels.
func (s *Subscription) Unsubscribe() {
	s.once.Do(func() {
		close(s.done)
	})
}

// closeChannels closes the delivery channels exactly once.
func (s *Subscription) closeChannels() {
	s.closeCh.Do(func() {
		if s.windowed {
			close(s.batchCh)
		} else {
			close(s.msgCh)
		}
	})
}

// Options configures a client connection.
type Options struct {
	// ReadTimeout is the deadline for reads from the server. Defaults to 35s
	// (slightly above the server's 30s keepalive interval).
	ReadTimeout time.Duration

	// WriteTimeout is the deadline for writes to the server. Defaults to 5s.
	WriteTimeout time.Duration
}

// Option is a functional option for Dial.
type Option func(*Options)

// WithReadTimeout sets the read deadline for server responses.
func WithReadTimeout(d time.Duration) Option {
	return func(o *Options) { o.ReadTimeout = d }
}

// WithWriteTimeout sets the write deadline for commands sent to the server.
func WithWriteTimeout(d time.Duration) Option {
	return func(o *Options) { o.WriteTimeout = d }
}

// Client is a connection to a Medelanden broker node.
type Client struct {
	conn    net.Conn
	reader  *bufio.Reader
	writer  *bufio.Writer
	opts    Options
	mu      sync.Mutex // protects writes
	closed  bool
	closeCh chan struct{}

	subsMu sync.RWMutex
	subs   map[string]*Subscription
}

// Dial connects to a Medelanden broker at the given address (host:port).
func Dial(addr string, opts ...Option) (*Client, error) {
	o := Options{
		ReadTimeout:  35 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	for _, fn := range opts {
		fn(&o)
	}

	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("medelanden: dial %s: %w", addr, err)
	}

	c := &Client{
		conn:    conn,
		reader:  bufio.NewReaderSize(conn, 64*1024),
		writer:  bufio.NewWriter(conn),
		opts:    o,
		closeCh: make(chan struct{}),
		subs:    make(map[string]*Subscription),
	}

	return c, nil
}

// Close shuts down the client connection.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.closeCh)
	c.mu.Unlock()

	c.subsMu.Lock()
	for _, sub := range c.subs {
		sub.Unsubscribe()
		sub.closeChannels()
	}
	c.subs = nil
	c.subsMu.Unlock()

	return c.conn.Close()
}

// PubOption configures a Publish call.
type PubOption func(*pubOpts)

type pubOpts struct {
	dedupKey   string
	producerTS uint64
	msgID      string
}

// WithDedupKey sets an application-defined deduplication key.
func WithDedupKey(key string) PubOption {
	return func(o *pubOpts) { o.dedupKey = key }
}

// WithProducerTS overrides the producer timestamp (unix nanoseconds). By
// default the current time is used.
func WithProducerTS(ts uint64) PubOption {
	return func(o *pubOpts) { o.producerTS = ts }
}

// WithMsgID sets an explicit message ID (uses MPUB instead of PUB). When not
// set, the server generates one.
func WithMsgID(id string) PubOption {
	return func(o *pubOpts) { o.msgID = id }
}

// Publish sends a message to the given subject and returns the assigned
// node sequence number.
func (c *Client) Publish(subject string, payload []byte, opts ...PubOption) (uint64, error) {
	o := pubOpts{
		dedupKey:   "-",
		producerTS: uint64(time.Now().UnixNano()),
	}
	for _, fn := range opts {
		fn(&o)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return 0, fmt.Errorf("medelanden: connection closed")
	}

	if err := c.conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout)); err != nil {
		return 0, err
	}

	var header string
	if o.msgID != "" {
		// MPUB <subject> <msg_id> <dedup_key> <producer_ts> <size>\r\n
		header = fmt.Sprintf("MPUB %s %s %s %d %d\r\n",
			subject, o.msgID, o.dedupKey, o.producerTS, len(payload))
	} else {
		// PUB <subject> <dedup_key> <producer_ts> <size>\r\n
		header = fmt.Sprintf("PUB %s %s %d %d\r\n",
			subject, o.dedupKey, o.producerTS, len(payload))
	}

	if _, err := c.writer.WriteString(header); err != nil {
		return 0, fmt.Errorf("medelanden: write header: %w", err)
	}
	if _, err := c.writer.Write(payload); err != nil {
		return 0, fmt.Errorf("medelanden: write payload: %w", err)
	}
	if _, err := c.writer.WriteString("\r\n"); err != nil {
		return 0, fmt.Errorf("medelanden: write trailer: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return 0, fmt.Errorf("medelanden: flush: %w", err)
	}

	return c.readOKSeq()
}

// Subscribe creates a simple (non-windowed) subscription. Messages are
// delivered individually to the returned Subscription's Messages channel.
func (c *Client) Subscribe(subject, consumer string) (*Subscription, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, fmt.Errorf("medelanden: connection closed")
	}

	if err := c.conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout)); err != nil {
		c.mu.Unlock()
		return nil, err
	}

	cmd := fmt.Sprintf("SUB %s %s\r\n", subject, consumer)
	if _, err := c.writer.WriteString(cmd); err != nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("medelanden: write SUB: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("medelanden: flush: %w", err)
	}

	if err := c.readOK(); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	c.mu.Unlock()

	sub := &Subscription{
		consumer: consumer,
		msgCh:    make(chan *Msg, 256),
		done:     make(chan struct{}),
	}

	c.subsMu.Lock()
	c.subs[consumer] = sub
	c.subsMu.Unlock()

	go c.readLoop(sub)

	return sub, nil
}

// SubscribeWindowed creates a windowed (MSUB) subscription. Messages are
// delivered as sorted, deduplicated batches to the returned Subscription's
// Batches channel.
func (c *Client) SubscribeWindowed(subject, consumer string, windowDuration, watermarkTimeout time.Duration) (*Subscription, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, fmt.Errorf("medelanden: connection closed")
	}

	if err := c.conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout)); err != nil {
		c.mu.Unlock()
		return nil, err
	}

	cmd := fmt.Sprintf("MSUB %s %s %s %s\r\n",
		subject, consumer, windowDuration.String(), watermarkTimeout.String())
	if _, err := c.writer.WriteString(cmd); err != nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("medelanden: write MSUB: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("medelanden: flush: %w", err)
	}

	if err := c.readOK(); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	c.mu.Unlock()

	sub := &Subscription{
		consumer: consumer,
		batchCh:  make(chan *WindowBatch, 64),
		windowed: true,
		done:     make(chan struct{}),
	}

	c.subsMu.Lock()
	c.subs[consumer] = sub
	c.subsMu.Unlock()

	go c.readLoopWindowed(sub)

	return sub, nil
}

// Ack acknowledges a single message.
func (c *Client) Ack(consumer, msgID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("medelanden: connection closed")
	}

	cmd := fmt.Sprintf("ACK %s %s\r\n", consumer, msgID)
	return c.sendAndReadOK(cmd)
}

// AckWindow acknowledges all messages up to a window end timestamp.
func (c *Client) AckWindow(consumer string, windowEndTS uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("medelanden: connection closed")
	}

	cmd := fmt.Sprintf("ACKW %s %d\r\n", consumer, windowEndTS)
	return c.sendAndReadOK(cmd)
}

// Resume retrieves the saved position map for a consumer, enabling failover
// to a different node.
func (c *Client) Resume(consumer string) (*ConsumerPositionMap, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, fmt.Errorf("medelanden: connection closed")
	}

	if err := c.conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout)); err != nil {
		return nil, err
	}

	cmd := fmt.Sprintf("RESUME %s\r\n", consumer)
	if _, err := c.writer.WriteString(cmd); err != nil {
		return nil, fmt.Errorf("medelanden: write RESUME: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return nil, fmt.Errorf("medelanden: flush: %w", err)
	}

	if err := c.conn.SetReadDeadline(time.Now().Add(c.opts.ReadTimeout)); err != nil {
		return nil, err
	}

	line, err := c.reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("medelanden: read response: %w", err)
	}
	line = strings.TrimRight(line, "\r\n")

	if strings.HasPrefix(line, "-ERR ") {
		return nil, fmt.Errorf("medelanden: %s", line[5:])
	}
	if !strings.HasPrefix(line, "+POSITIONS ") {
		return nil, fmt.Errorf("medelanden: unexpected response: %s", line)
	}

	jsonData := line[len("+POSITIONS "):]
	var pos ConsumerPositionMap
	if err := json.Unmarshal([]byte(jsonData), &pos); err != nil {
		return nil, fmt.Errorf("medelanden: parse positions: %w", err)
	}
	return &pos, nil
}

// Ping checks that the broker is alive. Returns an error if the broker does
// not respond with PONG.
func (c *Client) Ping() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("medelanden: connection closed")
	}

	if err := c.conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout)); err != nil {
		return err
	}

	if _, err := c.writer.WriteString("PING\r\n"); err != nil {
		return fmt.Errorf("medelanden: write PING: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return fmt.Errorf("medelanden: flush: %w", err)
	}

	if err := c.conn.SetReadDeadline(time.Now().Add(c.opts.ReadTimeout)); err != nil {
		return err
	}

	line, err := c.reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("medelanden: read PONG: %w", err)
	}
	line = strings.TrimRight(line, "\r\n")
	if line != "PONG" {
		return fmt.Errorf("medelanden: expected PONG, got %q", line)
	}
	return nil
}

// Info requests node information from the broker.
func (c *Client) Info() (*NodeInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, fmt.Errorf("medelanden: connection closed")
	}

	if err := c.conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout)); err != nil {
		return nil, err
	}

	if _, err := c.writer.WriteString("INFO\r\n"); err != nil {
		return nil, fmt.Errorf("medelanden: write INFO: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return nil, fmt.Errorf("medelanden: flush: %w", err)
	}

	if err := c.conn.SetReadDeadline(time.Now().Add(c.opts.ReadTimeout)); err != nil {
		return nil, err
	}

	line, err := c.reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("medelanden: read response: %w", err)
	}
	line = strings.TrimRight(line, "\r\n")

	if strings.HasPrefix(line, "-ERR ") {
		return nil, fmt.Errorf("medelanden: %s", line[5:])
	}
	if !strings.HasPrefix(line, "+INFO ") {
		return nil, fmt.Errorf("medelanden: unexpected response: %s", line)
	}

	jsonData := line[len("+INFO "):]
	var info NodeInfo
	if err := json.Unmarshal([]byte(jsonData), &info); err != nil {
		return nil, fmt.Errorf("medelanden: parse info: %w", err)
	}
	return &info, nil
}

// CreateStream creates a new stream on the broker.
func (c *Client) CreateStream(cfg StreamConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("medelanden: connection closed")
	}

	cfgJSON, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("medelanden: marshal config: %w", err)
	}

	cmd := fmt.Sprintf("STREAM CREATE %s\r\n", cfgJSON)
	return c.sendAndReadOK(cmd)
}

// DeleteStream deletes a stream on the broker.
func (c *Client) DeleteStream(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("medelanden: connection closed")
	}

	cmd := fmt.Sprintf("STREAM DELETE %s\r\n", name)
	return c.sendAndReadOK(cmd)
}

// ListStreams returns the names of all streams on the broker.
func (c *Client) ListStreams() ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, fmt.Errorf("medelanden: connection closed")
	}

	return c.sendAndReadJSONList("STREAM LIST\r\n", "+STREAM.LIST ")
}

// StreamInfo returns detailed state about a stream.
func (c *Client) StreamInfo(name string) (*StreamState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, fmt.Errorf("medelanden: connection closed")
	}

	line, err := c.sendAndReadLine(fmt.Sprintf("STREAM INFO %s\r\n", name))
	if err != nil {
		return nil, err
	}

	if !strings.HasPrefix(line, "+STREAM.INFO ") {
		return nil, fmt.Errorf("medelanden: unexpected response: %s", line)
	}

	var state StreamState
	if err := json.Unmarshal([]byte(line[len("+STREAM.INFO "):]), &state); err != nil {
		return nil, fmt.Errorf("medelanden: parse stream info: %w", err)
	}
	return &state, nil
}

// CreateConsumer creates a new consumer on the broker.
func (c *Client) CreateConsumer(cfg ConsumerConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("medelanden: connection closed")
	}

	cfgJSON, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("medelanden: marshal config: %w", err)
	}

	cmd := fmt.Sprintf("CONSUMER CREATE %s\r\n", cfgJSON)
	return c.sendAndReadOK(cmd)
}

// DeleteConsumer deletes a consumer on the broker.
func (c *Client) DeleteConsumer(stream, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("medelanden: connection closed")
	}

	cmd := fmt.Sprintf("CONSUMER DELETE %s %s\r\n", stream, name)
	return c.sendAndReadOK(cmd)
}

// ListConsumers returns the names of all consumers for a stream.
func (c *Client) ListConsumers(stream string) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, fmt.Errorf("medelanden: connection closed")
	}

	return c.sendAndReadJSONList(fmt.Sprintf("CONSUMER LIST %s\r\n", stream), "+CONSUMER.LIST ")
}

// ConsumerInfo returns detailed state about a consumer.
func (c *Client) ConsumerInfo(stream, name string) (*ConsumerState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, fmt.Errorf("medelanden: connection closed")
	}

	line, err := c.sendAndReadLine(fmt.Sprintf("CONSUMER INFO %s %s\r\n", stream, name))
	if err != nil {
		return nil, err
	}

	if !strings.HasPrefix(line, "+CONSUMER.INFO ") {
		return nil, fmt.Errorf("medelanden: unexpected response: %s", line)
	}

	var state ConsumerState
	if err := json.Unmarshal([]byte(line[len("+CONSUMER.INFO "):]), &state); err != nil {
		return nil, fmt.Errorf("medelanden: parse consumer info: %w", err)
	}
	return &state, nil
}

// --- internal helpers ---

// sendAndReadOK sends a command and expects a +OK response.
func (c *Client) sendAndReadOK(cmd string) error {
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout)); err != nil {
		return err
	}

	if _, err := c.writer.WriteString(cmd); err != nil {
		return fmt.Errorf("medelanden: write: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return fmt.Errorf("medelanden: flush: %w", err)
	}

	return c.readOK()
}

// readOK reads one line and checks for +OK (ignoring any trailing fields).
func (c *Client) readOK() error {
	if err := c.conn.SetReadDeadline(time.Now().Add(c.opts.ReadTimeout)); err != nil {
		return err
	}

	line, err := c.reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("medelanden: read response: %w", err)
	}
	line = strings.TrimRight(line, "\r\n")

	if strings.HasPrefix(line, "-ERR ") {
		return fmt.Errorf("medelanden: %s", line[5:])
	}
	if !strings.HasPrefix(line, "+OK") {
		return fmt.Errorf("medelanden: unexpected response: %s", line)
	}
	return nil
}

// readOKSeq reads a +OK <seq> response and returns the sequence number.
func (c *Client) readOKSeq() (uint64, error) {
	if err := c.conn.SetReadDeadline(time.Now().Add(c.opts.ReadTimeout)); err != nil {
		return 0, err
	}

	line, err := c.reader.ReadString('\n')
	if err != nil {
		return 0, fmt.Errorf("medelanden: read response: %w", err)
	}
	line = strings.TrimRight(line, "\r\n")

	if strings.HasPrefix(line, "-ERR ") {
		return 0, fmt.Errorf("medelanden: %s", line[5:])
	}
	if !strings.HasPrefix(line, "+OK") {
		return 0, fmt.Errorf("medelanden: unexpected response: %s", line)
	}

	parts := strings.Fields(line)
	if len(parts) < 2 {
		return 0, nil
	}
	seq, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("medelanden: parse seq: %w", err)
	}
	return seq, nil
}

// sendAndReadLine sends a command and reads back one line, checking for errors.
func (c *Client) sendAndReadLine(cmd string) (string, error) {
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout)); err != nil {
		return "", err
	}

	if _, err := c.writer.WriteString(cmd); err != nil {
		return "", fmt.Errorf("medelanden: write: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return "", fmt.Errorf("medelanden: flush: %w", err)
	}

	if err := c.conn.SetReadDeadline(time.Now().Add(c.opts.ReadTimeout)); err != nil {
		return "", err
	}

	line, err := c.reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("medelanden: read response: %w", err)
	}
	line = strings.TrimRight(line, "\r\n")

	if strings.HasPrefix(line, "-ERR ") {
		return "", fmt.Errorf("medelanden: %s", line[5:])
	}
	return line, nil
}

// sendAndReadJSONList sends a command and reads back a JSON list response.
func (c *Client) sendAndReadJSONList(cmd, prefix string) ([]string, error) {
	line, err := c.sendAndReadLine(cmd)
	if err != nil {
		return nil, err
	}

	if !strings.HasPrefix(line, prefix) {
		return nil, fmt.Errorf("medelanden: unexpected response: %s", line)
	}

	var names []string
	if err := json.Unmarshal([]byte(line[len(prefix):]), &names); err != nil {
		return nil, fmt.Errorf("medelanden: parse list: %w", err)
	}
	return names, nil
}

// readLoop reads individual MSG lines for simple subscriptions.
func (c *Client) readLoop(sub *Subscription) {
	defer func() {
		sub.closeChannels()
		c.subsMu.Lock()
		delete(c.subs, sub.consumer)
		c.subsMu.Unlock()
	}()

	for {
		select {
		case <-sub.done:
			return
		case <-c.closeCh:
			return
		default:
		}

		msg, err := c.readMsg()
		if err != nil {
			return
		}

		select {
		case sub.msgCh <- msg:
		case <-sub.done:
			return
		case <-c.closeCh:
			return
		}
	}
}

// readLoopWindowed reads WBATCH/MSG/WEND frames for windowed subscriptions.
func (c *Client) readLoopWindowed(sub *Subscription) {
	defer func() {
		sub.closeChannels()
		c.subsMu.Lock()
		delete(c.subs, sub.consumer)
		c.subsMu.Unlock()
	}()

	for {
		select {
		case <-sub.done:
			return
		case <-c.closeCh:
			return
		default:
		}

		batch, err := c.readWindowBatch()
		if err != nil {
			return
		}

		select {
		case sub.batchCh <- batch:
		case <-sub.done:
			return
		case <-c.closeCh:
			return
		}
	}
}

// readMsg reads a single MSG frame from the wire.
// Format: MSG <subject> <consumer_name> <producer_ts> <msg_id> <size>\r\n<payload>\r\n
func (c *Client) readMsg() (*Msg, error) {
	c.conn.SetReadDeadline(time.Now().Add(c.opts.ReadTimeout))

	line, err := c.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")

	// Skip PONG keepalives from the server
	if line == "PONG" {
		return c.readMsg()
	}

	parts := strings.Fields(line)
	if len(parts) < 6 || parts[0] != "MSG" {
		return nil, fmt.Errorf("medelanden: unexpected frame: %s", line)
	}

	ts, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("medelanden: parse producer_ts: %w", err)
	}
	size, err := strconv.Atoi(parts[5])
	if err != nil {
		return nil, fmt.Errorf("medelanden: parse size: %w", err)
	}

	payload := make([]byte, size)
	if _, err := io.ReadFull(c.reader, payload); err != nil {
		return nil, fmt.Errorf("medelanden: read payload: %w", err)
	}

	// Read trailing \r\n
	trail := make([]byte, 2)
	if _, err := io.ReadFull(c.reader, trail); err != nil {
		return nil, fmt.Errorf("medelanden: read trailer: %w", err)
	}

	return &Msg{
		Subject:    parts[1],
		Consumer:   parts[2],
		ProducerTS: ts,
		MsgID:      parts[4],
		Payload:    payload,
	}, nil
}

// readWindowBatch reads a complete WBATCH ... WEND frame.
func (c *Client) readWindowBatch() (*WindowBatch, error) {
	c.conn.SetReadDeadline(time.Now().Add(c.opts.ReadTimeout))

	// Read WBATCH header
	line, err := c.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")

	// Skip PONG keepalives
	if line == "PONG" {
		return c.readWindowBatch()
	}

	parts := strings.Fields(line)
	if len(parts) < 5 || parts[0] != "WBATCH" {
		return nil, fmt.Errorf("medelanden: expected WBATCH, got: %s", line)
	}

	windowStart, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("medelanden: parse window_start: %w", err)
	}
	windowEnd, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("medelanden: parse window_end: %w", err)
	}
	msgCount, err := strconv.Atoi(parts[4])
	if err != nil {
		return nil, fmt.Errorf("medelanden: parse msg_count: %w", err)
	}

	batch := &WindowBatch{
		Consumer:    parts[1],
		WindowStart: windowStart,
		WindowEnd:   windowEnd,
		Messages:    make([]*Msg, 0, msgCount),
	}

	// Read messages until WEND
	for {
		c.conn.SetReadDeadline(time.Now().Add(c.opts.ReadTimeout))

		line, err := c.reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimRight(line, "\r\n")

		if line == "WEND" {
			break
		}

		// MSG <subject> <producer_ts> <msg_id> <size>
		msgParts := strings.Fields(line)
		if len(msgParts) < 5 || msgParts[0] != "MSG" {
			return nil, fmt.Errorf("medelanden: unexpected frame in batch: %s", line)
		}

		ts, err := strconv.ParseUint(msgParts[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("medelanden: parse producer_ts: %w", err)
		}
		size, err := strconv.Atoi(msgParts[4])
		if err != nil {
			return nil, fmt.Errorf("medelanden: parse size: %w", err)
		}

		payload := make([]byte, size)
		if _, err := io.ReadFull(c.reader, payload); err != nil {
			return nil, fmt.Errorf("medelanden: read payload: %w", err)
		}

		trail := make([]byte, 2)
		if _, err := io.ReadFull(c.reader, trail); err != nil {
			return nil, fmt.Errorf("medelanden: read trailer: %w", err)
		}

		batch.Messages = append(batch.Messages, &Msg{
			Subject:    msgParts[1],
			ProducerTS: ts,
			MsgID:      msgParts[3],
			Payload:    payload,
			Consumer:   batch.Consumer,
		})
	}

	return batch, nil
}
