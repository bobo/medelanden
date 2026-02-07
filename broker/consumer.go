package broker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Consumer implements the server-side read pipeline with windowing,
// merge, deduplication, and watermark tracking.
type Consumer struct {
	mu     sync.Mutex
	config ConsumerConfig
	stream *Stream

	// Position tracking per source node
	positions map[string]*SourcePosition

	// Watermark tracking
	watermark     uint64 // current watermark (producer_ts)
	sourceProgress map[string]*sourceTracker

	// Window buffer
	windows map[int64]*Window // keyed by window start time

	// Output channel
	outputCh chan *WindowBatch

	// Peer data fetcher
	fetchPeerData func(nodeID string, startSeq uint64) ([]*Message, error)

	// State
	dataDir    string
	stopCh     chan struct{}
	stopped    bool
	nodeID     string
	started    bool

	// Metrics
	lateMessages uint64
	dedupCount   uint64
}

// SourcePosition tracks the consumer's position on a specific source node.
type SourcePosition struct {
	NodeID  string `json:"node_id"`
	LastSeq uint64 `json:"last_seq"`
	LastTS  uint64 `json:"last_ts"`
}

// sourceTracker tracks progress of a data source for watermark calculation.
type sourceTracker struct {
	nodeID   string
	lastTS   uint64
	lastSeen time.Time
	alive    bool
}

// Window is a time-bounded buffer that collects messages for ordering and dedup.
type Window struct {
	StartTS  uint64     `json:"start_ts"`
	EndTS    uint64     `json:"end_ts"`
	Messages []*Message `json:"messages"`
	Closed   bool       `json:"closed"`
}

// WindowBatch is a sorted, deduplicated batch of messages from a single window.
type WindowBatch struct {
	ConsumerName string     `json:"consumer_name"`
	WindowStart  uint64     `json:"window_start"`
	WindowEnd    uint64     `json:"window_end"`
	Messages     []*Message `json:"messages"`
}

// ConsumerPositionMap is the durable state of a consumer.
type ConsumerPositionMap struct {
	Consumer  string                     `json:"consumer"`
	Positions map[string]*SourcePosition `json:"positions"`
	Watermark uint64                     `json:"watermark"`
}

// NewConsumer creates a new durable consumer.
func NewConsumer(config ConsumerConfig, stream *Stream, nodeID, dataDir string) (*Consumer, error) {
	c := &Consumer{
		config:         config,
		stream:         stream,
		positions:      make(map[string]*SourcePosition),
		sourceProgress: make(map[string]*sourceTracker),
		windows:        make(map[int64]*Window),
		outputCh:       make(chan *WindowBatch, 100),
		dataDir:        dataDir,
		stopCh:         make(chan struct{}),
		nodeID:         nodeID,
	}

	// Load saved position
	if err := c.loadPosition(); err != nil {
		// Not fatal - start fresh
	}

	// Initialize source tracker for local node
	c.sourceProgress[nodeID] = &sourceTracker{
		nodeID:   nodeID,
		lastSeen: time.Now(),
		alive:    true,
	}

	return c, nil
}

// SetPeerFetcher sets the function used to fetch data from peers.
func (c *Consumer) SetPeerFetcher(fn func(nodeID string, startSeq uint64) ([]*Message, error)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fetchPeerData = fn
}

// Start begins the consumer's read pipeline.
func (c *Consumer) Start() error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return nil
	}
	c.started = true
	c.mu.Unlock()

	go c.readLoop()
	return nil
}

// Stop stops the consumer.
func (c *Consumer) Stop() error {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return nil
	}
	c.stopped = true
	c.mu.Unlock()

	close(c.stopCh)
	c.savePosition()
	return nil
}

// Output returns the channel that emits ordered, deduplicated window batches.
func (c *Consumer) Output() <-chan *WindowBatch {
	return c.outputCh
}

// Name returns the consumer name.
func (c *Consumer) Name() string {
	return c.config.Name
}

// Watermark returns the current watermark timestamp.
func (c *Consumer) Watermark() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.watermark
}

// Positions returns the current position map.
func (c *Consumer) Positions() map[string]*SourcePosition {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make(map[string]*SourcePosition)
	for k, v := range c.positions {
		cp := *v
		result[k] = &cp
	}
	return result
}

// SetPositions sets the consumer positions (for resume after failover).
func (c *Consumer) SetPositions(positions map[string]*SourcePosition, watermark uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.positions = positions
	c.watermark = watermark
}

// LateMessages returns the count of late messages seen.
func (c *Consumer) LateMessages() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lateMessages
}

// DedupCount returns the count of deduplicated messages.
func (c *Consumer) DedupCount() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dedupCount
}

// readLoop is the main consumer pipeline loop.
func (c *Consumer) readLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			c.collectMessages()
			c.advanceWatermark()
			c.emitWindows()
			c.mu.Unlock()
		case <-c.stopCh:
			return
		}
	}
}

// collectMessages reads new messages from local WAL and peers.
func (c *Consumer) collectMessages() {
	// Read from local WAL
	localPos := c.getPosition(c.nodeID)
	msgs, err := c.stream.ReadFrom(localPos.LastSeq + 1)
	if err == nil {
		for _, msg := range msgs {
			c.bufferMessage(msg)
			if msg.NodeSeq > localPos.LastSeq {
				localPos.LastSeq = msg.NodeSeq
			}
			if msg.ProducerTS > localPos.LastTS {
				localPos.LastTS = msg.ProducerTS
			}
		}
		// Update local source progress
		if tracker, ok := c.sourceProgress[c.nodeID]; ok {
			tracker.lastTS = localPos.LastTS
			tracker.lastSeen = time.Now()
			tracker.alive = true
		}
	}

	// Read from peers (if peer fetcher is configured)
	if c.fetchPeerData != nil {
		for nodeID, tracker := range c.sourceProgress {
			if nodeID == c.nodeID {
				continue
			}
			if !tracker.alive {
				continue
			}
			pos := c.getPosition(nodeID)
			peerMsgs, err := c.fetchPeerData(nodeID, pos.LastSeq+1)
			if err != nil {
				// Mark peer as potentially failing
				continue
			}
			for _, msg := range peerMsgs {
				c.bufferMessage(msg)
				if msg.NodeSeq > pos.LastSeq {
					pos.LastSeq = msg.NodeSeq
				}
				if msg.ProducerTS > pos.LastTS {
					pos.LastTS = msg.ProducerTS
				}
			}
			tracker.lastTS = pos.LastTS
			tracker.lastSeen = time.Now()
		}
	}
}

// bufferMessage places a message into the appropriate time window.
func (c *Consumer) bufferMessage(msg *Message) {
	// Check if this is a late arrival
	if c.watermark > 0 && msg.ProducerTS < c.watermark {
		c.lateMessages++
		if c.config.LatePolicy == LatePolicyDrop {
			return
		}
		// emit_unordered: mark as late and send directly
		if c.config.LatePolicy == LatePolicyEmitUnordered {
			lateCopy := *msg
			lateCopy.Late = true
			batch := &WindowBatch{
				ConsumerName: c.config.Name,
				WindowStart:  msg.ProducerTS,
				WindowEnd:    msg.ProducerTS,
				Messages:     []*Message{&lateCopy},
			}
			select {
			case c.outputCh <- batch:
			default:
			}
			return
		}
	}

	// Determine the window for this message
	windowDur := uint64(c.config.WindowDuration.Nanoseconds())
	if windowDur == 0 {
		windowDur = uint64(2 * time.Second)
	}
	windowStart := int64((msg.ProducerTS / windowDur) * windowDur)

	w, ok := c.windows[windowStart]
	if !ok {
		w = &Window{
			StartTS:  uint64(windowStart),
			EndTS:    uint64(windowStart) + windowDur,
			Messages: make([]*Message, 0, 64),
		}
		c.windows[windowStart] = w
	}

	if !w.Closed {
		w.Messages = append(w.Messages, msg)
	}
}

// getPosition returns the position for a source, creating it if needed.
func (c *Consumer) getPosition(nodeID string) *SourcePosition {
	pos, ok := c.positions[nodeID]
	if !ok {
		pos = &SourcePosition{NodeID: nodeID}
		c.positions[nodeID] = pos
	}
	return pos
}

// advanceWatermark updates the watermark based on source progress.
func (c *Consumer) advanceWatermark() {
	if len(c.sourceProgress) == 0 {
		return
	}

	now := time.Now()
	timeout := c.config.WatermarkTimeout

	var minTS uint64
	first := true

	for _, tracker := range c.sourceProgress {
		// Exclude dead sources
		if !tracker.alive && now.Sub(tracker.lastSeen) > timeout {
			continue
		}
		// If source hasn't been seen in timeout, mark it dead
		if tracker.alive && now.Sub(tracker.lastSeen) > timeout {
			tracker.alive = false
			continue
		}

		if tracker.lastTS == 0 {
			continue
		}

		if first || tracker.lastTS < minTS {
			minTS = tracker.lastTS
			first = false
		}
	}

	if first {
		return // no live sources with progress
	}

	// Watermark = min progress - window duration
	windowNanos := uint64(c.config.WindowDuration.Nanoseconds())
	if minTS > windowNanos {
		newWatermark := minTS - windowNanos
		if newWatermark > c.watermark {
			c.watermark = newWatermark
		}
	}
}

// emitWindows emits all windows that are behind the watermark.
func (c *Consumer) emitWindows() {
	if c.watermark == 0 {
		return
	}

	// Collect closeable windows
	var toEmit []int64
	for startTS, w := range c.windows {
		if !w.Closed && w.EndTS <= c.watermark {
			toEmit = append(toEmit, startTS)
		}
	}

	// Sort by start time
	sort.Slice(toEmit, func(i, j int) bool {
		return toEmit[i] < toEmit[j]
	})

	for _, startTS := range toEmit {
		w := c.windows[startTS]
		w.Closed = true

		// Sort messages by producer_ts, then by msg_id for stability
		sort.Slice(w.Messages, func(i, j int) bool {
			if w.Messages[i].ProducerTS != w.Messages[j].ProducerTS {
				return w.Messages[i].ProducerTS < w.Messages[j].ProducerTS
			}
			return w.Messages[i].MsgID < w.Messages[j].MsgID
		})

		// Deduplicate
		dedupedMsgs := c.dedup(w.Messages)

		if len(dedupedMsgs) > 0 {
			batch := &WindowBatch{
				ConsumerName: c.config.Name,
				WindowStart:  w.StartTS,
				WindowEnd:    w.EndTS,
				Messages:     dedupedMsgs,
			}

			select {
			case c.outputCh <- batch:
			default:
				// Channel full, drop oldest
			}
		}

		// Remove the window to free memory
		delete(c.windows, startTS)
	}
}

// dedup removes duplicate messages based on the configured dedup key.
func (c *Consumer) dedup(msgs []*Message) []*Message {
	if len(msgs) <= 1 {
		return msgs
	}

	seen := make(map[string]struct{}, len(msgs))
	result := make([]*Message, 0, len(msgs))

	for _, msg := range msgs {
		key := msg.EffectiveDedupKey(c.config.DedupKey)
		if _, exists := seen[key]; exists {
			c.dedupCount++
			continue
		}
		seen[key] = struct{}{}
		result = append(result, msg)
	}

	return result
}

// AddSource adds a new data source for watermark tracking.
func (c *Consumer) AddSource(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.sourceProgress[nodeID]; !ok {
		c.sourceProgress[nodeID] = &sourceTracker{
			nodeID:   nodeID,
			lastSeen: time.Now(),
			alive:    true,
		}
	}
}

// RemoveSource marks a source as dead.
func (c *Consumer) RemoveSource(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if tracker, ok := c.sourceProgress[nodeID]; ok {
		tracker.alive = false
	}
}

// positionFilePath returns the file path for persisting consumer position.
func (c *Consumer) positionFilePath() string {
	return filepath.Join(c.dataDir, "consumers", c.config.Name+".json")
}

// savePosition persists the consumer's position to disk.
func (c *Consumer) savePosition() error {
	c.mu.Lock()
	posMap := ConsumerPositionMap{
		Consumer:  c.config.Name,
		Positions: c.positions,
		Watermark: c.watermark,
	}
	c.mu.Unlock()

	data, err := json.MarshalIndent(posMap, "", "  ")
	if err != nil {
		return err
	}

	dir := filepath.Dir(c.positionFilePath())
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	return os.WriteFile(c.positionFilePath(), data, 0644)
}

// loadPosition loads the consumer's position from disk.
func (c *Consumer) loadPosition() error {
	data, err := os.ReadFile(c.positionFilePath())
	if err != nil {
		return err
	}

	var posMap ConsumerPositionMap
	if err := json.Unmarshal(data, &posMap); err != nil {
		return err
	}

	c.positions = posMap.Positions
	c.watermark = posMap.Watermark
	return nil
}

// InjectMessages directly buffers messages into the consumer's windows.
// Used for testing and for the read path when data is already available.
func (c *Consumer) InjectMessages(msgs []*Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, msg := range msgs {
		c.bufferMessage(msg)
	}
}

// UpdateSourceProgress updates the progress tracker for a source.
func (c *Consumer) UpdateSourceProgress(nodeID string, lastTS uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	tracker, ok := c.sourceProgress[nodeID]
	if !ok {
		tracker = &sourceTracker{
			nodeID: nodeID,
			alive:  true,
		}
		c.sourceProgress[nodeID] = tracker
	}
	tracker.lastTS = lastTS
	tracker.lastSeen = time.Now()
	tracker.alive = true
}

// ForceAdvanceWatermark sets the watermark to a specific value.
// Used for testing.
func (c *Consumer) ForceAdvanceWatermark(ts uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.watermark = ts
}

// FlushWindows forces emission of all closeable windows.
func (c *Consumer) FlushWindows() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.emitWindows()
}
