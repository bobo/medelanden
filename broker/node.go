package broker

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// Node is a single Medelanden broker instance. It ties together streams,
// cluster membership, replication, and consumers.
type Node struct {
	mu     sync.RWMutex
	config NodeConfig
	logger *zap.Logger

	streams       map[string]*Stream
	consumers     map[string]*Consumer
	cluster       *Cluster
	replicator    *Replicator
	grpcReplicator *GRPCReplicator
	peerServer    *PeerGRPCServer

	// Internal counters (kept for the JSON metrics endpoint)
	writesTotal atomic.Uint64
	writeLatSum atomic.Int64 // sum of write latencies in nanoseconds
	writeCount  atomic.Int64

	// Prometheus metrics
	promMetrics *Metrics
	promReg     *prometheus.Registry

	started bool
	stopped bool
	stopCh  chan struct{}
}

// NodeInfo is returned by the health endpoint.
type NodeInfo struct {
	Status  string                `json:"status"`
	NodeID  string                `json:"node_id"`
	Peers   map[string]PeerStatus `json:"peers"`
	Streams map[string]StreamInfo `json:"streams"`
}

// PeerStatus is the health status of a peer.
type PeerStatus struct {
	Status string `json:"status"`
	LagMs  int64  `json:"lag_ms"`
}

// StreamInfo is info about a stream on this node.
type StreamInfo struct {
	LocalMessages     uint64 `json:"local_messages"`
	ReplicationStatus string `json:"replication_status"`
}

// NodeMetrics contains metrics for the node.
type NodeMetrics struct {
	WritesTotal     uint64             `json:"writes_total"`
	AvgWriteLatUs   int64              `json:"avg_write_latency_us"`
	Streams         map[string]uint64  `json:"stream_message_counts"`
	PeerStatus      map[string]int     `json:"peer_status"`
	ConsumerMetrics map[string]ConsumerMetricSnapshot `json:"consumer_metrics"`
}

// ConsumerMetricSnapshot is a point-in-time snapshot of consumer metrics.
type ConsumerMetricSnapshot struct {
	Watermark    uint64 `json:"watermark"`
	LateMessages uint64 `json:"late_messages"`
	DedupCount   uint64 `json:"dedup_count"`
}

// NewNode creates a new broker node.
func NewNode(config NodeConfig) (*Node, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid node config: %w", err)
	}

	logger := config.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	if config.Limits == (LimitsConfig{}) {
		config.Limits = DefaultLimitsConfig()
	}

	MaxMessageSize = config.Limits.MaxMessageSize

	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	n := &Node{
		config:      config,
		logger:      logger,
		streams:     make(map[string]*Stream),
		consumers:   make(map[string]*Consumer),
		stopCh:      make(chan struct{}),
		promMetrics: m,
		promReg:     reg,
	}

	return n, nil
}

// PrometheusRegistry returns the Prometheus registry for this node.
func (n *Node) PrometheusRegistry() *prometheus.Registry {
	return n.promReg
}

// PromMetrics returns the Prometheus metrics for this node.
func (n *Node) PromMetrics() *Metrics {
	return n.promMetrics
}

// Start initializes cluster membership and replication.
func (n *Node) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.started {
		return nil
	}

	// Initialize cluster if peer address is configured
	if n.config.PeerAddr != "" {
		advertiseAddr := n.config.EffectiveAdvertisePeerAddr()
		clusterCfg := DefaultClusterConfig()
		n.cluster = NewCluster(n.config.ID, advertiseAddr, clusterCfg, n.logger.Named("cluster"))
		n.cluster.SetMetrics(n.promMetrics)
		n.cluster.SetStateFunc(n.clusterState)

		// Build TLS configs for peer connections
		var serverTLSCfg, clientTLSCfg *tls.Config
		if n.config.TLS.Enabled {
			var err error
			serverTLSCfg, err = BuildServerTLSConfig(n.config.TLS)
			if err != nil {
				return fmt.Errorf("build server TLS config: %w", err)
			}
			clientTLSCfg, err = BuildClientTLSConfig(n.config.TLS)
			if err != nil {
				return fmt.Errorf("build client TLS config: %w", err)
			}
			n.cluster.SetPeerTLS(clientTLSCfg)
		}

		// Start gRPC peer server on the bind address, but advertise the routable address
		n.peerServer = NewPeerGRPCServer(n.config.ID, n.config.PeerAddr, n.cluster, n.getStreams, n.clusterState, serverTLSCfg, n.logger.Named("peer-grpc"))
		if err := n.peerServer.Start(); err != nil {
			return fmt.Errorf("start gRPC peer server: %w", err)
		}

		// Start background gossip and failure detection
		n.cluster.StartBackground()

		if len(n.config.Seeds) > 0 {
			if err := n.cluster.Join(n.config.Seeds); err != nil {
				return fmt.Errorf("join cluster: %w", err)
			}
		}

		// Start gRPC-based replication
		n.grpcReplicator = NewGRPCReplicator(n.config.ID, n.cluster, n.getStreams, n.promMetrics, clientTLSCfg, n.logger.Named("replicator"))
		n.grpcReplicator.Start()
	}

	n.started = true
	n.logger.Info("node started", zap.String("id", n.config.ID))

	go n.metricsCollectorLoop()

	return nil
}

// Stop shuts down the node. It is safe to call multiple times.
func (n *Node) Stop(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.stopped {
		return nil
	}
	n.stopped = true
	n.logger.Info("node stopping", zap.String("id", n.config.ID))

	close(n.stopCh)

	// Stop consumers
	for _, c := range n.consumers {
		c.Stop()
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Stop gRPC replicator
	if n.grpcReplicator != nil {
		n.grpcReplicator.Stop()
	}

	// Stop legacy replicator
	if n.replicator != nil {
		n.replicator.Stop()
	}

	// Stop gRPC peer server
	if n.peerServer != nil {
		n.peerServer.Stop(ctx)
	}

	// Stop cluster
	if n.cluster != nil {
		n.cluster.Stop()
	}

	// Close streams
	for _, s := range n.streams {
		s.Close()
	}

	return nil
}

// metricsCollectorLoop periodically collects gauge-style Prometheus metrics.
func (n *Node) metricsCollectorLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			n.collectPrometheusMetrics()
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) collectPrometheusMetrics() {
	n.mu.RLock()
	for name, s := range n.streams {
		n.promMetrics.StreamMessages.WithLabelValues(name).Set(float64(s.MessageCount()))
	}
	for name, c := range n.consumers {
		n.promMetrics.ConsumerWatermark.WithLabelValues(name).Set(float64(c.Watermark()))
	}
	n.mu.RUnlock()

	if n.cluster != nil {
		alive, slow, dead := 0, 0, 0
		for _, p := range n.cluster.Peers() {
			switch p.State {
			case PeerAlive:
				alive++
			case PeerSlow:
				slow++
			case PeerDead:
				dead++
			}
		}
		n.promMetrics.ClusterPeers.WithLabelValues("alive").Set(float64(alive))
		n.promMetrics.ClusterPeers.WithLabelValues("slow").Set(float64(slow))
		n.promMetrics.ClusterPeers.WithLabelValues("dead").Set(float64(dead))
	}
}

// Logger returns the node's logger.
func (n *Node) Logger() *zap.Logger {
	return n.logger
}

// ID returns the node's unique identifier.
func (n *Node) ID() string {
	return n.config.ID
}

func (n *Node) Config() NodeConfig {
	return n.config
}

// CreateStream creates and registers a new stream on this node.
func (n *Node) CreateStream(cfg StreamConfig) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid stream config: %w", err)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if len(n.streams) >= n.config.Limits.MaxStreams {
		return fmt.Errorf("maximum number of streams (%d) reached", n.config.Limits.MaxStreams)
	}

	if _, exists := n.streams[cfg.Name]; exists {
		return fmt.Errorf("stream %s already exists", cfg.Name)
	}

	stream, err := NewStream(cfg, n.config.DataDir)
	if err != nil {
		return err
	}

	n.streams[cfg.Name] = stream
	n.logger.Info("stream created", zap.String("stream", cfg.Name))
	return nil
}

// GetStream returns a stream by name.
func (n *Node) GetStream(name string) *Stream {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.streams[name]
}

// Publish accepts a message, routes it to the matching stream, and returns
// the assigned sequence number. This is the hot path.
func (n *Node) Publish(msg *Message) (uint64, error) {
	start := time.Now()

	if msg.MsgID == "" {
		msg.MsgID = GenerateID()
	}
	msg.NodeID = n.config.ID

	if msg.DedupKey == "" {
		msg.DedupKey = msg.Subject + "|" + fmt.Sprintf("%d", msg.ProducerTS)
	}

	// Find matching stream
	n.mu.RLock()
	var targetStream *Stream
	for _, s := range n.streams {
		if s.MatchSubject(msg.Subject) {
			targetStream = s
			break
		}
	}
	n.mu.RUnlock()

	if targetStream == nil {
		return 0, fmt.Errorf("no stream matches subject %q", msg.Subject)
	}

	seq, err := targetStream.Publish(msg)
	if err != nil {
		n.promMetrics.PublishErrors.WithLabelValues(targetStream.Name()).Inc()
		return 0, err
	}

	n.writesTotal.Add(1)
	latency := time.Since(start)
	n.writeLatSum.Add(latency.Nanoseconds())
	n.writeCount.Add(1)

	streamName := targetStream.Name()
	n.promMetrics.PublishTotal.WithLabelValues(streamName).Inc()
	n.promMetrics.PublishLatency.WithLabelValues(streamName).Observe(latency.Seconds())

	return seq, nil
}

// FindStreamForSubject returns the name of the stream matching a subject.
func (n *Node) FindStreamForSubject(subject string) string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	for _, s := range n.streams {
		if s.MatchSubject(subject) {
			return s.Name()
		}
	}
	return ""
}

// CreateConsumer creates a new durable consumer.
func (n *Node) CreateConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid consumer config: %w", err)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if _, ok := n.consumers[cfg.Name]; !ok && len(n.consumers) >= n.config.Limits.MaxConsumers {
		return nil, fmt.Errorf("maximum number of consumers (%d) reached", n.config.Limits.MaxConsumers)
	}

	if existing, ok := n.consumers[cfg.Name]; ok {
		return existing, nil
	}

	stream, ok := n.streams[cfg.Stream]
	if !ok {
		return nil, fmt.Errorf("stream %s not found", cfg.Stream)
	}

	consumer, err := NewConsumer(cfg, stream, n.config.ID, n.config.DataDir)
	if err != nil {
		return nil, err
	}
	consumer.SetMetrics(n.promMetrics)

	// Add peer sources if cluster is available
	if n.cluster != nil {
		for _, peer := range n.cluster.LivePeers() {
			consumer.AddSource(peer.ID)
		}
	}

	if err := consumer.Start(); err != nil {
		return nil, err
	}

	n.consumers[cfg.Name] = consumer
	n.logger.Info("consumer created", zap.String("consumer", cfg.Name), zap.String("stream", cfg.Stream))
	return consumer, nil
}

// GetConsumer returns a consumer by name.
func (n *Node) GetConsumer(name string) *Consumer {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.consumers[name]
}

// Info returns the node's health information.
func (n *Node) Info() NodeInfo {
	info := NodeInfo{
		Status:  "ok",
		NodeID:  n.config.ID,
		Peers:   make(map[string]PeerStatus),
		Streams: make(map[string]StreamInfo),
	}

	if n.cluster != nil {
		for _, p := range n.cluster.Peers() {
			status := "dead"
			if p.State == PeerAlive {
				status = "alive"
			} else if p.State == PeerSlow {
				status = "slow"
			}
			info.Peers[p.ID] = PeerStatus{
				Status: status,
				LagMs:  p.LagMs,
			}
		}
	}

	n.mu.RLock()
	for name, s := range n.streams {
		info.Streams[name] = StreamInfo{
			LocalMessages:     s.MessageCount(),
			ReplicationStatus: "ok",
		}
	}
	n.mu.RUnlock()

	return info
}

// Metrics returns the node's metrics.
func (n *Node) Metrics() NodeMetrics {
	metrics := NodeMetrics{
		WritesTotal:     n.writesTotal.Load(),
		Streams:         make(map[string]uint64),
		PeerStatus:      make(map[string]int),
		ConsumerMetrics: make(map[string]ConsumerMetricSnapshot),
	}

	count := n.writeCount.Load()
	if count > 0 {
		metrics.AvgWriteLatUs = (n.writeLatSum.Load() / count) / 1000
	}

	n.mu.RLock()
	for name, s := range n.streams {
		metrics.Streams[name] = s.MessageCount()
	}
	for name, c := range n.consumers {
		metrics.ConsumerMetrics[name] = ConsumerMetricSnapshot{
			Watermark:    c.Watermark(),
			LateMessages: c.LateMessages(),
			DedupCount:   c.DedupCount(),
		}
	}
	n.mu.RUnlock()

	if n.cluster != nil {
		for _, p := range n.cluster.Peers() {
			metrics.PeerStatus[p.ID] = int(p.State)
		}
	}

	return metrics
}

// DeleteStream removes a stream and all its consumers.
func (n *Node) DeleteStream(name string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	stream, ok := n.streams[name]
	if !ok {
		return fmt.Errorf("stream %s not found", name)
	}

	// Stop and remove all consumers bound to this stream
	for cname, c := range n.consumers {
		if c.config.Stream == name {
			c.Stop()
			delete(n.consumers, cname)
		}
	}

	stream.Close()
	delete(n.streams, name)
	n.logger.Info("stream deleted", zap.String("stream", name))
	return nil
}

// ListStreams returns the names of all streams.
func (n *Node) ListStreams() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	names := make([]string, 0, len(n.streams))
	for name := range n.streams {
		names = append(names, name)
	}
	return names
}

// StreamState returns detailed state for a stream.
func (n *Node) StreamState(name string) (*StreamState, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	stream, ok := n.streams[name]
	if !ok {
		return nil, fmt.Errorf("stream %s not found", name)
	}

	consumerCount := 0
	for _, c := range n.consumers {
		if c.config.Stream == name {
			consumerCount++
		}
	}

	firstSeq := uint64(0)
	lastSeq := stream.LastSeq()
	if lastSeq > 0 {
		firstSeq = 1
	}

	return &StreamState{
		Name:          name,
		Config:        stream.Config(),
		Messages:      stream.MessageCount(),
		FirstSeq:      firstSeq,
		LastSeq:       lastSeq,
		ConsumerCount: consumerCount,
	}, nil
}

// DeleteConsumer stops and removes a consumer.
func (n *Node) DeleteConsumer(stream, name string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	consumer, ok := n.consumers[name]
	if !ok {
		return fmt.Errorf("consumer %s not found", name)
	}

	if consumer.config.Stream != stream {
		return fmt.Errorf("consumer %s belongs to stream %s, not %s", name, consumer.config.Stream, stream)
	}

	consumer.Stop()
	delete(n.consumers, name)
	n.logger.Info("consumer deleted", zap.String("consumer", name), zap.String("stream", stream))
	return nil
}

// ListConsumers returns the names of all consumers for a given stream.
func (n *Node) ListConsumers(stream string) []string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	var names []string
	for name, c := range n.consumers {
		if c.config.Stream == stream {
			names = append(names, name)
		}
	}
	return names
}

// ConsumerState returns detailed state for a consumer.
func (n *Node) ConsumerState(stream, name string) (*ConsumerState, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	consumer, ok := n.consumers[name]
	if !ok {
		return nil, fmt.Errorf("consumer %s not found", name)
	}

	if consumer.config.Stream != stream {
		return nil, fmt.Errorf("consumer %s belongs to stream %s, not %s", name, consumer.config.Stream, stream)
	}

	return &ConsumerState{
		Name:         name,
		Stream:       stream,
		Config:       consumer.config,
		Watermark:    consumer.Watermark(),
		LateMessages: consumer.LateMessages(),
		DedupCount:   consumer.DedupCount(),
		Positions:    consumer.Positions(),
	}, nil
}

// clusterState returns the current cluster state for gossip exchange.
func (n *Node) clusterState() ClusterState {
	state := ClusterState{
		NodeID:   n.config.ID,
		PeerAddr: n.config.EffectiveAdvertisePeerAddr(),
		Peers:    make(map[string]string),
	}

	if n.cluster != nil {
		for _, p := range n.cluster.Peers() {
			if p.ID != "" {
				state.Peers[p.ID] = p.PeerAddr
			}
		}
	}

	n.mu.RLock()
	for _, s := range n.streams {
		d := s.Digest()
		d.NodeID = n.config.ID
		state.Streams = append(state.Streams, d)
	}
	n.mu.RUnlock()

	return state
}

// getStreams returns a snapshot of the current streams map.
func (n *Node) getStreams() map[string]*Stream {
	n.mu.RLock()
	defer n.mu.RUnlock()
	result := make(map[string]*Stream, len(n.streams))
	for k, v := range n.streams {
		result[k] = v
	}
	return result
}
