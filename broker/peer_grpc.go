package broker

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	pb "medelanden/proto/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// PeerGRPCServer implements the gRPC PeerService for inter-node communication.
type PeerGRPCServer struct {
	pb.UnimplementedPeerServiceServer

	nodeID     string
	peerAddr   string
	cluster    *Cluster
	getStreams func() map[string]*Stream
	getState   func() ClusterState

	server   *grpc.Server
	listener net.Listener
}

// NewPeerGRPCServer creates a new gRPC peer server.
func NewPeerGRPCServer(nodeID, peerAddr string, cluster *Cluster, getStreams func() map[string]*Stream, getState func() ClusterState) *PeerGRPCServer {
	return &PeerGRPCServer{
		nodeID:     nodeID,
		peerAddr:   peerAddr,
		cluster:    cluster,
		getStreams:  getStreams,
		getState:   getState,
	}
}

// Start starts the gRPC server.
func (s *PeerGRPCServer) Start() error {
	ln, err := net.Listen("tcp", s.peerAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.peerAddr, err)
	}
	s.listener = ln

	s.server = grpc.NewServer()
	pb.RegisterPeerServiceServer(s.server, s)

	go func() {
		if err := s.server.Serve(ln); err != nil {
			// Log but don't crash
		}
	}()

	return nil
}

// Stop stops the gRPC server.
func (s *PeerGRPCServer) Stop() {
	if s.server != nil {
		s.server.GracefulStop()
	}
}

// Gossip implements the PeerService Gossip RPC.
func (s *PeerGRPCServer) Gossip(ctx context.Context, req *pb.GossipRequest) (*pb.GossipResponse, error) {
	// Update peer info from the incoming gossip
	if req.NodeId != s.nodeID {
		s.cluster.updatePeerFromGossip(req.NodeId, req.PeerAddr, req.Peers)
	}

	// Build our response
	state := s.getState()
	resp := &pb.GossipResponse{
		NodeId:   state.NodeID,
		PeerAddr: state.PeerAddr,
		Peers:    state.Peers,
	}

	for _, sd := range state.Streams {
		resp.StreamDigests = append(resp.StreamDigests, &pb.StreamDigestProto{
			Stream: sd.Stream,
			NodeId: sd.NodeID,
			MaxSeq: sd.MaxSeq,
			MaxTs:  sd.MaxTS,
		})
	}

	return resp, nil
}

// ExchangeDigest implements the PeerService ExchangeDigest RPC.
func (s *PeerGRPCServer) ExchangeDigest(ctx context.Context, req *pb.DigestRequest) (*pb.DigestResponse, error) {
	streams := s.getStreams()

	resp := &pb.DigestResponse{
		NodeId:  s.nodeID,
		Digests: make(map[string]*pb.StreamDigestProto),
	}

	for name, stream := range streams {
		d := stream.Digest()
		d.NodeID = s.nodeID
		resp.Digests[name] = &pb.StreamDigestProto{
			Stream: d.Stream,
			NodeId: d.NodeID,
			MaxSeq: d.MaxSeq,
			MaxTs:  d.MaxTS,
		}
	}

	return resp, nil
}

// PullMessages implements the PeerService PullMessages RPC.
func (s *PeerGRPCServer) PullMessages(ctx context.Context, req *pb.PullRequest) (*pb.PullResponse, error) {
	streams := s.getStreams()
	stream, ok := streams[req.Stream]
	if !ok {
		return nil, fmt.Errorf("stream %q not found", req.Stream)
	}

	limit := int(req.Limit)
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}

	endSeq := req.EndSeq
	if endSeq == 0 || endSeq > stream.LastSeq() {
		endSeq = stream.LastSeq()
	}
	if req.StartSeq+uint64(limit)-1 < endSeq {
		endSeq = req.StartSeq + uint64(limit) - 1
	}

	msgs, err := stream.Read(req.StartSeq, endSeq)
	if err != nil {
		return nil, err
	}

	resp := &pb.PullResponse{
		Stream: req.Stream,
		NodeId: s.nodeID,
	}

	for _, msg := range msgs {
		resp.Messages = append(resp.Messages, msgToProto(msg))
	}

	return resp, nil
}

// Ping implements the PeerService Ping RPC.
func (s *PeerGRPCServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{NodeId: s.nodeID}, nil
}

// --- gRPC Client Pool ---

// PeerGRPCClient manages gRPC connections to peers.
type PeerGRPCClient struct {
	mu    sync.RWMutex
	conns map[string]*grpc.ClientConn
}

// NewPeerGRPCClient creates a new client pool.
func NewPeerGRPCClient() *PeerGRPCClient {
	return &PeerGRPCClient{
		conns: make(map[string]*grpc.ClientConn),
	}
}

// GetClient returns a PeerService client for the given address.
func (c *PeerGRPCClient) GetClient(addr string) (pb.PeerServiceClient, error) {
	c.mu.RLock()
	conn, ok := c.conns[addr]
	c.mu.RUnlock()

	if ok {
		return pb.NewPeerServiceClient(conn), nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock
	if conn, ok := c.conns[addr]; ok {
		return pb.NewPeerServiceClient(conn), nil
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to peer %s: %w", addr, err)
	}

	c.conns[addr] = conn
	return pb.NewPeerServiceClient(conn), nil
}

// Close closes all connections.
func (c *PeerGRPCClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.conns {
		conn.Close()
	}
	c.conns = make(map[string]*grpc.ClientConn)
}

// --- Helper conversions ---

func msgToProto(msg *Message) *pb.Msg {
	return &pb.Msg{
		Subject:    msg.Subject,
		Payload:    msg.Payload,
		ProducerTs: msg.ProducerTS,
		DedupKey:   msg.DedupKey,
		MsgId:      msg.MsgID,
		NodeSeq:    msg.NodeSeq,
		NodeId:     msg.NodeID,
		Late:       msg.Late,
	}
}

func protoToMsg(m *pb.Msg) *Message {
	return &Message{
		Subject:    m.Subject,
		Payload:    m.Payload,
		ProducerTS: m.ProducerTs,
		DedupKey:   m.DedupKey,
		MsgID:      m.MsgId,
		NodeSeq:    m.NodeSeq,
		NodeID:     m.NodeId,
		Late:       m.Late,
	}
}

// --- gRPC-based Replicator ---

// GRPCReplicator handles anti-entropy replication over gRPC.
type GRPCReplicator struct {
	mu       sync.RWMutex
	nodeID   string
	cluster  *Cluster
	streams  func() map[string]*Stream
	client   *PeerGRPCClient
	interval time.Duration
	stopCh   chan struct{}
	stopped  bool

	promMetrics *Metrics
}

// NewGRPCReplicator creates a new gRPC-based replicator.
func NewGRPCReplicator(nodeID string, cluster *Cluster, streams func() map[string]*Stream, promMetrics *Metrics) *GRPCReplicator {
	return &GRPCReplicator{
		nodeID:      nodeID,
		cluster:     cluster,
		streams:     streams,
		client:      NewPeerGRPCClient(),
		interval:    1 * time.Second,
		stopCh:      make(chan struct{}),
		promMetrics: promMetrics,
	}
}

// Start begins the anti-entropy loop.
func (r *GRPCReplicator) Start() {
	go r.antiEntropyLoop()
}

// Stop stops the replicator and closes connections.
func (r *GRPCReplicator) Stop() {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	r.mu.Unlock()
	close(r.stopCh)
	r.client.Close()
}

func (r *GRPCReplicator) antiEntropyLoop() {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.antiEntropyRound()
		case <-r.stopCh:
			return
		}
	}
}

func (r *GRPCReplicator) antiEntropyRound() {
	peers := r.cluster.LivePeers()
	if len(peers) == 0 {
		return
	}

	streams := r.streams()

	for _, peer := range peers {
		if peer.ID == r.nodeID || peer.PeerAddr == "" {
			continue
		}

		go r.syncWithPeer(peer, streams)
	}
}

func (r *GRPCReplicator) syncWithPeer(peer *PeerInfo, streams map[string]*Stream) {
	client, err := r.client.GetClient(peer.PeerAddr)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Build our digests
	req := &pb.DigestRequest{
		NodeId:  r.nodeID,
		Digests: make(map[string]*pb.StreamDigestProto),
	}
	for name, s := range streams {
		d := s.Digest()
		req.Digests[name] = &pb.StreamDigestProto{
			Stream: d.Stream,
			NodeId: r.nodeID,
			MaxSeq: d.MaxSeq,
			MaxTs:  d.MaxTS,
		}
	}

	resp, err := client.ExchangeDigest(ctx, req)
	if err != nil {
		return
	}

	// Find streams where the peer has more data
	for streamName, peerDigest := range resp.Digests {
		stream, ok := streams[streamName]
		if !ok {
			continue
		}

		ourSeq := stream.LastSeq()
		if peerDigest.MaxSeq > ourSeq {
			r.pullMessages(client, streamName, ourSeq+1, peerDigest.MaxSeq, stream)
		}
	}
}

func (r *GRPCReplicator) pullMessages(client pb.PeerServiceClient, streamName string, startSeq, endSeq uint64, stream *Stream) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.PullMessages(ctx, &pb.PullRequest{
		Stream:   streamName,
		StartSeq: startSeq,
		EndSeq:   endSeq,
		Limit:    1000,
	})
	if err != nil {
		return
	}

	for _, m := range resp.Messages {
		msg := protoToMsg(m)
		stream.PublishReplicated(msg)
	}

	if r.promMetrics != nil && len(resp.Messages) > 0 {
		r.promMetrics.ReplicatedMsgsTotal.WithLabelValues(streamName, resp.NodeId).Add(float64(len(resp.Messages)))
		r.promMetrics.ReplicationPulls.WithLabelValues(streamName, resp.NodeId).Inc()
	}
}

// PullFromPeer pulls messages from a peer for the read path.
func (r *GRPCReplicator) PullFromPeer(peer *PeerInfo, streamName string, startSeq uint64, limit int) ([]*Message, error) {
	client, err := r.client.GetClient(peer.PeerAddr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.PullMessages(ctx, &pb.PullRequest{
		Stream:   streamName,
		StartSeq: startSeq,
		Limit:    int32(limit),
	})
	if err != nil {
		return nil, err
	}

	msgs := make([]*Message, 0, len(resp.Messages))
	for _, m := range resp.Messages {
		msgs = append(msgs, protoToMsg(m))
	}
	return msgs, nil
}
