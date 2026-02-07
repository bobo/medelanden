package broker

import (
	"context"
	"crypto/tls"
	"sync"
	"time"

	pb "medelanden/proto/pb"

	"go.uber.org/zap"
)

// PeerState represents the health state of a peer.
type PeerState int

const (
	PeerDead  PeerState = 0
	PeerAlive PeerState = 1
	PeerSlow  PeerState = 2
)

// PeerInfo holds metadata about a cluster peer.
type PeerInfo struct {
	ID       string    `json:"id"`
	PeerAddr string    `json:"peer_addr"`
	State    PeerState `json:"state"`
	LastSeen time.Time `json:"last_seen"`
	LagMs    int64     `json:"lag_ms"`
}

// Cluster manages peer discovery and membership via gRPC-based gossip.
type Cluster struct {
	mu     sync.RWMutex
	nodeID string
	addr   string // this node's peer address
	peers  map[string]*PeerInfo
	config ClusterConfig
	logger *zap.Logger

	getState func() ClusterState
	client   *PeerGRPCClient

	onJoin  func(peer *PeerInfo)
	onLeave func(peer *PeerInfo)

	promMetrics *Metrics

	stopCh  chan struct{}
	stopped bool
}

// ClusterState is exchanged between peers during gossip.
type ClusterState struct {
	NodeID   string            `json:"node_id"`
	PeerAddr string            `json:"peer_addr"`
	Streams  []StreamDigest    `json:"streams,omitempty"`
	Peers    map[string]string `json:"peers"` // nodeID -> peerAddr
}

// NewCluster creates a new cluster membership manager.
func NewCluster(nodeID, addr string, config ClusterConfig, logger *zap.Logger) *Cluster {
	return &Cluster{
		nodeID: nodeID,
		addr:   addr,
		peers:  make(map[string]*PeerInfo),
		config: config,
		logger: logger,
		client: NewPeerGRPCClient(nil),
		stopCh: make(chan struct{}),
	}
}

// SetPeerTLS replaces the cluster's gRPC client with one configured for TLS.
func (c *Cluster) SetPeerTLS(cfg *tls.Config) {
	if c.client != nil {
		c.client.Close()
	}
	c.client = NewPeerGRPCClient(cfg)
}

// SetStateFunc sets the function that returns the current cluster state.
func (c *Cluster) SetStateFunc(fn func() ClusterState) {
	c.getState = fn
}

// StartBackground starts the gossip and failure detection loops.
func (c *Cluster) StartBackground() {
	go c.gossipLoop()
	go c.failureDetectionLoop()
}

// Join connects to seed nodes and joins the cluster.
func (c *Cluster) Join(seeds []string) error {
	for _, seed := range seeds {
		if seed == c.addr {
			continue
		}
		c.mu.Lock()
		found := false
		for _, p := range c.peers {
			if p.PeerAddr == seed {
				found = true
				break
			}
		}
		if !found {
			c.peers["seed:"+seed] = &PeerInfo{
				PeerAddr: seed,
				State:    PeerAlive,
				LastSeen: time.Now(),
			}
		}
		c.mu.Unlock()
	}
	return nil
}

// updatePeerFromGossip updates the peer table from an incoming gossip message.
// connectedVia is the address we used to reach this peer (may differ from the
// peer's advertised address). It is used to clean up seed entries.
func (c *Cluster) updatePeerFromGossip(nodeID, peerAddr string, peers map[string]string, connectedVia string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if nodeID == c.nodeID {
		return
	}

	if p, ok := c.peers[nodeID]; ok {
		p.LastSeen = time.Now()
		p.State = PeerAlive
		p.PeerAddr = peerAddr
	} else {
		peer := &PeerInfo{
			ID:       nodeID,
			PeerAddr: peerAddr,
			State:    PeerAlive,
			LastSeen: time.Now(),
		}
		c.peers[nodeID] = peer
		c.logger.Info("peer joined", zap.String("peer", nodeID), zap.String("addr", peerAddr))
		if c.onJoin != nil {
			go c.onJoin(peer)
		}
	}

	// Remove seed entries: both the advertised address and the address
	// we actually connected through (which may be a DNS name that differs
	// from the advertised bind address).
	delete(c.peers, "seed:"+peerAddr)
	if connectedVia != "" && connectedVia != peerAddr {
		delete(c.peers, "seed:"+connectedVia)
	}

	// Learn about new peers
	for id, addr := range peers {
		if id != c.nodeID {
			if _, ok := c.peers[id]; !ok {
				c.peers[id] = &PeerInfo{
					ID:       id,
					PeerAddr: addr,
					State:    PeerAlive,
					LastSeen: time.Now(),
				}
			}
		}
	}
}

func (c *Cluster) gossipLoop() {
	ticker := time.NewTicker(c.config.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.gossipOnce()
		case <-c.stopCh:
			return
		}
	}
}

// SetMetrics sets the Prometheus metrics for the cluster.
func (c *Cluster) SetMetrics(m *Metrics) {
	c.promMetrics = m
}

func (c *Cluster) gossipOnce() {
	if c.getState == nil {
		return
	}
	if c.promMetrics != nil {
		c.promMetrics.GossipRoundsTotal.Inc()
	}
	state := c.getState()

	c.mu.RLock()
	peers := make([]*PeerInfo, 0, len(c.peers))
	for _, p := range c.peers {
		peers = append(peers, p)
	}
	c.mu.RUnlock()

	req := &pb.GossipRequest{
		NodeId:   state.NodeID,
		PeerAddr: state.PeerAddr,
		Peers:    state.Peers,
	}
	for _, sd := range state.Streams {
		req.StreamDigests = append(req.StreamDigests, &pb.StreamDigestProto{
			Stream: sd.Stream,
			NodeId: sd.NodeID,
			MaxSeq: sd.MaxSeq,
			MaxTs:  sd.MaxTS,
		})
	}

	for _, p := range peers {
		go func(peer *PeerInfo) {
			if peer.PeerAddr == "" {
				return
			}

			client, err := c.client.GetClient(peer.PeerAddr)
			if err != nil {
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			resp, err := client.Gossip(ctx, req)
			if err != nil {
				return
			}

			if resp.NodeId != "" && resp.NodeId != c.nodeID {
				c.updatePeerFromGossip(resp.NodeId, resp.PeerAddr, resp.Peers, peer.PeerAddr)
			}
		}(p)
	}
}

func (c *Cluster) failureDetectionLoop() {
	ticker := time.NewTicker(c.config.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			now := time.Now()
			for _, p := range c.peers {
				if p.State == PeerAlive && now.Sub(p.LastSeen) > c.config.FailureTimeout {
					p.State = PeerDead
					c.logger.Info("peer marked dead", zap.String("peer", p.ID), zap.Duration("since", now.Sub(p.LastSeen)))
					if c.onLeave != nil {
						go c.onLeave(p)
					}
				}
			}
			c.mu.Unlock()
		case <-c.stopCh:
			return
		}
	}
}

// Peers returns a snapshot of all known peers.
func (c *Cluster) Peers() []*PeerInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]*PeerInfo, 0, len(c.peers))
	for _, p := range c.peers {
		cp := *p
		result = append(result, &cp)
	}
	return result
}

// LivePeers returns peers that are currently alive.
func (c *Cluster) LivePeers() []*PeerInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]*PeerInfo, 0, len(c.peers))
	for _, p := range c.peers {
		if p.State == PeerAlive {
			cp := *p
			result = append(result, &cp)
		}
	}
	return result
}

// IsAlive returns whether a peer is considered alive.
func (c *Cluster) IsAlive(nodeID string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if p, ok := c.peers[nodeID]; ok {
		return p.State == PeerAlive
	}
	return false
}

// PeerCount returns the number of known peers.
func (c *Cluster) PeerCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.peers)
}

// OnJoin registers a callback for when a peer joins.
func (c *Cluster) OnJoin(fn func(peer *PeerInfo)) {
	c.onJoin = fn
}

// OnLeave registers a callback for when a peer is detected as dead.
func (c *Cluster) OnLeave(fn func(peer *PeerInfo)) {
	c.onLeave = fn
}

// Stop shuts down the cluster.
func (c *Cluster) Stop() error {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return nil
	}
	c.stopped = true
	c.mu.Unlock()

	close(c.stopCh)
	if c.client != nil {
		c.client.Close()
	}
	return nil
}
