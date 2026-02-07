package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// Replicator handles anti-entropy replication between nodes.
// It periodically exchanges stream digests with peers and pulls
// missing messages.
type Replicator struct {
	mu       sync.RWMutex
	nodeID   string
	cluster  *Cluster
	streams  func() map[string]*Stream // getter for current streams
	interval time.Duration
	stopCh   chan struct{}
	stopped  bool

	// Metrics
	replicationLag map[string]time.Duration // peer -> lag
	promMetrics    *Metrics
}

// ReplicationRequest asks a peer for messages in a range.
type ReplicationRequest struct {
	Stream   string `json:"stream"`
	NodeID   string `json:"node_id"`   // which node's data we want
	StartSeq uint64 `json:"start_seq"`
	EndSeq   uint64 `json:"end_seq"`
	Limit    int    `json:"limit"`
}

// ReplicationResponse contains messages from a peer.
type ReplicationResponse struct {
	Stream   string     `json:"stream"`
	NodeID   string     `json:"node_id"`
	Messages []*Message `json:"messages"`
}

// DigestExchange is sent during anti-entropy rounds.
type DigestExchange struct {
	NodeID  string                    `json:"node_id"`
	Digests map[string]StreamDigest   `json:"digests"` // stream -> digest
}

// NewReplicator creates a new replication manager.
func NewReplicator(nodeID string, cluster *Cluster, streams func() map[string]*Stream, promMetrics *Metrics) *Replicator {
	return &Replicator{
		nodeID:         nodeID,
		cluster:        cluster,
		streams:        streams,
		interval:       1 * time.Second,
		stopCh:         make(chan struct{}),
		replicationLag: make(map[string]time.Duration),
		promMetrics:    promMetrics,
	}
}

// Start begins the replication background process.
func (r *Replicator) Start() {
	go r.antiEntropyLoop()
}

// Stop stops the replicator.
func (r *Replicator) Stop() {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	r.mu.Unlock()
	close(r.stopCh)
}

// RegisterHTTPHandlers registers replication HTTP endpoints on the given mux.
func (r *Replicator) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/replication/digest", r.handleDigest)
	mux.HandleFunc("/replication/pull", r.handlePull)
}

func (r *Replicator) handleDigest(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var incoming DigestExchange
	if err := json.NewDecoder(req.Body).Decode(&incoming); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build our digest response
	streams := r.streams()
	response := DigestExchange{
		NodeID:  r.nodeID,
		Digests: make(map[string]StreamDigest),
	}
	for name, s := range streams {
		d := s.Digest()
		d.NodeID = r.nodeID
		response.Digests[name] = d
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (r *Replicator) handlePull(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var pullReq ReplicationRequest
	if err := json.NewDecoder(req.Body).Decode(&pullReq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	streams := r.streams()
	stream, ok := streams[pullReq.Stream]
	if !ok {
		http.Error(w, "stream not found", http.StatusNotFound)
		return
	}

	limit := pullReq.Limit
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}

	endSeq := pullReq.EndSeq
	if endSeq == 0 || endSeq > stream.LastSeq() {
		endSeq = stream.LastSeq()
	}
	if pullReq.StartSeq+uint64(limit)-1 < endSeq {
		endSeq = pullReq.StartSeq + uint64(limit) - 1
	}

	msgs, err := stream.Read(pullReq.StartSeq, endSeq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := ReplicationResponse{
		Stream:   pullReq.Stream,
		NodeID:   r.nodeID,
		Messages: msgs,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// antiEntropyLoop runs periodic anti-entropy with peers.
func (r *Replicator) antiEntropyLoop() {
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

func (r *Replicator) antiEntropyRound() {
	peers := r.cluster.LivePeers()
	if len(peers) == 0 {
		return
	}

	// Build our digests
	streams := r.streams()
	ourDigests := make(map[string]StreamDigest)
	for name, s := range streams {
		d := s.Digest()
		d.NodeID = r.nodeID
		ourDigests[name] = d
	}

	exchange := DigestExchange{
		NodeID:  r.nodeID,
		Digests: ourDigests,
	}

	data, err := json.Marshal(exchange)
	if err != nil {
		return
	}

	client := &http.Client{Timeout: 5 * time.Second}

	for _, peer := range peers {
		if peer.ID == r.nodeID || peer.PeerAddr == "" {
			continue
		}

		go r.syncWithPeer(client, peer, data, streams)
	}
}

func (r *Replicator) syncWithPeer(client *http.Client, peer *PeerInfo, digestData []byte, streams map[string]*Stream) {
	// Exchange digests
	url := fmt.Sprintf("http://%s/replication/digest", peer.PeerAddr)
	resp, err := client.Post(url, "application/json", bytes.NewReader(digestData))
	if err != nil {
		return
	}
	defer resp.Body.Close()

	var peerDigest DigestExchange
	if err := json.NewDecoder(resp.Body).Decode(&peerDigest); err != nil {
		return
	}

	// Find streams where the peer has more data than us
	for streamName, peerSD := range peerDigest.Digests {
		stream, ok := streams[streamName]
		if !ok {
			continue
		}

		ourSeq := stream.LastSeq()
		if peerSD.MaxSeq > ourSeq {
			// Pull missing messages
			r.pullMessages(client, peer, streamName, ourSeq+1, peerSD.MaxSeq, stream)
		}
	}
}

func (r *Replicator) pullMessages(client *http.Client, peer *PeerInfo, streamName string, startSeq, endSeq uint64, stream *Stream) {
	pullReq := ReplicationRequest{
		Stream:   streamName,
		StartSeq: startSeq,
		EndSeq:   endSeq,
		Limit:    1000,
	}

	data, err := json.Marshal(pullReq)
	if err != nil {
		return
	}

	peerLabel := peer.ID
	if peerLabel == "" {
		peerLabel = peer.PeerAddr
	}
	if r.promMetrics != nil {
		r.promMetrics.ReplicationPulls.WithLabelValues(streamName, peerLabel).Inc()
	}

	url := fmt.Sprintf("http://%s/replication/pull", peer.PeerAddr)
	resp, err := client.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}

	var pullResp ReplicationResponse
	if err := json.Unmarshal(body, &pullResp); err != nil {
		return
	}

	// Store replicated messages
	for _, msg := range pullResp.Messages {
		stream.PublishReplicated(msg)
	}
	if r.promMetrics != nil && len(pullResp.Messages) > 0 {
		r.promMetrics.ReplicatedMsgsTotal.WithLabelValues(streamName, peerLabel).Add(float64(len(pullResp.Messages)))
	}
}

// PullFromPeer pulls messages from a specific peer for the read path.
// Returns messages from startSeq onwards.
func (r *Replicator) PullFromPeer(peer *PeerInfo, streamName string, startSeq uint64, limit int) ([]*Message, error) {
	if peer.PeerAddr == "" {
		return nil, fmt.Errorf("peer has no address")
	}

	pullReq := ReplicationRequest{
		Stream:   streamName,
		StartSeq: startSeq,
		Limit:    limit,
	}

	data, err := json.Marshal(pullReq)
	if err != nil {
		return nil, err
	}

	client := &http.Client{Timeout: 5 * time.Second}
	url := fmt.Sprintf("http://%s/replication/pull", peer.PeerAddr)
	resp, err := client.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pullResp ReplicationResponse
	if err := json.NewDecoder(resp.Body).Decode(&pullResp); err != nil {
		return nil, err
	}

	return pullResp.Messages, nil
}

// ReplicationLag returns the replication lag for a given peer.
func (r *Replicator) ReplicationLag(peerID string) time.Duration {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.replicationLag[peerID]
}
