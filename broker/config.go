package broker

import "time"

// FsyncPolicy determines when the WAL is fsynced to disk.
type FsyncPolicy string

const (
	FsyncNone     FsyncPolicy = "none"
	FsyncInterval FsyncPolicy = "interval"
	FsyncEvery    FsyncPolicy = "every"
)

// LatePolicy determines handling of messages arriving after their window closed.
type LatePolicy string

const (
	LatePolicyDrop          LatePolicy = "drop"
	LatePolicyEmitUnordered LatePolicy = "emit_unordered"
)

// DeliverPolicy determines the starting position for a consumer.
type DeliverPolicy string

const (
	DeliverNew    DeliverPolicy = "new"
	DeliverAll    DeliverPolicy = "all"
	DeliverByTime DeliverPolicy = "by_time"
)

// StreamConfig configures a named message stream.
type StreamConfig struct {
	Name             string        `json:"name"`
	Subjects         []string      `json:"subjects"`
	MaxBytes         int64         `json:"max_bytes,omitempty"`
	MaxAge           time.Duration `json:"max_age,omitempty"`
	MaxMsgs          int64         `json:"max_msgs,omitempty"`
	ReplicationTarget int          `json:"replication_target"`
	FsyncPolicy      FsyncPolicy   `json:"fsync_policy"`
	FsyncInterval    time.Duration `json:"fsync_interval"`
	PlacementTags    []string      `json:"placement_tags,omitempty"`
	PlacementCount   int           `json:"placement_count,omitempty"`
}

// DefaultStreamConfig returns a StreamConfig with sensible defaults.
func DefaultStreamConfig(name string, subjects []string) StreamConfig {
	return StreamConfig{
		Name:              name,
		Subjects:          subjects,
		MaxAge:            24 * time.Hour,
		ReplicationTarget: 2,
		FsyncPolicy:       FsyncInterval,
		FsyncInterval:     100 * time.Millisecond,
	}
}

// ConsumerConfig configures a durable consumer.
type ConsumerConfig struct {
	Name             string        `json:"name"`
	Stream           string        `json:"stream"`
	Ordering         string        `json:"ordering"`
	WindowDuration   time.Duration `json:"window_duration"`
	WatermarkTimeout time.Duration `json:"watermark_timeout"`
	DedupKey         []string      `json:"dedup_key"`
	LatePolicy       LatePolicy    `json:"late_policy"`
	DeliverPolicy    DeliverPolicy `json:"deliver_policy"`
	DeliverFromTime  uint64        `json:"deliver_from_time,omitempty"`
	SubjectFilter    string        `json:"subject_filter,omitempty"`
}

// DefaultConsumerConfig returns a ConsumerConfig with sensible defaults.
func DefaultConsumerConfig(name, stream string) ConsumerConfig {
	return ConsumerConfig{
		Name:             name,
		Stream:           stream,
		Ordering:         "producer_ts",
		WindowDuration:   2 * time.Second,
		WatermarkTimeout: 5 * time.Second,
		DedupKey:         []string{"msg_id"},
		LatePolicy:       LatePolicyDrop,
		DeliverPolicy:    DeliverNew,
		SubjectFilter:    ">",
	}
}

// StreamState is a detailed snapshot of a stream's current state.
type StreamState struct {
	Name          string       `json:"name"`
	Config        StreamConfig `json:"config"`
	Messages      uint64       `json:"messages"`
	FirstSeq      uint64       `json:"first_seq"`
	LastSeq       uint64       `json:"last_seq"`
	ConsumerCount int          `json:"consumer_count"`
}

// ConsumerState is a detailed snapshot of a consumer's current state.
type ConsumerState struct {
	Name         string                     `json:"name"`
	Stream       string                     `json:"stream"`
	Config       ConsumerConfig             `json:"config"`
	Watermark    uint64                     `json:"watermark"`
	LateMessages uint64                     `json:"late_messages"`
	DedupCount   uint64                     `json:"dedup_count"`
	Positions    map[string]*SourcePosition `json:"positions"`
}

// NodeConfig configures a single broker node.
type NodeConfig struct {
	ID                string   `json:"id"`
	DataDir           string   `json:"data_dir"`
	BindAddr          string   `json:"bind_addr"`           // TCP address for client connections
	PeerAddr          string   `json:"peer_addr"`           // TCP address for peer-to-peer communication (listen)
	AdvertisePeerAddr string   `json:"advertise_peer_addr"` // Address advertised to peers (defaults to PeerAddr)
	Seeds             []string `json:"seeds,omitempty"`
}

// EffectiveAdvertisePeerAddr returns the address that should be advertised to peers.
// If AdvertisePeerAddr is set, it is used. Otherwise, PeerAddr is used.
func (c NodeConfig) EffectiveAdvertisePeerAddr() string {
	if c.AdvertisePeerAddr != "" {
		return c.AdvertisePeerAddr
	}
	return c.PeerAddr
}

// ClusterConfig configures cluster membership.
type ClusterConfig struct {
	Seeds            []string      `json:"seeds,omitempty"`
	GossipInterval   time.Duration `json:"gossip_interval"`
	FailureTimeout   time.Duration `json:"failure_timeout"`
}

// DefaultClusterConfig returns a ClusterConfig with sensible defaults.
func DefaultClusterConfig() ClusterConfig {
	return ClusterConfig{
		GossipInterval: 1 * time.Second,
		FailureTimeout: 5 * time.Second,
	}
}
