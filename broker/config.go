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

// NodeConfig configures a single broker node.
type NodeConfig struct {
	ID       string `json:"id"`
	DataDir  string `json:"data_dir"`
	BindAddr string `json:"bind_addr"` // TCP address for client connections
	PeerAddr string `json:"peer_addr"` // TCP address for peer-to-peer communication
	Seeds    []string `json:"seeds,omitempty"`
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
