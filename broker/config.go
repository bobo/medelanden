package broker

import (
	"fmt"
	"time"

	"go.uber.org/zap"
)

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

func (c StreamConfig) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("stream name is required")
	}
	if len(c.Subjects) == 0 {
		return fmt.Errorf("stream %q: at least one subject is required", c.Name)
	}
	for i, s := range c.Subjects {
		if s == "" {
			return fmt.Errorf("stream %q: subject[%d] is empty", c.Name, i)
		}
	}
	if c.MaxBytes < 0 {
		return fmt.Errorf("stream %q: max_bytes must be >= 0", c.Name)
	}
	if c.MaxAge < 0 {
		return fmt.Errorf("stream %q: max_age must be >= 0", c.Name)
	}
	if c.MaxMsgs < 0 {
		return fmt.Errorf("stream %q: max_msgs must be >= 0", c.Name)
	}
	switch c.FsyncPolicy {
	case "", FsyncNone, FsyncInterval, FsyncEvery:
	default:
		return fmt.Errorf("stream %q: invalid fsync_policy %q", c.Name, c.FsyncPolicy)
	}
	if c.FsyncPolicy == FsyncInterval && c.FsyncInterval <= 0 {
		return fmt.Errorf("stream %q: fsync_interval must be > 0 when fsync_policy is %q", c.Name, FsyncInterval)
	}
	if c.ReplicationTarget < 0 {
		return fmt.Errorf("stream %q: replication_target must be >= 0", c.Name)
	}
	return nil
}

func (c ConsumerConfig) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("consumer name is required")
	}
	if c.Stream == "" {
		return fmt.Errorf("consumer %q: stream is required", c.Name)
	}
	switch c.LatePolicy {
	case "", LatePolicyDrop, LatePolicyEmitUnordered:
	default:
		return fmt.Errorf("consumer %q: invalid late_policy %q", c.Name, c.LatePolicy)
	}
	switch c.DeliverPolicy {
	case "", DeliverNew, DeliverAll, DeliverByTime:
	default:
		return fmt.Errorf("consumer %q: invalid deliver_policy %q", c.Name, c.DeliverPolicy)
	}
	if c.WindowDuration < 0 {
		return fmt.Errorf("consumer %q: window_duration must be >= 0", c.Name)
	}
	if c.WatermarkTimeout < 0 {
		return fmt.Errorf("consumer %q: watermark_timeout must be >= 0", c.Name)
	}
	return nil
}

func (c NodeConfig) Validate() error {
	if c.ID == "" {
		return fmt.Errorf("node id is required")
	}
	if c.DataDir == "" {
		return fmt.Errorf("node %q: data_dir is required", c.ID)
	}
	if c.BindAddr == "" {
		return fmt.Errorf("node %q: bind_addr is required", c.ID)
	}
	if err := c.TLS.Validate(); err != nil {
		return fmt.Errorf("node %q: %w", c.ID, err)
	}
	if err := c.Auth.Validate(); err != nil {
		return fmt.Errorf("node %q: %w", c.ID, err)
	}
	return nil
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

type TLSConfig struct {
	Enabled  bool   `json:"enabled"`
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
	CAFile   string `json:"ca_file,omitempty"`
}

func (c TLSConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.CertFile == "" {
		return fmt.Errorf("tls: cert_file is required when TLS is enabled")
	}
	if c.KeyFile == "" {
		return fmt.Errorf("tls: key_file is required when TLS is enabled")
	}
	return nil
}

type AuthConfig struct {
	Enabled        bool          `json:"enabled"`
	AuthorizedKeys []string      `json:"authorized_keys"` // base64-encoded Ed25519 public keys
	ConnectTimeout time.Duration `json:"connect_timeout"`
}

func (c AuthConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if len(c.AuthorizedKeys) == 0 {
		return fmt.Errorf("auth: at least one authorized key is required when auth is enabled")
	}
	return nil
}

// NodeConfig configures a single broker node.
type NodeConfig struct {
	ID                string       `json:"id"`
	DataDir           string       `json:"data_dir"`
	BindAddr          string       `json:"bind_addr"`           // TCP address for client connections
	PeerAddr          string       `json:"peer_addr"`           // TCP address for peer-to-peer communication (listen)
	AdvertisePeerAddr string       `json:"advertise_peer_addr"` // Address advertised to peers (defaults to PeerAddr)
	Seeds             []string     `json:"seeds,omitempty"`
	Limits            LimitsConfig `json:"limits"`
	TLS               TLSConfig    `json:"tls"`
	Auth              AuthConfig   `json:"auth"`
	Logger            *zap.Logger  `json:"-"`
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

type LimitsConfig struct {
	MaxConnections int   `json:"max_connections"`
	MaxMessageSize int64 `json:"max_message_size"`
	MaxStreams     int   `json:"max_streams"`
	MaxConsumers   int   `json:"max_consumers"`
}

func DefaultLimitsConfig() LimitsConfig {
	return LimitsConfig{
		MaxConnections: 10000,
		MaxMessageSize: 64 * 1024 * 1024,
		MaxStreams:     1024,
		MaxConsumers:   4096,
	}
}
