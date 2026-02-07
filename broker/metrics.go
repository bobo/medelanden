package broker

import "github.com/prometheus/client_golang/prometheus"

const metricsNamespace = "medelanden"

// Metrics holds all Prometheus metrics for a Medelanden broker node.
type Metrics struct {
	// Publish path
	PublishTotal   *prometheus.CounterVec
	PublishLatency *prometheus.HistogramVec
	PublishErrors  *prometheus.CounterVec

	// Stream
	StreamMessages *prometheus.GaugeVec

	// Consumer
	ConsumerWatermark    *prometheus.GaugeVec
	ConsumerLateMessages *prometheus.CounterVec
	ConsumerDedupTotal   *prometheus.CounterVec

	// TCP server
	TCPConnections     prometheus.Gauge
	TCPCommandsTotal   *prometheus.CounterVec

	// Cluster
	ClusterPeers       *prometheus.GaugeVec
	GossipRoundsTotal  prometheus.Counter

	// Replication
	ReplicatedMsgsTotal *prometheus.CounterVec
	ReplicationPulls    *prometheus.CounterVec
}

// NewMetrics creates and registers all Prometheus metrics.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		PublishTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "publish_total",
			Help:      "Total number of messages published.",
		}, []string{"stream"}),

		PublishLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "publish_duration_seconds",
			Help:      "Latency of publish operations in seconds.",
			Buckets:   []float64{.00001, .00005, .0001, .0005, .001, .005, .01, .05, .1, .5, 1},
		}, []string{"stream"}),

		PublishErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "publish_errors_total",
			Help:      "Total number of failed publish operations.",
		}, []string{"stream"}),

		StreamMessages: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "stream_messages",
			Help:      "Current number of messages in each stream.",
		}, []string{"stream"}),

		ConsumerWatermark: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "consumer_watermark",
			Help:      "Current watermark timestamp for each consumer.",
		}, []string{"consumer"}),

		ConsumerLateMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "consumer_late_messages_total",
			Help:      "Total number of late-arriving messages per consumer.",
		}, []string{"consumer"}),

		ConsumerDedupTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "consumer_dedup_total",
			Help:      "Total number of deduplicated messages per consumer.",
		}, []string{"consumer"}),

		TCPConnections: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "tcp_connections",
			Help:      "Current number of active TCP client connections.",
		}),

		TCPCommandsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "tcp_commands_total",
			Help:      "Total number of TCP protocol commands processed.",
		}, []string{"command"}),

		ClusterPeers: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "cluster_peers",
			Help:      "Number of known cluster peers by state.",
		}, []string{"state"}),

		GossipRoundsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "gossip_rounds_total",
			Help:      "Total number of gossip rounds executed.",
		}),

		ReplicatedMsgsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "replicated_messages_total",
			Help:      "Total number of messages replicated from peers.",
		}, []string{"stream", "peer"}),

		ReplicationPulls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "replication_pulls_total",
			Help:      "Total number of replication pull requests.",
		}, []string{"stream", "peer"}),
	}

	reg.MustRegister(
		m.PublishTotal,
		m.PublishLatency,
		m.PublishErrors,
		m.StreamMessages,
		m.ConsumerWatermark,
		m.ConsumerLateMessages,
		m.ConsumerDedupTotal,
		m.TCPConnections,
		m.TCPCommandsTotal,
		m.ClusterPeers,
		m.GossipRoundsTotal,
		m.ReplicatedMsgsTotal,
		m.ReplicationPulls,
	)

	return m
}
