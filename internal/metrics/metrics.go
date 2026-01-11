package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	ActiveConnections prometheus.Gauge
	TotalConnections  prometheus.Counter
	ConnectionErrors  prometheus.Counter

	RequestsTotal    prometheus.Counter
	RequestDuration  prometheus.Histogram
	RequestSizeBytes prometheus.Histogram
	RequestsInFlight prometheus.Gauge

	GroupOperations *prometheus.CounterVec
	GroupLatency    *prometheus.HistogramVec
	GroupJn         prometheus.Counter
	GroupLv         prometheus.Counter
	GroupHb         prometheus.Counter
	ActiveGroups    prometheus.Counter
}

func NewMetrics(namespace string) *Metrics {
	const subsystem = "quic"

	return &Metrics{
		ActiveConnections: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "active_connections",
			Help:      "The current number of active QUIC connections",
		}),
		TotalConnections: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "connections_total",
			Help:      "The total number of QUIC connections established",
		}),
		ConnectionErrors: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "connection_errors_total",
			Help:      "The total number of QUIC connection errors",
		}),
		RequestsTotal: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "requests_total",
			Help:      "The total number of requests processed",
		}),
		RequestDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "request_duration_seconds",
			Help:      "The duration of requests in seconds",
			Buckets:   prometheus.DefBuckets,
		}),
		RequestSizeBytes: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "request_size_bytes",
			Help:      "The size of requests in bytes",
			Buckets:   prometheus.ExponentialBuckets(128, 2, 10),
		}),
		RequestsInFlight: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "requests_in_flight",
			Help:      "The current number of requests being processed",
		}),
		GroupOperations: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "group_operations_total",
			Help:      "The total number of Raft operations by type",
		}, []string{"operation"}),
		GroupLatency: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "group_operation_duration_seconds",
			Help:      "The duration of Raft operations in seconds",
			Buckets:   prometheus.DefBuckets,
		}, []string{"operation"}),
		GroupJn: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "group_jn",
			Help:      "The number of join group operations",
		}),
		GroupLv: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "group_lv",
			Help:      "The number of leave group operations",
		}),
		GroupHb: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "group_hb",
			Help:      "The number of heartbeat operations",
		}),
		ActiveGroups: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "active_groups",
			Help:      "The current number of active groups",
		}),
	}
}
