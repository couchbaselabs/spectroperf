package workload

import "github.com/prometheus/client_golang/prometheus"

var (
	// Prometheus metrics for attempted and failed operations
	opsAttempted = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "operations_total",
			Help: "How many user operations are attempted, partitioned by operation.",
		},
		[]string{"operation"},
	)
	opsFailed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "operations_failed_total",
			Help: "How many user operations failed, partitioned by operation.",
		},
		[]string{"operation"},
	)
	opDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "operation_duration_milliseconds",
			Help:    "Duration of user operations in milliseconds, partitioned by operation.",
			Buckets: []float64{25.0, 50.0, 100.0, 250.0, 500.0, 1500.0, 2500.0},
		},
		[]string{"operation"},
	)

	// Maps from the operation to an attempted/failed metric labelled with the operation
	attemptMetrics  = map[string]prometheus.Counter{}
	failedMetrics   = map[string]prometheus.Counter{}
	durationMetrics = map[string]prometheus.Observer{}
)
