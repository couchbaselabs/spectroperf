package workload

import (
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

type OperationPhase string

const (
	RampUp   OperationPhase = "RampUp"
	Steady   OperationPhase = "Steady"
	RampDown OperationPhase = "RampDown"
)

var (
	// Prometheus metrics for attempted and failed operations
	opsAttempted = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "operations_total",
			Help: "How many user operations are attempted, partitioned by operation.",
		},
		[]string{"operation", "phase"},
	)
	opsFailed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "operations_failed_total",
			Help: "How many user operations failed, partitioned by operation.",
		},
		[]string{"operation", "phase"},
	)
	opDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "operation_duration_milliseconds",
			Help:    "Duration of user operations in milliseconds, partitioned by operation.",
			Buckets: []float64{0.150, 0.225, 0.338, 0.506, 0.759, 1.139, 1.709, 2.563, 3.844, 5.767, 8.650, 12.975, 19.462, 29.193, 43.789, 65.684, 98.526, 147.789, 221.684, 332.526, 498.789, 748.183, 1122.274, 1683.411, 2525.117},
		},
		[]string{"operation", "phase"},
	)

	// Maps from the operation to an attempted/failed metric labelled with the operation
	attemptMetrics  = map[string]map[OperationPhase]prometheus.Counter{}
	failedMetrics   = map[string]map[OperationPhase]prometheus.Counter{}
	durationMetrics = map[string]map[OperationPhase]prometheus.Observer{}

	States = []OperationPhase{RampUp, Steady, RampDown}
)

func MetricState(start time.Time, end time.Time, rampTime time.Duration) OperationPhase {
	phase := Steady
	if time.Now().Sub(start) < rampTime {
		phase = RampUp
	} else if end.Sub(time.Now()) < rampTime {
		phase = RampDown
	}
	return phase
}
