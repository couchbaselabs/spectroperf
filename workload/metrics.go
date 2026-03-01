package workload

import (
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type OperationPhase string

const (
	RampUp                        OperationPhase = "RampUp"
	Steady                        OperationPhase = "Steady"
	RampDown                      OperationPhase = "RampDown"
	TotalOperationsMetric                        = "operations_total"
	TotalFailedOperationsMetric                  = "operations_failed_total"
	OperationDurationMillisMetric                = "operation_duration_milliseconds"
)

var (
	// Prometheus metrics for attempted and failed operations
	opsAttempted = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: TotalOperationsMetric,
			Help: "How many user operations are attempted, partitioned by operation.",
		},
		[]string{"operation", "phase", "users"},
	)
	opsFailed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: TotalFailedOperationsMetric,
			Help: "How many user operations failed, partitioned by operation.",
		},
		[]string{"operation", "phase", "users"},
	)
	opDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    OperationDurationMillisMetric,
			Help:    "Duration of user operations in milliseconds, partitioned by operation.",
			Buckets: prometheus.ExponentialBuckets(0.150, 1.5, 25),
		},
		[]string{"operation", "phase", "users"},
	)

	// Maps from the operation to an attempted/failed metric labelled with the operation
	attemptMetrics  = map[string]map[OperationPhase]map[int]prometheus.Counter{}
	failedMetrics   = map[string]map[OperationPhase]map[int]prometheus.Counter{}
	durationMetrics = map[string]map[OperationPhase]map[int]prometheus.Observer{}

	// In-memory histograms for local metric collection
	localHistograms     = make(map[string]map[int]*hdrhistogram.Histogram)
	localFailedCounters = make(map[string]map[int]*atomic.Int64)

	States = []OperationPhase{RampUp, Steady, RampDown}
)

// InitMetrics initialises the metrics labelled with the operations performed by the given workload
func InitMetrics(logger *zap.Logger, w Workload, numUsers []int) {
	// Create a non-global registry.
	reg := prometheus.NewRegistry()
	reg.MustRegister(opsAttempted)
	reg.MustRegister(opsFailed)
	reg.MustRegister(opDuration)

	// Setup metrics
	for _, operation := range w.Operations() {
		attemptMetrics[operation] = map[OperationPhase]map[int]prometheus.Counter{}
		failedMetrics[operation] = map[OperationPhase]map[int]prometheus.Counter{}
		durationMetrics[operation] = map[OperationPhase]map[int]prometheus.Observer{}

		localHistograms[operation] = make(map[int]*hdrhistogram.Histogram)
		localFailedCounters[operation] = make(map[int]*atomic.Int64)

		for _, phase := range States {
			attemptMetrics[operation][phase] = make(map[int]prometheus.Counter)
			failedMetrics[operation][phase] = make(map[int]prometheus.Counter)
			durationMetrics[operation][phase] = make(map[int]prometheus.Observer)

			for _, numUser := range numUsers {
				attemptMetrics[operation][phase][numUser] = opsAttempted.WithLabelValues(operation, string(phase), strconv.Itoa(numUser))
				failedMetrics[operation][phase][numUser] = opsFailed.WithLabelValues(operation, string(phase), strconv.Itoa(numUser))
				durationMetrics[operation][phase][numUser] = opDuration.WithLabelValues(operation, string(phase), strconv.Itoa(numUser))

				localHistograms[operation][numUser] = hdrhistogram.New(1, 60000000, 5)
				localFailedCounters[operation][numUser] = &atomic.Int64{}
			}
		}
	}

	// Expose metrics and custom registry via an HTTP server
	go func() {
		http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
		logger.Fatal("prometheus handler failed", zap.Error(http.ListenAndServe(":2112", nil)))
	}()
}

func MetricState(start time.Time, end time.Time, rampTime time.Duration) OperationPhase {
	phase := Steady
	if time.Now().Sub(start) < rampTime {
		phase = RampUp
	} else if end.Sub(time.Now()) < rampTime {
		phase = RampDown
	}
	return phase
}

type RunSummary struct {
	NumUsers                int                `json:"numUsers"`
	SteadyStateDurationSecs int                `json:"steadyStateDurationSecs"`
	Operations              []OperationSummary `json:"operations"`
}

type LatencyPercentiles struct {
	NinetyNinth  float64 `json:"ninetyNinth"`
	NinetyEighth float64 `json:"ninetyEighth"`
	NinetyFifth  float64 `json:"ninetyFifth"`
	Fiftieth     float64 `json:"fiftieth"`
}

// OperationSummary summarises the metrics for a whole spectroperf run for a
// given operation. It holds the total number of that operation attempted,
// the number that failed and some latency percentiles.
type OperationSummary struct {
	Name      string             `json:"name"`
	Total     int                `json:"total"`
	Failed    int                `json:"failed"`
	Latencies LatencyPercentiles `json:"latencyPercentiles"`
}

// summariseOperationMetrics generates a summary from HDR histograms.
func summariseOperationMetrics(op string, numUsers int) (*OperationSummary, error) {
	hist, histOk := localHistograms[op][numUsers]

	var totalSuccess int
	var latencies LatencyPercentiles

	if histOk {
		totalSuccess = int(hist.TotalCount())
		latencies = LatencyPercentiles{
			NinetyNinth:  getQuantile(hist, 99),
			NinetyEighth: getQuantile(hist, 98),
			NinetyFifth:  getQuantile(hist, 95),
			Fiftieth:     getQuantile(hist, 50),
		}
	}

	failed := 0
	if counter, ok := localFailedCounters[op][numUsers]; ok {
		failed = int(counter.Load())
	}

	if totalSuccess == 0 && failed == 0 {
		return nil, fmt.Errorf("no local steady state metrics recorded for operation %s, numUsers %d", op, numUsers)
	}

	summary := OperationSummary{
		Name:      op,
		Total:     totalSuccess + failed,
		Failed:    failed,
		Latencies: latencies,
	}
	return &summary, nil
}

func CreateSummary(logger *zap.Logger, numUsers int, w Workload, runTime, rampTime time.Duration) RunSummary {
	var summary RunSummary

	summary.NumUsers = numUsers
	summary.SteadyStateDurationSecs = int(runTime.Seconds() - (2 * rampTime.Seconds()))
	summary.Operations = make([]OperationSummary, len(w.Operations()))
	for i, op := range w.Operations() {
		metricSummary, err := summariseOperationMetrics(op, numUsers)
		if err != nil {
			logger.Warn("skipping local summary due to error", zap.Error(err), zap.String("operation", op), zap.Int("users", numUsers))
			// Create an empty summary so the array structure is maintained
			summary.Operations[i] = OperationSummary{Name: op}
			continue
		}
		summary.Operations[i] = *metricSummary
	}

	return summary
}

// getQuantile is a helper to safely get a quantile value from a histogram, returning 0 if the histogram is nil.
func getQuantile(hist *hdrhistogram.Histogram, quantile float64) float64 {
	if hist == nil || hist.TotalCount() == 0 {
		return 0
	}
	return float64(hist.ValueAtQuantile(quantile)) / 1000.0
}
