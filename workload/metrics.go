package workload

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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
		[]string{"operation", "phase"},
	)
	opsFailed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: TotalFailedOperationsMetric,
			Help: "How many user operations failed, partitioned by operation.",
		},
		[]string{"operation", "phase"},
	)
	opDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    OperationDurationMillisMetric,
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

// IsPrometheusRunning returns a bool that represents if prometheus is running.
func IsPrometheusRunning() bool {
	// Try to run the most basic prometheus metric
	_, err := executeQuery("up")
	return err == nil
}

// TotalOperations returns the total number of times the given operation
// was performed in the given range.
func TotalOperations(op string, timeRange int) (int, error) {
	query := fmt.Sprintf(`increase(%s{phase="Steady",operation="%s"}[%dm])`, TotalOperationsMetric, op, timeRange)
	total, err := processQuery(query, op)
	if err != nil {
		return 0, err
	}

	return int(math.Round(total)), nil
}

// TotalOperationsFailed returns the number of times a given operation
// failed in the given range.
func TotalOperationsFailed(op string, timeRange int) (int, error) {
	query := fmt.Sprintf(`increase(%s{phase="Steady",operation="%s"}[%dm])`, TotalFailedOperationsMetric, op, timeRange)
	total, err := processQuery(query, op)
	if err != nil {
		return 0, err
	}

	return int(math.Round(total)), nil
}

// NinetyNithPercentileLatency returns the 99th percentile latency for the given
// operation in milliseconds, in the given time range.
func NinetyNithPercentileLatency(op string, timeRange int) (float64, error) {
	query := fmt.Sprintf(
		`histogram_quantile(0.99, sum(rate(%s_bucket{operation="%s",phase="Steady"}[%dm])) by (le))`,
		OperationDurationMillisMetric,
		op,
		timeRange)

	return processQuery(query, op)
}

// FiftiethPercentileLatency returns the 50th percentile latency for the given
// operation in milliseconds, in the given time range.
func FiftiethPercentileLatency(op string, timeRange int) (float64, error) {
	query := fmt.Sprintf(
		`histogram_quantile(0.50, sum(rate(%s_bucket{operation="%s",phase="Steady"}[%dm])) by (le))`,
		OperationDurationMillisMetric,
		op,
		timeRange)

	return processQuery(query, op)
}

// The following structs combine to represent the output structure returned from
// the prometheus HTTP API.
type queryResult struct {
	Data queryData `json:"data"`
}

type queryData struct {
	Result []result `json:"result"`
}

type result struct {
	Value []any `json:"value"`
}

// executeQuery executes the given query against the prometheus instance running
// on ...
func executeQuery(query string) (*queryResult, error) {
	form := url.Values{}
	form.Add("query", query)

	req, err := http.NewRequest("POST", "http://localhost:9090/api/v1/query", strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	hc := http.Client{}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}

	bodyText, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	err = resp.Body.Close()
	if err != nil {
		return nil, err
	}

	var result queryResult
	err = json.Unmarshal(bodyText, &result)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

// processQuery executes the given query against the prometheus http api and
// parses the result. Returning the value resulting from the query if successful
// or zero and an error is logged.
func processQuery(query, op string) (float64, error) {
	result, err := executeQuery(query)
	if err != nil {
		return 0, fmt.Errorf("executing metric query: %w", err)
	}

	value, err := parseQueryResult(result)
	if err != nil {
		return 0, fmt.Errorf("parsing query result: %w", err)
	}

	return value, nil
}

// parseQueryResult reads the value of the queried metric from the struct
// retunred by the prometheus api.
func parseQueryResult(result *queryResult) (float64, error) {
	if len(result.Data.Result) == 0 {
		return 0, errors.New("no results for operation")
	}

	if len(result.Data.Result[0].Value) != 2 {
		zap.L().Info("")
		return 0, errors.New("no values for the result for operation")
	}

	stringValue := result.Data.Result[0].Value[1].(string)
	if stringValue == "NaN" {
		return 0, errors.New("result value was NaN")
	}

	parsed, err := strconv.ParseFloat(stringValue, 32)
	if err != nil {
		return 0, fmt.Errorf("parsing metric value as float: %w", err)
	}

	return parsed, nil
}
