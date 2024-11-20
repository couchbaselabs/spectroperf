package workload

import (
	"context"
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// A spectroperf Workload is defined by:
//   - The documents it operates on
//   - The Operations it performs on those documents
//   - The probability matrix defining the likelihood of one operation being followed by another
type Workload interface {
	// GenerateDocument creates a random document appropriate for the workload
	GenerateDocument(id string) DocType
	// Operations returns The list of operations that a workload supports
	Operations() []string
	// Probabilities returns the probability matrix for the workload
	Probabilities() [][]float64
	// Returns a map of operations to workload functions
	Functions() map[string]func(ctx context.Context, rctx Runctx) error
	// Setup performs any workload specific setup, e.g creating indexes
	Setup() error
}

// InitMetrics initialises the metrics labelled with the operations performed by the given workload
func InitMetrics(w Workload) {
	// Create a non-global registry.
	reg := prometheus.NewRegistry()
	reg.MustRegister(opsAttempted)
	reg.MustRegister(opsFailed)
	reg.MustRegister(opDuration)

	// Setup metrics
	for _, operation := range w.Operations() {
		attemptMetrics[operation] = opsAttempted.WithLabelValues(operation)
		failedMetrics[operation] = opsFailed.WithLabelValues(operation)
		durationMetrics[operation] = opDuration.WithLabelValues(operation)
	}

	// Expose metrics and custom registry via an HTTP server
	go func() {
		http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
		log.Fatal(http.ListenAndServe(":2112", nil))
	}()
}

// Setup uploads the documents generated by the workload, and calls the workloads Setup function
func Setup(w Workload, numItemsArg int, scp *gocb.Scope, coll *gocb.Collection) {
	numConc := 2000
	workChan := make(chan DocType, numConc)
	shutdownChan := make(chan struct{}, numConc)
	var wg sync.WaitGroup

	wg.Add(numConc)
	for i := 0; i < numConc; i++ {
		go func() {
			for {
				select {
				case doc := <-workChan:
					_, err := coll.Upsert(doc.Name, doc.Data, nil)
					if err != nil {
						panic(errors.Wrap(err, "Data load upsert failed."))
					}
				case <-shutdownChan:
					wg.Done()
					return
				}
			}
		}()
	}

	// Create a random document using the given workload definition
	for i := 0; i < numItemsArg; i++ {
		workChan <- w.GenerateDocument(fmt.Sprintf("u%d", i))
	}

	// Call the worloads own Setup function to perform any workload specific setup
	err := w.Setup()
	if err != nil {
		panic(errors.Wrap(err, "failed to setup workload"))
	}
}

func Run(w Workload, numUsers int, runTime time.Duration) {
	sigCh := make(chan os.Signal, 10)
	ctx, cancelFn := context.WithCancel(context.Background())

	go func() {
		<-sigCh
		cancelFn()
	}()

	// Signal handler for SIGTERM
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// Create a work group of goroutine runners sharing the same probabilities.
	var wg sync.WaitGroup

	wg.Add(numUsers)
	for i := 0; i < numUsers; i++ {
		go runLoop(ctx, w.Probabilities(), w.Functions(), w.Operations(), runTime, i, &wg)
	}

	wg.Wait()
}

func runLoop(
	ctx context.Context,
	probabilities [][]float64,
	functions map[string]func(context.Context, Runctx) error,
	operations []string,
	runTime time.Duration,
	runnerId int,
	wg *sync.WaitGroup) {

	// Current operation index
	currOpIndex := 0

	rng := rand.NewSource(int64(RandSeed + runnerId))
	r := rand.New(rng)

	timeout := time.After(runTime)

	slog := zap.L().Sugar()

	slog.Debugf("Starting runner %d…", runnerId)

	var runCtx Runctx
	runCtx.r = *r
	runCtx.l = *zap.L() // TODO: create a log for results
	// todo: move this into context.value, a KV store for junk

	for {
		select {
		case <-ctx.Done():
			slog.Debugf("Received cancel, stopping runner %d…", runnerId) // TODO: fix bug where only one runner stops
			wg.Done()
		case <-timeout:
			slog.Debugf("Run time reached, stopping runner %d…", runnerId)
			wg.Done()
		default:
			// Get the next operation index based on probabilities
			nextOpIndex := getNextOperation(currOpIndex, probabilities, r)
			// call the next function
			nextFunction := operations[nextOpIndex]
			slog.Debug(nextFunction)
			attemptMetrics[nextFunction].Inc()

			// sleep a random amount of time
			t := r.Int31n(5000-400) + 400
			time.Sleep(time.Duration(t) * time.Millisecond)

			start := time.Now()
			err := functions[operations[nextOpIndex]](ctx, runCtx)
			duration := time.Now().Sub(start)
			durationMetrics[nextFunction].Observe(float64(duration.Microseconds()) / 1000)

			if err != nil {
				slog.Error("operation failed", zap.String("operation", nextFunction), zap.Error(err))
				failedMetrics[nextFunction].Inc()
			}

			// update for next time
			currOpIndex = nextOpIndex
		}
	}
}

func getNextOperation(currOpIndex int, probabilities [][]float64, r *rand.Rand) int {
	// Get the probabilities for the current operation
	probRow := probabilities[currOpIndex]

	// Calculate a random value between 0 and the total probability (which should sum to 1)
	randVal := r.Float64()

	// Iterate through the probability row to select the next operation
	var cumulativeProb float64
	for i, prob := range probRow {
		cumulativeProb += prob
		if randVal < cumulativeProb {
			return i
		}
	}

	// Fallback: return the last operation if something goes wrong
	return len(probRow) - 1
}
