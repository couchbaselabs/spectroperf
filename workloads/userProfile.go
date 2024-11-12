//   Copyright 2024 Couchbase, Inc.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package workloads

import (
	"context"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/couchbase/gocb/v2"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var operations = []string{"fetchProfile", "updateProfile", "lockProfile", "findProfile", "findRelatedProfiles"}

var numItems = 200000

var numConc = 50000

var randSeed = 11211

var collection gocb.Collection
var scope gocb.Scope

type docType struct {
	Name string
	Data interface{}
}

type User struct {
	Name    string
	Email   string
	Created time.Time
	Status  string
	Enabled bool
}

type UserQueryResponse struct {
	Profiles User
}

type runctx struct {
	r rand.Rand
	l zap.Logger
}

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
			Buckets: []float64{25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 1500.0},
		},
		[]string{"operation"},
	)

	// Maps from the operation to an attempted/failed metric labelled with the operation
	attemptMetrics  = map[string]prometheus.Counter{}
	failedMetrics   = map[string]prometheus.Counter{}
	durationMetrics = map[string]prometheus.Observer{}
)

// Init is called to set up any configuration required for later Setup and Run
// functions in the workload.
func Init() {
	// Create a non-global registry.
	reg := prometheus.NewRegistry()
	reg.MustRegister(opsAttempted)
	reg.MustRegister(opsFailed)
	reg.MustRegister(opDuration)

	// Setup metrics
	for _, operation := range operations {
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

// Setup does any scaffolding required given an environment.
// For example, load the base data needed for the test.
func Setup(numItemsArg int, numConcArg int, scp *gocb.Scope, coll *gocb.Collection) {
	scope = *scp
	collection = *coll
	numItems = numItemsArg
	numConc = numConcArg

	// TODO: set up the FTS index for the 'find related'

	loadData()
}

// Fetch a random profile in the range of profiles
func fetchProfile(ctx context.Context, rctx runctx) error {
	p := fmt.Sprintf("u%d", rctx.r.Int31n(int32(numItems)))
	_, err := collection.Get(p, &gocb.GetOptions{Context: ctx})
	if err != nil {
		return fmt.Errorf("profile fetch failed: %s", err.Error())
	}
	rctx.l.Sugar().Debugf("fetching profile %s", p)
	return nil
}

func updateProfile(ctx context.Context, rctx runctx) error {
	p := fmt.Sprintf("u%d", rctx.r.Int31n(int32(numItems))) // Question to self, should I instead just grab this from context?  probably.
	result, err := collection.Get(p, nil)
	if err != nil {
		return fmt.Errorf("profile fetch during update failed: %s", err.Error())
	}

	var toUd User
	cerr := result.Content(&toUd)
	if cerr != nil {
		return fmt.Errorf("unable to load user into struct: %s", cerr.Error())
	}

	toUd.Status = gofakeit.Paragraph(1, rctx.r.Intn(8)+1, rctx.r.Intn(12)+1, "\n")

	_, uerr := collection.Upsert(p, toUd, nil)
	if uerr != nil {
		return fmt.Errorf("data load upsert failed: %s", uerr.Error())
	}
	return nil
}

func lockProfile(ctx context.Context, rctx runctx) error {
	p := fmt.Sprintf("u%d", rctx.r.Int31n(int32(numItems))) // Question to self, should I instead just grab this from context?  probably.
	result, err := collection.Get(p, nil)
	if err != nil {
		return fmt.Errorf("profile fetch during lock failed: %s", err.Error())
	}

	var toUd User
	result.Content(&toUd)

	toUd.Enabled = false

	_, uerr := collection.Upsert(p, toUd, nil) // replace with replace or subdoc
	if uerr != nil {
		return fmt.Errorf("data load upsert failed: %s", uerr.Error())
	}
	return nil
}

func findProfile(ctx context.Context, rctx runctx) error {
	toFind := fmt.Sprintf("%s%%", gofakeit.Letter())

	query := "SELECT * FROM profiles WHERE Email LIKE $email LIMIT 1"
	rctx.l.Sugar().Debugf("Querying with %s using param %s", query, toFind)
	params := make(map[string]interface{}, 1)
	params["email"] = toFind

	rows, err := scope.Query(query, &gocb.QueryOptions{NamedParameters: params, Adhoc: true})
	if err != nil {
		return fmt.Errorf("query failed: %s", err.Error())
	}

	for rows.Next() {
		var resp UserQueryResponse
		err := rows.Row(&resp)
		if err != nil {
			return fmt.Errorf("could not read next row: %s", err.Error())
		}
		rctx.l.Sugar().Debugf("Found a User: %+v", resp.Profiles)
	}

	err = rows.Err()
	if err != nil {
		return fmt.Errorf("error iterating the rows: %s", err.Error())
	}
	return nil
}

func findRelatedProfiles(ctx context.Context, rctx runctx) error {
	return nil

	// toFind := gofakeit.Paragraph(1, 1, ctx.r.Intn(12)+1, "\n") // one sentence to search

	// ctx.l.Sugar().Debugf("Searching for related profiles with string %s", toFind)
	// params := make(map[string]interface{}, 1)
	// params["email"] = toFind

	// matchResult, err := scope.Search(
	// 	"profile-statuses",
	// 	search.NewMatchQuery(tofind),
	// 	&gocb.SearchOptions{
	// 		Limit:  10,
	// 		Fields: []string{"description"},
	// 	},
	// )
	// if err != nil {
	// 	panic(err)
	// }

	// for matchResult.Next() {
	// 	row := matchResult.Row()
	// 	docID := row.ID
	// 	score := row.Score

	// 	var fields interface{}
	// 	err := row.Fields(&fields)
	// 	if err != nil {
	// 		panic(err)
	// 	}

	// 	fmt.Printf("Document ID: %s, search score: %f, fields included in result: %v\n", docID, score, fields)
	// }

	// // always check for errors after iterating
	// err = matchResult.Err()
	// if err != nil {
	// 	panic(err)
	// }

}

func Run(numConc int, runTime time.Duration) {
	// Map of function names to functions
	functions := map[string]func(ctx context.Context, rctx runctx) error{
		"fetchProfile":        fetchProfile,        // similar to login or looking at someone
		"updateProfile":       updateProfile,       // updating a status on the profile
		"lockProfile":         lockProfile,         // disable or enable a random profile (account lockout)
		"findProfile":         findProfile,         // find a profile by a secondary index (email address)
		"findRelatedProfiles": findRelatedProfiles, // look for people with similar interests
	}

	// Map operations to their corresponding indices
	operationIndices := map[string]int{}
	for i, op := range operations {
		operationIndices[op] = i
	}

	// Probability matrix
	probabilities := [][]float64{
		{0, 0.8, 0.1, 0.05, 0.05},
		{0.6, 0, 0.2, 0.1, 0.1},
		{0.5, 0.3, 0, 0.15, 0.05},
		{0.4, 0.3, 0.2, 0, 0.1},
		{0.3, 0.3, 0.2, 0.2, 0},
	}

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

	wg.Add(numConc)
	for i := 0; i < numConc; i++ {
		go runLoop(ctx, probabilities, functions, operations, runTime, i, &wg)
	}

	wg.Wait()

}

func runLoop(
	ctx context.Context,
	probabilities [][]float64,
	functions map[string]func(context.Context, runctx) error,
	operations []string,
	runTime time.Duration,
	runnerId int,
	wg *sync.WaitGroup) {

	// Current operation index
	currOpIndex := 0

	rng := rand.NewSource(int64(randSeed + runnerId))
	r := rand.New(rng)

	timeout := time.After(runTime)

	slog := zap.L().Sugar()

	slog.Debugf("Starting runner %d…", runnerId)

	var runCtx runctx
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
				slog.Debug("operation failed", zap.String("operation", nextFunction), zap.Error(err))
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

func loadData() {
	workChan := make(chan docType, numConc)
	shutdownChan := make(chan struct{}, numConc)
	var wg sync.WaitGroup

	wg.Add(numConc)
	for i := 0; i < numConc; i++ {
		go func() {
			for {
				select {
				case doc := <-workChan:
					_, err := collection.Upsert(doc.Name, doc.Data, nil)
					if err != nil {
						errors.Wrap(err, "Data load upsert failed.")
					}
				case <-shutdownChan:
					wg.Done()
					return
				}
			}
		}()
	}

	gofakeit.Seed(11211)
	rng := rand.NewSource(int64(randSeed))
	r := rand.New(rng)

	// Create a random document with a realistic size from name, email, status text and whether
	// or not the account is enabled.
	for i := 0; i < numItems; i++ {
		iu := User{
			Name:    gofakeit.Name(),
			Email:   gofakeit.Email(), // TODO: make the email actually based on the name (pedantic)
			Created: gofakeit.DateRange(time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local), time.Date(2025, 1, 1, 0, 0, 0, 0, time.Local)),
			Status:  gofakeit.Paragraph(1, r.Intn(8)+1, r.Intn(12)+1, "\n"),
			Enabled: true,
		}

		workChan <- docType{
			Name: fmt.Sprintf("u%d", i),
			Data: iu,
		}
	}

}
