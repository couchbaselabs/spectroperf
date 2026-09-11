package workloads

import (
	"context"
	"testing"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/spectroperf/configuration"
	"github.com/couchbaselabs/spectroperf/workload"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// probabilityTolerance is the slack allowed when checking that a probability row
// sums to 1, so that rows built from values such as 0.1 are not failed by float
// rounding.
const probabilityTolerance = 1e-9

// TestWorkloadConformance checks every registered workload against the invariants the
// runner relies on. A workload that fails here will misbehave or panic at run time,
// so new workloads must pass this before they are considered finished.
func TestWorkloadConformance(t *testing.T) {
	for _, name := range Names() {
		t.Run(name, func(t *testing.T) {
			w := buildWorkload(t, name)

			operations := w.Operations()
			probabilities := w.Probabilities()
			functions := w.Functions()

			require.NotEmpty(t, operations, "Operations must not be empty")

			// Operation names must be unique, otherwise Functions silently drops
			// one of the duplicates and the metrics for them are merged.
			seen := make(map[string]bool, len(operations))
			for _, operation := range operations {
				assert.False(t, seen[operation], "operation %q is listed more than once in Operations", operation)
				seen[operation] = true
			}

			// The runner indexes Probabilities by the current operation index and
			// treats each row as the distribution over the next operation, so the
			// matrix must be square with one row and column per operation.
			assert.Len(t, probabilities, len(operations), "Probabilities must have one row per operation")
			for i, row := range probabilities {
				assert.Len(t, row, len(probabilities), "Probabilities row %d must have one column per operation", i)
			}

			// Each row must be a complete distribution, otherwise the runner's
			// fallback quietly biases the workload towards the last operation.
			for i, row := range probabilities {
				total := 0.0
				for _, probability := range row {
					total += probability
				}
				assert.InDelta(t, 1.0, total, probabilityTolerance, "Probabilities row %d must sum to 1", i)
			}

			// The runner looks each operation up in Functions by name, so a missing
			// key is a nil call and an extra key is an operation that never runs.
			assert.ElementsMatch(t, operations, functionNames(functions), "Functions keys must exactly match Operations")
		})
	}
}

// buildWorkload constructs the named workload against an unconnected cluster. gocb
// connects lazily, so this is enough to exercise everything the conformance checks
// need without a live Couchbase.
func buildWorkload(t *testing.T, name string) workload.Workload {
	t.Helper()

	cluster, err := gocb.Connect("couchbase://localhost", gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{Username: "user", Password: "password"},
	})
	require.NoError(t, err, "failed to create cluster")

	config := &configuration.Config{
		DapiConnstr: "http://localhost:18093",
		Username:    "user",
		Password:    "password",
		NumItems:    100,
		Bucket:      "bucket",
		Scope:       "scope",
		Collection:  "collection",
	}

	w, err := New(name, zaptest.NewLogger(t, zaptest.Level(zap.ErrorLevel)), config, cluster)
	require.NoError(t, err, "failed to build workload")

	return w
}

func functionNames(functions map[string]func(ctx context.Context, rctx workload.Runctx) error) []string {
	names := make([]string, 0, len(functions))
	for name := range functions {
		names = append(names, name)
	}

	return names
}
