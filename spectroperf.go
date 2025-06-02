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
package main

import (
	"context"
	"crypto/x509"
	"errors"

	"fmt"
	"log"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/BurntSushi/toml"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/spectroperf/configuration"
	"github.com/couchbaselabs/spectroperf/workload"
	"github.com/couchbaselabs/spectroperf/workload/workloads"
	"go.uber.org/zap"
)

var wg sync.WaitGroup

func initLogger(debug bool) {
	if debug {
		zap.ReplaceGlobals(zap.Must(zap.NewDevelopment()))
	} else {
		zap.ReplaceGlobals(zap.Must(zap.NewProduction())) // TODO: replace this with a logger from CLI
	}
}

func main() {
	initLogger(false)

	flags := configuration.ParseFlags()

	config := configuration.DefaultConfig()
	if flags.ConfigFile != "" {
		_, err := toml.DecodeFile(flags.ConfigFile, &config)
		if err != nil {
			zap.L().Fatal("Error decoding config file", zap.Error(err))
		}
	}

	config = configuration.OverwriteConfigWithFlags(config)
	zap.L().Info("Successfully parsed config", zap.Any("Config", config))

	initLogger(config.Debug)

	if config.Connstr == "" {
		zap.L().Fatal("No connection string provided")
	}

	var sleep time.Duration
	var err error
	if config.Sleep == "" {
		zap.L().Info("no sleep set, random sleep duration will be used")
	} else {
		sleep, err = time.ParseDuration(config.Sleep)
		if err != nil {
			zap.L().Fatal("parsing sleep duration from config", zap.Error(err))
		}

		if sleep < time.Duration(time.Millisecond*100) {
			zap.L().Fatal("sleep cannot be less than 100ms, to increase throughput increase number of users")
		}
	}

	if !config.EnableTracing {
		if config.OtlpEndpoint != workload.DefaultOtlpEndpoint {
			zap.L().Fatal("Otlp endpoint provided but tracing disabled")
		}

		if config.OtelExporterHeaders != "" {
			zap.L().Fatal("OtelExporterHeaders provided but tracing disabled")
		}
	}

	caCertPool := x509.NewCertPool()
	if config.Cert != "" {
		caCert, err := os.ReadFile(config.Cert)
		if err != nil {
			zap.L().Fatal("Failed to read certificate", zap.String("error", err.Error()))
		}

		caCertPool.AppendCertsFromPEM(caCert)
	}

	if config.RampTime > config.RunTime/2 {
		zap.L().Fatal("Ramp time cannot be greater than half of the total runtime")
	}

	// Set up OpenTelemetry.
	otelShutdown, tracer, err := workload.SetupOTelSDK(context.Background(), config.OtlpEndpoint, config.EnableTracing, config.OtelExporterHeaders)
	if err != nil {
		return
	}
	// Handle shutdown properly so nothing leaks.
	defer func() {
		err = errors.Join(err, otelShutdown(context.Background()))
	}()

	opts := gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: config.Username,
			Password: config.Password,
		},
		SecurityConfig: gocb.SecurityConfig{TLSSkipVerify: config.TlsSkipVerify, TLSRootCAs: caCertPool},
		Tracer:         tracer,
	}

	cluster, err := gocb.Connect(config.Connstr, opts)
	if err != nil {
		log.Fatalf("Failed to connect to cluster: %s", err)
	}

	bucket := cluster.Bucket(config.Bucket)
	collection := bucket.Scope(config.Scope).Collection(config.Collection)

	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		zap.L().Fatal("Failed to connect to Bucket", zap.String("Bucket", config.Bucket), zap.String("error", err.Error()))
	}

	var w workload.Workload
	switch config.Workload {
	case "user-profile":
		w = workloads.NewUserProfile(config.NumItems, bucket.Name(), bucket.Scope(config.Scope), collection, cluster)
	case "user-profile-dapi":
		w = workloads.NewUserProfileDapi(config.DapiConnstr, config.Bucket, config.Scope, collection, config.NumItems, config.Username, config.Password, cluster)
	default:
		zap.L().Fatal("Unknown workload type", zap.String("workload", config.Workload))
	}

	var markovChain [][]float64
	if len(config.MarkovChain) != 0 {
		if err := validateMarkovChain(len(w.Operations()), config.MarkovChain); err != nil {
			zap.L().Fatal("invalid markov chain", zap.Error(err))
		}
		markovChain = config.MarkovChain
	}

	if config.OnlyOperation != "" {
		if len(config.MarkovChain) != 0 {
			zap.L().Fatal("cannot specify only-operation and a markov chain", zap.Error(err))
		}

		markovChain, err = buildMarkovChain(config.OnlyOperation, w)
		if err != nil {
			zap.L().Fatal("building markov chain", zap.Error(err))
		}
	}

	if len(markovChain) == 0 {
		zap.L().Info("neither markov chain or only operation specified, using built in workload proabilities")
		markovChain = w.Probabilities()
	}

	workload.InitMetrics(w)

	zap.L().Info("Setting up for workload", zap.String("workload", config.Workload))

	// call the setup function on the workload.
	workload.Setup(w, config.NumItems, bucket.Scope(config.Scope), collection)

	time.Sleep(5 * time.Second)

	zap.L().Info("Running workload…\n")
	workload.Run(w, markovChain, config.NumUsers, time.Duration(config.RunTime)*time.Minute, time.Duration(config.RampTime)*time.Minute, tracer, sleep)

	wg.Wait()

}

// validateMarkov chain checks that the markov chain from the config file
// valid by making sure that:
// - all rows sum to 1
// - is square
// - has dimensions equal to number of workload operations
func validateMarkovChain(workloadOperations int, mChain [][]float64) error {
	zap.L().Info("Validating Markov chain from config file")

	dimensionError := fmt.Errorf("Markov chain must be square array with dimensions equal to number of workload functions")

	if len(mChain) != workloadOperations {
		return dimensionError
	}

	for _, row := range mChain {
		if len(row) != workloadOperations {
			return dimensionError
		}

		var total float64
		for _, probability := range row {
			total += probability
		}

		if total != 1 {
			return fmt.Errorf("Markov Chain row does not sum to 1: %v", row)
		}
	}

	return nil
}

// buildMarkovChain builds a markov chain that will only perform the named
// operation from the chosen workload
func buildMarkovChain(operation string, w workload.Workload) ([][]float64, error) {
	zap.L().Info("building markov chain to perform one operation", zap.String("operation", operation))

	opIndex := slices.Index(w.Operations(), operation)
	if opIndex == -1 {
		return nil, fmt.Errorf("Chosen only-operation '%s' is not supported by workload", operation)
	}

	markovChain := make([][]float64, len(w.Operations()))
	row := make([]float64, len(w.Operations()))
	row[opIndex] = 1.0
	for i := 0; i < len(w.Operations()); i++ {
		markovChain[i] = row
	}

	return markovChain, nil
}
