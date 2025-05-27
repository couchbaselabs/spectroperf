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
	"flag"
	"fmt"
	"log"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/BurntSushi/toml"

	"github.com/couchbase/gocb/v2"
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
	config := parseFlags()

	if config.configFile != "" {
		_, err := toml.DecodeFile(config.configFile, &config)
		if err != nil {
			zap.L().Fatal("Error decoding config file", zap.Error(err))
		}
	}

	initLogger(config.Debug)
	zap.L().Info("Successfully parsed config", zap.Any("Config", config))

	if config.Connstr == "" {
		zap.L().Fatal("No connection string provided")
	}

	if config.SleepMillis != -1 && config.SleepMillis < 100 {
		zap.L().Fatal("sleep millis cannot be less than 100, to increase throughput increase number of users")
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
		w = workloads.NewUserProfile(config.NumItems, bucket.Scope(config.Scope), collection, cluster)
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
	workload.Run(w, markovChain, config.NumUsers, time.Duration(config.RunTime)*time.Minute, time.Duration(config.RampTime)*time.Minute, tracer, config.SleepMillis)

	wg.Wait()

}

type Flags struct {
	Connstr             string
	Cert                string
	Username            string
	Password            string
	Bucket              string
	Scope               string
	Collection          string
	NumItems            int
	NumUsers            int
	TlsSkipVerify       bool
	Workload            string
	DapiConnstr         string
	RunTime             int
	RampTime            int
	configFile          string
	OtlpEndpoint        string
	EnableTracing       bool
	OtelExporterHeaders string
	Debug               bool
	MarkovChain         [][]float64
	OnlyOperation       string
	SleepMillis         int
}

func parseFlags() Flags {
	flags := Flags{}
	flag.StringVar(&flags.Connstr, "connstr", "", "connection string of the cluster under test")
	flag.StringVar(&flags.Cert, "Cert", "", "path to certificate file")
	flag.StringVar(&flags.Username, "Username", "Administrator", "Username for cluster under test")
	flag.StringVar(&flags.Password, "password", "password", "password of the cluster under test")
	flag.StringVar(&flags.Bucket, "Bucket", "data", "Bucket name")
	flag.StringVar(&flags.Scope, "Scope", "identity", "Scope name")
	flag.StringVar(&flags.Collection, "Collection", "profiles", "Collection name")
	flag.IntVar(&flags.NumItems, "num-items", 200000, "number of docs to create")
	flag.IntVar(&flags.NumUsers, "num-users", 50000, "number of concurrent simulated users accessing the data")
	flag.BoolVar(&flags.TlsSkipVerify, "tls-skip-verify", false, "skip TLS certificate verification")
	flag.StringVar(&flags.Workload, "workload", "", "workload name")
	flag.StringVar(&flags.DapiConnstr, "dapi-connstr", "", "connection string for data api")
	flag.IntVar(&flags.RunTime, "run-time", 5, "total time to run the workload in minutes")
	flag.IntVar(&flags.RampTime, "ramp-time", 1, "length of ramp-up and ramp-down periods in minutes")
	flag.StringVar(&flags.configFile, "config-file", "", "path to configuration file")
	flag.StringVar(&flags.OtlpEndpoint, "otlp-endpoint", workload.DefaultOtlpEndpoint, "endpoint OTEL traces will be exported to")
	flag.BoolVar(&flags.EnableTracing, "enable-tracing", false, "enables OTEL tracing")
	flag.StringVar(&flags.OtelExporterHeaders, "otel-exporter-headers", "", "a comma seperated list of otlp expoter headers, e.g 'header1=value1,header2=value2'")
	flag.BoolVar(&flags.Debug, "debug", false, "turn on debug level logging")
	flag.StringVar(&flags.OnlyOperation, "only-operation", "", "the only operation to run from the workload")
	flag.IntVar(&flags.SleepMillis, "sleep-millis", -1, "time to sleep between operations in millisecconds")
	flag.Parse()

	return flags
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
