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
	"os"
	"slices"
	"sync"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/spectroperf/configuration"
	"github.com/couchbaselabs/spectroperf/workload"
	"github.com/couchbaselabs/spectroperf/workload/workloads"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var wg sync.WaitGroup
var cfgFile string

var rootCmd = &cobra.Command{
	Version: "1.0.0",

	Use:   "spectroperf",
	Short: "A performance analyzer, designed to execute mixed workloads against Couchbase",

	Run: func(cmd *cobra.Command, args []string) {
		startSpectroperf()
	},
}

func init() {
	rootCmd.Flags().StringVar(&cfgFile, "config-file", "", "path to configuration file")

	configFlags := pflag.NewFlagSet("", pflag.ContinueOnError)
	configFlags.String("connstr", "", "connection string of the cluster under test")
	configFlags.String("dapi-connstr", "", "connection string for data api")
	configFlags.String("username", "Administrator", "username for cluster under test")
	configFlags.String("password", "password", "password of the cluster under test")
	configFlags.String("cert", "", "path to certificate file")
	configFlags.Bool("tls-skip-verify", false, "skip tls certificate verification")
	configFlags.String("log-level", "info", "the log level to run at")
	configFlags.String("workload", "", "workload name")
	configFlags.Int("num-items", 500, "number of docs to create")
	configFlags.Int("num-users", 500, "number of concurrent simulated users accessing the data")
	configFlags.Int("run-time", 5, "total time to run the workload in minutes")
	configFlags.Int("ramp-time", 0, "length of ramp-up and ramp-down periods in minutes")
	configFlags.String("only-operation", "", "the only operation to run from the workload")
	configFlags.String("sleep", "", "time to sleep between operations")
	configFlags.String("bucket", "data", "bucket name")
	configFlags.String("scope", "identity", "scope name")
	configFlags.String("collection", "profiles", "collection name")
	configFlags.Bool("enable-tracing", false, "enables otel tracing")
	configFlags.String("otlp-endpoint", workload.DefaultOtlpEndpoint, "endpoint otel traces will be exported to")
	configFlags.String("otel-exporter-headers", "", "a comma seperated list of otel expoter headers, e.g 'header1=value1,header2=value2'")
	rootCmd.Flags().AddFlagSet(configFlags)

	_ = viper.BindPFlags(configFlags)
}

func getLogger() (zap.AtomicLevel, *zap.Logger) {
	logLevel := zap.NewAtomicLevel()
	logConfig := zap.NewProductionEncoderConfig()
	logConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	jsonEncoder := zapcore.NewJSONEncoder(logConfig)
	core := zapcore.NewTee(
		zapcore.NewCore(jsonEncoder, zapcore.AddSync(os.Stdout), logLevel),
	)
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	return logLevel, logger
}

func startSpectroperf() {
	// initialize the logger
	logLevel, logger := getLogger()

	logger.Info("parsed launch configuration", zap.String("config", cfgFile))

	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		err := viper.ReadInConfig()
		if err != nil {
			logger.Fatal("failed to load specified config file", zap.Error(err))
		}
	}

	config := configuration.ReadConfig(logger)
	parsedLogLevel, err := zapcore.ParseLevel(config.LogLevel)
	if err != nil {
		logger.Warn("invalid log level specified, using INFO instead")
		parsedLogLevel = zapcore.InfoLevel
	}
	logLevel.SetLevel(parsedLogLevel)

	if config.Connstr == "" {
		logger.Fatal("No connection string provided")
	}

	var sleep time.Duration
	if config.Sleep == "" {
		logger.Info("no sleep set, random sleep duration will be used")
	} else {
		sleep, err = time.ParseDuration(config.Sleep)
		if err != nil {
			logger.Fatal("parsing sleep duration from config", zap.Error(err))
		}

		if sleep < time.Duration(time.Millisecond*100) {
			logger.Fatal("sleep cannot be less than 100ms, to increase throughput increase number of users")
		}
	}

	if !config.EnableTracing {
		if config.OtlpEndpoint != workload.DefaultOtlpEndpoint {
			logger.Fatal("Otlp endpoint provided but tracing disabled")
		}

		if config.OtelExporterHeaders != "" {
			logger.Fatal("OtelExporterHeaders provided but tracing disabled")
		}
	}

	caCertPool := x509.NewCertPool()
	if config.Cert != "" {
		caCert, err := os.ReadFile(config.Cert)
		if err != nil {
			logger.Fatal("Failed to read certificate", zap.String("error", err.Error()))
		}

		caCertPool.AppendCertsFromPEM(caCert)
	}

	if config.RampTime > config.RunTime/2 {
		logger.Fatal("Ramp time cannot be greater than half of the total runtime")
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
		logger.Fatal("Failed to connect to cluster: %s", zap.Error(err))
	}

	bucket := cluster.Bucket(config.Bucket)
	collection := bucket.Scope(config.Scope).Collection(config.Collection)

	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		logger.Fatal("Failed to connect to Bucket", zap.String("Bucket", config.Bucket), zap.String("error", err.Error()))
	}

	var w workload.Workload
	switch config.Workload {
	case "user-profile":
		w = workloads.NewUserProfile(logger, bucket.Name(), config.NumItems, bucket.Scope(config.Scope), collection, cluster)
	case "user-profile-dapi":
		w = workloads.NewUserProfileDapi(logger, config, collection, cluster)
	default:
		logger.Fatal("Unknown workload type", zap.String("workload", config.Workload))
	}

	var markovChain [][]float64
	if len(config.MarkovChain) != 0 {
		if err := validateMarkovChain(len(w.Operations()), config.MarkovChain); err != nil {
			logger.Fatal("invalid markov chain", zap.Error(err))
		}
		markovChain = config.MarkovChain
	}

	if config.OnlyOperation != "" {
		if len(config.MarkovChain) != 0 {
			logger.Fatal("cannot specify only-operation and a markov chain", zap.Error(err))
		}

		markovChain, err = buildMarkovChain(config.OnlyOperation, w)
		if err != nil {
			logger.Fatal("building markov chain", zap.Error(err))
		}
	}

	if len(markovChain) == 0 {
		logger.Info("neither markov chain or only operation specified, using built in workload proabilities")
		markovChain = w.Probabilities()
	}

	workload.InitMetrics(w)

	logger.Info("Setting up for workload", zap.String("workload", config.Workload))

	// call the setup function on the workload.
	workload.Setup(w, logger, config.NumItems, bucket.Scope(config.Scope), collection)

	time.Sleep(5 * time.Second)

	logger.Info("Running workload…\n")
	workload.Run(w, logger, markovChain, config.NumUsers, time.Duration(config.RunTime)*time.Minute, time.Duration(config.RampTime)*time.Minute, tracer, sleep)

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

func main() {
	cobra.CheckErr(rootCmd.Execute())
}
