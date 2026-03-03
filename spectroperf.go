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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	gotel "github.com/couchbase/gocb-opentelemetry"
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

func main() {
	cobra.CheckErr(rootCmd.Execute())
}

func init() {
	rootCmd.Flags().StringVar(&cfgFile, "config-file", "", "path to configuration file")

	configFlags := pflag.NewFlagSet("", pflag.ContinueOnError)
	configFlags.String("connstr", "", "connection string of the cluster under test")
	configFlags.String("dapi-connstr", "", "connection string for data api")
	configFlags.String("username", configuration.DefaultUsername, "username for cluster under test")
	configFlags.String("password", configuration.DefaultPassword, "password of the cluster under test")
	configFlags.String("cert", "", "path to certificate file")
	configFlags.Bool("tls-skip-verify", false, "skip tls certificate verification")
	configFlags.String("log-level", configuration.DefaultLogLevel, "the log level to run at")
	configFlags.String("workload", "", "workload name")
	configFlags.Int("num-items", 500, "number of docs to create")
	configFlags.Int("num-users", 500, "number of concurrent simulated users accessing the data")
	configFlags.String("run-time", configuration.DefaultRunTime, "total time to run the workload (e.g. '5m', '30s')")
	configFlags.String("ramp-time", configuration.DefaultRampTime, "length of ramp-up and ramp-down periods (e.g. '1m', '30s')")
	configFlags.String("only-operation", "", "the only operation to run from the workload")
	configFlags.String("sleep", "", "time to sleep between operations")
	configFlags.String("bucket", configuration.DefaultBucket, "bucket name")
	configFlags.String("scope", configuration.DefaultScope, "scope name")
	configFlags.String("collection", configuration.DefaultCollection, "collection name")
	configFlags.Bool("enable-tracing", false, "enables otel tracing")
	configFlags.String("otlp-endpoint", configuration.DefaultOtlpEndpoint, "endpoint otel traces will be exported to")
	configFlags.String("otel-exporter-headers", "", "a comma seperated list of otel expoter headers, e.g 'header1=value1,header2=value2'")
	configFlags.Int("dial-timeout", configuration.DefaultDialTimeout, "TCP dial timeout in seconds for DAPI HTTP clients")
	configFlags.Int("response-header-timeout", configuration.DefaultResponseHeaderTimeout, "response header timeout in seconds for DAPI HTTP clients")
	configFlags.Int("request-timeout", configuration.DefaultRequestTimeout, "overall request timeout in seconds for DAPI HTTP clients")
	configFlags.Int("idle-conn-timeout", configuration.DefaultIdleConnTimeout, "idle connection timeout in seconds for DAPI HTTP clients")
	rootCmd.Flags().AddFlagSet(configFlags)

	_ = viper.BindPFlags(configFlags)
}

func getLogger(startTime string) (zap.AtomicLevel, *zap.Logger) {
	logLevel := zap.NewAtomicLevel()
	logConfig := zap.NewProductionEncoderConfig()
	logConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	jsonEncoder := zapcore.NewJSONEncoder(logConfig)
	filePath := fmt.Sprintf("%s/spectroperf.log", startTime)
	logFile, _ := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	writer := zapcore.AddSync(logFile)
	core := zapcore.NewTee(
		zapcore.NewCore(jsonEncoder, writer, logLevel),
		zapcore.NewCore(jsonEncoder, zapcore.AddSync(os.Stdout), logLevel),
	)
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	return logLevel, logger
}

func connectToCluster(config *configuration.Config, tracer *gotel.OpenTelemetryRequestTracer, logger *zap.Logger) (*gocb.Cluster, error) {
	var caCertPool *x509.CertPool
	if config.Cert != "" {
		caCert, err := os.ReadFile(config.Cert)
		if err != nil {
			return nil, fmt.Errorf("failed to read certificate: %w", err)
		}

		caCertPool, err = x509.SystemCertPool()
		if err != nil {
			logger.Warn("failed to load system cert pool, creating new cert pool with provided certificate only", zap.Error(err))
			caCertPool = x509.NewCertPool()
		}

		ok := caCertPool.AppendCertsFromPEM(caCert)
		if !ok {
			return nil, fmt.Errorf("failed to append certificate")
		}
	}

	opts := gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: config.Username,
			Password: config.Password,
		},
		SecurityConfig: gocb.SecurityConfig{TLSSkipVerify: config.TlsSkipVerify, TLSRootCAs: caCertPool},
		Tracer:         tracer,
	}

	logger.Info("Connecting to cluster", zap.String("connstr", config.Connstr))
	cluster, err := gocb.Connect(config.Connstr, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to cluster: %w", err)
	}

	logger.Info("Successfully connected to cluster")

	return cluster, nil
}

func startSpectroperf() {
	startTime := time.Now().UTC().Format("2006-01-02-15:04")
	if err := os.Mkdir(startTime, 0755); err != nil {
		fmt.Printf("creating directory for spectroperf artefacts: %v\n", err)
		return
	}

	logLevel, logger := getLogger(startTime)

	if cfgFile != "" {
		logger.Info("config file provided", zap.String("config", cfgFile))

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

	execConfig, err := configuration.CreateExecutionConfig(logger, config)
	if err != nil {
		logger.Fatal("failed to create execution config", zap.Error(err))
	}

	// Set up OpenTelemetry.
	otelShutdown, tracer, err := workload.SetupOTelSDK(context.Background(), logger, config)
	if err != nil {
		return
	}
	// Handle shutdown properly so nothing leaks.
	defer func() {
		err = errors.Join(err, otelShutdown(context.Background()))
	}()

	cluster, err := connectToCluster(config, tracer, logger)
	if err != nil {
		logger.Fatal("failed to connect to cluster", zap.Error(err))
	}
	bucket := cluster.Bucket(config.Bucket)

	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		logger.Fatal("Failed to connect to Bucket", zap.String("Bucket", config.Bucket), zap.String("error", err.Error()))
	}

	var w workload.Workload
	switch config.Workload {
	case "user-profile":
		w = workloads.NewUserProfile(logger, config, cluster)
	case "user-profile-dapi":
		w = workloads.NewUserProfileDapi(logger, config, cluster)
	case "basic-dapi":
		w = workloads.NewBasicDapi(logger, config, cluster)
	case "basic":
		w = workloads.NewBasic(logger, config, cluster)
	default:
		logger.Fatal("Unknown workload type", zap.String("workload", config.Workload))
	}

	markovChain, err := configuration.CreateMarkovChain(logger, config, w.Operations(), w.Probabilities())
	if err != nil {
		logger.Fatal("failed to create markov chain", zap.Error(err))
	}

	workload.InitMetrics(w, logger)

	logger.Info("Setting up for workload", zap.String("workload", config.Workload))

	// call the setup function on the workload.
	collection := bucket.Scope(execConfig.Scope).Collection(execConfig.Collection)
	workload.Setup(w, logger, execConfig.NumItems, collection)

	workload.Run(w, logger, execConfig, markovChain, tracer)

	if err := configuration.WriteConfig(config, startTime, w.Probabilities()); err != nil {
		logger.Fatal("writing config to file", zap.Error(err))
	}

	logger.Info("successfully written config file")

	if !workload.PrometheusIsRunning() {
		logger.Info("skipping writing metrics to file as prometheus is not running")
		return
	}

	logger.Info("scraping operation metrics from prometheus to write to file")

	// Add a minute onto the range to make sure none of the metrics are missed.
	runTimeMinutes := int(execConfig.RunTime.Minutes())
	if runTimeMinutes < 1 {
		runTimeMinutes = 1
	}

	timeRange := runTimeMinutes + 1
	metricSummaries := map[string]workload.OperationSummary{}
	for _, op := range w.Operations() {
		summary, err := workload.SummariseOperationMetrics(op, timeRange)
		if err != nil {
			logger.Info("skipping operation due to error", zap.Error(err), zap.String("operation", op))
			continue
		}

		metricSummaries[op] = *summary
	}

	summaryOutput := map[string]any{}
	summaryOutput["metricSummaries"] = metricSummaries
	summaryOutput["steadyStateDurationMins"] = (execConfig.RunTime - (2 * execConfig.RampTime)).Minutes()

	bytes, err := json.Marshal(summaryOutput)
	if err != nil {
		logger.Fatal("marshalling metric summary", zap.Error(err), zap.Any("summary", summaryOutput))
	}

	filePath := fmt.Sprintf("%s/metrics.json", startTime)
	if err := os.WriteFile(filePath, bytes, 0644); err != nil {
		logger.Fatal("writing metric summary to file", zap.Error(err), zap.String("path", filePath))
	}
}
