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
	"time"

	gotel "github.com/couchbase/gocb-opentelemetry"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/spectroperf/configuration"
	"github.com/couchbaselabs/spectroperf/workload"
	"github.com/couchbaselabs/spectroperf/workload/workloads"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

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

	configFlags := configuration.NewFlagSet()
	rootCmd.Flags().AddFlagSet(configFlags)
	cobra.CheckErr(configuration.BindFlagSet(configFlags))
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

	workload.InitMetrics(logger, w, execConfig.NumUsers)

	logger.Info("Setting up for workload", zap.String("workload", config.Workload))

	// call the setup function on the workload.
	collection := bucket.Scope(execConfig.Scope).Collection(execConfig.Collection)
	workload.Setup(w, logger, execConfig.NumItems, collection)

	workload.Run(w, logger, execConfig, markovChain, tracer)

	if err := configuration.WriteConfig(config, startTime, w.Probabilities()); err != nil {
		logger.Fatal("writing config to file", zap.Error(err))
	}

	logger.Info("successfully written config file")

	summary := []workload.RunSummary{}
	for _, numUsers := range execConfig.NumUsers {
		s := workload.CreateSummary(logger, numUsers, w, execConfig.RunTime, execConfig.RampTime)
		summary = append(summary, s)
	}

	filePath := fmt.Sprintf("%s/metrics.json", startTime)
	if err := writeSummary(summary, filePath); err != nil {
		logger.Fatal("writing summary to file", zap.Error(err), zap.String("path", filePath))
	}

	logger.Info("wrote metrics to file", zap.String("path", filePath))
}

// writeSummary marshals the summary to a pretty-printed 'metrics.json' file
// within the startTime directory.
func writeSummary(summary []workload.RunSummary, path string) error {
	summaryBytes, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return fmt.Errorf("marshalling summary: %w", err)
	}

	if err := os.WriteFile(path, summaryBytes, 0644); err != nil {
		return fmt.Errorf("writing metric summary to file: %w", err)
	}

	return nil
}
