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
	"github.com/BurntSushi/toml"
	"log"
	"os"
	"sync"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/spectroperf/workload"
	"github.com/couchbaselabs/spectroperf/workload/workloads"
	"go.uber.org/zap"
)

var wg sync.WaitGroup

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction())) // TODO: replace this with a logger from CLI
}

func main() {
	config := parseFlags()

	if config.configFile != "" {
		_, err := toml.DecodeFile(config.configFile, &config)
		if err != nil {
			zap.L().Fatal("Error decoding config file", zap.Error(err))
		}
	}

	zap.L().Info("Successfully parsed config", zap.String("Config", fmt.Sprintf("%+v", config)))

	if config.Connstr == "" {
		zap.L().Fatal("No connection string provided")
	}

	caCert, err := os.ReadFile(config.Cert)
	if err != nil {
		zap.L().Fatal("Failed to read certificate", zap.String("error", err.Error()))
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	if config.RampTime > config.RunTime/2 {
		zap.L().Fatal("Ramp time cannot be greater than half of the total runtime")
	}

	// TODO: add a param to set this up if debugging gocb issues.  Probably with the system logger.
	// gocb.SetLogger(gocb.VerboseStdioLogger())

	// TODO: sometimes you need a Cert for couchbase2://, and then need to laod it and pass it as part of the security config
	// caCert, err := os.ReadFile("gateway-CA.crt")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// caCertPool := x509.NewCertPool()
	// caCertPool.AppendCertsFromPEM(caCert)

	// Set up OpenTelemetry.
	otelShutdown, tracer, err := workload.SetupOTelSDK(context.Background(), config.OtlpEndpoint, config.EnableTracing)
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
		SecurityConfig: gocb.SecurityConfig{TLSSkipVerify: config.TlsSkipVerify},
		Tracer:         tracer,
	}

	cluster, err := gocb.Connect(config.Connstr, opts)
	if err != nil {
		log.Fatal(fmt.Sprintf("Failed to connect to cluster: %s", err))
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
		w = workloads.NewUserProfile(config.NumItems, bucket.Scope(config.Scope), collection)
	case "user-profile-dapi":
		w = workloads.NewUserProfileDapi(config.DapiConnstr, config.Bucket, config.Scope, config.Collection, config.NumItems, config.Username, config.Password)
	default:
		zap.L().Fatal("Unknown workload type", zap.String("workload", config.Workload))
	}

	workload.InitMetrics(w)

	zap.L().Info("Setting up for workload", zap.String("workload", config.Workload))

	// call the setup function on the workload.
	workload.Setup(w, config.NumItems, bucket.Scope(config.Scope), collection)

	time.Sleep(5 * time.Second)

	zap.L().Info("Running workload…\n")
	workload.Run(w, config.NumUsers, time.Duration(config.RunTime)*time.Minute, time.Duration(config.RampTime)*time.Minute, tracer)

	wg.Wait()

}

type Flags struct {
	Connstr       string
	Cert          string
	Username      string
	Password      string
	Bucket        string
	Scope         string
	Collection    string
	NumItems      int
	NumUsers      int
	TlsSkipVerify bool
	Workload      string
	DapiConnstr   string
	RunTime       int
	RampTime      int
	configFile    string
	OtlpEndpoint  string
	EnableTracing bool
}

func parseFlags() Flags {
	flags := Flags{}
	flag.StringVar(&flags.Connstr, "connstr", "", "connection string of the cluster under test")
	flag.StringVar(&flags.Cert, "Cert", "rootCA.crt", "path to certificate file")
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
	flag.StringVar(&flags.OtlpEndpoint, "otlp-endpoint", "localhost:4318", "endpoint OTEL traces will be exported to")
	flag.BoolVar(&flags.EnableTracing, "enable-tracing", false, "enables OTEL tracing")
	flag.Parse()

	return flags
}
