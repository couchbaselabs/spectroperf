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
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/spectroperf/workloads"
	"go.uber.org/zap"
)

var wg sync.WaitGroup

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction())) // TODO: replace this with a logger from CLI
}

func main() {
	flags := parseFlags()

	if flags.connstr == "" {
		zap.L().Fatal("No connection string provided")
	}

	caCert, err := os.ReadFile(flags.cert)
	if err != nil {
		zap.L().Fatal("Failed to read certificate", zap.String("error", err.Error()))
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// for _, url := range os.Args[1:] {
	// 	local, n, err := fetch(url)
	// 	if err != nil {
	// 		fmt.Fprintf(os.Stderr, "fetch %s: %v\n", url, err)
	// 		continue
	// 	}
	// 	fmt.Fprintf(os.Stderr, "%s => %s (%d bytes).\n", url, local, n)
	// }

	opts := gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: flags.username,
			Password: flags.password,
		},
	}

	cluster, err := gocb.Connect(flags.connstr, opts)
	if err != nil {
		log.Fatal(fmt.Sprintf("Failed to connect to cluster: %s", err))
	}

	bucket := cluster.Bucket(flags.bucket)
	collection := bucket.Scope(flags.scope).Collection(flags.collection)

	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		zap.L().Fatal("Failed to connect to bucket", zap.String("bucket", flags.bucket), zap.String("error", err.Error()))
	}

	workloads.Init()

	zap.L().Info("Setting up for workload…\n")

	// call the setup function on the workload.
	workloads.Setup(flags.numItems, flags.numUsers, bucket.Scope(flags.scope), collection, flags.dapiConnstr, flags.username, flags.password)

	time.Sleep(5 * time.Second)

	zap.L().Info("Running workload…\n")
	workloads.Run(128, time.Duration(5)*time.Minute)

	wg.Wait()

}

type Flags struct {
	connstr     string
	dapiConnstr string
	cert        string
	username    string
	password    string
	bucket      string
	scope       string
	collection  string
	numItems    int
	numUsers    int
}

func parseFlags() Flags {
	flags := Flags{}
	flag.StringVar(&flags.connstr, "connstr", "", "connection string of the cluster under test")
	flag.StringVar(&flags.dapiConnstr, "dapi-connstr", "", "connection for data api")
	flag.StringVar(&flags.cert, "cert", "rootCA.crt", "path to certificate file")
	flag.StringVar(&flags.username, "username", "Administrator", "username for cluster under test")
	flag.StringVar(&flags.password, "password", "password", "password of the cluster under test")
	flag.StringVar(&flags.bucket, "bucket", "data", "bucket name")
	flag.StringVar(&flags.scope, "scope", "identity", "scope name")
	flag.StringVar(&flags.collection, "collection", "profiles", "collection name")
	flag.IntVar(&flags.numItems, "num-items", 200000, "number of docs to create")
	flag.IntVar(&flags.numUsers, "num-users", 50000, "number of concurrent simulated users accessing the data")
	flag.Parse()

	zap.L().Info("Parsed flags", zap.String("flags", fmt.Sprintf("%+v", flags)))

	return flags
}
