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

	caCert, err := os.ReadFile("rootCA.crt")
	if err != nil {
		log.Fatal(err)
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

	bucketName := "data"

	opts := gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: "Administrator",
			Password: "password",
		},
	}

	cluster, err := gocb.Connect("couchbase://192.168.107.3:30690", opts)
	if err != nil {
		panic(err)
	}

	bucket := cluster.Bucket(bucketName)
	collection := bucket.Scope("identity").Collection("profiles")

	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		log.Fatal(err)
	}

	zap.L().Info("Setting up for workload…\n")

	// call the setup function on the workload.
	workloads.Setup(200000, 50000, bucket.Scope("identity"), collection) // TODO: replace all of these arguments with CLI inputs or defaults

	time.Sleep(5 * time.Second)

	zap.L().Info("Running workload…\n")
	workloads.Run(128, time.Duration(5)*time.Minute)

	wg.Wait()

}
