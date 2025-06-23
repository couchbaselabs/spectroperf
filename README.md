# Spectroperf

## Overview
Spectroperf is a performance analyzer, designed to execute mixed workloads against various Couchbase configurations.

It is intended to meet a few goals:
1. Represent a mixed workload of operations against a system
	1. Yet still be tune-able by adjusting the mix of operations
1. Not exhibit the coordinated omission problem found in other workload generators like YCSB.
1. Allow for this common set of workload operations to have different implementation backends.

Spectroperf is not itself a framework for executing a large amount of workload.
Instead, it is built with the expectation that you may use an external job controller to scale up Spectroperf runners, aggregating their statistics.

You may ask, what is [coordinated omission](https://groups.google.com/g/mechanical-sympathy/c/icNZJejUHfE/m/BfDekfBEs_sJ?pli=1)?  
This is a term coined by Gil Tene of Azul Systems where, in summary, artificial workload generators will often reduce the rate of requests based on the response of the underlying system.
This is unrealistic.
An example of this concept… imagine you run a large email hosting website.
If you have a partial outage on your site that doesn't affect availability but does affect service times, you will still have the same number of users logging in and trying to check their mail.
They will have slower service times, to be sure.
Some real users may generate a little less workload, but not much.
Workload generators that are a tight busy loop will tend to have a drop in throughput when the system under test's latency rises.
In turn, this actually tends to make statistics about latencies kind of useless.
It will also tend to show any small variation in latency, even a slight increase in tail latency, as a big drop in throughput.

The name comes from the Nebula metaphor from which we've named our newer edge-proxy pieces such as the gRPC interface (protostellar), its implementation (stellar-gateway, a.k.a. Cloud Native Gateway) and related pieces.
In astronomy, you may use spectography to understand what something far away is made up of, and in this way spectroperf lets us understand raw Couchbase, Couchbase through the Cloud Native Gateway, etc.

## Status

### Milestones
0. Load boring docs (done!)
1. Running a bunch of concurrency (goroutines?) simulating a bunch of users with random think time doing operations. (done!)
2. Having stats for those operations exported via prometheus and loggable to a file
3. Defining workloads in go code with some kind of 'annotation' (done! can't really do that with go, used map)
4. Defining workload mixes (done!)
5. Data API backend (done!)
6. Lots of parameterization

#### Open Questions
Should the ops for the workload just be codified?  Or no?  Impacts stats?

#### Stats

- Number of operations attempted
- Number of errors (successes can be implied)
- Histogram of response times for operations


## Workload Definitions

At the moment, Spectroperf mimics a user profile which is a variable length JSON document with a few fields.
It simulates a few different operations that might actually happen with a real user profile.

Operation types
* fetchProfile,        // similar to login or looking at someone
* updateProfile,       // updating a status on the profile
* lockProfile,         // disable or enable a random profile (account lockout)
* findProfile,         // find a profile by a secondary index (email address)
* findRelatedProfiles, // look for people with similar interests

## Usage

The simplest command to run Spectroperf against your Couchbase cluster is: 

```
go run spectroperf.go --connstr <cluster-connection-string> --workload user-profile
```

This will run Spectroperf against the given cluster, with the user-profile workload with default settings for all optional flags. 

### Flags 

The optional flags are as follows: 

* --bucket string                  bucket name (default "data")
* --cert string                    path to certificate file
* --collection string              collection name (default "profiles")
* --config-file string             path to configuration file
* --connstr string                 connection string of the cluster under test
* --dapi-connstr string            connection string for data api
* --enable-tracing                 enables otel tracing
* -h, --help                           help for spectroperf
* --log-level string               the log level to run at (default "info")
* --num-items int                  number of docs to create (default 500)
* --num-users int                  number of concurrent simulated users accessing the data (default 500)
* --only-operation string          the only operation to run from the workload
* --otel-exporter-headers string   a comma seperated list of otel expoter headers, e.g 'header1=value1,header2=value2'
* --otlp-endpoint string           endpoint otel traces will be exported to (default "localhost:4318")
* --password string                password of the cluster under test (default "password")
* --ramp-time int                  length of ramp-up and ramp-down periods in minutes
* --run-time int                   total time to run the workload in minutes (default 5)
* --scope string                   scope name (default "identity")
* --sleep string                   time to sleep between operations
* --tls-skip-verify                skip tls certificate verification
* --username string                username for cluster under test (default "Administrator")
* -v, --version                        version for spectroperf
* --workload string                workload name

When running against Data API a normal connection string is still required, e.g: 

```
go run spectroperf.go --connstr <cluster-connection-string> --dapi-connstr <data-API-connection-string> --workload user-profile-dapi
```

This is because Data API will only be used for the running of the workload, while the `connstr` will be used to upload the documents initially.

### Config file

Spectroperf also supports the use of a toml config file, for example:

```
connstr = "couchbases://cb.mzahaa-9f4svarv.aws-guardians.nonprod-project-avengers.com"
dapi-connstr = "https://ut2kfz1m6-xdo7gs.data.sandbox.nonprod-project-avengers.com"
run-time = 10
ramp-time = 1
workload = "user-profile-dapi"
num-users = 5000
num-items = 5000
tls-skip-verify = false
password = "Password123!"
sleep = "1s"
```
If you saved this as `config.toml` you would then pass this path to spectroperf using the `config-file` flag: 

```
go run spectroperf.go --config-file ./config.toml
```

You can use a combination of config file and flags and the flags will take precedence. 
This can be useful when repeating runs against the same cluster wiht just a single config change.
For example the following command would perform the same run above with 500,000 users: 

```
go run spectroperf.go --config-file ./config.toml --num-users 500000
```

The only setting that can be configured through the config file and not via command line flags is the `markov-chain`.
This is the probability matrix that defines the mix of operations for the workload.
For example the default markov-chain for the `user-profile` workloads is: 

```
func (w userProfile) Operations() []string {
	return []string{"fetchProfile", "updateProfile", "lockProfile", "findProfile", "findRelatedProfiles"}
}

func (w userProfile) Probabilities() [][]float64 {
	return [][]float64{
		{0, 0.7, 0.1, 0.15, 0.05},
		{0.8, 0, 0.1, 0.05, 0.05},
		{0.7, 0.2, 0, 0.05, 0.05},
		{0.6, 0.2, 0.15, 0, 0.05},
		{0.6, 0.2, 0.15, 0.05, 0},
	}
}
```

Each row represents the probability of selecting the next operation from the given one.
The first row corresponds to `findProfile`, if we just did a `findProfile` then the chances of performing each operation next are:

* findProfile 			- 	0%
* updateProfile 		- 	70%
* lockProfile 			- 	10%
* findProfile 			- 	15%
* findRelatedProfiles	- 	5%

If you wanted to reduce the chances of an update after a find to 60% and increase lockProfile to 20% you would set the markov-chain in the config file as:

```
markov-chain = [[0.0, 0.6, 0.2, 0.15, 0.05],
				[0.8, 0.0, 0.1, 0.05, 0.05],
				[0.7, 0.2, 0.0, 0.05, 0.05],
				[0.6, 0.2, 0.15, 0.0, 0.05],
				[0.6, 0.2, 0.15, 0.05, 0.0]]
```

## Metrics

Spectroperf produces Prometheus metrics on:

1. Number of successful operations by type
2. Number of failed operations by type
3. Operation duration as a histogram by type

These are exposed on port `2112` and can be scraped by using by running Prometheus with the config file in this repo: 

```
prometheus --config.file=prometheus.yml
```

This will scrape the metrics from Spectroperf at `localhost:2112` and export them on `localhost:9090`. 

### Grafana

The best way to visualise these metrics is using Grafana, this can be done as follows: 

1. Run Grafana locally and add a new DataSource with the `Prometheus Server Url = http://localhost:9090` (obviusly this will be different if you edit the Prometheus config file)
2. Import the Grafana dashboard from the Json definition in: `Grafana_dashboard.json`

The dashboard is split into three sections `ramp-up`, `steady` and `ramp-down`.
The `ramp-up` phase is the first minute of the workload, and the `ramp-down` is the last minute while `steady` is the time in the middle. 
Feel free to edit the dashboard to perform the analysis required, this definition was just given as a starting point. 

## Artifacts

Spectroperf will produces artefacts summarising a run.
These will be put in a directory named with a formatted timestamp, e.g: `2025-06-12-07:23`

### metrics.json

If prometheus is running as configured using `prometheus.yml` then spectroperf will scrape a summary of the metrics and output them to a file when the run finishes.
For example: 

```
> cat 2025-06-12-07:23/metrics.json | jq
{
  "metricSummaries": {
    "fetchProfile": {
      "total": 479,
      "failed": 0,
      "latencyPercentiles": {
        "ninetyNinth": 10.881699562072754,
        "ninetyEighth": 7.323820114135742,
        "ninetyFifth": 3.377350091934204,
        "fiftieth": 0.7590000033378601
      }
    },
    "findProfile": {
      "total": 115,
      "failed": 0,
      "latencyPercentiles": {
        "ninetyNinth": 29.047035217285156,
        "ninetyEighth": 26.468320846557617,
        "ninetyFifth": 19.36932945251465,
        "fiftieth": 7.208499908447266
      }
    },
    "findRelatedProfiles": {
      "total": 49,
      "failed": 0,
      "latencyPercentiles": {
        "ninetyNinth": 27.73335075378418,
        "ninetyEighth": 26.273700714111328,
        "ninetyFifth": 21.894750595092773,
        "fiftieth": 8.518954277038574
      }
    },
    "lockProfile": {
      "total": 117,
      "failed": 0,
      "latencyPercentiles": {
        "ninetyNinth": 12.85966682434082,
        "ninetyEighth": 11.302666664123535,
        "ninetyFifth": 7.304599761962891,
        "fiftieth": 1.3289999961853027
      }
    },
    "updateProfile": {
      "total": 421,
      "failed": 0,
      "latencyPercentiles": {
        "ninetyNinth": 16.575284957885742,
        "ninetyEighth": 10.570300102233887,
        "ninetyFifth": 6.554280757904053,
        "fiftieth": 1.433912992477417
      }
    }
  },
  "steadyStateDurationMins": 1
}
```

## Contributing

Pull requests are welcome and please file issues on Github.

## License

Spectroperf is licensed under Apache 2.0 and Copyright 2024 to Couchbase, Inc.

## Acknowledgements

Some of the approach here was inspired by the Faban project from the mid-2000s.