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
2. Having stats for those operations exported via prometheus and loggable to a file (done!)
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

A workload is a set of operations plus a markov chain giving the probability of each operation
following each other operation. The name passed to `--workload` must be one of the workloads
registered in `workload/workloads/registry.go`:

| `--workload` | Backend | Operations |
| --- | --- | --- |
| `user-profile` | gocb (SDK) | `fetchProfile`, `updateProfile`, `lockProfile`, `findProfile`, `findRelatedProfiles` |
| `user-profile-dapi` | Data API (HTTP) | as above |
| `basic` | gocb (SDK) | `get`, `set`, `query`, `fullTextSearch` |
| `basic-dapi` | Data API (HTTP) | as above |

The `user-profile` workloads mimic a user profile, a variable length JSON document with a few
fields, and simulate a few different operations that might actually happen with a real user profile:

* fetchProfile,        // similar to login or looking at someone
* updateProfile,       // updating a status on the profile
* lockProfile,         // disable or enable a random profile (account lockout)
* findProfile,         // find a profile by a secondary index (email address)
* findRelatedProfiles, // look for people with similar interests

See [AGENTS.md](AGENTS.md) for how to add a new workload.

## Usage

The simplest command to run Spectroperf against your Couchbase cluster is: 

```
go run spectroperf.go --connstr <cluster-connection-string> --workload user-profile
```

This will run Spectroperf against the given cluster, with the user-profile workload with default settings for all optional flags. 

### Flags 

The optional flags are as follows: 

```
      --bucket string                  bucket name (default "data")
      --cert string                    path to certificate file
      --collection string              collection name (default "profiles")
      --config-file string             path to configuration file
      --connstr string                 connection string of the cluster under test
      --dapi-connstr string            connection string for data api
      --dial-timeout int               TCP dial timeout in seconds for DAPI HTTP clients (default 10)
      --enable-tracing                 enables otel tracing
  -h, --help                           help for spectroperf
      --idle-conn-timeout int          idle connection timeout in seconds for DAPI HTTP clients (default 30)
      --log-level string               the log level to run at (default "info")
      --num-items int                  number of docs to create (default 500)
      --num-users ints                 number of concurrent simulated users; single value or comma-separated list for stepped runs
      --only-operation string          the only operation to run from the workload
      --otel-exporter-headers string   comma separated list of otel exporter headers, e.g 'header1=value1,header2=value2'
      --otlp-endpoint string           endpoint otel traces will be exported to (default "localhost:4318")
      --password string                password of the cluster under test (default "password")
      --ramp-time string               length of ramp-up and ramp-down periods (e.g. '1m', '30s') (default "0m")
      --request-timeout int            overall request timeout in seconds for DAPI HTTP clients (default 60)
      --response-header-timeout int    response header timeout in seconds for DAPI HTTP clients (default 30)
      --results string                 override the results directory path (relative or absolute; defaults to directory named with run start timestamp)
      --run-time string                total time to run the workload (e.g. '5m', '30s') (default "5m")
      --scope string                   scope name (default "identity")
      --sleep string                   time to sleep between operations
      --tls-skip-verify                skip tls certificate verification
      --username string                username for cluster under test (default "Administrator")
  -v, --version                        version for spectroperf
      --workload string                workload name
```

A few of these are easy to get wrong:

* `--run-time`, `--ramp-time` and `--sleep` are **duration strings**, not numbers.
  They are parsed with Go's `time.ParseDuration`, so they need a unit: `30s`, `5m`, `1h30m`.
  A bare number such as `--run-time 5` is rejected with `missing unit in duration "5"`.
* `--num-users` takes a **list**. A single value (`--num-users 500`) runs once with that many
  users; a comma-separated list (`--num-users 100,200,400`) runs the workload once per step
  and reports each step separately in `metrics.json`.
* `--sleep` must be at least `100ms`. To drive more throughput, raise `--num-users` instead.
* `--ramp-time` cannot exceed half of `--run-time`.
* `--workload` must be one of the registered workload names listed under
  [Workload Definitions](#workload-definitions).

When running against Data API a normal connection string is still required, e.g: 

```
go run spectroperf.go --connstr <cluster-connection-string> --dapi-connstr <data-API-connection-string> --workload user-profile-dapi
```

This is because Data API will only be used for the running of the workload, while the `connstr` will be used to upload the documents initially.

### Config file

Spectroperf also supports the use of a toml config file, for example:

```
connstr = "couchbases://cb.<cluster-id>.cloud.couchbase.com"
dapi-connstr = "https://<dapi-id>.data.cloud.couchbase.com"
run-time = "10m"
ramp-time = "1m"
workload = "user-profile-dapi"
num-users = 5000
num-items = 5000
tls-skip-verify = false
password = "<password>"
sleep = "1s"
```
Config file keys use the same names as the command line flags, with the same types: durations
are quoted strings such as `"10m"`, and `num-users` may be a single integer or an array like
`[100, 200, 400]`. Keys that do not match a flag name are silently ignored, so a typo or a
camelCase key such as `runTime` leaves the setting at its default rather than raising an error.

If you saved this as `config.toml` you would then pass this path to spectroperf using the `config-file` flag: 

```
go run spectroperf.go --config-file ./config.toml
```

You can use a combination of config file and flags and the flags will take precedence. 
This can be useful when repeating runs against the same cluster with just a single config change.
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

Rows and columns are both in `Operations()` order, so for the `user-profile` workloads that
order is `fetchProfile`, `updateProfile`, `lockProfile`, `findProfile`, `findRelatedProfiles`.
Each row represents the probability of selecting the next operation given the operation just performed.
The first row therefore corresponds to `fetchProfile`; if we just did a `fetchProfile` then the chances of performing each operation next are:

* fetchProfile 			- 	0%
* updateProfile 		- 	70%
* lockProfile 			- 	10%
* findProfile 			- 	15%
* findRelatedProfiles	- 	5%

Each row must sum to 1 and the matrix must be square with one row and column per operation, or the run fails at startup.
`markov-chain` and `--only-operation` cannot be used together; `--only-operation` builds its own
chain that runs the named operation exclusively.

If you wanted to reduce the chances of an update after a `fetchProfile` to 60% and increase `lockProfile` to 20% you would set the markov-chain in the config file as:

```
markov-chain = [[0.0, 0.6, 0.2, 0.15, 0.05],
				[0.8, 0.0, 0.1, 0.05, 0.05],
				[0.7, 0.2, 0.0, 0.05, 0.05],
				[0.6, 0.2, 0.15, 0.0, 0.05],
				[0.6, 0.2, 0.15, 0.05, 0.0]]
```

## Metrics

Spectroperf produces Prometheus metrics on:

1. `operations_total` - number of operations **attempted** (successes can be implied by subtracting the failures)
2. `operations_failed_total` - number of operations that returned an error
3. `operation_duration_milliseconds` - operation duration as a histogram, recorded for **successful operations only**

All three are labelled with `operation`, `phase` (`RampUp`, `Steady` or `RampDown`) and `users`
(the `num-users` value for the step), so a stepped run can be broken down per step.

These are exposed on port `2112` and can be scraped by running Prometheus with the config file in this repo: 

```
prometheus --config.file=prometheus.yml
```

This will scrape the metrics from Spectroperf at `localhost:2112` and export them on `localhost:9090`. 

### Grafana

The best way to visualise these metrics is using Grafana, this can be done as follows: 

1. Run Grafana locally and add a new DataSource with the `Prometheus Server Url = http://localhost:9090` (obviously this will be different if you edit the Prometheus config file)
2. Import the Grafana dashboard from the Json definition in: `Grafana_dashboard.json`

The dashboard is split into three sections `ramp-up`, `steady` and `ramp-down`, matching the
`phase` label on the metrics. The `ramp-up` phase is the first `ramp-time` of the workload and
`ramp-down` is the last `ramp-time`, with `steady` being the time in between. With the default
`ramp-time` of `0m` there is no ramp at all and everything is reported as `steady`. 
Feel free to edit the dashboard to perform the analysis required, this definition was just given as a starting point. 

## Artifacts

Spectroperf produces artefacts summarising a run: `metrics.json`, `config.toml` and `spectroperf.log`.
These are put in a directory named with a UTC timestamp of the run start, e.g. `2025-06-12-07:23`.
Pass `--results <path>` (or set `results` in the config file) to write them to a directory of your choosing instead.

### metrics.json

When the run finishes spectroperf writes a summary of the run to `metrics.json`.
This is generated in process from HDR histograms kept by the runner, so it does not require
Prometheus to be running.

The file is a JSON **array** with one entry per `num-users` value, so a stepped run
(`num-users = [1000, 2000]`) produces two entries. Each entry reports:

* `numUsers` - the number of simulated users for that step
* `steadyStateDurationSecs` - `run-time` minus both ramp periods, in seconds
* `operations` - one entry per workload operation, in `Operations()` order

Only the **steady state** phase is counted; operations performed during ramp-up and
ramp-down are excluded. `total` is successes plus failures, `failed` is the failures alone,
and the latency percentiles cover successful operations only and are in **milliseconds**.

For example:

```
> cat 2025-06-12-07:23/metrics.json | jq
[
  {
    "numUsers": 1000,
    "steadyStateDurationSecs": 290,
    "operations": [
      {
        "name": "fetchProfile",
        "total": 122476,
        "failed": 0,
        "latencyPercentiles": {
          "ninetyNinth": 18.001,
          "ninetyEighth": 13.777,
          "ninetyFifth": 8.445,
          "fiftieth": 0.805
        }
      },
      {
        "name": "updateProfile",
        "total": 99088,
        "failed": 0,
        "latencyPercentiles": {
          "ninetyNinth": 27.75,
          "ninetyEighth": 21.803,
          "ninetyFifth": 14.563,
          "fiftieth": 1.782
        }
      },
      {
        "name": "lockProfile",
        "total": 28146,
        "failed": 0,
        "latencyPercentiles": {
          "ninetyNinth": 26.884,
          "ninetyEighth": 21.856,
          "ninetyFifth": 14.825,
          "fiftieth": 1.724
        }
      },
      {
        "name": "findProfile",
        "total": 39304,
        "failed": 0,
        "latencyPercentiles": {
          "ninetyNinth": 61.861,
          "ninetyEighth": 52.16,
          "ninetyFifth": 39.105,
          "fiftieth": 6.699
        }
      },
      {
        "name": "findRelatedProfiles",
        "total": 13108,
        "failed": 0,
        "latencyPercentiles": {
          "ninetyNinth": 74.303,
          "ninetyEighth": 63.552,
          "ninetyFifth": 47.199,
          "fiftieth": 8.519
        }
      }
    ]
  }
]
```

If an operation never ran - for example because `only-operation` restricted the mix - it still
appears in the array, with zeroed totals and percentiles.

### config.toml

The fully resolved configuration for the run, with any settings left at their default value
stripped out. This is the file to keep if you want to reproduce a run: pass it back in with
`--config-file`.

### spectroperf.log

The JSON structured log for the run, written alongside the same output on stdout.

## Contributing

Pull requests are welcome and please file issues on Github.

## License

Spectroperf is licensed under Apache 2.0 and Copyright 2024 to Couchbase, Inc.

## Acknowledgements

Some of the approach here was inspired by the Faban project from the mid-2000s.