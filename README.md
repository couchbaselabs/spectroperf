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
5. Data API backend
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
The optional flags are as follows: 

* `--bucket`: needs to be created manually by the User, defaults to `data`
* `--scope`:  needs to be created manually by the User, defaults to `identitiy`
* `--collection`: needs to be created manually by the User, defaults to `profiles`
* `--username`: defaults to `Administrator`
* `--password`: defaults to `password`
* `--tls-skip-verify`: used to skip TLS verification when contacting the cluster, defaults to `false`
* `--cert`: the path to the cluster tls-certificate, defaults to `rootCA.crt`
* `--num-items`: the number of documents to be loaded into the Cluster, defaults to `200000`
* `--num-users`: the number of concurrent simulated Users (threads), defaults to `50000`
* `--dapi-connstr`: the Data API connection string required when using a Data API workload and has no default value

When running against Data API a normal connection string is still required, e.g: 

```
go run spectroperf.go --connstr <cluster-connection-string> --dapi-connstr <data-API-connection-string> --workload user-profile-dapi
```

This is because Data API will only be used for the running of the workload, while the `connstr` will be used to upload the documents initially.

## Contributing

Pull requests are welcome and please file issues on Github.

## License

Spectroperf is licensed under Apache 2.0 and Copyright 2024 to Couchbase, Inc.

## Acknowledgements

Some of the approach here was inspired by the Faban project from the mid-2000s.