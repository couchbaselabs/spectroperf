# spectroperf (Agent Brief)

This repository defines a workload generation tool called spectroperf.

## Approach

- Do as little work as possible to achieve the task at hand
- Before making code changes propose the steps and get my review
- When discussing a task ask before proposing any edits to files
- Do not be overly agreeable, critisise my suggestions if there appear to be issues
- Do not use underscores in function or variable names
- When writing tests use the assert librar y to establish success

## Project Layout (High-level)

- spectroperf.go in root package reads the config and orchestrates the workload
- *.go files in workloads contain generic files for running different mixes of operations against a couchbase cluster
- workload/workloads contains a set of workloads, each consisting of a mix of operations and a markov chain defining the probabilities of each operation 
- configuration/config.go holds the code responsible for parsing the config file and flags

## Adding a workload

A workload is a mix of operations plus a markov chain giving the probability of each
operation following each other operation. To add one:

1. Create a new file in `workload/workloads/` implementing the `Workload` interface
   from `workload/workload.go`: `GenerateDocument`, `Operations`, `Probabilities`,
   `Functions` and `Setup`. Follow an existing workload such as `basic.go` for shape.
2. Give it a `New<Name>` constructor with the signature
   `func(logger *zap.Logger, config *configuration.Config, cluster *gocb.Cluster) workload.Workload`.
3. Register it in the `registry` map in `workload/workloads/registry.go`, keyed by the
   name users will pass to `--workload`. The registry is what `spectroperf.go` looks the
   workload up in, so an unregistered workload cannot be run.

`Operations`, `Probabilities` and `Functions` must agree with each other or the runner
will misbehave at run time rather than fail fast. `TestWorkloadConformance` in
`workload/workloads/conformance_test.go` checks this for every registered workload:

- `Probabilities` is square, with one row and column per entry in `Operations`
- every probability row sums to 1
- the keys of `Functions` are exactly the set of `Operations`
- operation names are unique

Because the test iterates the registry, a newly registered workload is picked up
automatically with no change to the test.

Run it with:

```
go test ./workload/workloads/ -run TestWorkloadConformance -v
```

or run the whole suite with `make test`.

**Do not report a new or modified workload as complete until this test passes.** If it
fails, fix the workload rather than the test; the assertions encode what the runner in
`workload/workload.go` actually requires.

## Security / Secrets

- Never commit credentials, connection strings, tokens, or private endpoints.
- Be careful with logs/traces: avoid printing auth material from test env vars.
