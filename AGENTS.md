# spectroperf (Agent Brief)

This repository defines a workload generation tool called spectroperf.

## Approach

- Do as little work as possible to achieve the task at hand
- Before making code changes propose the steps and get my review
- When discussing a task ask before proposing any edits to files
- Do not be overly agreeable, criticise my suggestions if there appear to be issues
- Do not use underscores in function or variable names
- When writing tests use the assert library to establish success

## Project Layout (High-level)

- spectroperf.go in root package reads the config and orchestrates the workload
- workload/*.go holds the generic machinery for running any workload: workload.go (the runner and
  the Workload interface), metrics.go (prometheus metrics plus the HDR histograms behind
  metrics.json), tracing.go and utils.go
- workload/workloads contains a set of workloads, each consisting of a mix of operations and a markov chain defining the probabilities of each operation
- configuration/ holds config parsing: config.go (config file and flags), flags.go (flag
  definitions and defaults), executionConfig.go (validation and duration parsing) and markov.go
  (markov chain selection and validation)

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

A new workload needs no Grafana dashboard changes: the panels are driven by an `$operation`
variable populated from `label_values(operations_total, operation)`, so a new workload's
operations appear on their own once it runs.

## Running a workload

Build the binary with `make build`, then start from `configs/example.toml`, the only config
tracked in the repo. Copy it, set `connstr` and the credentials of the cluster under test, and
pass the copy with `--config-file`:

```
./spectroperf --config-file ./configs/my-cluster.toml
```

Leave the example itself untouched, and never commit a config holding a real endpoint or
password: everything else under `configs/` is gitignored for exactly that reason. Flags take
precedence over config file keys, so one saved config covers repeated runs with a single
setting varied (`--num-users 100`).

The bucket, scope and collection named in the config must already exist - spectroperf does not
create them, and a run against a missing one fails at setup. Workloads do create their own
query and FTS indexes during `Setup`, so those need no preparation.

Start `make monitoring` before the run to watch it live in Grafana. A run started with
`--results <name>` writes its artefacts to `<name>/`, which needs adding to `.gitignore`.

## Monitoring

`make monitoring` starts Prometheus and Grafana in containers, pre-wired to scrape spectroperf
and serve `Grafana_dashboard.json` (Grafana on :3000 with no login, Prometheus on :9090). Docker
is the only prerequisite. Spectroperf stays a host binary, so Prometheus scrapes it at
`host.docker.internal:2112`; the stack is `docker-compose.yml` plus `monitoring/`.
`make monitoring-down` stops it, `make monitoring-clean` also drops the scraped metrics.

## Configuration gotchas

- `run-time`, `ramp-time` and `sleep` are duration strings parsed with `time.ParseDuration`, so
  they always need a unit (`"30s"`, `"5m"`). A bare TOML integer such as `run-time = 12` is read
  as the string `"12"` and fatals at startup with `missing unit in duration "12"`.
- `num-users` is an `[]int`: a single integer or a TOML array such as `[100, 200, 400]`, which
  runs the workload once per step.
- Config file keys must match the flag names exactly. Viper silently ignores unknown keys, so a
  camelCase key like `runTime` leaves the setting at its default rather than erroring.
- The example configs in `configs/` are untracked local files, not part of the repo.

## Security / Secrets

- Never commit credentials, connection strings, tokens, or private endpoints.
- Be careful with logs/traces: avoid printing auth material from test env vars.
