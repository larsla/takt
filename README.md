# takt

takt is a workflow engine in the style of [Cadence](https://cadenceworkflow.io/) that runs on [NATS](https://nats.io/) JetStream and nothing else. Its Go SDK keeps Cadence's packages, types and signatures, so moving a Cadence service over is mostly an import rewrite ([migration guide](docs/migration.md)).

It ships as a single binary with the server, a CLI and a web UI. For development it can also embed NATS.

## Quick start

```sh
make build
./takt dev            # NATS, server and web UI on http://127.0.0.1:8088, domain "default"
go run ./examples/helloworld
```

A workflow and its worker, written the Cadence way:

```go
func helloWorldWorkflow(ctx workflow.Context, name string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
	})
	var result string
	err := workflow.ExecuteActivity(ctx, helloWorldActivity, name).Get(ctx, &result)
	return result, err
}

func helloWorldActivity(ctx context.Context, name string) (string, error) {
	return "Hello " + name + "!", nil
}

func main() {
	service, closeConn, _ := takt.Dial(nats.DefaultURL, nil)
	defer closeConn()

	w := worker.New(service, "default", "hello", worker.Options{})
	w.RegisterWorkflow(helloWorldWorkflow)
	w.RegisterActivity(helloWorldActivity)
	_ = w.Start()
	defer w.Stop()

	c := client.NewClient(service, "default", nil)
	run, _ := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		TaskList:                     "hello",
		ExecutionStartToCloseTimeout: time.Minute,
	}, helloWorldWorkflow, "takt")
	var greeting string
	_ = run.Get(context.Background(), &greeting)
	fmt.Println(greeting)
}
```

More samples live in [examples/](examples): signals, timers, child workflows, retries with heartbeat progress, queries, local activities, cancellation and cron. Each one is tested against an embedded stack.

## What's in it

- **Workflows** with deterministic replay: activities, local activities, timers, signals, queries, child workflows, external signals and cancellation, side effects, versioning, continue-as-new, cancellation with disconnected contexts, search attributes.
- **Activities** with retry policies, all four timeouts, heartbeats that carry progress across retries, and asynchronous completion.
- **Cron schedules**, delayed starts and workflow-level retry policies.
- **Visibility**: list and count workflows with a small query language (`WorkflowType = 'order' AND CloseTime = missing`).
- **`testsuite`**: unit tests for workflows and activities on a mock clock, with testify mocks, the same API as `go.uber.org/cadence/testsuite`.
- **Replay tool**: `worker.NewWorkflowReplayer` checks recorded histories against changed code.
- **Web UI**: workflow lists, a live event timeline, pending activities, stack traces, queries, task list pollers and backlog, cron workflows, and signal/cancel/terminate actions.
- **CLI**: `takt domain ...` and `takt workflow start|list|show|describe|signal|query|cancel|terminate`, with Cadence's flag names.

## How it uses NATS

| Resource | What it holds |
|---|---|
| `TAKT_HISTORY` stream | One message per batch of history events, appended with an expected-sequence check |
| `TAKT_TASKS` work queue | Decision and activity task stubs; workers pull them directly |
| `TAKT_TIMERS` stream | Timers and timeouts as JetStream scheduled messages |
| `TAKT_TRANSFER` work queue | Work that crosses workflows: child starts, signals, close notifications |
| `takt_executions` KV | Each run's mutable state, the source of truth, updated with compare-and-set |
| `takt_visibility` KV | Listing records, mirrored into an in-memory index per server |
| `takt_domains`, `takt_pollers`, `takt_meta` KV | Domains, poller heartbeats, schema version |

The server is stateless. Several `takt server` processes can share a NATS cluster and race safely on the same workflow. Every state change is a compare-and-set on the run's KV entry, followed by appending its events and publishing its tasks. A crash between those steps is finished by the next reader or a sweeper. Requires nats-server 2.12 or newer for message schedules.

## Running

```sh
takt server --nats-url nats://nats:4222 --ui-addr :8088 [--ui-write] [--replicas 3]
takt server --embedded-nats --data-dir ./data      # single node with NATS inside
takt dev                                            # embedded, temporary data, default domain
```

The web UI has no authentication. It is read-only unless `--ui-write` is set (on by default in `takt dev`). Bind it to a private address.

Every server flag can also come from the environment: `--nats-url` is `TAKT_NATS_URL`, `--metrics-addr` is `TAKT_METRICS_ADDR`. `--nats-creds` points at a NATS credentials file.

### Docker

The image is built from `scratch` and holds only the static binary and CA certificates. It runs as uid/gid 65532, listens on `:8088` (UI), `:9090` (metrics) and, with embedded NATS, `:4222`.

```sh
docker run --rm -p 8088:8088 -p 4222:4222 ghcr.io/larsla/takt:<version> dev --nats-host 0.0.0.0
docker run --rm ghcr.io/larsla/takt:<version> server --nats-url nats://nats:4222
make docker        # build takt:dev locally
```

### Kubernetes

The chart in [charts/takt](charts/takt) runs `takt server` as a Deployment against an existing NATS cluster. It's published to `oci://ghcr.io/larsla/charts/takt`.

```sh
helm install takt oci://ghcr.io/larsla/charts/takt --version <version> \
  --set nats.url=nats://nats.nats:4222 --set nats.streamReplicas=3
kubectl exec deploy/takt -- /takt domain register --domain default
```

Pods run as non-root with a read-only root filesystem. [values.yaml](charts/takt/values.yaml) covers the uid/gid (`podSecurityContext`), image registry and repository, pull secrets, extra volumes and mounts, `replicaCount`, the autoscaler, a PodDisruptionBudget, an Ingress for the UI and a Prometheus ServiceMonitor. `embeddedNats.enabled=true` runs NATS inside a single pod for trying things out.

### Metrics

`--metrics-addr` serves Prometheus metrics. Server and SDK metrics use Cadence's names with a `takt` prefix, so Cadence dashboards carry over with a find-and-replace. [docs/metrics.md](docs/metrics.md) lists them.

## Releases

Pushing a tag like `v1.2.3` runs the tests, then publishes the multi-arch image to `ghcr.io/larsla/takt` (tags `1.2.3`, `1.2` and `1`), the chart to `oci://ghcr.io/larsla/charts` with chart and app version `1.2.3`, and a GitHub release with binaries. Pull requests and pushes to `main` run `make check`, govulncheck, an image build with a smoke test, and a chart install on kind.

## Development

```sh
make generate   # templ, Tailwind CSS, .gen/go/shared aliases (output is committed)
make check      # generate + lint + gosec + tests with -race and the 70% coverage gate
make vuln       # govulncheck (needs network)
```

Tests start an embedded NATS server, so they need nothing installed.
