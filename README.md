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

## Development

```sh
make generate   # templ, Tailwind CSS, .gen/go/shared aliases (output is committed)
make check      # generate + lint + gosec + tests with -race and the 70% coverage gate
make vuln       # govulncheck (needs network)
```

Tests start an embedded NATS server, so they need nothing installed.
