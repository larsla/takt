# Migrating from Cadence

takt's Go SDK keeps the package layout, type names and function signatures of `go.uber.org/cadence`. For most services the migration is an import rewrite plus one changed line where the service client is built.

## 1. Rewrite imports

```sh
grep -rl 'go.uber.org/cadence' --include='*.go' . | xargs sed -i \
  -e 's#go.uber.org/cadence/.gen/go/shared#github.com/larsla/takt/.gen/go/shared#g' \
  -e 's#go.uber.org/cadence#github.com/larsla/takt#g'
```

| Cadence | takt |
|---|---|
| `go.uber.org/cadence` | `github.com/larsla/takt` |
| `go.uber.org/cadence/workflow` | `github.com/larsla/takt/workflow` |
| `go.uber.org/cadence/activity` | `github.com/larsla/takt/activity` |
| `go.uber.org/cadence/client` | `github.com/larsla/takt/client` |
| `go.uber.org/cadence/worker` | `github.com/larsla/takt/worker` |
| `go.uber.org/cadence/encoded` | `github.com/larsla/takt/encoded` |
| `go.uber.org/cadence/testsuite` | `github.com/larsla/takt/testsuite` |
| `go.uber.org/cadence/.gen/go/shared` | `github.com/larsla/takt/.gen/go/shared` |
| `go.uber.org/cadence/interceptors` | `github.com/larsla/takt/interceptors` |
| `go.uber.org/cadence/mocks` | `github.com/larsla/takt/mocks` |

`mocks` exists from takt v0.0.2. Like Cadence's, its mocks work as zero values (`&mocks.Client{}`, then `AssertExpectations(t)`), so shared helpers don't need a `*testing.T`. `mocks.NewClient(t)` also registers the assertion with `t.Cleanup`.

Logging stays on `go.uber.org/zap` and metrics on `github.com/uber-go/tally`, so loggers and scopes you pass in today keep working. Metric names start with `takt-` instead of `cadence-`; [metrics.md](metrics.md) lists them.

## 2. Replace the service client

Cadence code builds a yarpc dispatcher and a `workflowserviceclient.Interface`. takt talks to NATS instead:

```go
// Before
dispatcher := yarpc.NewDispatcher(yarpc.Config{ /* tchannel or grpc outbound */ })
service := workflowserviceclient.New(dispatcher.ClientConfig("cadence-frontend"))

// After
nc, err := nats.Connect("nats://127.0.0.1:4222")
service, err := takt.NewServiceClient(nc, nil)
```

`takt.Dial(url, nil)` does both steps. The result goes where the yarpc client went:

```go
c := client.NewClient(service, "my-domain", &client.Options{})
w := worker.New(service, "my-domain", "my-task-list", worker.Options{})
```

## 3. Run a server

`takt dev` starts NATS (embedded), the takt server and the web UI, and registers a `default` domain. In production, run `takt server --nats-url nats://...` against a NATS cluster with JetStream. Several `takt server` processes can share one NATS cluster.

Register other domains with the `DomainClient`, as with Cadence. [operations.md](operations.md) covers how tasks reach workers when none is polling, scaling, and rotating NATS credentials.

## What behaves differently

- **Shared types have plain fields.** `.gen/go/shared` holds aliases of takt's `api` types. Cadence's Thrift structs use pointers (`WorkflowId *string`), takt's use values (`WorkflowID string`). Reading works unchanged: every struct has Cadence's nil-safe getters (`GetWorkflowId()`, `IsSetRunId()`), and the string enums have `String()` and `Ptr()`. Code that builds shared structs needs edits: drop the pointer helpers and use takt's field names, so `&shared.WorkflowExecution{WorkflowId: common.StringPtr(id)}` becomes `&shared.WorkflowExecution{WorkflowID: id}`. The compiler finds every place.
- **Sticky execution uses a sticky task list per worker.** A worker keeps each run it handled in memory (`worker.SetStickyWorkflowCacheSize`, default 10000) and polls its own sticky task list with half its decision pollers. The server sends a run's decision tasks there. A task nobody takes within `StickyScheduleToStartTimeout` (default 5s) goes back to the normal task list, where any worker can take it and replays the history in full. A stopping worker hands its runs back right away. Queries don't use the sticky task list: whichever worker answers replays the history.
- **Sessions work as in Cadence, with one task list per session worker.** Set `EnableSessionWorker` on the workers that should host sessions. `CreateSession` puts a creation activity on `<task list>__internal_session_creation`. A worker with a free slot (`MaxConcurrentSessionExecutionSize`, default 1000) takes it and heartbeats it for as long as the session lasts. The session's activities go to a task list only that worker polls (`__session:<uuid>@<host>`), and the web UI doesn't list those. A full worker stops taking creation tasks, so `CreateSession` waits for another worker until `CreationTimeout`, and Cadence's "too many outstanding sessions" error only happens on `RecreateSession`. If the worker dies, the session fails once `HeartbeatTimeout` (default 20s) passes. A worker that stops cleanly fails its sessions right away, so workflows don't wait out the heartbeat. As in Cadence, failure cancels the session context, and later activities in it return `ErrSessionFailed`.
- **Workflows and activities resolve by function name too.** In Cadence, `client.StartWorkflow(ctx, opts, fn)` sends `fn`'s registered name only when `fn` was registered globally (`workflow.RegisterWithOptions`). A name given to `w.RegisterWorkflowWithOptions` on a worker stays in that worker's registry, so the client sends the Go function name, and the workflow never runs. takt resolves both ways. A client also checks workers in the same process and uses their name when they all agree on one. A worker given a type nobody registered, which is the Go function name of a workflow or activity it registered under another name, runs that function. A run started by function name keeps that name as its type in visibility.
- **Local activities run inside the decision task.** Keep them shorter than the decision task timeout. takt does not extend the decision task while they run.
- **`TaskListActivitiesPerSecond` is approximate.** Each worker takes an equal share of the limit and recounts the task list's workers every 10 seconds, so the total drifts while workers come and go.
- **`StartWorkflowAsync` and `SignalWithStartWorkflowAsync` are synchronous.** takt has no start queue. They start the workflow and return its ID.
- **`WithFailureCategory(FailureCategoryFatal)` means "don't retry".** The activity fails at once, whatever its retry policy allows. `WithNextRetryDelay` replaces the policy's backoff for the next attempt.
- **Names carry a takt prefix.** Metrics start with `takt-` ([metrics.md](metrics.md)), tracing spans are `takt-ExecuteWorkflow`, `takt-ExecuteActivity` and `takt-ExecuteLocalActivity`, and span tags are `taktWorkflowID` and so on.
- **`GetTotalHistoryBytes` is an estimate.** The worker adds up the JSON size of the events it saw. Use it the way you use Cadence's, as a signal to continue as new.
- **The testsuite reruns cron workflows right away.** With `SetWorkflowCronSchedule`, each run starts as soon as the previous one closes, on the same mock clock, as in Cadence's test environment. Set `SetWorkflowCronMaxIterations`, or the test runs forever.
- **Visibility queries support a subset.** `ListWorkflow` and `CountWorkflow` accept comparisons (`=`, `!=`, `<`, `<=`, `>`, `>=`) joined with `AND`, `OR` and parentheses, plus `ORDER BY`. Fields are `WorkflowID`, `RunID`, `WorkflowType`, `TaskList`, `CloseStatus`, `StartTime`, `CloseTime`, `ExecutionTime` and custom search attributes; `CloseTime = missing` selects open runs. Each server answers from an in-memory index, so there is no Elasticsearch and no full-text search.
- **Running workflows don't move.** Start new runs on takt and let old ones finish on Cadence. [Running both engines](#running-cadence-and-takt-side-by-side) says how. takt's history JSON uses Cadence's field names, so `worker.ReplayWorkflowHistoryFromJSONFile` can read files exported with `cadence workflow show --output_filename`. Replaying them can still fail where takt's SDK generates IDs or encodes markers (side effects, versions, local activities) differently from Cadence's.

### Options takt accepts and ignores

These compile and do nothing, because they tune machinery takt doesn't have. Ignoring them doesn't change what your workflows do.

| Option | Why it has no effect |
|---|---|
| `worker.Options.MinConcurrentActivityTaskPollers`, `MinConcurrentDecisionTaskPollers`, `AutoScalerOptions`, `PollerAutoScaler*` | No poller autoscaler: a worker always runs `MaxConcurrent*TaskPollers` pollers. |
| `worker.Options.IsolationGroup`, `client.Options.IsolationGroup` | No isolation groups: any worker on the task list can get any task. |
| `FeatureFlags`, `worker.Options.WorkerBugPorts`, `workflow.ChildWorkflowOptions.Bugports` | They switch Cadence client behaviors that takt doesn't copy. |
| `QueryConsistencyLevel` | One cluster, so every level reads the same state. |
| `workflow.Type.Path`, `activity.Type.Path` | Informational in Cadence too. |
| `worker.EnableVerboseLogging` | Use the zap logger's level instead. |

## Running Cadence and takt side by side

An open Cadence run can't move to takt: its state is its history, and only Cadence can keep executing that history. So the cut-over runs both engines until the Cadence runs have closed.

1. **Keep the Cadence build running.** Deploy the takt build of a service next to its last Cadence build. The two can't share a binary, because the SDKs' `workflow.Context` types differ. The Cadence workers keep polling until nothing is open there.
2. **Start new runs on takt.** Switch every `StartWorkflow`, `ExecuteWorkflow` and `SignalWithStartWorkflow` call to the takt client.
3. **Route calls on existing runs.** Signals, queries, cancels and describes go to the engine that has the run. A small wrapper around both clients is enough: try takt, and on `*shared.EntityNotExistsError` call Cadence. If workflow IDs must stay unique across both engines, also check Cadence before a start, because neither engine knows about the other's runs.
4. **Children stay with their parent.** A child starts on the engine that runs its parent's worker. A Cadence parent finishes its children on Cadence. `SignalExternalWorkflow` can't cross engines either. Where a workflow on one engine must signal a run on the other, do it from an activity that uses the other engine's client.
5. **Move cron workflows between fire times.** Terminate the Cadence cron run, then start it on takt with the same schedule, and `FirstRunAt` set to the next fire time if a skipped fire matters.
6. **Hand off long-running workflows at continue-as-new.** For a workflow that loops with `ContinueAsNew`, deploy a last Cadence version that, in place of `ContinueAsNew`, runs an activity that starts the next run on takt with the same arguments, then completes. The loop carries on, on takt.
7. **Drain.** When Cadence's `ListOpenWorkflow` for the domain is empty, shut down the Cadence workers.

A tool that copies open Cadence runs to takt would have to carry each run's state across, and that state lives in the history and the workflow code, so there's no general way to do it. The continue-as-new hand-off is the general form of that: the workflow decides what state the next run needs.

takt has no mode for replaying Cadence histories with Cadence's IDs and marker encoding. Under this procedure no takt worker ever executes a Cadence history, so such a mode would only test the replayer, not anything that runs in production. The import rewrite doesn't change workflow logic. Keep replaying Cadence histories with Cadence's replayer in the Cadence build for as long as that build changes, and replay takt's own histories in the takt build, as described under the workflow shadower below.

## What takt leaves out, and what to do instead

These Cadence features have no takt equivalent. The API is left out on purpose, so code that uses it fails to compile and you find every use before the migration instead of in production.

**Schedules** (`client.ScheduleClient` and the `Schedule*` types). Cadence's schedules are a second scheduler next to cron workflows. takt's cron workflows cover the common cases: `CronSchedule` with `CronOverlapPolicy` (skip or buffer one), `JitterStart` and `FirstRunAt`. To pause, terminate the cron workflow and start it again later. To backfill, start one run per missed period with an explicit workflow ID such as `report-2026-10-30`, so it can't run twice.

**Workflow reset** (`Client.ResetWorkflow`). Reset rewrites history to an earlier point and depends on Cadence's history branches, which takt's append-only NATS stream doesn't have. Fix the code, check the fix against the stuck run's history with `worker.ReplayWorkflowExecution`, then terminate the run and start a new one with the same workflow ID (`WorkflowIDReusePolicyAllowDuplicate`). Pass whatever progress it made as input. For long workflows that you expect to restart, carry their state in `ContinueAsNew` arguments, so a new run can resume from the last checkpoint.

**Archival** (`Client.ListArchivedWorkflow` and the domain archival settings). takt keeps closed runs for the domain's retention period and then deletes them. If you need them for longer, register the domain with a larger `--retention` or raise it with `DomainClient.Update`. For a permanent record, export histories before they expire with `takt workflow show --json -w <id> > history.json`, or back up the `TAKT_HISTORY` stream with `nats stream backup`.

**Cross-cluster and active-active domains** (`DomainClient.Failover`, `StartWorkflowOptions.ActiveClusterSelectionPolicy`, `ClusterAttribute`, global domains). takt runs as one logical cluster. For availability, run it against a NATS cluster spread across zones and start `takt server` with `--replicas 3`, so every stream and bucket has three copies. Across regions, use a NATS supercluster.

**The workflow shadower** (`worker.NewWorkflowShadower`, `ShadowOptions`, `EnableShadowWorker`). The shadower replays production histories against new code before a deploy. A Go test in CI does the same with less machinery. `ReplayWorkflowExecutions` lists the runs a visibility query matches, newest first, and replays each:

```go
func TestReplayProduction(t *testing.T) {
	service, closeFn, err := takt.Dial(os.Getenv("TAKT_NATS_URL"), nil)
	if err != nil {
		t.Skip("no takt cluster:", err)
	}
	defer closeFn()
	r := worker.NewWorkflowReplayer()
	r.RegisterWorkflow(OrderWorkflow) // register as your workers do
	results, err := r.ReplayWorkflowExecutions(context.Background(), service, nil, "orders",
		"StartTime > '"+time.Now().Add(-7*24*time.Hour).Format(time.RFC3339)+"'", 500)
	if err != nil {
		t.Fatal(err)
	}
	for _, res := range results {
		if res.Err != nil {
			t.Errorf("%s %s (%s): %v", res.Execution.ID, res.Execution.RunID, res.WorkflowType, res.Err)
		}
	}
}
```

A nondeterministic change comes back as a `*takt.NonDeterministicError`. This has to be a program or test of yours rather than a `takt replay` command, because the replay runs your workflow code. To replay exported files instead, use `WorkflowReplayer.ReplayWorkflowHistoryFromJSONFile`.

**Authorization providers** (`worker.NewAdminJwtAuthorizationProvider`, `NewOAuthAuthorizationProvider`, `Options.Authorization`). takt clients talk to NATS, so NATS does the authentication and authorization. Pass a credentials file, nkey or JWT to `takt.Dial` as a `nats.Option` (`nats.UserCredentials("svc.creds")`), use TLS, and limit each service's account to the subjects it needs.

**`Client.RefreshWorkflowTasks`**. Cadence uses it to regenerate lost tasks. In takt, a task stub stays in JetStream until a worker has started the task, a started task has a timeout behind it, and each server's sweeper republishes tasks that a crash left unpublished, so there is nothing to refresh. `ResetStickyTaskList` hands a run that waits on a gone worker back to its task list.

**`Client.GetSearchAttributes`**. takt has no search attribute schema and accepts any key, so there is no list to return. Keep the list of keys your code uses in the code.

**`worker.Options.WorkerStats` and `FeatureFlags.MetricEmitMode`**. They expose Cadence client internals. Use the worker's metrics ([metrics.md](metrics.md)) instead.
