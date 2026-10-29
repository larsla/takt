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

Logging stays on `go.uber.org/zap` and metrics on `github.com/uber-go/tally`, so loggers and scopes you pass in today keep working.

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

Register other domains with the `DomainClient`, as with Cadence.

## What behaves differently

- **Shared types are aliases of `api` types with plain fields.** Cadence's Thrift structs use pointers (`WorkflowId *string`). takt's use values (`WorkflowID string`). Type switches on errors such as `*shared.EntityNotExistsError` and `*shared.WorkflowExecutionAlreadyStartedError` work unchanged. Code that builds request structs by hand, such as `RegisterDomainRequest`, needs its fields updated.
- **No sticky execution.** Every decision task replays the workflow's full history. `DisableStickyExecution` and `StickyScheduleToStartTimeout` are accepted and ignored. Very long histories cost more CPU per decision than on Cadence.
- **Local activities run inside the decision task.** Keep them shorter than the decision task timeout. takt does not extend the decision task while they run.
- **Sessions, context propagators and tracing are not supported.** The related worker options are accepted and ignored.
- **Visibility queries support a subset.** `ListWorkflow` and `CountWorkflow` accept `AND`-joined comparisons on `WorkflowID`, `RunID`, `WorkflowType`, `CloseStatus`, `StartTime`, `CloseTime`, `ExecutionTime` and exact matches on search attributes. There is no Elasticsearch.
- **Not implemented:** workflow reset, archival, cross-cluster replication and the workflow shadower.
- **Running workflows don't move.** Start new runs on takt and let old ones finish on Cadence. takt's history JSON uses Cadence's field names, so `worker.ReplayWorkflowHistoryFromJSONFile` can read files exported with `cadence workflow show --output_filename`. Replaying them can still fail where takt's SDK generates IDs or encodes markers (side effects, versions, local activities) differently from Cadence's.
