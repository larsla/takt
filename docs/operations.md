# Operating takt

## How tasks reach workers

When a workflow schedules a decision or an activity, the server publishes a small task stub to the `TAKT_TASKS` JetStream stream, on the subject `takt.task.<domain>.<task list>.<decision|activity>`. Each task list has one durable consumer, shared by every worker that polls it. A worker fetches a stub from JetStream itself, asks a takt server to start the task (`RecordDecisionTaskStarted` or `RecordActivityTaskStarted`), and acks the stub once the server answers. The server drops stubs whose task has already started, timed out or been canceled.

What this guarantees:

- **A task queued while no worker polls waits.** `TAKT_TASKS` is a work-queue stream with no age limit, so a stub stays until a worker takes it. A worker that starts polling later gets the backlog right away, oldest first.
- **A worker that dies holding a stub delays the task by at most 30 seconds.** That is the consumer's ack wait. After it, JetStream redelivers the stub to another poller.
- **Timeouts keep running while the task waits.** An activity that nobody starts within `ScheduleToStartTimeout` times out, as in Cadence, and a schedule-to-start timeout isn't retried. Give activities on task lists that may have no workers for a while a long `ScheduleToStartTimeout`.
- **A started task belongs to the worker that started it.** If that worker dies, nobody else can take the task until `StartToCloseTimeout` passes, or `HeartbeatTimeout` if the activity heartbeats. Then the retry policy decides. A long activity without heartbeats can therefore sit "started" well after its worker is gone, as it would on Cadence. The workflow page in the web UI, and `DescribeWorkflowExecution`, show whether a pending activity is `SCHEDULED` (waiting for a worker) or `STARTED` (waiting for a timeout).

To see a task list nobody serves, alert on `takt_tasklist_backlog_age_seconds` rising while `takt_tasklist_pollers` is 0 ([metrics.md](metrics.md)). The Task lists page of the web UI shows the same numbers.

Stubs of tasks that timed out stay in the stream until a poller drops them. If you retire a task list for good, purge its subjects: `nats stream purge TAKT_TASKS --subject 'takt.task.<domain>.<task list>.>'`. The `<domain>` and `<task list>` tokens are the encoded names that `nats stream subjects TAKT_TASKS` lists.

## Scaling

No takt server owns a task list. Workers pull stubs straight from JetStream, and any server answers any request, because NATS spreads requests over the servers in a queue group. Adding servers adds capacity for the API, timers and the work that runs when tasks start and complete. It doesn't split a task list. Cadence's `numTasklistWritePartitions` has no equivalent and isn't needed for that reason.

A busy task list is limited by:

- **Pollers.** Each poll fetches one stub. Raise `MaxConcurrentDecisionTaskPollers` and `MaxConcurrentActivityTaskPollers`, or run more workers.
- **The JetStream stream and consumer.** All task lists share `TAKT_TASKS`, which has one leader in a NATS cluster. Each task list's consumer has its own leader, and NATS spreads those over the cluster. A replicated file stream handles tens of thousands of messages a second on ordinary hardware. That is the ceiling for task throughput across all task lists together, not per task list.
- **One run at a time.** The server changes a run's state with compare-and-swap, so concurrent updates to one run retry. This limits how fast a single run can go, not a task list.

If you outgrow one stream, split work across domains or task lists first. Sharding `TAKT_TASKS` across several streams isn't implemented.

## NATS credentials

takt authenticates at the NATS connection. There is no per-call token.

**Rotating credentials without a restart.** Use `nats.UserCredentials("svc.creds")` for clients and workers (the `takt` binary does this for `--nats-creds`). nats.go reads the file again on every reconnect. To rotate:

1. Write the new credentials to the same path.
2. Revoke the old user, or reload the NATS server with the old credentials removed. NATS closes connections that use them.
3. Clients reconnect with the new file.

During the reconnect, which normally takes milliseconds, nats.go buffers outgoing requests and sends them once it's back. A request that doesn't get an answer within its timeout fails with a timeout or `nats.ErrConnectionClosed`. Workers retry polls and responses by themselves: a lost response leaves the task to its timeout, and the task then runs again. Client calls return the error to you, and you decide whether to retry. Retrying `StartWorkflow` with the same workflow ID and `StartWorkflowOptions.RequestID` starts one run. A retried signal can arrive twice, because a call that timed out may still have reached the server, the same as with Cadence. Make signal handlers tolerate duplicates. For credentials that come from a vault, `nats.UserJWT` and `nats.TokenHandler` take callbacks that nats.go calls on every connect.

**Recording who started a run.** Use a context propagator. It writes values from the caller's `context.Context` into the header of `StartWorkflow`, `SignalWithStartWorkflow` and child workflows. The server stores the header in the `WorkflowExecutionStarted` event, and the workflow reads it back into its context. Set `client.Options.ContextPropagators` on the client and `worker.Options.ContextPropagators` on the worker (see `workflow.ContextPropagator`). This is how Cadence code carries caller metadata as well.

The principal in that header is whatever the caller claims, as it is with Cadence's `Identity`. takt servers don't see which NATS user sent a request. If the claim must be trusted, give each principal its own NATS user and limit it to the domains and subjects it may use. Then the NATS account boundary enforces what the header records.
