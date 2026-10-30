# Metrics

takt uses the metric names Cadence uses, with `takt` in place of `cadence` as the prefix. A Cadence dashboard needs the prefix changed in its queries and should then mostly work. Metrics that describe Cadence internals takt doesn't have, such as persistence, shards and matching, don't exist.

## Server

`takt server --metrics-addr :9090` serves Prometheus metrics on `/metrics`, and `/healthz` on the same port. The Helm chart turns this on by default. Timers are histograms in seconds. The Go runtime and process collectors are included.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `takt_requests` | counter | `operation` | API calls, one `operation` per method (`StartWorkflowExecution`, `RespondDecisionTaskCompleted`, ...) |
| `takt_latency` | histogram | `operation` | API call latency |
| `takt_errors` | counter | `operation` | Calls that failed with an internal error |
| `takt_errors_<kind>` | counter | `operation` | Calls that failed with a known error: `bad_request`, `entity_not_exists`, `execution_already_started`, `workflow_execution_already_completed`, `domain_already_exists`, `cancellation_already_requested`, `query_failed`, `service_busy`, `limit_exceeded`, `context_timeout` |
| `takt_workflow_started` | counter | `domain`, `workflowtype` | Runs started, including continue-as-new, cron and retry runs |
| `takt_workflow_success`, `_failed`, `_timeout`, `_cancel`, `_terminate`, `_continued_as_new` | counter | `domain`, `workflowtype` | Runs closed, by close status |
| `takt_history_count` | histogram | `domain`, `workflowtype` | History length of closed runs, in events |
| `takt_decision_timeout` | counter | `domain`, `workflowtype` | Decision tasks that timed out |
| `takt_activity_timeout` | counter | `domain`, `workflowtype`, `timeout_type` | Activity timeouts |
| `takt_task_requests`, `takt_task_errors` | counter | `domain`, `operation` | Timer and transfer tasks processed, and those that failed and will be retried |
| `takt_task_latency` | histogram | `domain`, `operation` | Time to process a timer or transfer task |

The `operation` of timer and transfer tasks uses Cadence's names: `TimerActiveTaskUserTimer`, `TimerActiveTaskActivityTimeout`, `TimerActiveTaskActivityRetryTimer`, `TimerActiveTaskDecisionTimeout`, `TimerActiveTaskDecisionBackoffTimer`, `TimerActiveTaskWorkflowTimeout`, `TimerActiveTaskWorkflowBackoffTimer`, `TransferActiveTaskStartChildExecution`, `TransferActiveTaskSignalExecution`, `TransferActiveTaskCancelExecution` and `TransferActiveTaskCloseExecution`.

## Workers and clients

The SDK reports to the `tally.Scope` in `worker.Options.MetricsScope` and `client.Options.MetricsScope`, as Cadence's client does. Names use dashes; a Prometheus reporter turns them into underscores (`takt-decision-poll-total` becomes `takt_decision_poll_total`).

Worker metrics carry `domain`, `tasklist` and `workertype` (`DecisionWorker`, `ActivityWorker` or `LocalActivityWorker`). Metrics about one workflow or activity type also carry `workflowtype` or `activitytype`.

| Metric | Meaning |
|---|---|
| `takt-worker-start`, `takt-poller-start` | Workers and pollers started |
| `takt-decision-poll-total`, `-succeed`, `-no-task`, `-failed`, `-latency` | Decision task polls |
| `takt-decision-scheduled-to-start-latency` | Time a decision task waited for a worker |
| `takt-decision-execution-latency`, `takt-decision-execution-failed` | Running the workflow code for a decision task |
| `takt-decision-response-latency`, `takt-decision-response-failed` | Reporting the decisions to the server |
| `takt-decision-task-completed` | Decision tasks completed |
| `takt-decision-task-panic`, `takt-non-deterministic-error` | Workflow panics and nondeterminism |
| `takt-workflow-completed`, `-failed`, `-canceled`, `-continue-as-new` | Runs the worker closed |
| `takt-workflow-endtoend-latency` | Time from run start to close |
| `takt-workflow-get-history-total`, `-succeed`, `-failed`, `-latency` | Fetching extra history pages for a decision task |
| `takt-activity-poll-total`, `-succeed`, `-no-task`, `-failed`, `-latency` | Activity task polls |
| `takt-activity-scheduled-to-start-latency` | Time an activity task waited for a worker |
| `takt-activity-execution-latency`, `takt-activity-execution-failed`, `takt-activity-task-panic` | Running activity code |
| `takt-activity-task-completed`, `-failed`, `-canceled` | Activity results reported |
| `takt-activity-response-latency`, `takt-activity-response-failed` | Reporting activity results |
| `takt-activity-endtoend-latency` | Time from activity scheduled to result reported |
| `takt-local-activity-total`, `-failed`, `-timeout`, `-canceled`, `-panic`, `-execution-latency` | Local activities |
| `takt-sticky-cache-hit`, `-miss`, `-evict`, `takt-sticky-cache-size` | The worker's workflow cache |

Every service call is also counted in a sub scope named after the operation, as in Cadence: `takt-<Operation>.takt-request`, `.takt-error`, `.takt-invalid-request` and `.takt-latency`. With Prometheus these come out as `takt_StartWorkflowExecution_takt_request` and so on. Client error kinds (bad request, not found, already started and similar) count as `takt-invalid-request`; anything else is `takt-error`.

`workflow.GetMetricsScope` and `activity.GetMetricsScope` return the worker's scope tagged with the workflow or activity type. The workflow scope records nothing during replay.
