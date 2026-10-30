package views

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/larsla/takt/api"
)

// Nav is the navigation state shown in the header.
type Nav struct {
	Domain  string
	Domains []string
	Active  string
}

// DomainRow is one row of the domains page.
type DomainRow struct {
	Name          string
	Description   string
	RetentionDays int32
	Open          int64
}

// WorkflowRow is one row of a workflow list.
type WorkflowRow struct {
	WorkflowID   string
	RunID        string
	Type         string
	Status       string
	Start        time.Time
	Close        time.Time
	HistoryLen   int64
	CronSchedule string
	Execution    time.Time
}

// WorkflowList is the workflows page.
type WorkflowList struct {
	Nav        Nav
	Status     string
	WorkflowID string
	Type       string
	Query      string
	Rows       []WorkflowRow
	NextToken  string
	Error      string
	Count      int64
}

// Field is a labeled value.
type Field struct {
	Label string
	Value string
	Href  string
	Mono  bool
}

// Payload is a decoded payload ready to show.
type Payload struct {
	Label string
	Body  string
}

// Activity is a pending activity.
type Activity struct {
	ID            string
	Type          string
	State         string
	Attempt       int32
	MaxAttempts   int32
	Scheduled     time.Time
	LastStarted   time.Time
	LastHeartbeat time.Time
	Heartbeat     string
	LastFailure   string
	Worker        string
}

// Child is a pending child workflow.
type Child struct {
	WorkflowID string
	RunID      string
	Type       string
	Href       string
}

// Event is one history event.
type Event struct {
	ID       int64
	Type     string
	Category string
	Time     time.Time
	Offset   string
	Summary  string
	JSON     string
}

// Workflow is the workflow detail page.
type Workflow struct {
	Nav        Nav
	WorkflowID string
	RunID      string
	Type       string
	Status     string
	Running    bool
	Fields     []Field
	Payloads   []Payload
	Activities []Activity
	Children   []Child
	Decision   string
	Events     []Event
	CanWrite   bool
	Error      string
}

// TaskList is one task list row.
type TaskList struct {
	Name    string
	Type    string
	Pollers int
	Backlog int64
	Href    string
}

// Poller is one poller of a task list.
type Poller struct {
	Identity   string
	LastAccess time.Time
}

// TaskLists is the task lists page.
type TaskLists struct {
	Nav   Nav
	Lists []TaskList
	Name  string
	Kind  string
	Polls []Poller
	Error string
}

// DecodePayload renders a payload: newline-separated JSON values are
// pretty-printed, anything else is shown as text or base64.
func DecodePayload(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	var parts []string
	for {
		var v interface{}
		if err := dec.Decode(&v); err != nil {
			if err.Error() == "EOF" && len(parts) > 0 {
				break
			}
			if isText(b) {
				return string(b)
			}
			return "base64:" + base64.StdEncoding.EncodeToString(b)
		}
		out, _ := json.MarshalIndent(v, "", "  ")
		parts = append(parts, string(out))
	}
	return strings.Join(parts, "\n")
}

func isText(b []byte) bool {
	for _, c := range b {
		if c < 0x09 || (c > 0x0d && c < 0x20) {
			return false
		}
	}
	return true
}

// payloadKeys are event attributes that hold encoded payloads.
var payloadKeys = map[string]bool{
	"input": true, "result": true, "details": true, "heartbeatDetails": true, "lastCompletionResult": true,
	"control": true, "failureDetails": true, "lastFailureDetails": true, "continuedFailureDetails": true,
	"executionContext": true, "signalInput": true,
}

// EventJSON pretty-prints an event's attributes with payloads decoded.
func EventJSON(e *api.HistoryEvent) string {
	raw, err := json.Marshal(e)
	if err != nil {
		return err.Error()
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return string(raw)
	}
	delete(m, "eventId")
	delete(m, "timestamp")
	delete(m, "eventType")
	var attrs interface{} = m
	if len(m) == 1 {
		for _, v := range m {
			attrs = v
		}
	}
	out, _ := json.MarshalIndent(decodePayloads(attrs), "", "  ")
	return string(out)
}

func decodePayloads(v interface{}) interface{} {
	switch x := v.(type) {
	case map[string]interface{}:
		for k, val := range x {
			if s, ok := val.(string); ok && payloadKeys[k] {
				if b, err := base64.StdEncoding.DecodeString(s); err == nil {
					x[k] = payloadValue(b)
					continue
				}
			}
			if k == "fields" || k == "indexedFields" {
				if fm, ok := val.(map[string]interface{}); ok {
					for fk, fv := range fm {
						if s, ok := fv.(string); ok {
							if b, err := base64.StdEncoding.DecodeString(s); err == nil {
								fm[fk] = payloadValue(b)
							}
						}
					}
					continue
				}
			}
			x[k] = decodePayloads(val)
		}
		return x
	case []interface{}:
		for i := range x {
			x[i] = decodePayloads(x[i])
		}
		return x
	}
	return v
}

// payloadValue turns a payload into a JSON value: a single JSON document
// as itself, several as a list, other bytes as a string.
func payloadValue(b []byte) interface{} {
	dec := json.NewDecoder(bytes.NewReader(b))
	var vals []interface{}
	for {
		var v interface{}
		if err := dec.Decode(&v); err != nil {
			break
		}
		vals = append(vals, v)
	}
	switch {
	case len(vals) == 1:
		return vals[0]
	case len(vals) > 1:
		return vals
	case isText(b):
		return string(b)
	}
	return "base64:" + base64.StdEncoding.EncodeToString(b)
}

// Ago formats how long ago t was.
func Ago(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	d := time.Since(t)
	if d < 0 {
		return "in " + roundDuration(-d)
	}
	return roundDuration(d) + " ago"
}

func roundDuration(d time.Duration) string {
	switch {
	case d < time.Second:
		return "just now"
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}

// Stamp formats a time for display.
func Stamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Local().Format("2006-01-02 15:04:05")
}

// ShortID shortens a run ID.
func ShortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// WorkflowHref links to a workflow run.
func WorkflowHref(domain, workflowID, runID string) string {
	q := url.Values{"id": {workflowID}}
	if runID != "" {
		q.Set("run", runID)
	}
	return "/domains/" + url.PathEscape(domain) + "/workflow?" + q.Encode()
}

// StatusVariant picks the badge variant for a status.
func StatusVariant(status string) string {
	switch status {
	case "RUNNING":
		return "default"
	case "COMPLETED", "CONTINUED_AS_NEW":
		return "secondary"
	case "FAILED", "TIMED_OUT", "TERMINATED":
		return "destructive"
	default:
		return "outline"
	}
}

// EventCategory groups event types for coloring.
func EventCategory(t api.EventType) string {
	s := string(t)
	switch {
	case strings.HasPrefix(s, "Decision"):
		return "decision"
	case strings.HasPrefix(s, "Activity") || strings.Contains(s, "ActivityTask"):
		return "activity"
	case strings.HasPrefix(s, "Timer") || strings.Contains(s, "Timer"):
		return "timer"
	case strings.Contains(s, "Child"):
		return "child"
	case strings.Contains(s, "Signal"):
		return "signal"
	case strings.HasPrefix(s, "WorkflowExecution"):
		return "workflow"
	}
	return "other"
}

// EventSummary picks the most useful fields of an event.
func EventSummary(e *api.HistoryEvent) string {
	switch {
	case e.WorkflowExecutionStartedEventAttributes != nil:
		a := e.WorkflowExecutionStartedEventAttributes
		return fmt.Sprintf("%s on %s", a.WorkflowType.Name, a.TaskList.Name)
	case e.ActivityTaskScheduledEventAttributes != nil:
		a := e.ActivityTaskScheduledEventAttributes
		return fmt.Sprintf("%s (id %s)", a.ActivityType.Name, a.ActivityID)
	case e.ActivityTaskStartedEventAttributes != nil:
		a := e.ActivityTaskStartedEventAttributes
		return fmt.Sprintf("attempt %d by %s", a.Attempt, a.Identity)
	case e.ActivityTaskFailedEventAttributes != nil:
		return e.ActivityTaskFailedEventAttributes.Reason
	case e.ActivityTaskTimedOutEventAttributes != nil:
		return string(e.ActivityTaskTimedOutEventAttributes.TimeoutType)
	case e.TimerStartedEventAttributes != nil:
		a := e.TimerStartedEventAttributes
		return fmt.Sprintf("timer %s for %ds", a.TimerID, a.StartToFireTimeoutSeconds)
	case e.TimerFiredEventAttributes != nil:
		return "timer " + e.TimerFiredEventAttributes.TimerID
	case e.WorkflowExecutionSignaledEventAttributes != nil:
		return e.WorkflowExecutionSignaledEventAttributes.SignalName
	case e.MarkerRecordedEventAttributes != nil:
		return e.MarkerRecordedEventAttributes.MarkerName
	case e.StartChildWorkflowExecutionInitiatedEventAttributes != nil:
		a := e.StartChildWorkflowExecutionInitiatedEventAttributes
		return fmt.Sprintf("%s (%s)", a.WorkflowType.Name, a.WorkflowID)
	case e.WorkflowExecutionFailedEventAttributes != nil:
		return e.WorkflowExecutionFailedEventAttributes.Reason
	case e.WorkflowExecutionTerminatedEventAttributes != nil:
		return e.WorkflowExecutionTerminatedEventAttributes.Reason
	case e.DecisionTaskStartedEventAttributes != nil:
		return e.DecisionTaskStartedEventAttributes.Identity
	case e.DecisionTaskFailedEventAttributes != nil:
		return string(e.DecisionTaskFailedEventAttributes.Cause)
	case e.SignalExternalWorkflowExecutionInitiatedEventAttributes != nil:
		a := e.SignalExternalWorkflowExecutionInitiatedEventAttributes
		return fmt.Sprintf("%s to %s", a.SignalName, a.WorkflowExecution.WorkflowID)
	}
	return ""
}

// QueryResult is the outcome of a query or action form.
type QueryResult struct {
	OK      bool
	Message string
	Body    string
}
