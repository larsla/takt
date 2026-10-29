package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/larsla/takt/api"
)

func (e *Engine) DescribeWorkflowExecution(ctx context.Context, req *api.DescribeWorkflowExecutionRequest) (*api.DescribeWorkflowExecutionResponse, error) {
	s, err := e.loadRun(ctx, req.Domain, req.Execution)
	if err != nil {
		return nil, err
	}
	resp := &api.DescribeWorkflowExecutionResponse{
		ExecutionConfiguration: &api.WorkflowExecutionConfiguration{
			TaskList:                            &api.TaskList{Name: s.TaskList()},
			ExecutionStartToCloseTimeoutSeconds: s.Start.ExecutionStartToCloseTimeoutSeconds,
			TaskStartToCloseTimeoutSeconds:      s.Start.TaskStartToCloseTimeoutSeconds,
		},
		WorkflowExecutionInfo: executionInfo(s),
	}
	ids := make([]int64, 0, len(s.Activities))
	for id := range s.Activities {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		ai := s.Activities[id]
		state := api.PendingActivityStateScheduled
		switch {
		case ai.CancelRequested:
			state = api.PendingActivityStateCancelRequested
		case ai.Started:
			state = api.PendingActivityStateStarted
		}
		pa := &api.PendingActivityInfo{
			ActivityID:             ai.ActivityID,
			ActivityType:           &api.ActivityType{Name: ai.ActivityType},
			State:                  state,
			HeartbeatDetails:       ai.HeartbeatDetails,
			LastHeartbeatTimestamp: ai.LastHeartbeat,
			LastStartedTimestamp:   ai.StartedTime,
			Attempt:                ai.Attempt,
			ScheduledTimestamp:     ai.AttemptScheduledTime,
			ExpirationTimestamp:    ai.ExpirationTime,
			LastFailureReason:      ai.LastFailureReason,
			LastFailureDetails:     ai.LastFailureDetails,
			LastWorkerIdentity:     ai.StartedIdentity,
		}
		if ai.RetryPolicy != nil {
			pa.MaximumAttempts = ai.RetryPolicy.MaximumAttempts
		}
		resp.PendingActivities = append(resp.PendingActivities, pa)
	}
	for _, ci := range s.Children {
		resp.PendingChildren = append(resp.PendingChildren, &api.PendingChildExecutionInfo{
			Domain:            ci.Domain,
			WorkflowID:        ci.WorkflowID,
			RunID:             ci.RunID,
			WorkflowTypeName:  ci.WorkflowType,
			InitiatedID:       ci.InitiatedID,
			ParentClosePolicy: ci.ParentClosePolicy,
		})
	}
	sort.Slice(resp.PendingChildren, func(i, j int) bool {
		return resp.PendingChildren[i].InitiatedID < resp.PendingChildren[j].InitiatedID
	})
	if d := s.Decision; d != nil {
		pd := &api.PendingDecisionInfo{
			State:                      api.PendingDecisionStateScheduled,
			ScheduledTimestamp:         d.ScheduledTime,
			Attempt:                    d.Attempt,
			OriginalScheduledTimestamp: d.ScheduledTime,
		}
		if d.StartedID != 0 {
			pd.State = api.PendingDecisionStateStarted
			pd.StartedTimestamp = d.StartedTime
		}
		resp.PendingDecision = pd
	}
	return resp, nil
}

func executionInfo(s *State) *api.WorkflowExecutionInfo {
	info := &api.WorkflowExecutionInfo{
		Execution:        s.Execution(),
		Type:             s.Start.WorkflowType,
		StartTime:        s.StartTime,
		CloseTime:        s.CloseTime,
		HistoryLength:    s.NextEventID - 1,
		ParentDomain:     s.Start.ParentWorkflowDomain,
		ParentExecution:  s.Start.ParentWorkflowExecution,
		ExecutionTime:    s.ExecutionTime,
		Memo:             s.Start.Memo,
		SearchAttributes: s.SearchAttributes,
		TaskList:         s.TaskList(),
		CronSchedule:     s.Start.CronSchedule,
	}
	if !s.Running() {
		cs := s.CloseStatus
		info.CloseStatus = &cs
	}
	return info
}

func (e *Engine) QueryWorkflow(ctx context.Context, req *api.QueryWorkflowRequest) (*api.QueryWorkflowResponse, error) {
	if req.Query == nil || req.Query.QueryType == "" {
		return nil, badRequest("query type is not set")
	}
	s, err := e.loadRun(ctx, req.Domain, req.Execution)
	if err != nil {
		return nil, err
	}
	if !s.Running() {
		reject := req.QueryRejectCondition == api.QueryRejectConditionNotOpen ||
			(req.QueryRejectCondition == api.QueryRejectConditionNotCompletedCleanly && s.CloseStatus != api.CloseStatusCompleted)
		if reject {
			cs := s.CloseStatus
			return &api.QueryWorkflowResponse{QueryRejected: &api.QueryRejected{CloseStatus: &cs}}, nil
		}
	}
	body, _ := json.Marshal(&api.WorkerQueryRequest{
		Domain:       s.Domain,
		Execution:    s.Execution(),
		WorkflowType: s.Start.WorkflowType,
		Query:        req.Query,
	})
	qctx, cancel := context.WithTimeout(ctx, e.queryTimeout)
	defer cancel()
	msg, err := e.nc.RequestWithContext(qctx, api.QuerySubject(s.Domain, s.TaskList()), body)
	if errors.Is(err, nats.ErrNoResponders) {
		return nil, &api.QueryFailedError{Message: "no worker is polling task list " + s.TaskList()}
	}
	if err != nil {
		return nil, &api.QueryFailedError{Message: "query timed out: " + err.Error()}
	}
	var wr api.WorkerQueryResponse
	if err := json.Unmarshal(msg.Data, &wr); err != nil {
		return nil, &api.QueryFailedError{Message: "malformed query response"}
	}
	if wr.ErrorMessage != "" {
		return nil, &api.QueryFailedError{Message: wr.ErrorMessage}
	}
	return &api.QueryWorkflowResponse{QueryResult: wr.Result}, nil
}

type pollerRecord struct {
	Identity       string `json:"identity"`
	LastAccessTime int64  `json:"lastAccessTime"`
}

func pollerKey(domain, taskList string, t api.TaskListType, identity string) string {
	return pollerPrefix(domain, taskList, t) + api.Token(identity)
}

func pollerPrefix(domain, taskList string, t api.TaskListType) string {
	kind := "d"
	if t == api.TaskListTypeActivity {
		kind = "a"
	}
	return api.Token(domain) + "." + api.Token(taskList) + "." + kind + "."
}

// RecordPoller notes that a worker is polling a task list.
func (e *Engine) RecordPoller(ctx context.Context, req *api.RecordPollerRequest) error {
	if req.Domain == "" || req.TaskList == "" {
		return badRequest("domain and task list are required")
	}
	data, _ := json.Marshal(pollerRecord{Identity: req.Identity, LastAccessTime: e.nowNano()})
	_, err := e.pollers.Put(ctx, pollerKey(req.Domain, req.TaskList, req.TaskListType, req.Identity), data)
	return err
}

func (e *Engine) DescribeTaskList(ctx context.Context, req *api.DescribeTaskListRequest) (*api.DescribeTaskListResponse, error) {
	if req.TaskList == nil || req.TaskList.Name == "" {
		return nil, badRequest("task list is not set")
	}
	tlType := req.TaskListType
	if tlType == "" {
		tlType = api.TaskListTypeDecision
	}
	resp := &api.DescribeTaskListResponse{Pollers: []*api.PollerInfo{}}
	lister, err := e.pollers.ListKeysFiltered(ctx, pollerPrefix(req.Domain, req.TaskList.Name, tlType)+"*")
	if err != nil {
		return nil, err
	}
	for k := range lister.Keys() {
		entry, err := e.pollers.Get(ctx, k)
		if err != nil {
			continue
		}
		var p pollerRecord
		if json.Unmarshal(entry.Value(), &p) == nil {
			resp.Pollers = append(resp.Pollers, &api.PollerInfo{Identity: p.Identity, LastAccessTime: p.LastAccessTime})
		}
	}
	sort.Slice(resp.Pollers, func(i, j int) bool { return resp.Pollers[i].Identity < resp.Pollers[j].Identity })
	if req.IncludeTaskListStatus {
		backlog, err := e.backlog(ctx, req.Domain, req.TaskList.Name, tlType)
		if err != nil {
			return nil, err
		}
		resp.TaskListStatus = &api.TaskListStatus{BacklogCountHint: backlog}
	}
	return resp, nil
}

func (e *Engine) backlog(ctx context.Context, domain, taskList string, t api.TaskListType) (int64, error) {
	subject := api.TaskSubject(domain, taskList, t)
	info, err := e.tasks.Info(ctx, jetstream.WithSubjectFilter(subject))
	if err != nil {
		return 0, err
	}
	return clampInt64(info.State.Subjects[subject]), nil
}

// ListTaskLists returns the task lists of a domain that have pollers or a backlog.
func (e *Engine) ListTaskLists(ctx context.Context, req *api.ListTaskListsRequest) (*api.ListTaskListsResponse, error) {
	if req.Domain == "" {
		return nil, badRequest("domain is not set")
	}
	type tlKey struct {
		name string
		t    api.TaskListType
	}
	found := map[tlKey]*api.TaskListSummary{}
	get := func(name string, t api.TaskListType) *api.TaskListSummary {
		k := tlKey{name, t}
		if found[k] == nil {
			found[k] = &api.TaskListSummary{Name: name, TaskListType: t}
		}
		return found[k]
	}
	domTok := api.Token(req.Domain)
	lister, err := e.pollers.ListKeysFiltered(ctx, domTok+".>")
	if err != nil {
		return nil, err
	}
	for k := range lister.Keys() {
		parts := strings.Split(k, ".")
		if len(parts) != 4 {
			continue
		}
		name, err := api.FromToken(parts[1])
		if err != nil {
			continue
		}
		t := api.TaskListTypeDecision
		if parts[2] == "a" {
			t = api.TaskListTypeActivity
		}
		get(name, t).Pollers++
	}
	info, err := e.tasks.Info(ctx, jetstream.WithSubjectFilter("takt.task."+domTok+".>"))
	if err != nil {
		return nil, err
	}
	for subject, n := range info.State.Subjects {
		parts := strings.Split(subject, ".")
		if len(parts) != 5 {
			continue
		}
		name, err := api.FromToken(parts[3])
		if err != nil {
			continue
		}
		t := api.TaskListTypeDecision
		if parts[4] == "activity" {
			t = api.TaskListTypeActivity
		}
		get(name, t).Backlog = clampInt64(n)
	}
	resp := &api.ListTaskListsResponse{TaskLists: []*api.TaskListSummary{}}
	for _, s := range found {
		resp.TaskLists = append(resp.TaskLists, s)
	}
	sort.Slice(resp.TaskLists, func(i, j int) bool {
		a, b := resp.TaskLists[i], resp.TaskLists[j]
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		return a.TaskListType < b.TaskListType
	})
	return resp, nil
}
