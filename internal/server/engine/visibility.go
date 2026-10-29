package engine

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/larsla/takt/api"
)

// VisibilityRecord is the listing entry for one run.
type VisibilityRecord struct {
	Domain           string                           `json:"domain"`
	WorkflowID       string                           `json:"workflowId"`
	RunID            string                           `json:"runId"`
	WorkflowType     string                           `json:"workflowType"`
	TaskList         string                           `json:"taskList"`
	StartTime        int64                            `json:"startTime"`
	ExecutionTime    int64                            `json:"executionTime"`
	CloseTime        int64                            `json:"closeTime,omitempty"`
	CloseStatus      api.WorkflowExecutionCloseStatus `json:"closeStatus,omitempty"`
	HistoryLength    int64                            `json:"historyLength"`
	Memo             *api.Memo                        `json:"memo,omitempty"`
	SearchAttributes *api.SearchAttributes            `json:"searchAttributes,omitempty"`
	ParentDomain     string                           `json:"parentDomain,omitempty"`
	ParentExecution  *api.WorkflowExecution           `json:"parentExecution,omitempty"`
	CronSchedule     string                           `json:"cronSchedule,omitempty"`
}

func visibilityKey(domain, runID string) string { return api.Token(domain) + "." + runID }

func (e *Engine) writeVisibility(ctx context.Context, s *State) error {
	r := &VisibilityRecord{
		Domain:           s.Domain,
		WorkflowID:       s.WorkflowID,
		RunID:            s.RunID,
		WorkflowType:     s.WorkflowType(),
		TaskList:         s.TaskList(),
		StartTime:        s.StartTime,
		ExecutionTime:    s.ExecutionTime,
		CloseTime:        s.CloseTime,
		CloseStatus:      s.CloseStatus,
		HistoryLength:    s.NextEventID - 1,
		Memo:             s.Start.Memo,
		SearchAttributes: s.SearchAttributes,
		ParentDomain:     s.Start.ParentWorkflowDomain,
		ParentExecution:  s.Start.ParentWorkflowExecution,
		CronSchedule:     s.Start.CronSchedule,
	}
	data, _ := json.Marshal(r)
	_, err := e.vis.Put(ctx, visibilityKey(s.Domain, s.RunID), data)
	return err
}

func (r *VisibilityRecord) info() *api.WorkflowExecutionInfo {
	info := &api.WorkflowExecutionInfo{
		Execution:        &api.WorkflowExecution{WorkflowID: r.WorkflowID, RunID: r.RunID},
		Type:             &api.WorkflowType{Name: r.WorkflowType},
		StartTime:        r.StartTime,
		CloseTime:        r.CloseTime,
		HistoryLength:    r.HistoryLength,
		ParentDomain:     r.ParentDomain,
		ParentExecution:  r.ParentExecution,
		ExecutionTime:    r.ExecutionTime,
		Memo:             r.Memo,
		SearchAttributes: r.SearchAttributes,
		TaskList:         r.TaskList,
		CronSchedule:     r.CronSchedule,
	}
	if r.CloseStatus != "" {
		cs := r.CloseStatus
		info.CloseStatus = &cs
	}
	return info
}

// records returns the visibility records of a domain that match keep,
// newest start first.
func (e *Engine) records(ctx context.Context, domain string, keep func(*VisibilityRecord) bool) ([]*VisibilityRecord, error) {
	lister, err := e.vis.ListKeysFiltered(ctx, api.Token(domain)+".*")
	if err != nil {
		return nil, err
	}
	var out []*VisibilityRecord
	for k := range lister.Keys() {
		entry, err := e.vis.Get(ctx, k)
		if err != nil {
			continue
		}
		r := &VisibilityRecord{}
		if json.Unmarshal(entry.Value(), r) != nil {
			continue
		}
		if keep(r) {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].StartTime != out[j].StartTime {
			return out[i].StartTime > out[j].StartTime
		}
		return out[i].RunID < out[j].RunID
	})
	return out, nil
}

type listToken struct {
	Offset int `json:"offset"`
}

// page cuts one page out of recs.
func page(recs []*VisibilityRecord, size int32, token []byte) (*api.ListWorkflowExecutionsResponse, error) {
	var tok listToken
	if len(token) > 0 {
		if err := json.Unmarshal(token, &tok); err != nil {
			return nil, badRequest("invalid next page token")
		}
	}
	if size <= 0 {
		size = 100
	}
	resp := &api.ListWorkflowExecutionsResponse{Executions: []*api.WorkflowExecutionInfo{}}
	start := min(tok.Offset, len(recs))
	end := min(start+int(size), len(recs))
	for _, r := range recs[start:end] {
		resp.Executions = append(resp.Executions, r.info())
	}
	if end < len(recs) {
		resp.NextPageToken, _ = json.Marshal(listToken{Offset: end})
	}
	return resp, nil
}

func commonFilter(r *VisibilityRecord, st *api.StartTimeFilter, ef *api.WorkflowExecutionFilter, tf *api.WorkflowTypeFilter) bool {
	if st != nil {
		if st.EarliestTime > 0 && r.StartTime < st.EarliestTime {
			return false
		}
		if st.LatestTime > 0 && r.StartTime > st.LatestTime {
			return false
		}
	}
	if ef != nil {
		if ef.WorkflowID != "" && r.WorkflowID != ef.WorkflowID {
			return false
		}
		if ef.RunID != "" && r.RunID != ef.RunID {
			return false
		}
	}
	if tf != nil && tf.Name != "" && r.WorkflowType != tf.Name {
		return false
	}
	return true
}

func (e *Engine) ListOpenWorkflowExecutions(ctx context.Context, req *api.ListOpenWorkflowExecutionsRequest) (*api.ListOpenWorkflowExecutionsResponse, error) {
	recs, err := e.records(ctx, req.Domain, func(r *VisibilityRecord) bool {
		return r.CloseStatus == "" && commonFilter(r, req.StartTimeFilter, req.ExecutionFilter, req.TypeFilter)
	})
	if err != nil {
		return nil, err
	}
	return page(recs, req.MaximumPageSize, req.NextPageToken)
}

func (e *Engine) ListClosedWorkflowExecutions(ctx context.Context, req *api.ListClosedWorkflowExecutionsRequest) (*api.ListClosedWorkflowExecutionsResponse, error) {
	recs, err := e.records(ctx, req.Domain, func(r *VisibilityRecord) bool {
		if r.CloseStatus == "" || (req.StatusFilter != nil && r.CloseStatus != *req.StatusFilter) {
			return false
		}
		return commonFilter(r, req.StartTimeFilter, req.ExecutionFilter, req.TypeFilter)
	})
	if err != nil {
		return nil, err
	}
	return page(recs, req.MaximumPageSize, req.NextPageToken)
}
