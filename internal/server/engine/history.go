package engine

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/larsla/takt/api"
)

type historyToken struct {
	Seq uint64 `json:"seq"`
}

// readHistory reads batches of s starting at stream sequence from (0 means
// the beginning) until at least max events. It returns the events, the
// sequence to continue from and whether more batches follow.
func (e *Engine) readHistory(ctx context.Context, s *State, from uint64, maxEvents int) ([]*api.HistoryEvent, []byte, error) {
	events, next, more, err := e.readBatches(ctx, s, from, maxEvents)
	if err != nil || !more {
		return events, nil, err
	}
	tok, _ := json.Marshal(historyToken{Seq: next})
	return events, tok, nil
}

func (e *Engine) readBatches(ctx context.Context, s *State, from uint64, maxEvents int) ([]*api.HistoryEvent, uint64, bool, error) {
	subject := api.HistorySubject(s.Domain, s.WorkflowID, s.RunID)
	seq := max(from, 1)
	var events []*api.HistoryEvent
	for seq <= s.HistorySeq {
		msg, err := e.history.GetMsg(ctx, seq, jetstream.WithGetMsgSubject(subject))
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			break
		}
		if err != nil {
			return nil, 0, false, err
		}
		var h api.History
		if err := json.Unmarshal(msg.Data, &h); err != nil {
			return nil, 0, false, err
		}
		events = append(events, h.Events...)
		seq = msg.Sequence + 1
		if maxEvents > 0 && len(events) >= maxEvents && msg.Sequence < s.HistorySeq {
			return events, seq, true, nil
		}
	}
	return events, seq, false, nil
}

func (e *Engine) GetWorkflowExecutionHistory(ctx context.Context, req *api.GetWorkflowExecutionHistoryRequest) (*api.GetWorkflowExecutionHistoryResponse, error) {
	if req.Execution == nil || req.Execution.WorkflowID == "" {
		return nil, badRequest("workflow ID is not set")
	}
	runID, err := e.resolveRunID(ctx, req.Domain, req.Execution)
	if err != nil {
		return nil, err
	}
	var tok historyToken
	if len(req.NextPageToken) > 0 {
		if err := json.Unmarshal(req.NextPageToken, &tok); err != nil {
			return nil, badRequest("invalid next page token")
		}
	}
	pageSize := int(req.MaximumPageSize)
	if pageSize <= 0 {
		pageSize = e.pageSize
	}
	key := runKey(req.Domain, req.Execution.WorkflowID, runID)
	closeOnly := req.HistoryEventFilterType == api.HistoryEventFilterTypeCloseEvent

	var sub *nats.Subscription
	if req.WaitForNewEvent {
		// Subscribe before reading so no batch slips in between.
		sub, err = e.nc.SubscribeSync(api.HistorySubject(req.Domain, req.Execution.WorkflowID, runID))
		if err != nil {
			return nil, err
		}
		defer func() { _ = sub.Unsubscribe() }()
	}
	deadline := time.Now().Add(e.longPoll)
	if d, ok := ctx.Deadline(); ok && d.Add(-time.Second).Before(deadline) {
		deadline = d.Add(-time.Second)
	}

	for {
		s, _, err := e.load(ctx, key)
		if err != nil {
			return nil, err
		}
		if closeOnly {
			if !s.Running() {
				ev, err := e.closeEvent(ctx, s)
				if err != nil {
					return nil, err
				}
				return &api.GetWorkflowExecutionHistoryResponse{History: &api.History{Events: []*api.HistoryEvent{ev}}}, nil
			}
		} else {
			events, next, more, err := e.readBatches(ctx, s, tok.Seq, pageSize)
			if err != nil {
				return nil, err
			}
			if len(events) > 0 || !req.WaitForNewEvent || !s.Running() {
				resp := &api.GetWorkflowExecutionHistoryResponse{History: &api.History{Events: events}}
				if more || (req.WaitForNewEvent && s.Running()) {
					resp.NextPageToken, _ = json.Marshal(historyToken{Seq: next})
				}
				if resp.History.Events == nil {
					resp.History.Events = []*api.HistoryEvent{}
				}
				return resp, nil
			}
		}
		if !req.WaitForNewEvent {
			return &api.GetWorkflowExecutionHistoryResponse{History: &api.History{Events: []*api.HistoryEvent{}}}, nil
		}
		wait := time.Until(deadline)
		if wait <= 0 {
			token := req.NextPageToken
			if len(token) == 0 {
				token, _ = json.Marshal(tok)
			}
			return &api.GetWorkflowExecutionHistoryResponse{History: &api.History{Events: []*api.HistoryEvent{}}, NextPageToken: token}, nil
		}
		if _, err := sub.NextMsg(wait); err != nil && !errors.Is(err, nats.ErrTimeout) {
			return nil, err
		}
	}
}
