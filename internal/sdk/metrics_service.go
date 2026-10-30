package sdk

import (
	"context"

	"github.com/larsla/takt/api"
)

// The methods below record Cadence's per-operation request metrics around
// each call of the wrapped service.

func (s *metricsService) RegisterDomain(ctx context.Context, req *api.RegisterDomainRequest) error {
	op := s.op("RegisterDomain")
	err := s.Service.RegisterDomain(ctx, req)
	op.done(err)
	return err
}

func (s *metricsService) DescribeDomain(ctx context.Context, req *api.DescribeDomainRequest) (*api.DescribeDomainResponse, error) {
	op := s.op("DescribeDomain")
	resp, err := s.Service.DescribeDomain(ctx, req)
	op.done(err)
	return resp, err
}

func (s *metricsService) UpdateDomain(ctx context.Context, req *api.UpdateDomainRequest) (*api.UpdateDomainResponse, error) {
	op := s.op("UpdateDomain")
	resp, err := s.Service.UpdateDomain(ctx, req)
	op.done(err)
	return resp, err
}

func (s *metricsService) ListDomains(ctx context.Context, req *api.ListDomainsRequest) (*api.ListDomainsResponse, error) {
	op := s.op("ListDomains")
	resp, err := s.Service.ListDomains(ctx, req)
	op.done(err)
	return resp, err
}

func (s *metricsService) StartWorkflowExecution(ctx context.Context, req *api.StartWorkflowExecutionRequest) (*api.StartWorkflowExecutionResponse, error) {
	op := s.op("StartWorkflowExecution")
	resp, err := s.Service.StartWorkflowExecution(ctx, req)
	op.done(err)
	return resp, err
}

func (s *metricsService) SignalWorkflowExecution(ctx context.Context, req *api.SignalWorkflowExecutionRequest) error {
	op := s.op("SignalWorkflowExecution")
	err := s.Service.SignalWorkflowExecution(ctx, req)
	op.done(err)
	return err
}

func (s *metricsService) SignalWithStartWorkflowExecution(ctx context.Context, req *api.SignalWithStartWorkflowExecutionRequest) (*api.StartWorkflowExecutionResponse, error) {
	op := s.op("SignalWithStartWorkflowExecution")
	resp, err := s.Service.SignalWithStartWorkflowExecution(ctx, req)
	op.done(err)
	return resp, err
}

func (s *metricsService) RequestCancelWorkflowExecution(ctx context.Context, req *api.RequestCancelWorkflowExecutionRequest) error {
	op := s.op("RequestCancelWorkflowExecution")
	err := s.Service.RequestCancelWorkflowExecution(ctx, req)
	op.done(err)
	return err
}

func (s *metricsService) TerminateWorkflowExecution(ctx context.Context, req *api.TerminateWorkflowExecutionRequest) error {
	op := s.op("TerminateWorkflowExecution")
	err := s.Service.TerminateWorkflowExecution(ctx, req)
	op.done(err)
	return err
}

func (s *metricsService) GetWorkflowExecutionHistory(ctx context.Context, req *api.GetWorkflowExecutionHistoryRequest) (*api.GetWorkflowExecutionHistoryResponse, error) {
	op := s.op("GetWorkflowExecutionHistory")
	resp, err := s.Service.GetWorkflowExecutionHistory(ctx, req)
	op.done(err)
	return resp, err
}

func (s *metricsService) DescribeWorkflowExecution(ctx context.Context, req *api.DescribeWorkflowExecutionRequest) (*api.DescribeWorkflowExecutionResponse, error) {
	op := s.op("DescribeWorkflowExecution")
	resp, err := s.Service.DescribeWorkflowExecution(ctx, req)
	op.done(err)
	return resp, err
}

func (s *metricsService) QueryWorkflow(ctx context.Context, req *api.QueryWorkflowRequest) (*api.QueryWorkflowResponse, error) {
	op := s.op("QueryWorkflow")
	resp, err := s.Service.QueryWorkflow(ctx, req)
	op.done(err)
	return resp, err
}

func (s *metricsService) PollForDecisionTask(ctx context.Context, req *api.PollForDecisionTaskRequest) (*api.PollForDecisionTaskResponse, error) {
	op := s.op("PollForDecisionTask")
	resp, err := s.Service.PollForDecisionTask(ctx, req)
	op.done(err)
	return resp, err
}

func (s *metricsService) RespondDecisionTaskCompleted(ctx context.Context, req *api.RespondDecisionTaskCompletedRequest) (*api.RespondDecisionTaskCompletedResponse, error) {
	op := s.op("RespondDecisionTaskCompleted")
	resp, err := s.Service.RespondDecisionTaskCompleted(ctx, req)
	op.done(err)
	return resp, err
}

func (s *metricsService) RespondDecisionTaskFailed(ctx context.Context, req *api.RespondDecisionTaskFailedRequest) error {
	op := s.op("RespondDecisionTaskFailed")
	err := s.Service.RespondDecisionTaskFailed(ctx, req)
	op.done(err)
	return err
}

func (s *metricsService) PollForActivityTask(ctx context.Context, req *api.PollForActivityTaskRequest) (*api.PollForActivityTaskResponse, error) {
	op := s.op("PollForActivityTask")
	resp, err := s.Service.PollForActivityTask(ctx, req)
	op.done(err)
	return resp, err
}

func (s *metricsService) RecordActivityTaskHeartbeat(ctx context.Context, req *api.RecordActivityTaskHeartbeatRequest) (*api.RecordActivityTaskHeartbeatResponse, error) {
	op := s.op("RecordActivityTaskHeartbeat")
	resp, err := s.Service.RecordActivityTaskHeartbeat(ctx, req)
	op.done(err)
	return resp, err
}

func (s *metricsService) RecordActivityTaskHeartbeatByID(ctx context.Context, req *api.RecordActivityTaskHeartbeatByIDRequest) (*api.RecordActivityTaskHeartbeatResponse, error) {
	op := s.op("RecordActivityTaskHeartbeatByID")
	resp, err := s.Service.RecordActivityTaskHeartbeatByID(ctx, req)
	op.done(err)
	return resp, err
}

func (s *metricsService) RespondActivityTaskCompleted(ctx context.Context, req *api.RespondActivityTaskCompletedRequest) error {
	op := s.op("RespondActivityTaskCompleted")
	err := s.Service.RespondActivityTaskCompleted(ctx, req)
	op.done(err)
	return err
}

func (s *metricsService) RespondActivityTaskCompletedByID(ctx context.Context, req *api.RespondActivityTaskCompletedByIDRequest) error {
	op := s.op("RespondActivityTaskCompletedByID")
	err := s.Service.RespondActivityTaskCompletedByID(ctx, req)
	op.done(err)
	return err
}

func (s *metricsService) RespondActivityTaskFailed(ctx context.Context, req *api.RespondActivityTaskFailedRequest) error {
	op := s.op("RespondActivityTaskFailed")
	err := s.Service.RespondActivityTaskFailed(ctx, req)
	op.done(err)
	return err
}

func (s *metricsService) RespondActivityTaskFailedByID(ctx context.Context, req *api.RespondActivityTaskFailedByIDRequest) error {
	op := s.op("RespondActivityTaskFailedByID")
	err := s.Service.RespondActivityTaskFailedByID(ctx, req)
	op.done(err)
	return err
}

func (s *metricsService) RespondActivityTaskCanceled(ctx context.Context, req *api.RespondActivityTaskCanceledRequest) error {
	op := s.op("RespondActivityTaskCanceled")
	err := s.Service.RespondActivityTaskCanceled(ctx, req)
	op.done(err)
	return err
}

func (s *metricsService) RespondActivityTaskCanceledByID(ctx context.Context, req *api.RespondActivityTaskCanceledByIDRequest) error {
	op := s.op("RespondActivityTaskCanceledByID")
	err := s.Service.RespondActivityTaskCanceledByID(ctx, req)
	op.done(err)
	return err
}

func (s *metricsService) ListOpenWorkflowExecutions(ctx context.Context, req *api.ListOpenWorkflowExecutionsRequest) (*api.ListOpenWorkflowExecutionsResponse, error) {
	op := s.op("ListOpenWorkflowExecutions")
	resp, err := s.Service.ListOpenWorkflowExecutions(ctx, req)
	op.done(err)
	return resp, err
}

func (s *metricsService) ListClosedWorkflowExecutions(ctx context.Context, req *api.ListClosedWorkflowExecutionsRequest) (*api.ListClosedWorkflowExecutionsResponse, error) {
	op := s.op("ListClosedWorkflowExecutions")
	resp, err := s.Service.ListClosedWorkflowExecutions(ctx, req)
	op.done(err)
	return resp, err
}

func (s *metricsService) ListWorkflowExecutions(ctx context.Context, req *api.ListWorkflowExecutionsRequest) (*api.ListWorkflowExecutionsResponse, error) {
	op := s.op("ListWorkflowExecutions")
	resp, err := s.Service.ListWorkflowExecutions(ctx, req)
	op.done(err)
	return resp, err
}

func (s *metricsService) CountWorkflowExecutions(ctx context.Context, req *api.CountWorkflowExecutionsRequest) (*api.CountWorkflowExecutionsResponse, error) {
	op := s.op("CountWorkflowExecutions")
	resp, err := s.Service.CountWorkflowExecutions(ctx, req)
	op.done(err)
	return resp, err
}

func (s *metricsService) DescribeTaskList(ctx context.Context, req *api.DescribeTaskListRequest) (*api.DescribeTaskListResponse, error) {
	op := s.op("DescribeTaskList")
	resp, err := s.Service.DescribeTaskList(ctx, req)
	op.done(err)
	return resp, err
}

func (s *metricsService) ResetStickyTaskList(ctx context.Context, req *api.ResetStickyTaskListRequest) (*api.ResetStickyTaskListResponse, error) {
	op := s.op("ResetStickyTaskList")
	resp, err := s.Service.ResetStickyTaskList(ctx, req)
	op.done(err)
	return resp, err
}

func (s *metricsService) ListTaskLists(ctx context.Context, req *api.ListTaskListsRequest) (*api.ListTaskListsResponse, error) {
	op := s.op("ListTaskLists")
	resp, err := s.Service.ListTaskLists(ctx, req)
	op.done(err)
	return resp, err
}
