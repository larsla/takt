// Package transport implements api.Service over NATS. API calls are NATS
// requests to the takt server; task polls pull stubs straight from the
// JetStream task stream and exchange them with the server for full tasks.
package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"

	"github.com/larsla/takt/api"
)

// Options tunes the client.
type Options struct {
	// RequestTimeout applies to calls whose context has no deadline. Defaults to 10s.
	RequestTimeout time.Duration
	// PollTimeout is how long one poll waits for a task. Defaults to 30s.
	PollTimeout time.Duration
}

// Client is an api.Service backed by NATS.
type Client struct {
	nc   *nats.Conn
	js   jetstream.JetStream
	opts Options

	mu        sync.Mutex
	consumers map[string]jetstream.Consumer
	pollerAt  map[string]time.Time
}

var _ api.Service = (*Client)(nil)

// New returns a client using nc.
func New(nc *nats.Conn, opts Options) (*Client, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	if opts.RequestTimeout == 0 {
		opts.RequestTimeout = 10 * time.Second
	}
	if opts.PollTimeout == 0 {
		opts.PollTimeout = 30 * time.Second
	}
	return &Client{
		nc:        nc,
		js:        js,
		opts:      opts,
		consumers: map[string]jetstream.Consumer{},
		pollerAt:  map[string]time.Time{},
	}, nil
}

// Conn returns the underlying connection.
func (c *Client) Conn() *nats.Conn { return c.nc }

func (c *Client) call(ctx context.Context, method string, req, resp any) error {
	return c.callTimeout(ctx, method, req, resp, c.opts.RequestTimeout)
}

func (c *Client) callTimeout(ctx context.Context, method string, req, resp any, timeout time.Duration) error {
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	msg, err := c.nc.RequestWithContext(ctx, api.APISubject(method), data)
	if errors.Is(err, nats.ErrNoResponders) {
		return &api.ServiceBusyError{Message: "no takt server is reachable"}
	}
	if err != nil {
		return err
	}
	if code := msg.Header.Get(micro.ErrorCodeHeader); code != "" {
		return api.DecodeError(code, msg.Header.Get(micro.ErrorHeader), msg.Data)
	}
	if resp == nil {
		return nil
	}
	if err := json.Unmarshal(msg.Data, resp); err != nil {
		return fmt.Errorf("decode %s response: %w", method, err)
	}
	return nil
}

func (c *Client) RegisterDomain(ctx context.Context, req *api.RegisterDomainRequest) error {
	return c.call(ctx, api.MethodRegisterDomain, req, nil)
}

func (c *Client) DescribeDomain(ctx context.Context, req *api.DescribeDomainRequest) (*api.DescribeDomainResponse, error) {
	resp := &api.DescribeDomainResponse{}
	return resp, c.call(ctx, api.MethodDescribeDomain, req, resp)
}

func (c *Client) UpdateDomain(ctx context.Context, req *api.UpdateDomainRequest) (*api.UpdateDomainResponse, error) {
	resp := &api.UpdateDomainResponse{}
	return resp, c.call(ctx, api.MethodUpdateDomain, req, resp)
}

func (c *Client) ListDomains(ctx context.Context, req *api.ListDomainsRequest) (*api.ListDomainsResponse, error) {
	resp := &api.ListDomainsResponse{}
	return resp, c.call(ctx, api.MethodListDomains, req, resp)
}

func (c *Client) StartWorkflowExecution(ctx context.Context, req *api.StartWorkflowExecutionRequest) (*api.StartWorkflowExecutionResponse, error) {
	resp := &api.StartWorkflowExecutionResponse{}
	return resp, c.call(ctx, api.MethodStartWorkflowExecution, req, resp)
}

func (c *Client) SignalWorkflowExecution(ctx context.Context, req *api.SignalWorkflowExecutionRequest) error {
	return c.call(ctx, api.MethodSignalWorkflowExecution, req, nil)
}

func (c *Client) SignalWithStartWorkflowExecution(ctx context.Context, req *api.SignalWithStartWorkflowExecutionRequest) (*api.StartWorkflowExecutionResponse, error) {
	resp := &api.StartWorkflowExecutionResponse{}
	return resp, c.call(ctx, api.MethodSignalWithStartWorkflowExecution, req, resp)
}

func (c *Client) RequestCancelWorkflowExecution(ctx context.Context, req *api.RequestCancelWorkflowExecutionRequest) error {
	return c.call(ctx, api.MethodRequestCancelWorkflowExecution, req, nil)
}

func (c *Client) TerminateWorkflowExecution(ctx context.Context, req *api.TerminateWorkflowExecutionRequest) error {
	return c.call(ctx, api.MethodTerminateWorkflowExecution, req, nil)
}

func (c *Client) GetWorkflowExecutionHistory(ctx context.Context, req *api.GetWorkflowExecutionHistoryRequest) (*api.GetWorkflowExecutionHistoryResponse, error) {
	resp := &api.GetWorkflowExecutionHistoryResponse{}
	timeout := c.opts.RequestTimeout
	if req.WaitForNewEvent {
		// The server holds long polls for up to 30s.
		timeout = time.Minute
	}
	return resp, c.callTimeout(ctx, api.MethodGetWorkflowExecutionHistory, req, resp, timeout)
}

func (c *Client) DescribeWorkflowExecution(ctx context.Context, req *api.DescribeWorkflowExecutionRequest) (*api.DescribeWorkflowExecutionResponse, error) {
	resp := &api.DescribeWorkflowExecutionResponse{}
	return resp, c.call(ctx, api.MethodDescribeWorkflowExecution, req, resp)
}

func (c *Client) QueryWorkflow(ctx context.Context, req *api.QueryWorkflowRequest) (*api.QueryWorkflowResponse, error) {
	resp := &api.QueryWorkflowResponse{}
	return resp, c.callTimeout(ctx, api.MethodQueryWorkflow, req, resp, 30*time.Second)
}

func (c *Client) RespondDecisionTaskCompleted(ctx context.Context, req *api.RespondDecisionTaskCompletedRequest) (*api.RespondDecisionTaskCompletedResponse, error) {
	resp := &api.RespondDecisionTaskCompletedResponse{}
	return resp, c.call(ctx, api.MethodRespondDecisionTaskCompleted, req, resp)
}

func (c *Client) RespondDecisionTaskFailed(ctx context.Context, req *api.RespondDecisionTaskFailedRequest) error {
	return c.call(ctx, api.MethodRespondDecisionTaskFailed, req, nil)
}

func (c *Client) RecordActivityTaskHeartbeat(ctx context.Context, req *api.RecordActivityTaskHeartbeatRequest) (*api.RecordActivityTaskHeartbeatResponse, error) {
	resp := &api.RecordActivityTaskHeartbeatResponse{}
	return resp, c.call(ctx, api.MethodRecordActivityTaskHeartbeat, req, resp)
}

func (c *Client) RecordActivityTaskHeartbeatByID(ctx context.Context, req *api.RecordActivityTaskHeartbeatByIDRequest) (*api.RecordActivityTaskHeartbeatResponse, error) {
	resp := &api.RecordActivityTaskHeartbeatResponse{}
	return resp, c.call(ctx, api.MethodRecordActivityTaskHeartbeatByID, req, resp)
}

func (c *Client) RespondActivityTaskCompleted(ctx context.Context, req *api.RespondActivityTaskCompletedRequest) error {
	return c.call(ctx, api.MethodRespondActivityTaskCompleted, req, nil)
}

func (c *Client) RespondActivityTaskCompletedByID(ctx context.Context, req *api.RespondActivityTaskCompletedByIDRequest) error {
	return c.call(ctx, api.MethodRespondActivityTaskCompletedByID, req, nil)
}

func (c *Client) RespondActivityTaskFailed(ctx context.Context, req *api.RespondActivityTaskFailedRequest) error {
	return c.call(ctx, api.MethodRespondActivityTaskFailed, req, nil)
}

func (c *Client) RespondActivityTaskFailedByID(ctx context.Context, req *api.RespondActivityTaskFailedByIDRequest) error {
	return c.call(ctx, api.MethodRespondActivityTaskFailedByID, req, nil)
}

func (c *Client) RespondActivityTaskCanceled(ctx context.Context, req *api.RespondActivityTaskCanceledRequest) error {
	return c.call(ctx, api.MethodRespondActivityTaskCanceled, req, nil)
}

func (c *Client) RespondActivityTaskCanceledByID(ctx context.Context, req *api.RespondActivityTaskCanceledByIDRequest) error {
	return c.call(ctx, api.MethodRespondActivityTaskCanceledByID, req, nil)
}

func (c *Client) ListOpenWorkflowExecutions(ctx context.Context, req *api.ListOpenWorkflowExecutionsRequest) (*api.ListOpenWorkflowExecutionsResponse, error) {
	resp := &api.ListOpenWorkflowExecutionsResponse{}
	return resp, c.call(ctx, api.MethodListOpenWorkflowExecutions, req, resp)
}

func (c *Client) ListClosedWorkflowExecutions(ctx context.Context, req *api.ListClosedWorkflowExecutionsRequest) (*api.ListClosedWorkflowExecutionsResponse, error) {
	resp := &api.ListClosedWorkflowExecutionsResponse{}
	return resp, c.call(ctx, api.MethodListClosedWorkflowExecutions, req, resp)
}

func (c *Client) ListWorkflowExecutions(ctx context.Context, req *api.ListWorkflowExecutionsRequest) (*api.ListWorkflowExecutionsResponse, error) {
	resp := &api.ListWorkflowExecutionsResponse{}
	return resp, c.call(ctx, api.MethodListWorkflowExecutions, req, resp)
}

func (c *Client) CountWorkflowExecutions(ctx context.Context, req *api.CountWorkflowExecutionsRequest) (*api.CountWorkflowExecutionsResponse, error) {
	resp := &api.CountWorkflowExecutionsResponse{}
	return resp, c.call(ctx, api.MethodCountWorkflowExecutions, req, resp)
}

func (c *Client) DescribeTaskList(ctx context.Context, req *api.DescribeTaskListRequest) (*api.DescribeTaskListResponse, error) {
	resp := &api.DescribeTaskListResponse{}
	return resp, c.call(ctx, api.MethodDescribeTaskList, req, resp)
}

func (c *Client) ListTaskLists(ctx context.Context, req *api.ListTaskListsRequest) (*api.ListTaskListsResponse, error) {
	resp := &api.ListTaskListsResponse{}
	return resp, c.call(ctx, api.MethodListTaskLists, req, resp)
}

// Task polling

func (c *Client) PollForDecisionTask(ctx context.Context, req *api.PollForDecisionTaskRequest) (*api.PollForDecisionTaskResponse, error) {
	if req.TaskList == nil || req.TaskList.Name == "" {
		return nil, &api.BadRequestError{Message: "task list is not set"}
	}
	resp := &api.PollForDecisionTaskResponse{}
	found, err := c.poll(ctx, req.Domain, req.TaskList.Name, api.TaskListTypeDecision, req.Identity, func(stub api.TaskStub) error {
		r := &api.RecordDecisionTaskStartedRequest{Stub: stub, Identity: req.Identity, RequestID: uuid.NewString()}
		return c.call(ctx, api.MethodRecordDecisionTaskStarted, r, resp)
	})
	if err != nil || !found {
		return &api.PollForDecisionTaskResponse{}, err
	}
	return resp, nil
}

func (c *Client) PollForActivityTask(ctx context.Context, req *api.PollForActivityTaskRequest) (*api.PollForActivityTaskResponse, error) {
	if req.TaskList == nil || req.TaskList.Name == "" {
		return nil, &api.BadRequestError{Message: "task list is not set"}
	}
	resp := &api.PollForActivityTaskResponse{}
	found, err := c.poll(ctx, req.Domain, req.TaskList.Name, api.TaskListTypeActivity, req.Identity, func(stub api.TaskStub) error {
		r := &api.RecordActivityTaskStartedRequest{Stub: stub, Identity: req.Identity, RequestID: uuid.NewString()}
		return c.call(ctx, api.MethodRecordActivityTaskStarted, r, resp)
	})
	if err != nil || !found {
		return &api.PollForActivityTaskResponse{}, err
	}
	return resp, nil
}

// poll fetches stubs until start accepts one, the poll times out or ctx ends.
// Stubs the server rejects as stale are acked and dropped.
func (c *Client) poll(ctx context.Context, domain, taskList string, t api.TaskListType, identity string, start func(api.TaskStub) error) (bool, error) {
	c.recordPoller(ctx, domain, taskList, t, identity)
	cons, err := c.consumer(ctx, domain, taskList, t)
	if err != nil {
		return false, err
	}
	deadline := time.Now().Add(c.opts.PollTimeout)
	for ctx.Err() == nil {
		wait := min(time.Until(deadline), 5*time.Second)
		if wait <= 0 {
			return false, nil
		}
		batch, err := cons.Fetch(1, jetstream.FetchMaxWait(wait))
		if err != nil {
			return false, err
		}
		for msg := range batch.Messages() {
			var stub api.TaskStub
			if json.Unmarshal(msg.Data(), &stub) != nil {
				_ = msg.Term()
				continue
			}
			err := start(stub)
			var nf *api.EntityNotExistsError
			switch {
			case err == nil:
				_ = msg.Ack()
				return true, nil
			case errors.As(err, &nf):
				_ = msg.Ack()
			default:
				_ = msg.Nak()
				return false, err
			}
		}
		if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) {
			return false, err
		}
	}
	return false, ctx.Err()
}

func (c *Client) consumer(ctx context.Context, domain, taskList string, t api.TaskListType) (jetstream.Consumer, error) {
	name := api.TaskConsumerName(domain, taskList, t)
	c.mu.Lock()
	cons := c.consumers[name]
	c.mu.Unlock()
	if cons != nil {
		return cons, nil
	}
	cons, err := c.js.CreateOrUpdateConsumer(ctx, api.TaskStream, jetstream.ConsumerConfig{
		Durable:           name,
		FilterSubject:     api.TaskSubject(domain, taskList, t),
		AckPolicy:         jetstream.AckExplicitPolicy,
		AckWait:           30 * time.Second,
		MaxDeliver:        -1,
		InactiveThreshold: 24 * time.Hour,
	})
	if err != nil {
		return nil, fmt.Errorf("task list consumer: %w", err)
	}
	c.mu.Lock()
	c.consumers[name] = cons
	c.mu.Unlock()
	return cons, nil
}

// recordPoller tells the server about this poller at most every 10s.
func (c *Client) recordPoller(ctx context.Context, domain, taskList string, t api.TaskListType, identity string) {
	key := domain + "\x00" + taskList + "\x00" + string(t) + "\x00" + identity
	c.mu.Lock()
	last := c.pollerAt[key]
	due := time.Since(last) > 10*time.Second
	if due {
		c.pollerAt[key] = time.Now()
	}
	c.mu.Unlock()
	if !due {
		return
	}
	req := &api.RecordPollerRequest{Domain: domain, TaskList: taskList, TaskListType: t, Identity: identity}
	_ = c.call(ctx, api.MethodRecordPoller, req, nil)
}

// ListenQueries answers queries for workflows on a task list. Workers of
// the same task list share the load through a queue group.
func (c *Client) ListenQueries(domain, taskList string, handle func(context.Context, *api.WorkerQueryRequest) *api.WorkerQueryResponse) (func(), error) {
	sub, err := c.nc.QueueSubscribe(api.QuerySubject(domain, taskList), "takt-workers", func(m *nats.Msg) {
		go func() {
			var req api.WorkerQueryRequest
			if json.Unmarshal(m.Data, &req) != nil || req.Query == nil || req.Execution == nil {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			b, _ := json.Marshal(handle(ctx, &req))
			_ = m.Respond(b)
		}()
	})
	if err != nil {
		return nil, err
	}
	return func() { _ = sub.Unsubscribe() }, nil
}
