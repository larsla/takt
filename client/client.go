// Package client mirrors go.uber.org/cadence/client: starting and managing
// workflows from outside a worker.
package client

import (
	"github.com/larsla/takt/api"
	"github.com/larsla/takt/internal/sdk"
)

type (
	// Client starts and manages workflows.
	Client = sdk.Client
	// DomainClient manages domains.
	DomainClient = sdk.DomainClient
	// Options configures a Client.
	Options = sdk.ClientOptions
	// StartWorkflowOptions configures a workflow start.
	StartWorkflowOptions = sdk.StartWorkflowOptions
	// WorkflowRun is a started workflow.
	WorkflowRun = sdk.WorkflowRun
	// HistoryEventIterator iterates over a workflow history.
	HistoryEventIterator = sdk.HistoryEventIterator
	// QueryWorkflowWithOptionsRequest is the request of QueryWorkflowWithOptions.
	QueryWorkflowWithOptionsRequest = sdk.QueryWorkflowWithOptionsRequest
	// QueryWorkflowWithOptionsResponse is the response of QueryWorkflowWithOptions.
	QueryWorkflowWithOptionsResponse = sdk.QueryWorkflowWithOptionsResponse
	// WorkflowIDReusePolicy says when a workflow ID may be reused.
	WorkflowIDReusePolicy = sdk.WorkflowIDReusePolicy
	// ParentClosePolicy says what happens to a child when its parent closes.
	ParentClosePolicy = sdk.ParentClosePolicy
)

const (
	WorkflowIDReusePolicyAllowDuplicateFailedOnly = sdk.WorkflowIDReusePolicyAllowDuplicateFailedOnly
	WorkflowIDReusePolicyAllowDuplicate           = sdk.WorkflowIDReusePolicyAllowDuplicate
	WorkflowIDReusePolicyRejectDuplicate          = sdk.WorkflowIDReusePolicyRejectDuplicate
	WorkflowIDReusePolicyTerminateIfRunning       = sdk.WorkflowIDReusePolicyTerminateIfRunning

	ParentClosePolicyTerminate     = sdk.ParentClosePolicyTerminate
	ParentClosePolicyRequestCancel = sdk.ParentClosePolicyRequestCancel
	ParentClosePolicyAbandon       = sdk.ParentClosePolicyAbandon

	// QueryTypeStackTrace returns the stacks of a workflow's goroutines.
	QueryTypeStackTrace = sdk.QueryTypeStackTrace
)

// NewClient creates a client for a domain. Build service with
// takt.NewServiceClient.
func NewClient(service api.Service, domain string, options *Options) Client {
	return sdk.NewClient(service, domain, options)
}

// NewDomainClient creates a client for domain management.
func NewDomainClient(service api.Service, options *Options) DomainClient {
	return sdk.NewDomainClient(service, options)
}
