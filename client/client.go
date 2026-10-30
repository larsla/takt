// Package client mirrors go.uber.org/cadence/client: starting and managing
// workflows from outside a worker.
package client

import (
	"errors"

	"github.com/larsla/takt/api"
	"github.com/larsla/takt/encoded"
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
	// CancelOption values are functional options for CancelWorkflow:
	// WithCancelReason.
	CancelOption = sdk.Option
	// GetWorkflowHistoryWithOptionsRequest is the request of
	// GetWorkflowHistoryWithOptions.
	GetWorkflowHistoryWithOptionsRequest = sdk.GetWorkflowHistoryWithOptionsRequest
	// DescribeWorkflowExecutionWithOptionsRequest is the request of
	// DescribeWorkflowExecutionWithOptions.
	DescribeWorkflowExecutionWithOptionsRequest = sdk.DescribeWorkflowExecutionWithOptionsRequest
	// QueryConsistencyLevel is accepted for compatibility: takt has one
	// cluster, so every level reads the same state.
	QueryConsistencyLevel = sdk.QueryConsistencyLevel
	// FeatureFlags is accepted for compatibility and ignored.
	FeatureFlags = sdk.FeatureFlags
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
	// QueryTypeQueryTypes returns the query types a workflow answers.
	QueryTypeQueryTypes = sdk.QueryTypeQueryTypes

	QueryConsistencyLevelUnspecified = sdk.QueryConsistencyLevelUnspecified
	QueryConsistencyLevelEventual    = sdk.QueryConsistencyLevelEventual
	QueryConsistencyLevelStrong      = sdk.QueryConsistencyLevelStrong
)

// WithCancelReason sets the reason CancelWorkflow records in the
// workflow's history.
func WithCancelReason(reason string) CancelOption { return sdk.WithCancelReason(reason) }

// NewValue wraps data encoded by the default data converter, for tests
// of code that takes an encoded.Value.
func NewValue(data []byte) encoded.Value { return sdk.NewValue(data) }

// NewValues wraps data encoded by the default data converter.
func NewValues(data []byte) encoded.Values { return sdk.NewValues(data) }

// IsWorkflowError reports whether err is an error a workflow or activity
// returned, as opposed to a failure to reach the service.
func IsWorkflowError(err error) bool {
	var (
		custom    *sdk.CustomError
		canceled  *sdk.CanceledError
		generic   *sdk.GenericError
		timeout   *sdk.TimeoutError
		terminate *sdk.TerminatedError
		panicked  *sdk.PanicError
		can       *sdk.ContinueAsNewError
		unknown   *sdk.UnknownExternalWorkflowExecutionError
	)
	return errors.As(err, &custom) || errors.As(err, &canceled) || errors.As(err, &generic) ||
		errors.As(err, &timeout) || errors.As(err, &terminate) || errors.As(err, &panicked) ||
		errors.As(err, &can) || errors.As(err, &unknown)
}

// NewClient creates a client for a domain. Build service with
// takt.NewServiceClient.
func NewClient(service api.Service, domain string, options *Options) Client {
	return sdk.NewClient(service, domain, options)
}

// NewDomainClient creates a client for domain management.
func NewDomainClient(service api.Service, options *Options) DomainClient {
	return sdk.NewDomainClient(service, options)
}
