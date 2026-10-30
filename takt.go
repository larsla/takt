// Package takt is a workflow engine on NATS with a Go SDK that mirrors
// Cadence's (go.uber.org/cadence). This root package mirrors
// go.uber.org/cadence: retry policies, error types and the service client.
//
// Migrating from Cadence means changing import paths and replacing the yarpc
// service client with NewServiceClient:
//
//	nc, _ := nats.Connect(nats.DefaultURL)
//	service, _ := takt.NewServiceClient(nc, nil)
//	c := client.NewClient(service, "my-domain", nil)
//	w := worker.New(service, "my-domain", "my-task-list", worker.Options{})
package takt

import (
	"time"

	"github.com/nats-io/nats.go"

	"github.com/larsla/takt/api"
	"github.com/larsla/takt/internal/sdk"
	"github.com/larsla/takt/internal/transport"
)

type (
	// RetryPolicy says how to retry failed activities and workflows.
	RetryPolicy = sdk.RetryPolicy
	// CustomError carries an application-defined reason and details.
	CustomError = sdk.CustomError
	// CustomErrorOption is passed to NewCustomError among the details:
	// WithFailureCategory, WithNextRetryDelay.
	CustomErrorOption = sdk.CustomErrorOption
	// NonDeterministicError reports workflow code that no longer matches
	// its history.
	NonDeterministicError = sdk.NonDeterministicError
	// CanceledError is returned when an operation was canceled.
	CanceledError = sdk.CanceledError
	// TimeoutError is returned when an activity or child workflow timed out.
	TimeoutError = sdk.TimeoutError
	// GenericError wraps a plain error returned by remote code.
	GenericError = sdk.GenericError
	// TerminatedError is returned when a child workflow was terminated.
	TerminatedError = sdk.TerminatedError
	// PanicError holds a panic in workflow or activity code.
	PanicError = sdk.PanicError
	// UnknownExternalWorkflowExecutionError is returned when signaling or
	// canceling a workflow that does not exist.
	UnknownExternalWorkflowExecutionError = sdk.UnknownExternalWorkflowExecutionError

	// ServiceClient is the workflow service clients and workers talk to. It
	// takes the place of Cadence's workflowserviceclient.Interface.
	ServiceClient = api.Service
)

// LibraryVersion is the version of go.uber.org/cadence whose API this SDK
// follows.
const LibraryVersion = "1.4.0"

// ErrNoData is returned when decoding a value that is not there.
var ErrNoData = sdk.ErrNoData

// NewCustomError creates an error with a reason and details.
func NewCustomError(reason string, details ...interface{}) *CustomError {
	return sdk.NewCustomError(reason, details...)
}

// NewCanceledError creates a CanceledError.
func NewCanceledError(details ...interface{}) *CanceledError {
	return sdk.NewCanceledError(details...)
}

// NewTimeoutError creates a TimeoutError.
func NewTimeoutError(timeoutType api.TimeoutType, details ...interface{}) *TimeoutError {
	return sdk.NewTimeoutError(timeoutType, details...)
}

// NewHeartbeatTimeoutError creates a heartbeat TimeoutError.
func NewHeartbeatTimeoutError(details ...interface{}) *TimeoutError {
	return sdk.NewHeartbeatTimeoutError(details...)
}

// WithFailureCategory sets the failure category of an activity error.
// FailureCategoryFatal stops the server from retrying the activity.
func WithFailureCategory(c api.FailureCategory) CustomErrorOption { return sdk.WithFailureCategory(c) }

// WithNextRetryDelay replaces the retry policy's backoff before the next
// attempt of the activity that returns the error.
func WithNextRetryDelay(d time.Duration) CustomErrorOption { return sdk.WithNextRetryDelay(d) }

// IsCanceledError reports whether err is a CanceledError.
func IsCanceledError(err error) bool { return sdk.IsCanceledError(err) }

// IsCustomError reports whether err is a CustomError.
func IsCustomError(err error) bool { return sdk.IsCustomError(err) }

// IsGenericError reports whether err is a GenericError.
func IsGenericError(err error) bool { return sdk.IsGenericError(err) }

// IsTimeoutError reports whether err is a TimeoutError.
func IsTimeoutError(err error) bool { return sdk.IsTimeoutError(err) }

// IsTerminatedError reports whether err is a TerminatedError.
func IsTerminatedError(err error) bool { return sdk.IsTerminatedError(err) }

// IsPanicError reports whether err is a PanicError.
func IsPanicError(err error) bool { return sdk.IsPanicError(err) }

// IsWorkflowExecutionAlreadyStartedError reports whether err says a workflow
// with the same ID is already running.
func IsWorkflowExecutionAlreadyStartedError(err error) bool {
	return sdk.IsWorkflowExecutionAlreadyStartedError(err)
}

// ServiceOptions tunes the NATS service client.
type ServiceOptions struct {
	// RequestTimeout applies to calls whose context has no deadline.
	// Defaults to 10s.
	RequestTimeout time.Duration
	// PollTimeout is how long one task poll waits. Defaults to 30s.
	PollTimeout time.Duration
}

// NewServiceClient returns a ServiceClient that talks to takt over nc.
func NewServiceClient(nc *nats.Conn, options *ServiceOptions) (ServiceClient, error) {
	var o ServiceOptions
	if options != nil {
		o = *options
	}
	return transport.New(nc, transport.Options{RequestTimeout: o.RequestTimeout, PollTimeout: o.PollTimeout})
}

// Dial connects to NATS at url and returns a ServiceClient. The connection
// reconnects forever; close it with the returned function.
func Dial(url string, options *ServiceOptions, natsOptions ...nats.Option) (ServiceClient, func(), error) {
	opts := append([]nats.Option{nats.Name("takt-client"), nats.MaxReconnects(-1)}, natsOptions...)
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, nil, err
	}
	service, err := NewServiceClient(nc, options)
	if err != nil {
		nc.Close()
		return nil, nil, err
	}
	return service, nc.Close, nil
}
