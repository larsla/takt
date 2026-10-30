// Package interceptors mirrors go.uber.org/cadence/interceptors: hooks
// around workflow code. Set them with worker.Options.
// WorkflowInterceptorChainFactories.
package interceptors

import "github.com/larsla/takt/internal/sdk"

type (
	// WorkflowInterceptorFactory creates one link of a workflow's
	// interceptor chain.
	WorkflowInterceptorFactory = sdk.WorkflowInterceptorFactory
	// WorkflowInterceptor intercepts the workflow function and the workflow
	// API calls it makes. Its code runs as workflow code and must be
	// deterministic.
	WorkflowInterceptor = sdk.WorkflowInterceptor
	// WorkflowInterceptorBase forwards every call to Next. Embed it and
	// override the calls to intercept.
	WorkflowInterceptorBase = sdk.WorkflowInterceptorBase
)
