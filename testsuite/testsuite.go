// Package testsuite mirrors go.uber.org/cadence/testsuite: unit testing
// workflows and activities without a server.
//
// Workflows run against a mock clock that jumps to the next timer when
// nothing is running, so a workflow that sleeps for a day finishes at once.
// Activities run their real code unless mocked with OnActivity.
//
//	type UnitTestSuite struct {
//		suite.Suite
//		testsuite.WorkflowTestSuite
//	}
//
//	func (s *UnitTestSuite) Test_Workflow() {
//		env := s.NewTestWorkflowEnvironment()
//		env.OnActivity(MyActivity, mock.Anything).Return("mocked", nil)
//		env.ExecuteWorkflow(MyWorkflow, "input")
//		s.True(env.IsWorkflowCompleted())
//		s.NoError(env.GetWorkflowError())
//	}
package testsuite

import "github.com/larsla/takt/internal/sdk"

type (
	// WorkflowTestSuite creates test environments. Embed it in a testify suite.
	WorkflowTestSuite = sdk.WorkflowTestSuite
	// TestWorkflowEnvironment runs one workflow in memory.
	TestWorkflowEnvironment = sdk.TestWorkflowEnvironment
	// TestActivityEnvironment runs activities directly.
	TestActivityEnvironment = sdk.TestActivityEnvironment
	// MockCallWrapper configures a mocked call.
	MockCallWrapper = sdk.MockCallWrapper
)

// ErrMockStartChildWorkflowFailed, returned by a mocked child workflow,
// makes the child fail to start.
var ErrMockStartChildWorkflowFailed = sdk.ErrMockStartChildWorkflowFailed
