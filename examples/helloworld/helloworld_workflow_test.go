package main

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/larsla/takt/testsuite"
)

// UnitTestSuite is the cadence-samples unit test for this workflow; only the
// import paths changed.
type UnitTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *UnitTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.RegisterWorkflow(helloWorldWorkflow)
	s.env.RegisterActivity(helloWorldActivity)
}

func (s *UnitTestSuite) AfterTest(suiteName, testName string) {
	s.env.AssertExpectations(s.T())
}

func (s *UnitTestSuite) Test_Success() {
	s.env.ExecuteWorkflow(helloWorldWorkflow, "world")

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
	var result string
	s.NoError(s.env.GetWorkflowResult(&result))
	s.Equal("Hello world!", result)
}

func (s *UnitTestSuite) Test_ActivityMocked() {
	s.env.OnActivity(helloWorldActivity, mock.Anything, "world").Return("Hi world!", nil)
	s.env.ExecuteWorkflow(helloWorldWorkflow, "world")

	var result string
	s.NoError(s.env.GetWorkflowResult(&result))
	s.Equal("Hi world!", result)
}

func TestUnitTestSuite(t *testing.T) {
	suite.Run(t, new(UnitTestSuite))
}
