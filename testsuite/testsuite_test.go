package testsuite_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/larsla/takt"
	"github.com/larsla/takt/activity"
	"github.com/larsla/takt/encoded"
	"github.com/larsla/takt/testsuite"
	"github.com/larsla/takt/worker"
	"github.com/larsla/takt/workflow"
)

type UnitTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func TestUnitTestSuite(t *testing.T) {
	suite.Run(t, new(UnitTestSuite))
}

var activityOptions = workflow.ActivityOptions{
	ScheduleToStartTimeout: time.Minute,
	StartToCloseTimeout:    time.Minute,
	HeartbeatTimeout:       20 * time.Second,
}

func greetActivity(_ context.Context, name string) (string, error) { return "Hello " + name + "!", nil }

func greetingWorkflow(ctx workflow.Context, name string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, activityOptions)
	var s string
	err := workflow.ExecuteActivity(ctx, greetActivity, name).Get(ctx, &s)
	return s, err
}

func (s *UnitTestSuite) Test_RealActivity() {
	env := s.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(greetingWorkflow)
	env.RegisterActivity(greetActivity)
	var started, completed int
	env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, args encoded.Values) {
		started++
		var name string
		s.NoError(args.Get(&name))
		s.Equal("takt", name)
	})
	env.SetOnActivityCompletedListener(func(info *activity.Info, result encoded.Value, err error) {
		completed++
		s.NoError(err)
	})
	env.ExecuteWorkflow(greetingWorkflow, "takt")
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
	var result string
	s.NoError(env.GetWorkflowResult(&result))
	s.Equal("Hello takt!", result)
	s.Equal(1, started)
	s.Equal(1, completed)
}

func (s *UnitTestSuite) Test_MockedActivity() {
	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(greetActivity, mock.Anything, "takt").Return("mocked", nil).Once()
	env.ExecuteWorkflow(greetingWorkflow, "takt")
	var result string
	s.NoError(env.GetWorkflowResult(&result))
	s.Equal("mocked", result)
	env.AssertExpectations(s.T())
}

func (s *UnitTestSuite) Test_MockedActivityFunction() {
	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(greetActivity, mock.Anything, mock.Anything).Return(func(ctx context.Context, name string) (string, error) {
		return "fn:" + name, nil
	})
	env.ExecuteWorkflow(greetingWorkflow, "x")
	var result string
	s.NoError(env.GetWorkflowResult(&result))
	s.Equal("fn:x", result)
}

func (s *UnitTestSuite) Test_MockedActivityError() {
	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(greetActivity, mock.Anything, mock.Anything).Return("", takt.NewCustomError("bad", "detail"))
	env.ExecuteWorkflow(greetingWorkflow, "x")
	s.True(env.IsWorkflowCompleted())
	var custom *takt.CustomError
	s.True(errors.As(env.GetWorkflowError(), &custom))
	s.Equal("bad", custom.Reason())
	var d string
	s.NoError(custom.Details(&d))
	s.Equal("detail", d)
}

// timeoutRaceWorkflow races an activity against a timer.
func timeoutRaceWorkflow(ctx workflow.Context) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, activityOptions)
	f := workflow.ExecuteActivity(ctx, greetActivity, "slow")
	timer := workflow.NewTimer(ctx, 10*time.Minute)
	winner := ""
	sel := workflow.NewSelector(ctx)
	sel.AddFuture(f, func(workflow.Future) { winner = "activity" })
	sel.AddFuture(timer, func(workflow.Future) { winner = "timer" })
	sel.Select(ctx)
	return winner, nil
}

func (s *UnitTestSuite) Test_MockDelayAndTimer() {
	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(greetActivity, mock.Anything, mock.Anything).Return("late", nil).After(time.Hour)
	var timers, fired int
	env.SetOnTimerScheduledListener(func(string, time.Duration) { timers++ })
	env.SetOnTimerFiredListener(func(string) { fired++ })
	start := env.Now()
	env.ExecuteWorkflow(timeoutRaceWorkflow)
	var winner string
	s.NoError(env.GetWorkflowResult(&winner))
	s.Equal("timer", winner)
	s.Equal(1, timers)
	s.Equal(1, fired)
	s.GreaterOrEqual(env.Now().Sub(start), 10*time.Minute)

	env = s.NewTestWorkflowEnvironment()
	env.OnActivity(greetActivity, mock.Anything, mock.Anything).Return("quick", nil).After(time.Minute)
	env.ExecuteWorkflow(timeoutRaceWorkflow)
	s.NoError(env.GetWorkflowResult(&winner))
	s.Equal("activity", winner)
}

func sleepyWorkflow(ctx workflow.Context) (time.Duration, error) {
	start := workflow.Now(ctx)
	if err := workflow.Sleep(ctx, 24*time.Hour); err != nil {
		return 0, err
	}
	return workflow.Now(ctx).Sub(start), nil
}

func (s *UnitTestSuite) Test_MockClock() {
	env := s.NewTestWorkflowEnvironment()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	env.SetStartTime(start)
	began := time.Now()
	env.ExecuteWorkflow(sleepyWorkflow)
	s.Less(time.Since(began), 2*time.Second)
	var slept time.Duration
	s.NoError(env.GetWorkflowResult(&slept))
	s.Equal(24*time.Hour, slept)
	s.Equal(start.Add(24*time.Hour), env.Now())
}

func signalQueryWorkflow(ctx workflow.Context) (int, error) {
	total := 0
	if err := workflow.SetQueryHandler(ctx, "total", func() (int, error) { return total, nil }); err != nil {
		return 0, err
	}
	ch := workflow.GetSignalChannel(ctx, "add")
	for total < 10 {
		var n int
		ch.Receive(ctx, &n)
		total += n
	}
	return total, nil
}

func (s *UnitTestSuite) Test_SignalsAndQueries() {
	env := s.NewTestWorkflowEnvironment()
	for i, n := range []int{3, 3, 4} {
		env.RegisterDelayedCallback(func() {
			if i == 2 {
				// The workflow has processed the first two signals by now.
				v, err := env.QueryWorkflow("total")
				s.NoError(err)
				var total int
				s.NoError(v.Get(&total))
				s.Equal(6, total)
			}
			env.SignalWorkflow("add", n)
		}, time.Duration(i+1)*time.Minute)
	}
	env.ExecuteWorkflow(signalQueryWorkflow)
	var total int
	s.NoError(env.GetWorkflowResult(&total))
	s.Equal(10, total)
	v, err := env.QueryWorkflow("total")
	s.NoError(err)
	s.NoError(v.Get(&total))
	s.Equal(10, total)
	_, err = env.QueryWorkflow("unknown")
	s.Error(err)
}

func (s *UnitTestSuite) Test_SignalBeforeStart() {
	env := s.NewTestWorkflowEnvironment()
	env.SignalWorkflow("add", 10)
	env.ExecuteWorkflow(signalQueryWorkflow)
	var total int
	s.NoError(env.GetWorkflowResult(&total))
	s.Equal(10, total)
}

func cancelableWorkflow(ctx workflow.Context) error {
	ctx = workflow.WithActivityOptions(ctx, activityOptions)
	err := workflow.Sleep(ctx, time.Hour)
	if !takt.IsCanceledError(err) {
		return errors.New("expected cancellation")
	}
	dctx, _ := workflow.NewDisconnectedContext(ctx)
	if err := workflow.ExecuteActivity(dctx, greetActivity, "cleanup").Get(dctx, nil); err != nil {
		return err
	}
	return err
}

func (s *UnitTestSuite) Test_CancelWorkflow() {
	env := s.NewTestWorkflowEnvironment()
	env.RegisterActivity(greetActivity)
	var canceledTimers int
	env.SetOnTimerCancelledListener(func(string) { canceledTimers++ })
	env.RegisterDelayedCallback(env.CancelWorkflow, time.Minute)
	env.ExecuteWorkflow(cancelableWorkflow)
	s.True(env.IsWorkflowCompleted())
	s.True(takt.IsCanceledError(env.GetWorkflowError()))
	s.Equal(1, canceledTimers)
}

func childWorkflow(ctx workflow.Context, n int) (int, error) {
	if err := workflow.Sleep(ctx, time.Minute); err != nil {
		return 0, err
	}
	return n * 10, nil
}

func parentWorkflow(ctx workflow.Context) (int, error) {
	ctx = workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: "child-1", ExecutionStartToCloseTimeout: time.Hour,
	})
	var r int
	err := workflow.ExecuteChildWorkflow(ctx, childWorkflow, 4).Get(ctx, &r)
	return r, err
}

func (s *UnitTestSuite) Test_ChildWorkflow() {
	env := s.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(childWorkflow)
	var childStarted, childDone int
	env.SetOnChildWorkflowStartedListener(func(info *workflow.Info, _ workflow.Context, args encoded.Values) {
		childStarted++
		s.Equal("child-1", info.WorkflowExecution.ID)
		s.Equal("default-test-workflow-id", info.ParentWorkflowExecution.ID)
	})
	env.SetOnChildWorkflowCompletedListener(func(*workflow.Info, encoded.Value, error) { childDone++ })
	env.ExecuteWorkflow(parentWorkflow)
	var r int
	s.NoError(env.GetWorkflowResult(&r))
	s.Equal(40, r)
	s.Equal(1, childStarted)
	s.Equal(1, childDone)

	env = s.NewTestWorkflowEnvironment()
	env.OnWorkflow(childWorkflow, mock.Anything, 4).Return(7, nil)
	env.ExecuteWorkflow(parentWorkflow)
	s.NoError(env.GetWorkflowResult(&r))
	s.Equal(7, r)
}

var flaky atomic.Int32

func flakyActivity(context.Context) (string, error) {
	if flaky.Add(1) < 3 {
		return "", errors.New("transient")
	}
	return "ok", nil
}

func retryingWorkflow(ctx workflow.Context) (string, error) {
	ao := activityOptions
	ao.RetryPolicy = &takt.RetryPolicy{InitialInterval: time.Minute, MaximumAttempts: 5}
	ctx = workflow.WithActivityOptions(ctx, ao)
	var s string
	err := workflow.ExecuteActivity(ctx, flakyActivity).Get(ctx, &s)
	return s, err
}

func (s *UnitTestSuite) Test_ActivityRetry() {
	flaky.Store(0)
	env := s.NewTestWorkflowEnvironment()
	env.RegisterActivity(flakyActivity)
	start := env.Now()
	env.ExecuteWorkflow(retryingWorkflow)
	var r string
	s.NoError(env.GetWorkflowResult(&r))
	s.Equal("ok", r)
	s.Equal(int32(3), flaky.Load())
	s.Equal(3*time.Minute, env.Now().Sub(start), "backoff of 1m then 2m on the mock clock")
}

func (s *UnitTestSuite) Test_WorkflowTimeout() {
	env := s.NewTestWorkflowEnvironment()
	env.SetWorkflowTimeout(time.Hour)
	env.ExecuteWorkflow(sleepyWorkflow)
	var te *takt.TimeoutError
	s.True(errors.As(env.GetWorkflowError(), &te))
}

func blockedWorkflow(ctx workflow.Context) error {
	workflow.GetSignalChannel(ctx, "never").Receive(ctx, nil)
	return nil
}

func (s *UnitTestSuite) Test_TestTimeout() {
	env := s.NewTestWorkflowEnvironment()
	env.SetTestTimeout(100 * time.Millisecond)
	func() {
		defer func() {
			r := recover()
			s.NotNil(r)
			s.Contains(r.(string), "test timeout")
			s.Contains(r.(string), "never.Receive")
		}()
		env.ExecuteWorkflow(blockedWorkflow)
	}()
	s.False(env.IsWorkflowCompleted())
}

func upper(_ context.Context, s string) (string, error) { return strings.ToUpper(s), nil }

func versionedWorkflow(ctx workflow.Context) (string, error) {
	v := workflow.GetVersion(ctx, "change", workflow.DefaultVersion, 1)
	var se int
	if err := workflow.SideEffect(ctx, func(workflow.Context) interface{} { return 42 }).Get(&se); err != nil {
		return "", err
	}
	var m string
	if err := workflow.MutableSideEffect(ctx, "m", func(workflow.Context) interface{} { return "same" },
		func(a, b interface{}) bool { return a == b }).Get(&m); err != nil {
		return "", err
	}
	ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{ScheduleToCloseTimeout: time.Second})
	var u string
	if err := workflow.ExecuteLocalActivity(ctx, upper, "local").Get(ctx, &u); err != nil {
		return "", err
	}
	if err := workflow.UpsertSearchAttributes(ctx, map[string]interface{}{"k": "v"}); err != nil {
		return "", err
	}
	if workflow.IsReplaying(ctx) {
		return "", errors.New("never replaying in tests")
	}
	return strings.Join([]string{
		map[workflow.Version]string{workflow.DefaultVersion: "old", 1: "new"}[v],
		u, m,
	}, "/") + "/" + string(rune('0'+se/10)) + string(rune('0'+se%10)), nil
}

func (s *UnitTestSuite) Test_VersionsSideEffectsLocalActivities() {
	env := s.NewTestWorkflowEnvironment()
	var laStarted, laDone int
	env.SetOnLocalActivityStartedListener(func(*activity.Info, context.Context, []interface{}) { laStarted++ })
	env.SetOnLocalActivityCompletedListener(func(*activity.Info, encoded.Value, error) { laDone++ })
	env.ExecuteWorkflow(versionedWorkflow)
	var r string
	s.NoError(env.GetWorkflowResult(&r))
	s.Equal("new/LOCAL/same/42", r)
	s.Equal(1, laStarted)
	s.Equal(1, laDone)

	env = s.NewTestWorkflowEnvironment()
	env.OnGetVersion("change", workflow.DefaultVersion, 1).Return(workflow.DefaultVersion)
	env.OnActivity(upper, mock.Anything, "local").Return("MOCKED", nil)
	env.OnUpsertSearchAttributes(mock.Anything).Return(nil)
	env.ExecuteWorkflow(versionedWorkflow)
	s.NoError(env.GetWorkflowResult(&r))
	s.Equal("old/MOCKED/same/42", r)
	env.AssertExpectations(s.T())
}

func signalerWorkflow(ctx workflow.Context) error {
	if err := workflow.SignalExternalWorkflow(ctx, "other", "", "hi", "there").Get(ctx, nil); err != nil {
		return err
	}
	return workflow.RequestCancelExternalWorkflow(ctx, "other", "").Get(ctx, nil)
}

func (s *UnitTestSuite) Test_ExternalSignalAndCancel() {
	env := s.NewTestWorkflowEnvironment()
	env.OnSignalExternalWorkflow("default-test-domain", "other", "", "hi", "there").Return(nil).Once()
	env.OnRequestCancelExternalWorkflow("default-test-domain", "other", "").Return(nil).Once()
	env.ExecuteWorkflow(signalerWorkflow)
	s.NoError(env.GetWorkflowError())
	env.AssertExpectations(s.T())

	// Without mocks the target must be a workflow in the environment. The
	// error reaches GetWorkflowError through its wire form, as a GenericError.
	env = s.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(signalerWorkflow)
	s.EqualError(env.GetWorkflowError(), "UnknownExternalWorkflowExecution")
}

func continuingWorkflow(ctx workflow.Context, n int) error {
	return workflow.NewContinueAsNewError(ctx, continuingWorkflow, n+1)
}

func panicWorkflow(workflow.Context) error { panic("kaboom") }

func failingWorkflow(workflow.Context) error { return errors.New("plain") }

func (s *UnitTestSuite) Test_ErrorKinds() {
	env := s.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(continuingWorkflow, 1)
	var can *workflow.ContinueAsNewError
	s.True(errors.As(env.GetWorkflowError(), &can))

	env = s.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(panicWorkflow)
	var pe *takt.PanicError
	s.True(errors.As(env.GetWorkflowError(), &pe))
	s.Equal("kaboom", pe.Error())

	env = s.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(failingWorkflow)
	var ge *takt.GenericError
	s.True(errors.As(env.GetWorkflowError(), &ge))
	s.Equal("plain", ge.Error())
}

func asyncActivity(ctx context.Context) (string, error) {
	tokens <- activity.GetInfo(ctx).TaskToken
	return "", activity.ErrResultPending
}

var tokens = make(chan []byte, 1)

func asyncWorkflow(ctx workflow.Context) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, activityOptions)
	var r string
	err := workflow.ExecuteActivity(ctx, asyncActivity).Get(ctx, &r)
	return r, err
}

func (s *UnitTestSuite) Test_AsyncActivity() {
	env := s.NewTestWorkflowEnvironment()
	env.RegisterActivity(asyncActivity)
	go func() {
		token := <-tokens
		s.NoError(env.CompleteActivity(token, "async result", nil))
	}()
	env.ExecuteWorkflow(asyncWorkflow)
	var r string
	s.NoError(env.GetWorkflowResult(&r))
	s.Equal("async result", r)
	s.Error(env.CompleteActivity([]byte("unknown"), nil, nil))
}

func heartbeatActivity(ctx context.Context) (int, error) {
	start := 0
	if activity.HasHeartbeatDetails(ctx) {
		if err := activity.GetHeartbeatDetails(ctx, &start); err != nil {
			return 0, err
		}
	}
	for i := start; i < start+3; i++ {
		activity.RecordHeartbeat(ctx, i)
	}
	info := activity.GetInfo(ctx)
	if info.ActivityType.Name == "" || activity.GetLogger(ctx) == nil || activity.GetMetricsScope(ctx) == nil ||
		activity.GetWorkerStopChannel(ctx) == nil {
		return 0, errors.New("missing activity context")
	}
	return start + 3, nil
}

func failActivity(context.Context) error { return takt.NewCustomError("nope", 1) }

func (s *UnitTestSuite) Test_ActivityEnvironment() {
	env := s.NewTestActivityEnvironment()
	env.RegisterActivity(heartbeatActivity)
	var beats []int
	env.SetOnActivityHeartbeatListener(func(_ *activity.Info, details encoded.Values) {
		var i int
		s.NoError(details.Get(&i))
		beats = append(beats, i)
	})
	env.SetHeartbeatDetails(10)
	v, err := env.ExecuteActivity(heartbeatActivity)
	s.NoError(err)
	var r int
	s.NoError(v.Get(&r))
	s.Equal(13, r)
	s.Equal([]int{10, 11, 12}, beats)

	_, err = env.ExecuteActivity(failActivity)
	var custom *takt.CustomError
	s.True(errors.As(err, &custom))
	s.Equal("nope", custom.Reason())

	v, err = env.ExecuteLocalActivity(upper, "x")
	s.NoError(err)
	var u string
	s.NoError(v.Get(&u))
	s.Equal("X", u)

	_, err = env.ExecuteActivity("unregistered")
	s.Error(err)
	_, err = env.ExecuteActivity(upper, 1)
	s.Error(err)
}

func (s *UnitTestSuite) Test_WorkerOptionsAndTaskLists() {
	env := s.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{})
	env.RegisterActivity(greetActivity)
	env.SetActivityTaskList("elsewhere", greetActivity)
	env.ExecuteWorkflow(greetingWorkflow, "x")
	var te *takt.TimeoutError
	s.True(errors.As(env.GetWorkflowError(), &te), "activity on a task list nobody serves times out")

	env = s.NewTestWorkflowEnvironment()
	env.SetLastCompletionResult("previous")
	s.NoError(env.SetMemoOnStart(map[string]interface{}{"m": 1}))
	s.NoError(env.SetSearchAttributesOnStart(map[string]interface{}{"s": "v"}))
	env.ExecuteWorkflow(func(ctx workflow.Context) (string, error) {
		var prev string
		if !workflow.HasLastCompletionResult(ctx) {
			return "", errors.New("no last result")
		}
		if err := workflow.GetLastCompletionResult(ctx, &prev); err != nil {
			return "", err
		}
		info := workflow.GetInfo(ctx)
		if info.Memo == nil || info.SearchAttributes == nil {
			return "", errors.New("memo or search attributes missing")
		}
		return prev, nil
	})
	var prev string
	s.NoError(env.GetWorkflowResult(&prev))
	s.Equal("previous", prev)
}

func countingCronWorkflow(ctx workflow.Context) (int, error) {
	n := 0
	if workflow.HasLastCompletionResult(ctx) {
		if err := workflow.GetLastCompletionResult(ctx, &n); err != nil {
			return 0, err
		}
	}
	return n + 1, nil
}

func (s *UnitTestSuite) TestCronIterations() {
	env := s.NewTestWorkflowEnvironment()
	env.SetWorkflowCronSchedule("* * * * *").SetWorkflowCronMaxIterations(3)
	env.ExecuteWorkflow(countingCronWorkflow)
	var n int
	s.NoError(env.GetWorkflowResult(&n))
	s.Equal(4, n) // the first run and three more
}

func bufferedSignalsWorkflow(ctx workflow.Context) ([]string, error) {
	ch := workflow.GetSignalChannel(ctx, "s")
	var got []string
	for len(got) < 3 {
		var v string
		ch.Receive(ctx, &v)
		got = append(got, v)
		// Everything sent together is already buffered.
		for ch.ReceiveAsync(&v) {
			got = append(got, v+"*")
		}
	}
	return got, nil
}

func (s *UnitTestSuite) TestSignalWorkflowSkippingDecision() {
	env := s.NewTestWorkflowEnvironment()
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflowSkippingDecision("s", "a")
		env.SignalWorkflowSkippingDecision("s", "b")
	}, time.Second)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow("s", "c") }, 2*time.Second)
	env.ExecuteWorkflow(bufferedSignalsWorkflow)
	var got []string
	s.NoError(env.GetWorkflowResult(&got))
	s.Equal([]string{"a", "b*", "c*"}, got)
}

func childFailsToStartWorkflow(ctx workflow.Context) (string, error) {
	ctx = workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{ExecutionStartToCloseTimeout: time.Minute})
	f := workflow.ExecuteChildWorkflow(ctx, countingCronWorkflow)
	if err := f.GetChildWorkflowExecution().Get(ctx, nil); err != nil {
		return "start failed", nil
	}
	return "started", nil
}

func (s *UnitTestSuite) TestMockExtras() {
	env := s.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(countingCronWorkflow)
	env.OnWorkflow(countingCronWorkflow, mock.Anything).Return(0, testsuite.ErrMockStartChildWorkflowFailed)
	var ran atomic.Bool
	env.OnActivity(greetActivity, mock.Anything, mock.Anything).Return("x", nil).Maybe().Run(func(mock.Arguments) { ran.Store(true) })
	env.ExecuteWorkflow(childFailsToStartWorkflow)
	var got string
	s.NoError(env.GetWorkflowResult(&got))
	s.Equal("start failed", got)
	s.False(ran.Load())
	env.AssertExpectations(s.T())
	s.NotEmpty(env.GetRegisteredWorkflows())
	s.NotEmpty(env.GetRegisteredActivities())

	env = s.NewTestWorkflowEnvironment()
	env.OnActivity(greetActivity, mock.Anything, mock.Anything).Return("mocked", nil).Run(func(args mock.Arguments) {
		ran.Store(args.String(1) == "ann")
	})
	env.ExecuteWorkflow(greetingWorkflow, "ann")
	s.NoError(env.GetWorkflowResult(&got))
	s.Equal("mocked", got)
	s.True(ran.Load())
}
