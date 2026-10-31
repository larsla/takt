package workflow

import "github.com/larsla/takt/internal/sdk"

type (
	// SessionInfo describes a session: SessionID identifies it and HostName
	// is the host of the worker running it.
	SessionInfo = sdk.SessionInfo
	// SessionOptions configures a session.
	SessionOptions = sdk.SessionOptions
)

// ErrSessionFailed is returned by an activity run in a session that has
// failed. The activity is not scheduled.
var ErrSessionFailed = sdk.ErrSessionFailed

// CreateSession creates a session and returns a context for its
// activities, which all run on the same worker. Workers host sessions when
// started with EnableSessionWorker.
//
//	so := &workflow.SessionOptions{ExecutionTimeout: time.Hour, CreationTimeout: time.Minute}
//	sessionCtx, err := workflow.CreateSession(ctx, so)
//	if err != nil {
//		return err
//	}
//	defer workflow.CompleteSession(sessionCtx)
//	err = workflow.ExecuteActivity(sessionCtx, download, url).Get(sessionCtx, &path)
func CreateSession(ctx Context, sessionOptions *SessionOptions) (Context, error) {
	return sdk.CreateSession(ctx, sessionOptions)
}

// RecreateSession creates a session on the worker of an earlier one, from
// the token SessionInfo.GetRecreateToken returned.
func RecreateSession(ctx Context, recreateToken []byte, sessionOptions *SessionOptions) (Context, error) {
	return sdk.RecreateSession(ctx, recreateToken, sessionOptions)
}

// CompleteSession ends the session in ctx and frees its slot on the worker.
func CompleteSession(ctx Context) { sdk.CompleteSession(ctx) }

// GetSessionInfo returns the most recent session in ctx, or nil.
func GetSessionInfo(ctx Context) *SessionInfo { return sdk.GetSessionInfo(ctx) }
