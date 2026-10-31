package api

import "time"

// unixNanoOrZero turns a Unix nanosecond timestamp into a time, with 0
// (unset) as the zero time.
func unixNanoOrZero(ns int64) time.Time {
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// StartTimeOrZero returns when the run started, or the zero time.
func (v *WorkflowExecutionInfo) StartTimeOrZero() time.Time {
	if v == nil {
		return time.Time{}
	}
	return unixNanoOrZero(v.StartTime)
}

// CloseTimeOrZero returns when the run closed, or the zero time while it
// is open.
func (v *WorkflowExecutionInfo) CloseTimeOrZero() time.Time {
	if v == nil {
		return time.Time{}
	}
	return unixNanoOrZero(v.CloseTime)
}
