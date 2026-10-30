package api

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestErrorRoundTrip(t *testing.T) {
	errs := []error{
		&BadRequestError{Message: "bad"},
		&InternalServiceError{Message: "oops"},
		&EntityNotExistsError{Message: "gone"},
		&WorkflowExecutionAlreadyCompletedError{Message: "done"},
		&WorkflowExecutionAlreadyStartedError{Message: "running", RunID: "r1", StartRequestID: "q"},
		&DomainAlreadyExistsError{Message: "dup"},
		&CancellationAlreadyRequestedError{Message: "again"},
		&QueryFailedError{Message: "q"},
		&LimitExceededError{Message: "slow down"},
		&ServiceBusyError{Message: "busy"},
	}
	for _, want := range errs {
		code, data := EncodeError(fmt.Errorf("wrapped: %w", want))
		got := DecodeError(code, "desc", data)
		if got.Error() != want.Error() {
			t.Errorf("%s: got %v, want %v", code, got, want)
		}
		if !strings.HasPrefix(got.Error(), code) {
			t.Errorf("message %q lacks code %s", got.Error(), code)
		}
	}
	var started *WorkflowExecutionAlreadyStartedError
	code, data := EncodeError(&WorkflowExecutionAlreadyStartedError{RunID: "r1"})
	if !errors.As(DecodeError(code, "", data), &started) || started.RunID != "r1" {
		t.Fatal("fields lost in round trip")
	}

	code, data = EncodeError(errors.New("plain"))
	var ise *InternalServiceError
	if code != "InternalServiceError" || !errors.As(DecodeError(code, "", data), &ise) || ise.Message != "plain" {
		t.Fatalf("plain error: %s %s", code, data)
	}
	if err := DecodeError("Unknown", "desc", nil); !errors.As(err, &ise) || ise.Message != "desc" {
		t.Fatalf("unknown code: %v", err)
	}
	if err := DecodeError("BadRequestError", "desc", []byte("{")); !errors.As(err, &ise) {
		t.Fatalf("bad body: %v", err)
	}
}

func TestTokens(t *testing.T) {
	for _, s := range []string{"simple", "with.dots and spaces", "*>", "ünïcode"} {
		tok := Token(s)
		if strings.ContainsAny(tok, ".*> ") {
			t.Errorf("token %q of %q is not subject safe", tok, s)
		}
		back, err := FromToken(tok)
		if err != nil || back != s {
			t.Errorf("round trip %q: %q %v", s, back, err)
		}
	}
	if _, err := FromToken("!!"); err == nil {
		t.Error("expected error for invalid token")
	}

	tt := TaskToken{Domain: "d", WorkflowID: "w", RunID: "r", ScheduleID: 5, Attempt: 2, ActivityID: "a"}
	got, err := DecodeTaskToken(tt.Encode())
	if err != nil || got != tt {
		t.Fatalf("task token: %+v %v", got, err)
	}
	for _, bad := range [][]byte{nil, []byte("{"), []byte(`{"d":"x"}`)} {
		if _, err := DecodeTaskToken(bad); err == nil {
			t.Errorf("token %q accepted", bad)
		}
	}
}

func TestSubjects(t *testing.T) {
	if got := TaskSubject("d", "tl", TaskListTypeActivity); !strings.HasSuffix(got, ".activity") {
		t.Error(got)
	}
	if got := TaskSubject("d", "tl", TaskListTypeDecision); !strings.HasSuffix(got, ".decision") {
		t.Error(got)
	}
	if got := TaskConsumerName("d", "tl", TaskListTypeDecision); strings.ContainsAny(got, ".*> ") {
		t.Error(got)
	}
	if HistorySubject("d", "w.x", "r") != "takt.hist."+Token("d")+"."+Token("w.x")+".r" {
		t.Error(HistorySubject("d", "w.x", "r"))
	}
	if QuerySubject("d", "tl") != "takt.query."+Token("d")+"."+Token("tl") {
		t.Error(QuerySubject("d", "tl"))
	}
	if APISubject(MethodStartWorkflowExecution) != "takt.api.StartWorkflowExecution" {
		t.Error(APISubject(MethodStartWorkflowExecution))
	}
}

func TestGetters(t *testing.T) {
	var nilEvent *HistoryEvent
	if nilEvent.GetEventId() != 0 || nilEvent.IsSetEventType() || nilEvent.GetWorkflowExecutionStartedEventAttributes() != nil {
		t.Fatal("nil receiver should read as zero values")
	}
	ev := &HistoryEvent{EventID: 3, EventType: EventTypeWorkflowExecutionStarted}
	if ev.GetEventId() != 3 || !ev.IsSetEventId() || ev.GetEventType().String() != "WorkflowExecutionStarted" {
		t.Fatalf("getters: %d %v", ev.GetEventId(), ev.GetEventType())
	}
	if *EventTypeTimerFired.Ptr() != EventTypeTimerFired {
		t.Fatal("Ptr")
	}
	exec := &WorkflowExecution{WorkflowID: "w"}
	if exec.GetWorkflowId() != "w" || exec.GetWorkflowID() != "w" || exec.IsSetRunId() {
		t.Fatal("ID getters")
	}
	child := &PendingChildExecutionInfo{WorkflowTypeName: "t"}
	if child.GetWorkflowTypName() != "t" {
		t.Fatal("Cadence's misspelled getter")
	}
}
