package api

import (
	"encoding/json"
	"errors"
)

// Service errors. The names match Cadence's shared error types so code that
// type-switches on them keeps working after migration.

type BadRequestError struct {
	Message string `json:"message"`
}

type InternalServiceError struct {
	Message string `json:"message"`
}

type EntityNotExistsError struct {
	Message string `json:"message"`
}

type WorkflowExecutionAlreadyCompletedError struct {
	Message string `json:"message"`
}

type WorkflowExecutionAlreadyStartedError struct {
	Message        string `json:"message,omitempty"`
	StartRequestID string `json:"startRequestId,omitempty"`
	RunID          string `json:"runId,omitempty"`
}

type DomainAlreadyExistsError struct {
	Message string `json:"message"`
}

type CancellationAlreadyRequestedError struct {
	Message string `json:"message"`
}

type QueryFailedError struct {
	Message string `json:"message"`
}

type LimitExceededError struct {
	Message string `json:"message"`
}

type ServiceBusyError struct {
	Message string `json:"message"`
}

func (e *BadRequestError) Error() string      { return "BadRequestError: " + e.Message }
func (e *InternalServiceError) Error() string { return "InternalServiceError: " + e.Message }
func (e *EntityNotExistsError) Error() string { return "EntityNotExistsError: " + e.Message }
func (e *WorkflowExecutionAlreadyCompletedError) Error() string {
	return "WorkflowExecutionAlreadyCompletedError: " + e.Message
}
func (e *WorkflowExecutionAlreadyStartedError) Error() string {
	return "WorkflowExecutionAlreadyStartedError: " + e.Message
}
func (e *DomainAlreadyExistsError) Error() string { return "DomainAlreadyExistsError: " + e.Message }
func (e *CancellationAlreadyRequestedError) Error() string {
	return "CancellationAlreadyRequestedError: " + e.Message
}
func (e *QueryFailedError) Error() string   { return "QueryFailedError: " + e.Message }
func (e *LimitExceededError) Error() string { return "LimitExceededError: " + e.Message }
func (e *ServiceBusyError) Error() string   { return "ServiceBusyError: " + e.Message }

// codedError is implemented by every service error.
type codedError interface {
	error
	errorCode() string
}

func (*BadRequestError) errorCode() string      { return "BadRequestError" }
func (*InternalServiceError) errorCode() string { return "InternalServiceError" }
func (*EntityNotExistsError) errorCode() string { return "EntityNotExistsError" }
func (*WorkflowExecutionAlreadyCompletedError) errorCode() string {
	return "WorkflowExecutionAlreadyCompletedError"
}
func (*WorkflowExecutionAlreadyStartedError) errorCode() string {
	return "WorkflowExecutionAlreadyStartedError"
}
func (*DomainAlreadyExistsError) errorCode() string { return "DomainAlreadyExistsError" }
func (*CancellationAlreadyRequestedError) errorCode() string {
	return "CancellationAlreadyRequestedError"
}
func (*QueryFailedError) errorCode() string   { return "QueryFailedError" }
func (*LimitExceededError) errorCode() string { return "LimitExceededError" }
func (*ServiceBusyError) errorCode() string   { return "ServiceBusyError" }

var errorFactories = map[string]func() codedError{
	"BadRequestError":                        func() codedError { return &BadRequestError{} },
	"InternalServiceError":                   func() codedError { return &InternalServiceError{} },
	"EntityNotExistsError":                   func() codedError { return &EntityNotExistsError{} },
	"WorkflowExecutionAlreadyCompletedError": func() codedError { return &WorkflowExecutionAlreadyCompletedError{} },
	"WorkflowExecutionAlreadyStartedError":   func() codedError { return &WorkflowExecutionAlreadyStartedError{} },
	"DomainAlreadyExistsError":               func() codedError { return &DomainAlreadyExistsError{} },
	"CancellationAlreadyRequestedError":      func() codedError { return &CancellationAlreadyRequestedError{} },
	"QueryFailedError":                       func() codedError { return &QueryFailedError{} },
	"LimitExceededError":                     func() codedError { return &LimitExceededError{} },
	"ServiceBusyError":                       func() codedError { return &ServiceBusyError{} },
}

// EncodeError returns the wire code and JSON body for err. Errors that are
// not service errors become InternalServiceError.
func EncodeError(err error) (code string, data []byte) {
	var ce codedError
	if !errors.As(err, &ce) {
		ce = &InternalServiceError{Message: err.Error()}
	}
	data, _ = json.Marshal(ce)
	return ce.errorCode(), data
}

// DecodeError rebuilds a typed error from its wire code and JSON body.
func DecodeError(code, description string, data []byte) error {
	f, ok := errorFactories[code]
	if !ok {
		return &InternalServiceError{Message: description}
	}
	err := f()
	if len(data) == 0 || json.Unmarshal(data, err) != nil {
		return &InternalServiceError{Message: description}
	}
	return err
}
