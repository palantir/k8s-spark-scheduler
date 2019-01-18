// Package werror defines an error type that can store safe and unsafe parameters and can wrap other errors.
package werror

import (
	"fmt"

	"github.com/palantir/witchcraft-go-params"
)

// Error returns a new error with the provided message and parameters.
//
// The message should not contain any formatted parameters -- instead, use the SafeParam* or UnsafeParam* functions
// to create error parameters.
//
// Example:
//
//	password, ok := config["password"]
//	if !ok {
//		return werror.Error("configuration is missing password")
//	}
//
func Error(msg string, params ...Param) error {
	return newWerror(msg, nil, params...)
}

// Wrap returns a new error with the provided message and stores the provided error as its cause.
//
// The message should not contain any formatted parameters -- instead use the SafeParam* or UnsafeParam* functions
// to create error parameters.
//
// Example:
//
//	users, err := getUser(userID)
//	if err != nil {
//		return werror.Wrap(err, "failed to get user", werror.SafeParam("userId", userID))
//	}
//
func Wrap(err error, msg string, params ...Param) error {
	if err == nil {
		return nil
	}
	return newWerror(msg, err, params...)
}

// Convert err to werror error.
//
// If err is not a werror-based error, then a new werror error is created using the message from err.
// Otherwise, returns unchanged err.
//
// Example:
//
//	file, err := os.Open("file.txt")
//	if err != nil {
//		return werror.Convert(err)
//	}
//
func Convert(err error) error {
	if err == nil {
		return err
	}
	switch err.(type) {
	case *werror:
		return err
	default:
		return Error(err.Error())
	}
}

// RootCause returns the initial cause of an error.
//
// Traverses the cause hierarchy until it reaches an error which has no cause and returns that error.
func RootCause(err error) error {
	for {
		causer, ok := err.(causer)
		if !ok {
			return err
		}
		cause := causer.Cause()
		if cause == nil {
			return err
		}
		err = cause
	}
}

// ParamsFromError returns all of the safe and unsafe parameters stored in the provided error.
//
// If the error implements the causer interface, then the returned parameters will include all of the parameters stored
// in the causes as well.
//
// All of the keys and parameters of the map are flattened.
//
// Parameters are added from the outermost error to the innermost error. This means that, if multiple errors declare
// different values for the same keys, the values for the most specific (deepest) error will be the ones in the returned
// maps.
func ParamsFromError(err error) (safeParams map[string]interface{}, unsafeParams map[string]interface{}) {
	safeParams = make(map[string]interface{})
	unsafeParams = make(map[string]interface{})
	visitErrorParams(err, func(k string, v interface{}, safe bool) {
		if safe {
			safeParams[k] = v
		} else {
			unsafeParams[k] = v
		}
	})
	return safeParams, unsafeParams
}

// ParamFromError returns the value of the parameter for the given key, or nil if no such key exists. Checks the
// parameters of the provided error and all of its causes. If the error and its causes contain multiple values for the
// same key, the most specific (deepest) value will be returned.
func ParamFromError(err error, key string) (value interface{}, safe bool) {
	visitErrorParams(err, func(k string, v interface{}, s bool) {
		if k == key {
			value = v
			safe = s
		}
	})
	return value, safe
}

// visitErrorParams calls the provided visitor function on all of the parameters stored in the provided error and any of
// its causes. The function is invoked on all of the parameters stored in the provided error, then all of the parameters
// in the cause of the provided error, and so on. There are no guarantees made about the order in which the parameters
// will be called for a given error.
func visitErrorParams(err error, visitor func(k string, v interface{}, safe bool)) {
	allErrs := []error{err}
	for currErr := err; ; {
		causer, ok := currErr.(causer)
		if !ok || causer.Cause() == nil {
			// current error does not have a cause
			break
		}
		allErrs = append(allErrs, causer.Cause())
		currErr = causer.Cause()
	}
	for _, currErr := range allErrs {
		we, ok := currErr.(*werror)
		if !ok {
			// if error is not a *werror but is a ParamStorer, then use the SafeParams() and UnsafeParams() functions to
			// extract parameters. Need to handle the *werror case separately to prevent infinite recursion.
			if ps, ok := currErr.(wparams.ParamStorer); ok {
				for k, v := range ps.SafeParams() {
					visitor(k, v, true)
				}
				for k, v := range ps.UnsafeParams() {
					visitor(k, v, false)
				}
			}
			continue
		}
		for k, v := range we.params {
			visitor(k, v.value, v.safe)
		}
	}
}

// werror is an error type consisting of an underlying error and safe and unsafe params associated with that error.
type werror struct {
	message string
	cause   error
	stack   *stack
	params  map[string]paramValue
}

type paramValue struct {
	safe  bool
	value interface{}
}

// causer interface is compatible with the interface used by pkg/errors.
type causer interface {
	Cause() error
}

func newWerror(message string, cause error, params ...Param) error {
	we := &werror{
		message: message,
		cause:   cause,
		stack:   callers(),
		params:  make(map[string]paramValue),
	}
	for _, p := range params {
		p.apply(we)
	}
	return we
}

// Error returns the message for this error by delegating to the stored error. The error consists only of the message
// and does not include any other information such as safe/unsafe parameters or cause.
func (e *werror) Error() string {
	if e.cause == nil {
		return e.message
	}
	return e.message + ": " + e.cause.Error()
}

// Cause returns the underlying cause of this error or nil if there is none.
func (e *werror) Cause() error {
	return e.cause
}

func (e *werror) SafeParams() map[string]interface{} {
	safe, _ := ParamsFromError(e)
	return safe
}

func (e *werror) UnsafeParams() map[string]interface{} {
	_, unsafe := ParamsFromError(e)
	return unsafe
}

// Format formats the error using the provided format state. Delegates to stored error.
func (e *werror) Format(state fmt.State, verb rune) {
	if verb == 'v' && state.Flag('+') {
		// Multi-line extra verbose format starts with cause first followed up by current error metadata.
		e.formatCause(state, verb)
		e.formatMessage(state, verb)
		e.formatParameters(state, verb)
		e.formatStack(state, verb)
	} else {
		e.formatMessage(state, verb)
		e.formatParameters(state, verb)
		e.formatStack(state, verb)
		e.formatCause(state, verb)
	}
}

func (e *werror) formatMessage(state fmt.State, verb rune) {
	if e.message == "" {
		return
	}
	switch verb {
	case 's', 'q', 'v':
		fmt.Fprint(state, e.message)
	}
}

func (e *werror) formatParameters(state fmt.State, verb rune) {
	safe := make(map[string]interface{}, len(e.params))
	for k, v := range e.params {
		if v.safe {
			safe[k] = v.value
		}
	}
	if len(safe) == 0 {
		return
	}
	if verb != 'v' {
		return
	}
	if e.message != "" {
		// Whitespace before the message.
		fmt.Fprint(state, " ")
	}
	fmt.Fprintf(state, "%+v", safe)
}

func (e *werror) formatStack(state fmt.State, verb rune) {
	if e.stack == nil {
		return
	}
	if verb != 'v' || !state.Flag('+') {
		return
	}
	e.stack.Format(state, verb)
}

func (e *werror) formatCause(state fmt.State, verb rune) {
	if e.cause == nil {
		return
	}
	switch verb {
	case 'v':
		if state.Flag('+') {
			fmt.Fprintf(state, "%+v\n", e.cause)
		} else {
			fmt.Fprintf(state, ": %v", e.cause)
		}
	case 's':
		fmt.Fprintf(state, ": %s", e.cause)
	case 'q':
		fmt.Fprintf(state, ": %q", e.cause)
	}
}
