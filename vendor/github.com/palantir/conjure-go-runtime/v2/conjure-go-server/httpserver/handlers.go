// Copyright (c) 2020 Palantir Technologies. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package httpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/palantir/conjure-go-runtime/v2/conjure-go-contract/errors"
	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
)

type ErrorHandler func(ctx context.Context, statusCode int, err error)
type StatusMapper func(err error) int

type handler struct {
	handleFn func(http.ResponseWriter, *http.Request) error
	statusFn StatusMapper
	errorFn  ErrorHandler
}

// NewJSONHandler returns a http.Handler which will convert a returned error into a corresponding status code, and
// handle the error according to the provided ErrorHandler. The provided 'fn' function is not expected to write
// a response in the http.ResponseWriter if it returns a non-nil error. If a non-nil error is returned, the
// mapped status code from the provided StatusMapper will be returned.
func NewJSONHandler(fn func(http.ResponseWriter, *http.Request) error, statusFn StatusMapper, errorFn ErrorHandler) http.Handler {
	return &handler{
		handleFn: fn,
		statusFn: statusFn,
		errorFn:  errorFn,
	}
}

// ServeHTTP implements the http.Handler interface
func (h handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := h.handleFn(w, r); err != nil {
		status := h.status(err)
		h.handleError(r.Context(), status, err)
		cause := getSerializableCause(err)
		switch e := cause.(type) {
		case errors.Error:
			// if error is a conjure error, use WriteErrorResponse utility
			errors.WriteErrorResponse(w, e)
		case json.Marshaler:
			// else if error is a json marshaler, write as json
			WriteJSONResponse(w, e, status)
		default:
			// Fall back to string encoding
			http.Error(w, err.Error(), status)
		}
	}
}

// status returns the http status code from the provided err
func (h handler) status(err error) int {
	if h.statusFn != nil {
		return h.statusFn(err)
	}
	return http.StatusInternalServerError
}

// handleError calls the handler's provided ErrorHandler with the provided error
func (h handler) handleError(ctx context.Context, statusCode int, err error) {
	if h.errorFn != nil {
		h.errorFn(ctx, statusCode, err)
	}
}

// StatusCodeMapper maps a provided error to an HTTP status code.
// If the error's RootCause is a conjure error, the status mapping to the errorCode field is used.
// If the provided error is a contains the legacy httpStatusCode parameter, that value is used.
// Otherwise, returns http.StatusInternalServerError (500).
func StatusCodeMapper(err error) int {
	if conjureErr := errors.GetConjureError(err); conjureErr != nil {
		return conjureErr.Code().StatusCode()
	}
	if legacyCode := legacyErrorCode(err); legacyCode != 0 {
		return legacyCode
	}
	return http.StatusInternalServerError
}

// legacyErrorCode extracts error codes set by the deprecated witchcraft-go-server/rest package.
// It returns 0 if not found. New code should use conjure errors.
func legacyErrorCode(err error) int {
	statusCodeParam, _ := werror.ParamFromError(err, legacyHTTPStatusCodeParamKey)
	if statusCodeParam == nil {
		return 0
	}
	statusCodeInt, ok := statusCodeParam.(int)
	if !ok {
		return 0
	}
	return statusCodeInt
}

// ErrHandler is an ErrorHandler that creates a log in the provided context's svc1log logger when an error is received.
// The log output is printed at the ERROR level if the status code is >= 500; otherwise, it is printed at INFO level.
// This preserves request-scoped logging configuration added by wrouter.
func ErrHandler(ctx context.Context, statusCode int, err error) {
	logger := svc1log.FromContext(ctx)

	logFn := logger.Info
	if statusCode >= 500 {
		logFn = logger.Error
	}
	logFn(
		fmt.Sprintf("error handling request: %s", err.Error()),
		svc1log.Stacktrace(err),
	)
}

func getSerializableCause(err error) error {
	if conjureErr := errors.GetConjureError(err); conjureErr != nil {
		return conjureErr
	}
	if marshaler := getJSONMarshaler(err); marshaler != nil {
		return marshaler
	}
	return err
}

func getJSONMarshaler(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(json.Marshaler); ok {
		return err
	}
	if werr, ok := err.(werror.Werror); ok {
		return getJSONMarshaler(werr.Cause())
	}
	return nil
}
