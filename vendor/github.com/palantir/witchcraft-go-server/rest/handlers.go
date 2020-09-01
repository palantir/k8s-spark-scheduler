// Copyright (c) 2018 Palantir Technologies. All rights reserved.
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

package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

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
//
// Deprecated: Prefer server utilities in github.com/palantir/conjure-go-runtime/conjure-go-server/httpserver.
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
		var jsonErr interface{}
		if marshaler, ok := err.(json.Marshaler); ok {
			jsonErr = marshaler
		} else {
			// Fall back to string encoding
			jsonErr = err.Error()
		}
		WriteJSONResponse(w, jsonErr, status)
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

// StatusCodeMapper maps a provided error to an HTTP status code. If the provided error contains a non-zero status code added
// using the StatusCode ErrorParam, returns that status code; otherwise, returns http.StatusInternalServerError.
//
// Deprecated: Prefer server utilities in github.com/palantir/conjure-go-runtime/v2/conjure-go-server/httpserver.
func StatusCodeMapper(err error) int {
	safe, _ := werror.ParamsFromError(err)
	statusCode, ok := safe[httpStatusCodeParamKey]
	if !ok {
		return http.StatusInternalServerError
	}
	statusCodeInt, ok := statusCode.(int)
	if !ok || statusCodeInt == 0 {
		return http.StatusInternalServerError
	}
	return statusCodeInt
}

// ErrHandler is an ErrorHandler that creates a log in the provided context's svc1log logger when an error is received.
// The log output is printed at the ERROR level if the status code is >= 500; otherwise, it is printed at INFO level.
// This preserves request-scoped logging configuration added by wrouter.
//
// Deprecated: Prefer server utilities in github.com/palantir/conjure-go-runtime/v2/conjure-go-server/httpserver.
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
