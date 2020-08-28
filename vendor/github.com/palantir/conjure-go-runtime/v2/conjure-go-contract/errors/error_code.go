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

// Copyright 2016 Palantir Technologies. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package errors

import (
	"encoding"
	"fmt"
	"net/http"
)

// ErrorCode is an enum describing error category.
//
// Each error code has associated HTTP status codes.
type ErrorCode int16

var (
	_ fmt.Stringer             = ErrorCode(0)
	_ encoding.TextMarshaler   = ErrorCode(0)
	_ encoding.TextUnmarshaler = new(ErrorCode)
)

const (
	_ ErrorCode = iota // there is no good candidate for zero value

	// PermissionDenied has status code 403 Forbidden.
	PermissionDenied
	// InvalidArgument has status code 400 BadRequest.
	InvalidArgument
	// NotFound  has status code 404 NotFound.
	NotFound
	// Conflict has status code 409 Conflict.
	Conflict
	// RequestEntityTooLarge has status code 413 RequestEntityTooLarge.
	RequestEntityTooLarge
	// FailedPrecondition has status code 500 InternalServerError.
	FailedPrecondition
	// Internal has status code 500 InternalServerError.
	Internal
	// Timeout has status code 500 InternalServerError.
	Timeout
	// CustomClient has status code 400 BadRequest.
	CustomClient
	// CustomServer has status code 500 InternalServerError.
	CustomServer
)

// StatusCode returns HTTP status code associated with this error code.
func (ec ErrorCode) StatusCode() int {
	switch ec {
	case PermissionDenied:
		return http.StatusForbidden
	case InvalidArgument:
		return http.StatusBadRequest
	case NotFound:
		return http.StatusNotFound
	case Conflict:
		return http.StatusConflict
	case RequestEntityTooLarge:
		return http.StatusRequestEntityTooLarge
	case FailedPrecondition:
		return http.StatusInternalServerError
	case Internal:
		return http.StatusInternalServerError
	case Timeout:
		return http.StatusInternalServerError
	case CustomClient:
		return http.StatusBadRequest
	case CustomServer:
		return http.StatusInternalServerError
	}
	return http.StatusInternalServerError
}

// String representation of this error code.
//
// For example "NOT_FOUND", "CONFLICT", "PERMISSION_DENIED" or "TIMEOUT".
func (ec ErrorCode) String() string {
	switch ec {
	case PermissionDenied:
		return "PERMISSION_DENIED"
	case InvalidArgument:
		return "INVALID_ARGUMENT"
	case NotFound:
		return "NOT_FOUND"
	case Conflict:
		return "CONFLICT"
	case RequestEntityTooLarge:
		return "REQUEST_ENTITY_TOO_LARGE"
	case FailedPrecondition:
		return "FAILED_PRECONDITION"
	case Internal:
		return "INTERNAL"
	case Timeout:
		return "TIMEOUT"
	case CustomClient:
		return "CUSTOM_CLIENT"
	case CustomServer:
		return "CUSTOM_SERVER"
	}
	return fmt.Sprintf("<invalid error code: %d>", ec)
}

// MarshalText implements encoding.TextMarshaler.
func (ec ErrorCode) MarshalText() ([]byte, error) {
	return []byte(ec.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (ec *ErrorCode) UnmarshalText(data []byte) error {
	switch string(data) {
	case "PERMISSION_DENIED":
		*ec = PermissionDenied
	case "INVALID_ARGUMENT":
		*ec = InvalidArgument
	case "NOT_FOUND":
		*ec = NotFound
	case "CONFLICT":
		*ec = Conflict
	case "REQUEST_ENTITY_TOO_LARGE":
		*ec = RequestEntityTooLarge
	case "FAILED_PRECONDITION":
		*ec = FailedPrecondition
	case "INTERNAL":
		*ec = Internal
	case "TIMEOUT":
		*ec = Timeout
	case "CUSTOM_CLIENT":
		*ec = CustomClient
	case "CUSTOM_SERVER":
		*ec = CustomServer
	default:
		return fmt.Errorf(`errors: unknown error code string`)
	}
	return nil
}
