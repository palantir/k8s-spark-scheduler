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

package errors

import (
	werror "github.com/palantir/witchcraft-go-error"
	wparams "github.com/palantir/witchcraft-go-params"
)

// NewWrappedError is a convenience function for adding an underlying error to a conjure error
// as additional context. This exists so that the conjure error becomes the RootCause, which is
// used to extract the conjure error for serialization when returned by a server handler.
//
// The conjure error is wrapped using err's Error() message, and params if ParamStorer is implemented.
// All other context is discarded, including cause stack (i.e. stacktrace) and type information.
//
// DEPRECATED: Use WrapWithNewError for generic errors or WrapWithMyErrorType for conjure-generated errors.
func NewWrappedError(conjureErr Error, err error) error {
	if storer, ok := err.(wparams.ParamStorer); ok {
		return werror.Wrap(conjureErr, err.Error(), werror.Params(storer))
	}
	return werror.Wrap(conjureErr, err.Error())
}

// GetConjureError recursively searches for an error of type Error in a chain of causes. It returns the first
// instance that it finds, or nil if one is not found.
func GetConjureError(err error) Error {
	if err == nil {
		return nil
	}
	if conjureErr, ok := err.(Error); ok {
		return conjureErr
	}
	if werr, ok := err.(werror.Causer); ok {
		return GetConjureError(werr.Cause())
	}
	return nil
}
