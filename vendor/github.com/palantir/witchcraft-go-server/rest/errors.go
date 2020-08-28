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
	werror "github.com/palantir/witchcraft-go-error"
)

const (
	httpStatusCodeParamKey = "httpStatusCode"
)

type errorMetadata struct {
	statusCode int
}

type ErrorParam interface {
	apply(error *errorMetadata)
}

type errorParamFunc func(metadata *errorMetadata)

func (f errorParamFunc) apply(err *errorMetadata) {
	f(err)
}

// NewError creates an error which may include an HTTP status code using the StatusCode parameter.
//
// Deprecated: Prefer conjure errors provided by github.com/palantir/conjure-go-runtime/v2/conjure-go-contract/errors.
func NewError(err error, params ...ErrorParam) error {
	e := errorMetadata{
		statusCode: StatusCodeMapper(err),
	}
	for _, param := range params {
		param.apply(&e)
	}
	return werror.Wrap(err, "witchcraft-server rest error", werror.SafeParam(httpStatusCodeParamKey, e.statusCode))
}

func StatusCode(code int) ErrorParam {
	return errorParamFunc(func(err *errorMetadata) {
		err.statusCode = code
	})
}
