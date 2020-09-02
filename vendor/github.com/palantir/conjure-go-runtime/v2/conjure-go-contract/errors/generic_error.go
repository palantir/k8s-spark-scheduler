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
	"encoding/json"
	"fmt"

	"github.com/palantir/conjure-go-runtime/v2/conjure-go-contract/codecs"
	"github.com/palantir/pkg/uuid"
	wparams "github.com/palantir/witchcraft-go-params"
)

func newGenericError(errorType ErrorType, params wparams.ParamStorer) genericError {
	return genericError{
		errorType:       errorType,
		errorInstanceID: uuid.NewUUID(),
		params:          params,
	}
}

// genericError is general purpose implementation the Error interface.
//
// It can only be created with exported constructors, which guarantee correctness of the data.
type genericError struct {
	errorType       ErrorType
	errorInstanceID uuid.UUID
	params          wparams.ParamStorer
}

var (
	_ fmt.Stringer     = genericError{}
	_ Error            = genericError{}
	_ json.Marshaler   = genericError{}
	_ json.Unmarshaler = &genericError{}
)

// String representation of an error.
//
// For example:
//
//  "CONFLICT Facebook:LikeAlreadyGiven (00010203-0405-0607-0809-0a0b0c0d0e0f)".
//
func (e genericError) String() string {
	return fmt.Sprintf("%s (%s)", e.errorType, e.errorInstanceID)
}

func (e genericError) Error() string {
	return e.String()
}

func (e genericError) Code() ErrorCode {
	return e.errorType.code
}

func (e genericError) Name() string {
	return e.errorType.name
}

func (e genericError) InstanceID() uuid.UUID {
	return e.errorInstanceID
}

func (e genericError) SafeParams() map[string]interface{} {
	// Copy safe params map (so we don't mutate the underlying one) and add errorInstanceId
	safeParams := make(map[string]interface{}, len(e.params.SafeParams())+1)
	for k, v := range e.params.SafeParams() {
		safeParams[k] = v
	}
	safeParams["errorInstanceId"] = e.errorInstanceID
	return safeParams
}

func (e genericError) UnsafeParams() map[string]interface{} {
	return e.params.UnsafeParams()
}

func (e genericError) MarshalJSON() ([]byte, error) {
	marshaledParameters, err := codecs.JSON.Marshal(mergeParams(e.params))
	if err != nil {
		return nil, err
	}
	return codecs.JSON.Marshal(SerializableError{
		ErrorCode:       e.errorType.code,
		ErrorName:       e.errorType.name,
		ErrorInstanceID: e.errorInstanceID,
		Parameters:      marshaledParameters,
	})
}

func (e *genericError) UnmarshalJSON(data []byte) (err error) {
	var se SerializableError
	if err := codecs.JSON.Unmarshal(data, &se); err != nil {
		return err
	}
	if e.errorType, err = NewErrorType(se.ErrorCode, se.ErrorName); err != nil {
		return err
	}
	e.errorInstanceID = se.ErrorInstanceID

	if len(se.Parameters) > 0 {
		params := make(map[string]interface{})
		if err := codecs.JSON.Unmarshal(se.Parameters, &params); err != nil {
			return err
		}
		e.params = wparams.NewUnsafeParamStorer(params)
	} else {
		e.params = wparams.NewParamStorer()
	}
	return nil
}

func mergeParams(storer wparams.ParamStorer) map[string]interface{} {
	safeParams, unsafeParams := storer.SafeParams(), storer.UnsafeParams()
	params := make(map[string]interface{}, len(safeParams)+len(unsafeParams))
	for k, v := range unsafeParams {
		params[k] = v
	}
	for k, v := range safeParams {
		params[k] = v
	}
	return params
}
