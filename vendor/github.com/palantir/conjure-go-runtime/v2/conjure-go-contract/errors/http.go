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

package errors

import (
	"encoding/json"
	"net/http"

	"github.com/palantir/conjure-go-runtime/v2/conjure-go-contract/codecs"
)

// WriteErrorResponse writes error to the response writer.
//
// TODO This function is subject to change.
func WriteErrorResponse(w http.ResponseWriter, e Error) {
	var marshaledError []byte
	var err error

	// First try to marshal with custom handling (if present)
	if marshaler, ok := e.(json.Marshaler); ok {
		marshaledError, err = codecs.JSON.Marshal(marshaler)
	}
	// If we fail, use best-effort conversion to SerializableError.
	if marshaledError == nil || err != nil {
		params, err := codecs.JSON.Marshal(mergeParams(e)) // on failure, params will be nil
		if err != nil {
			params = nil
		}
		// This should never fail, since all fields other than params are primitives
		// and we fall back to empty params if they fail above. Nothing we can do otherwise.
		marshaledError, _ = codecs.JSON.Marshal(SerializableError{
			ErrorCode:       e.Code(),
			ErrorName:       e.Name(),
			ErrorInstanceID: e.InstanceID(),
			Parameters:      params,
		})
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(e.Code().StatusCode())
	_, _ = w.Write(marshaledError) // There is nothing we can do on write failure.
}
