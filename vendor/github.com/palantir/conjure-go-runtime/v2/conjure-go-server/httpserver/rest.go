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
	"net/http"
	"strings"

	"github.com/palantir/pkg/safejson"
	werror "github.com/palantir/witchcraft-go-error"
)

// WriteJSONResponse marshals the provided object to JSON using a JSON encoder with SetEscapeHTML(false) and writes the
// resulting JSON as a JSON response to the provided http.ResponseWriter with the provided status code. If marshaling
// the provided object as JSON results in an error, writes a 500 response with the text content of the error.
func WriteJSONResponse(w http.ResponseWriter, obj interface{}, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := safejson.Encoder(w).Encode(obj); err != nil {
		// if JSON encode failed, send error response. If JSON encode succeeded but write failed, then this
		// should be a no-op since the socket failed anyway.
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// ParseBearerTokenHeader parses a bearer token value out of the Authorization header. It expects a header with a key
// of 'Authorization' and a value of 'bearer {token}'. ParseBearerTokenHeader will return the token value, or an error
// if the Authorization header is missing, an empty string, or is not in the format expected.
func ParseBearerTokenHeader(req *http.Request) (string, error) {
	authHeader := req.Header.Get("Authorization")
	if authHeader == "" {
		return "", werror.Error("Authorization header not found")
	}
	headerSplit := strings.Split(authHeader, " ")
	if len(headerSplit) != 2 || strings.ToLower(headerSplit[0]) != "bearer" {
		return "", werror.Error("Illegal authorization header, expected Bearer")
	}
	return headerSplit[1], nil
}
