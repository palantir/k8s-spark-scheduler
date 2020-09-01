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

package wrouter

import (
	"net/http"
)

type SegmentType int

const (
	LiteralSegment SegmentType = iota
	PathParamSegment
	TrailingPathParamSegment
)

type PathSegment struct {
	Type  SegmentType
	Value string
}

type RouterImpl interface {
	// RouterImpl must implement http.Handler
	http.Handler

	// Register registers the provided handler for this router for the provided method (GET, POST, etc.) and
	// pathSegments.
	Register(method string, pathSegments []PathSegment, handler http.Handler)

	// PathParams returns the path parameters for the provided request. The size of the returned slice must match
	// the number of parameters in the path. The variable names of the parameters in the path are provided in order
	// in the "pathVarNames" slice.
	PathParams(req *http.Request, pathVarNames []string) map[string]string

	// RegisterNotFoundHandler registers a handler that is used to handle any requests that do not match any registered routes on the router.
	// If not provided, the implementation's default behavior is used (typically returns an http.Error with a 404 response code).
	RegisterNotFoundHandler(handler http.Handler)
}
