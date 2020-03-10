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
	"time"

	"github.com/palantir/pkg/metrics"
)

type Router interface {
	// Register registers the provided handler for this router for the provided method (GET, POST, etc.) and path.
	// The RouteParam parameters specifies any path, query or header parameters that should be considered safe or
	// forbidden for the purposes of logging.
	Register(method, path string, handler http.Handler, params ...RouteParam) error

	// RegisteredRoutes returns a slice of all of the routes registered with this router in sorted order.
	RegisteredRoutes() []RouteSpec

	// Get is a shorthand for Register(http.MethodGet, path, handler, params...)
	Get(path string, handler http.Handler, params ...RouteParam) error

	// Head is a shorthand for Register(http.MethodHead, path, handler, params...)
	Head(path string, handler http.Handler, params ...RouteParam) error

	// Post is a shorthand for Register(http.MethodPost, path, handler, params...)
	Post(path string, handler http.Handler, params ...RouteParam) error

	// Put is a shorthand for Register(http.MethodPut, path, handler, params...)
	Put(path string, handler http.Handler, params ...RouteParam) error

	// Patch is a shorthand for Register(http.MethodPatch, path, handler, params...)
	Patch(path string, handler http.Handler, params ...RouteParam) error

	// Delete is a shorthand for Register(http.MethodDelete, path, handler, params...)
	Delete(path string, handler http.Handler, params ...RouteParam) error

	// Subrouter returns a new Router that is a child of this Router. A child router is effectively an alias to the root
	// router -- any routes registered on the child router are registered on the root router with all of the prefixes up
	// to the child router.
	Subrouter(path string, params ...RouteParam) Router

	// Path returns the path stored by this router. The path is empty for the root router, while subrouters will return
	// only the portion of the path managed by the subrouter.
	Path() string

	// Parent returns the parent router of this Router, or nil if this router is the root router.
	Parent() Router

	// RootRouter returns the RootRouter for this router (which may be itself).
	RootRouter() RootRouter
}

type RequestHandlerMiddleware func(rw http.ResponseWriter, r *http.Request, next http.Handler)

type RouteHandlerMiddleware func(rw http.ResponseWriter, r *http.Request, reqVals RequestVals, next RouteRequestHandler)

type RouteRequestHandler func(rw http.ResponseWriter, r *http.Request, reqVals RequestVals)

// RouteSpec is the specification of a full route. Consists of the HTTP method and path template for the route.
type RouteSpec struct {
	Method       string
	PathTemplate string
}

type RequestVals struct {
	Spec          RouteSpec
	PathParamVals map[string]string
	ParamPerms    RouteParamPerms
	MetricTags    metrics.Tags
}

type ResponseVals struct {
	RespStatus  int
	RespSize    int64
	ReqDuration time.Duration
}

type routeSpecs []RouteSpec

func (r routeSpecs) Len() int      { return len(r) }
func (r routeSpecs) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r routeSpecs) Less(i, j int) bool {
	if r[i].PathTemplate != r[j].PathTemplate {
		return r[i].PathTemplate < r[j].PathTemplate
	}
	return r[i].Method < r[j].Method
}

type pathParamsContextKeyType string

const pathParamsContextKey = pathParamsContextKeyType("wrouterPathParams")

func PathParams(r *http.Request) map[string]string {
	params, ok := r.Context().Value(pathParamsContextKey).(map[string]string)
	if !ok {
		return nil
	}
	return params
}
