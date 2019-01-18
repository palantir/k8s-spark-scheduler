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
	"bytes"
	"fmt"
	"net/http"
	"strings"
)

type subrouter struct {
	rPath   string
	rParent Router
}

func (s *subrouter) Register(method, path string, handler http.Handler, params ...RouteParam) error {
	rootRouter, basePath := s.getRootRouterAndPath()
	return rootRouter.Register(method, fmt.Sprint(basePath, path), handler, params...)
}

func (s *subrouter) RegisteredRoutes() []RouteSpec {
	rootRouter, basePath := s.getRootRouterAndPath()
	var specs []RouteSpec
	for _, spec := range rootRouter.RegisteredRoutes() {
		if !strings.HasPrefix(spec.PathTemplate, basePath) {
			continue
		}
		specs = append(specs, spec)
	}
	return specs
}

func (s *subrouter) Get(path string, handler http.Handler, params ...RouteParam) error {
	return s.Register(http.MethodGet, path, handler, params...)
}

func (s *subrouter) Head(path string, handler http.Handler, params ...RouteParam) error {
	return s.Register(http.MethodHead, path, handler, params...)
}

func (s *subrouter) Post(path string, handler http.Handler, params ...RouteParam) error {
	return s.Register(http.MethodPost, path, handler, params...)
}

func (s *subrouter) Put(path string, handler http.Handler, params ...RouteParam) error {
	return s.Register(http.MethodPut, path, handler, params...)
}

func (s *subrouter) Patch(path string, handler http.Handler, params ...RouteParam) error {
	return s.Register(http.MethodPatch, path, handler, params...)
}

func (s *subrouter) Delete(path string, handler http.Handler, params ...RouteParam) error {
	return s.Register(http.MethodDelete, path, handler, params...)
}

func (s *subrouter) Subrouter(path string) Router {
	return &subrouter{
		rPath:   path,
		rParent: s,
	}
}

func (s *subrouter) Path() string {
	return s.rPath
}

func (s *subrouter) Parent() Router {
	return s.rParent
}

func (s *subrouter) RootRouter() RootRouter {
	return s.rParent.RootRouter()
}

func (s *subrouter) getRootRouterAndPath() (Router, string) {
	var currRouteSegment Router = s
	var parts []string
	for currRouteSegment.Parent() != nil {
		parts = append(parts, currRouteSegment.Path())
		currRouteSegment = currRouteSegment.Parent()
	}

	// parts contain all parent path parts in reverse order: join backwards to get path
	pathBuf := &bytes.Buffer{}
	for i := len(parts) - 1; i >= 0; i-- {
		fmt.Fprint(pathBuf, parts[i])
	}

	// currRouteSegment is the root router
	return currRouteSegment, pathBuf.String()
}
