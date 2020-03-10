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

package witchcraft

import (
	"net/http"

	"github.com/palantir/witchcraft-go-server/wrouter"
)

// newMultiRouterImpl returns a new multiRouterImpl that uses the provided routers unless the root routers that back the
// provided routers are the same, in which case the main router is returned.
func newMultiRouterImpl(mainRouter, mgmtRouter wrouter.Router) wrouter.Router {
	if mainRouter.RootRouter() == mgmtRouter.RootRouter() {
		return mainRouter
	}
	return &multiRouterImpl{
		mainRouter: mainRouter,
		mgmtRouter: mgmtRouter,
	}
}

// multiRouterImpl is an implementation of wrouter.Router that bundles a main router and a management router. In
// general, all of the functions are delegated to the main router -- the primary difference is that functions that
// return RootRouter or Router interfaces also returned wrapped ones. The motivation of this is to ensure that any calls
// that register middleware on the root routers registers the middleware on both the main and the management router (if
// they are distinct).
type multiRouterImpl struct {
	mainRouter wrouter.Router
	mgmtRouter wrouter.Router
}

func (m *multiRouterImpl) Register(method, path string, handler http.Handler, params ...wrouter.RouteParam) error {
	return m.mainRouter.Register(method, path, handler, params...)
}

func (m *multiRouterImpl) RegisteredRoutes() []wrouter.RouteSpec {
	return m.mainRouter.RegisteredRoutes()
}

func (m *multiRouterImpl) Get(path string, handler http.Handler, params ...wrouter.RouteParam) error {
	return m.mainRouter.Get(path, handler, params...)
}

func (m *multiRouterImpl) Head(path string, handler http.Handler, params ...wrouter.RouteParam) error {
	return m.mainRouter.Head(path, handler, params...)
}

func (m *multiRouterImpl) Post(path string, handler http.Handler, params ...wrouter.RouteParam) error {
	return m.mainRouter.Post(path, handler, params...)
}

func (m *multiRouterImpl) Put(path string, handler http.Handler, params ...wrouter.RouteParam) error {
	return m.mainRouter.Put(path, handler, params...)
}

func (m *multiRouterImpl) Patch(path string, handler http.Handler, params ...wrouter.RouteParam) error {
	return m.mainRouter.Patch(path, handler, params...)
}

func (m *multiRouterImpl) Delete(path string, handler http.Handler, params ...wrouter.RouteParam) error {
	return m.mainRouter.Delete(path, handler, params...)
}

func (m *multiRouterImpl) Subrouter(path string, params ...wrouter.RouteParam) wrouter.Router {
	return &multiRouterImpl{
		mainRouter: m.mainRouter.Subrouter(path, params...),
		mgmtRouter: m.mgmtRouter.Subrouter(path, params...),
	}
}

func (m *multiRouterImpl) Path() string {
	return m.mainRouter.Path()
}

func (m *multiRouterImpl) Parent() wrouter.Router {
	return &multiRouterImpl{
		mainRouter: m.mainRouter.Parent(),
		mgmtRouter: m.mgmtRouter.Parent(),
	}
}

func (m *multiRouterImpl) RootRouter() wrouter.RootRouter {
	return &multiRootRouterImpl{
		multiRouterImpl: &multiRouterImpl{
			mainRouter: m.mainRouter.RootRouter(),
			mgmtRouter: m.mgmtRouter.RootRouter(),
		},
		mainRouter: m.mainRouter.RootRouter(),
		mgmtRouter: m.mgmtRouter.RootRouter(),
	}
}

// multiRootRouterImpl is an implementation of wrouter.RootRouter that bundles a main router and a management router. In
// general, all of the functions are delegated to the main router -- the primary difference is that the functions that
// add middleware to the routers will add the provided middleware to both routers (if they are distinct). The functions
// that return Router and RootRouter also return wrapped implementations to ensure this behavior.
type multiRootRouterImpl struct {
	*multiRouterImpl

	mainRouter wrouter.RootRouter
	mgmtRouter wrouter.RootRouter
}

func (m *multiRootRouterImpl) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	m.mainRouter.ServeHTTP(rw, req)
}

func (m *multiRootRouterImpl) AddRequestHandlerMiddleware(handlers ...wrouter.RequestHandlerMiddleware) {
	m.mainRouter.AddRequestHandlerMiddleware(handlers...)

	// register middleware for the management router as well only if it differs from the main one
	if m.mainRouter != m.mgmtRouter {
		m.mgmtRouter.AddRequestHandlerMiddleware(handlers...)
	}
}

func (m *multiRootRouterImpl) AddRouteHandlerMiddleware(handlers ...wrouter.RouteHandlerMiddleware) {
	m.mainRouter.AddRouteHandlerMiddleware(handlers...)

	// register middleware for the management router as well only if it differs from the main one
	if m.mainRouter != m.mgmtRouter {
		m.mgmtRouter.AddRouteHandlerMiddleware(handlers...)
	}
}
