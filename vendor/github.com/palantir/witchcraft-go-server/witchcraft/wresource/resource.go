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

package wresource

import (
	"net/http"

	"github.com/palantir/pkg/metrics"
	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-server/wrouter"
)

const (
	ResourceTagName = "service-name"
	MethodTagName   = "method"
	EndpointTagName = "endpoint"
)

// Resource provides a way to register a collection of endpoints. All of the routes registered using Resource will be
// tagged with the "service-name" and "endpoint" metrics.
type Resource interface {
	// Register performs the same operation as the Router.Register, but registers the route using the provided
	// resourceName as the resource name that will be used as the tag in the recorded metric.
	Register(endpointName, method, path string, handler http.Handler, params ...wrouter.RouteParam) error

	// Get is a shorthand for Register(endpointName, http.MethodGet, handler, params...)
	Get(endpointName, path string, handler http.Handler, params ...wrouter.RouteParam) error

	// Head is a shorthand for Register(endpointName, http.MethodHead, handler, params...)
	Head(endpointName, path string, handler http.Handler, params ...wrouter.RouteParam) error

	// Post is a shorthand for Register(endpointName, http.MethodPost, handler, params...)
	Post(endpointName, path string, handler http.Handler, params ...wrouter.RouteParam) error

	// Put is a shorthand for Register(endpointName, http.MethodPut, handler, params...)
	Put(endpointName, path string, handler http.Handler, params ...wrouter.RouteParam) error

	// Patch is a shorthand for Register(endpointName, http.MethodPatch, handler, params...)
	Patch(endpointName, path string, handler http.Handler, params ...wrouter.RouteParam) error

	// Delete is a shorthand for Register(endpointName, http.MethodDelete, handler, params...)
	Delete(endpointName, path string, handler http.Handler, params ...wrouter.RouteParam) error
}

func New(resourceName string, router wrouter.Router) Resource {
	return &resourceImpl{
		resourceName: resourceName,
		router:       router,
	}
}

type resourceImpl struct {
	// the name of the resource used for metric logging.
	resourceName string
	router       wrouter.Router
}

func (r *resourceImpl) Register(endpointName, method, path string, handler http.Handler, params ...wrouter.RouteParam) error {
	var tags metrics.Tags
	resourceTag, err := metrics.NewTag(ResourceTagName, r.resourceName)
	if err != nil {
		return werror.Wrap(err, "failed to create metric resourceTag")
	}
	tags = append(tags, resourceTag)

	methodTag, err := metrics.NewTag(MethodTagName, method)
	if err != nil {
		return werror.Wrap(err, "failed to create metric methodTag")
	}
	tags = append(tags, methodTag)

	endpointTag, err := metrics.NewTag(EndpointTagName, endpointName)
	if err != nil {
		return werror.Wrap(err, "failed to create metric endpointTag")
	}
	tags = append(tags, endpointTag)

	return r.router.Register(method, path, handler, append(params, wrouter.MetricTags(tags))...)
}

func (r *resourceImpl) Get(endpointName, path string, handler http.Handler, params ...wrouter.RouteParam) error {
	return r.Register(endpointName, http.MethodGet, path, handler, params...)
}

func (r *resourceImpl) Head(endpointName, path string, handler http.Handler, params ...wrouter.RouteParam) error {
	return r.Register(endpointName, http.MethodHead, path, handler, params...)
}

func (r *resourceImpl) Post(endpointName, path string, handler http.Handler, params ...wrouter.RouteParam) error {
	return r.Register(endpointName, http.MethodPost, path, handler, params...)
}

func (r *resourceImpl) Put(endpointName, path string, handler http.Handler, params ...wrouter.RouteParam) error {
	return r.Register(endpointName, http.MethodPut, path, handler, params...)
}

func (r *resourceImpl) Patch(endpointName, path string, handler http.Handler, params ...wrouter.RouteParam) error {
	return r.Register(endpointName, http.MethodPatch, path, handler, params...)
}

func (r *resourceImpl) Delete(endpointName, path string, handler http.Handler, params ...wrouter.RouteParam) error {
	return r.Register(endpointName, http.MethodDelete, path, handler, params...)
}
