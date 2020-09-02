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

package routes

import (
	"net/http"

	"github.com/palantir/conjure-go-runtime/v2/conjure-go-server/httpserver"
	"github.com/palantir/witchcraft-go-server/status"
	"github.com/palantir/witchcraft-go-server/witchcraft/refreshable"
	"github.com/palantir/witchcraft-go-server/witchcraft/wresource"
)

func AddLivenessRoutes(resource wresource.Resource, source status.Source) error {
	return resource.Get("liveness", status.LivenessEndpoint, handler(source))
}

func AddReadinessRoutes(resource wresource.Resource, source status.Source) error {
	return resource.Get("readiness", status.ReadinessEndpoint, handler(source))
}

func AddHealthRoutes(resource wresource.Resource, source status.HealthCheckSource, sharedSecret refreshable.String, healthStatusChangeHandlers []status.HealthStatusChangeHandler) error {
	return resource.Get("health", status.HealthEndpoint, status.NewHealthCheckHandler(source, sharedSecret, healthStatusChangeHandlers))
}

// handler returns an HTTP handler that writes a response based on the provided source. The status code of the response
// is determined based on the status reported by the source and the status metadata returned by the source is written as
// JSON in the response body.
func handler(source status.Source) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		respCode, metadata := source.Status()

		// if metadata is nil, create an empty json object instead of returning 'null' which http-remoting rejects.
		if metadata == nil {
			metadata = struct{}{}
		}

		httpserver.WriteJSONResponse(w, metadata, respCode)
	})
}
