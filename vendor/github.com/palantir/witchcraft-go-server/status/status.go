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

package status

import (
	"context"
	"net/http"
	"sync/atomic"

	"github.com/palantir/conjure-go-runtime/v2/conjure-go-server/httpserver"
	"github.com/palantir/witchcraft-go-server/conjure/witchcraft/api/health"
	"github.com/palantir/witchcraft-go-server/witchcraft/refreshable"
)

var HealthStateStatusCodes = map[health.HealthState]int{
	health.HealthStateHealthy:   http.StatusOK,
	health.HealthStateDeferring: 518,
	health.HealthStateSuspended: 519,
	health.HealthStateRepairing: 520,
	health.HealthStateWarning:   521,
	health.HealthStateError:     522,
	health.HealthStateTerminal:  523,
}

// Source provides status that should be sent as a response.
type Source interface {
	Status() (respStatus int, metadata interface{})
}

// HealthCheckSource provides the SLS health status that should be sent as a response.
// Refer to the SLS specification for more information.
type HealthCheckSource interface {
	HealthStatus(ctx context.Context) health.HealthStatus
}

type combinedHealthCheckSource struct {
	healthCheckSources []HealthCheckSource
}

func NewCombinedHealthCheckSource(healthCheckSources ...HealthCheckSource) HealthCheckSource {
	return &combinedHealthCheckSource{
		healthCheckSources: healthCheckSources,
	}
}

func (c *combinedHealthCheckSource) HealthStatus(ctx context.Context) health.HealthStatus {
	result := health.HealthStatus{
		Checks: map[health.CheckType]health.HealthCheckResult{},
	}
	for _, healthCheckSource := range c.healthCheckSources {
		for k, v := range healthCheckSource.HealthStatus(ctx).Checks {
			result.Checks[k] = v
		}
	}
	return result
}

// HealthHandler is responsible for checking the health-check-shared-secret if it is provided and
// invoking a HealthCheckSource if the secret is correct or unset.
type healthHandlerImpl struct {
	healthCheckSharedSecret refreshable.String
	check                   HealthCheckSource
	previousHealth          *atomic.Value
	changeHandler           HealthStatusChangeHandler
}

func NewHealthCheckHandler(checkSource HealthCheckSource, sharedSecret refreshable.String, healthStatusChangeHandlers []HealthStatusChangeHandler) http.Handler {
	previousHealth := &atomic.Value{}
	previousHealth.Store(health.HealthStatus{})
	allHandlers := []HealthStatusChangeHandler{loggingHealthStatusChangeHandler()}
	if len(healthStatusChangeHandlers) > 0 {
		allHandlers = append(allHandlers, healthStatusChangeHandlers...)
	}
	return &healthHandlerImpl{
		healthCheckSharedSecret: sharedSecret,
		check:                   checkSource,
		previousHealth:          previousHealth,
		changeHandler:           multiHealthStatusChangeHandler(allHandlers),
	}
}

func (h *healthHandlerImpl) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	metadata, newHealthStatusCode := h.computeNewHealthStatus(req)
	previousHealth := h.previousHealth.Load()
	if previousHealth != nil {
		if previousHealthTyped, ok := previousHealth.(health.HealthStatus); ok && checksDiffer(previousHealthTyped.Checks, metadata.Checks) {
			h.changeHandler.HandleHealthStatusChange(req.Context(), previousHealthTyped, metadata)
		}
	}

	h.previousHealth.Store(metadata)

	httpserver.WriteJSONResponse(w, metadata, newHealthStatusCode)
}

func (h *healthHandlerImpl) computeNewHealthStatus(req *http.Request) (health.HealthStatus, int) {
	if sharedSecret := h.healthCheckSharedSecret.CurrentString(); sharedSecret != "" {
		token, err := httpserver.ParseBearerTokenHeader(req)
		if err != nil || sharedSecret != token {
			return health.HealthStatus{}, http.StatusUnauthorized
		}
	}
	metadata := h.check.HealthStatus(req.Context())
	return metadata, HealthStatusCode(metadata)
}

func HealthStatusCode(metadata health.HealthStatus) int {
	worst := http.StatusOK
	for _, result := range metadata.Checks {
		code, ok := HealthStateStatusCodes[result.State]
		if !ok {
			code = http.StatusInternalServerError
		}
		if worst < code {
			worst = code
		}
	}
	return worst
}

func checksDiffer(previousChecks, newChecks map[health.CheckType]health.HealthCheckResult) bool {
	if len(previousChecks) != len(newChecks) {
		return true
	}
	for previousCheckType, previouscheckResult := range previousChecks {
		newCheckResult, checkTypePresent := newChecks[previousCheckType]
		if !checkTypePresent {
			return true
		}
		if previouscheckResult.State != newCheckResult.State {
			return true
		}
	}
	return false
}
