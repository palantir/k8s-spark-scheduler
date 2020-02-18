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

	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-server/conjure/witchcraft/api/health"
	"github.com/palantir/witchcraft-go-server/rest"
	"github.com/palantir/witchcraft-go-server/witchcraft/refreshable"
)

var HealthStateStatusCodes = map[health.HealthState]int{
	health.HealthStateHealthy:   http.StatusOK,
	health.HealthStateUnknown:   500,
	health.HealthStateDeferring: 518,
	health.HealthStateSuspended: 519,
	health.HealthStateRepairing: 520,
	health.HealthStateWarning:   521,
	health.HealthStateError:     522,
	health.HealthStateTerminal:  523,
}

type healthStatusWithCode struct {
	statusCode int
	checks     map[health.CheckType]health.HealthCheckResult
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
}

func NewHealthCheckHandler(checkSource HealthCheckSource, sharedSecret refreshable.String) http.Handler {
	previousHealth := &atomic.Value{}
	previousHealth.Store(&healthStatusWithCode{
		statusCode: 0,
		checks:     map[health.CheckType]health.HealthCheckResult{},
	})
	return &healthHandlerImpl{
		healthCheckSharedSecret: sharedSecret,
		check:                   checkSource,
		previousHealth:          previousHealth,
	}
}

func (h *healthHandlerImpl) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	metadata, newHealthStatusCode := h.computeNewHealthStatus(req)
	newHealth := &healthStatusWithCode{
		statusCode: newHealthStatusCode,
		checks:     metadata.Checks,
	}
	previousHealth := h.previousHealth.Load()
	if previousHealth != nil {
		if previousHealthTyped, ok := previousHealth.(*healthStatusWithCode); ok {
			logIfHealthChanged(req.Context(), previousHealthTyped, newHealth)
		}
	}

	h.previousHealth.Store(newHealth)

	rest.WriteJSONResponse(w, metadata, newHealthStatusCode)
}

func (h *healthHandlerImpl) computeNewHealthStatus(req *http.Request) (health.HealthStatus, int) {
	if sharedSecret := h.healthCheckSharedSecret.CurrentString(); sharedSecret != "" {
		token, err := rest.ParseBearerTokenHeader(req)
		if err != nil || sharedSecret != token {
			return health.HealthStatus{}, http.StatusUnauthorized
		}
	}
	metadata := h.check.HealthStatus(req.Context())
	return metadata, healthStatusCode(metadata)
}

func healthStatusCode(metadata health.HealthStatus) int {
	worst := http.StatusOK
	for _, result := range metadata.Checks {
		code := HealthStateStatusCodes[result.State]
		if worst < code {
			worst = code
		}
	}
	return worst
}

func logIfHealthChanged(ctx context.Context, previousHealth, newHealth *healthStatusWithCode) {
	if previousHealth.statusCode != newHealth.statusCode {
		params := map[string]interface{}{
			"previousHealthStatusCode": previousHealth.statusCode,
			"newHealthStatusCode":      newHealth.statusCode,
			"newHealthStatus":          newHealth.checks,
		}
		if newHealth.statusCode == http.StatusOK {
			svc1log.FromContext(ctx).Info("Health status code changed.", svc1log.SafeParams(params))
		} else {
			svc1log.FromContext(ctx).Error("Health status code changed.", svc1log.SafeParams(params))
		}
		return
	} else if checksDiffer(previousHealth.checks, newHealth.checks) {
		svc1log.FromContext(ctx).Info("Health checks content changed without status change.", svc1log.SafeParams(map[string]interface{}{
			"statusCode":      newHealth.statusCode,
			"newHealthStatus": newHealth.checks,
		}))
	}
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
