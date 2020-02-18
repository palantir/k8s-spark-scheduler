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
	"context"
	"net/http"
	"strconv"
	"sync/atomic"

	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-server/conjure/witchcraft/api/health"
)

type ServerState int32

const (
	ServerIdle ServerState = iota
	ServerInitializing
	ServerRunning
)

func (s ServerState) String() string {
	switch s {
	case ServerIdle:
		return "idle"
	case ServerInitializing:
		return "initializing"
	case ServerRunning:
		return "running"
	default:
		return "unknown state: " + strconv.Itoa(int(s))
	}
}

type serverStateManager struct {
	serverRunning int32
}

func (s *serverStateManager) Start() error {
	// state went from Idle to Initializing: OK
	if atomic.CompareAndSwapInt32(&s.serverRunning, int32(ServerIdle), int32(ServerInitializing)) {
		return nil
	}

	// error if current state is not idle/failed to transition to initializing
	switch s.State() {
	case ServerInitializing:
		return werror.Error("server is already initializing and must be stopped before it can be started again")
	case ServerRunning:
		return werror.Error("server is already running and must be stopped before it can be started again")
	default:
		return werror.Error("server is in an unknown state and must be stopped before it can be started again")
	}
}

func (s *serverStateManager) Running() bool {
	return s.State() == ServerRunning
}

func (s *serverStateManager) State() ServerState {
	return ServerState(atomic.LoadInt32(&s.serverRunning))
}

func (s *serverStateManager) setState(state ServerState) {
	atomic.StoreInt32(&s.serverRunning, int32(state))
}

func (s *serverStateManager) Status() (int, interface{}) {
	if !s.Running() {
		return http.StatusServiceUnavailable, nil
	}
	return http.StatusOK, nil
}

func (s *serverStateManager) HealthStatus(ctx context.Context) health.HealthStatus {
	state := health.HealthStateHealthy
	if !s.Running() {
		state = health.HealthStateTerminal
	}
	return health.HealthStatus{
		Checks: map[health.CheckType]health.HealthCheckResult{
			"SERVER_STATUS": {
				Type:  "SERVER_STATUS",
				State: state,
			},
		},
	}
}
