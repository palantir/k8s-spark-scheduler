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
	"fmt"
	"net/http"
	netpprof "net/http/pprof"
	"runtime/pprof"

	"github.com/palantir/pkg/metrics"
	"github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-server/config"
	"github.com/palantir/witchcraft-go-server/status"
	"github.com/palantir/witchcraft-go-server/status/routes"
	"github.com/palantir/witchcraft-go-server/witchcraft/internal/middleware"
	"github.com/palantir/witchcraft-go-server/witchcraft/refreshable"
	"github.com/palantir/witchcraft-go-server/witchcraft/wresource"
	"github.com/palantir/witchcraft-go-server/wrouter"
	"github.com/palantir/witchcraft-go-server/wrouter/whttprouter"
	"github.com/palantir/witchcraft-go-tracing/wtracing"
)

func (s *Server) initRouters(installCfg config.Install) (rRouter wrouter.Router, rMgmtRouter wrouter.Router) {
	routerWithContextPath := createRouter(whttprouter.New(), installCfg.Server.ContextPath)
	mgmtRouterWithContextPath := routerWithContextPath
	if mgmtPort := installCfg.Server.ManagementPort; mgmtPort != 0 && mgmtPort != installCfg.Server.Port {
		mgmtRouterWithContextPath = createRouter(whttprouter.New(), installCfg.Server.ContextPath)
	}
	return routerWithContextPath, mgmtRouterWithContextPath
}

func (s *Server) addRoutes(mgmtRouterWithContextPath wrouter.Router, runtimeCfg refreshableBaseRuntimeConfig) error {
	// add debugging endpoint to management router
	if err := addDebuggingRoutes(mgmtRouterWithContextPath); err != nil {
		return werror.Wrap(err, "failed to register debugging routes")
	}

	statusResource := wresource.New("status", mgmtRouterWithContextPath)

	// add health endpoints
	if err := routes.AddHealthRoutes(statusResource, status.NewCombinedHealthCheckSource(append(s.healthCheckSources, &s.stateManager)...), refreshable.NewString(runtimeCfg.Map(func(in interface{}) interface{} {
		return in.(config.Runtime).HealthChecks.SharedSecret
	}))); err != nil {
		return werror.Wrap(err, "failed to register health routes")
	}

	// add liveness endpoints
	if s.livenessSource == nil {
		s.livenessSource = &s.stateManager
	}
	if err := routes.AddLivenessRoutes(statusResource, s.livenessSource); err != nil {
		return werror.Wrap(err, "failed to register liveness routes")
	}

	// add readiness endpoints
	if s.readinessSource == nil {
		s.readinessSource = &s.stateManager
	}
	if err := routes.AddReadinessRoutes(statusResource, s.readinessSource); err != nil {
		return werror.Wrap(err, "failed to register readiness routes")
	}
	return nil
}

func (s *Server) addMiddleware(rootRouter wrouter.RootRouter, registry metrics.RootRegistry, tracerOptions []wtracing.TracerOption) {
	rootRouter.AddRequestHandlerMiddleware(
		// add middleware that recovers from panics
		middleware.NewRequestPanicRecovery(),
		// add middleware that injects loggers into request context
		middleware.NewRequestContextLoggers(
			s.svcLogger,
			s.evtLogger,
			s.auditLogger,
			s.metricLogger,
		),
		// add middleware that extracts UID, SID, and TokenID into context for loggers, sets a tracer on the context and
		// starts a root span and sets it on the context.
		middleware.NewRequestExtractIDs(
			s.svcLogger,
			s.trcLogger,
			tracerOptions,
			s.idsExtractor,
		),
	)

	// add middleware that records HTTP request stats as metrics in registry
	rootRouter.AddRequestHandlerMiddleware(middleware.NewRequestMetricRequestMeter(registry))

	// add user-provided middleware
	rootRouter.AddRequestHandlerMiddleware(s.handlers...)

	// add route middleware
	rootRouter.AddRouteHandlerMiddleware(middleware.NewRouteRequestLog(s.reqLogger, nil))
	rootRouter.AddRouteHandlerMiddleware(middleware.NewRouteLogTraceSpan())
}

func createRouter(routerImpl wrouter.RouterImpl, ctxPath string) wrouter.Router {
	routerHandler := wrouter.New(routerImpl)

	var routerWithContextPath wrouter.Router
	routerWithContextPath = routerHandler
	if ctxPath != "/" {
		// only create subrouter if context path is non-empty
		routerWithContextPath = routerHandler.Subrouter(ctxPath)
	}
	return routerWithContextPath
}

func addDebuggingRoutes(router wrouter.Router) error {
	debugger := wresource.New("debug", router.Subrouter("/debug"))
	if err := debugger.Get("pprofIndex", "/pprof/", http.HandlerFunc(netpprof.Index)); err != nil {
		return err
	}
	if err := debugger.Get("pprofCmdLine", "/pprof/cmdline", http.HandlerFunc(netpprof.Cmdline)); err != nil {
		return err
	}
	if err := debugger.Get("pprofCpuProfile", "/pprof/profile", http.HandlerFunc(netpprof.Profile)); err != nil {
		return err
	}
	if err := debugger.Get("pprofSymbol", "/pprof/symbol", http.HandlerFunc(netpprof.Symbol)); err != nil {
		return err
	}
	if err := debugger.Get("pprofTrace", "/pprof/trace", http.HandlerFunc(netpprof.Trace)); err != nil {
		return err
	}
	return debugger.Get("pprofHeapProfile", "/pprof/heap", http.HandlerFunc(heap))
}

// heap responds with the pprof-formatted heap profile.
func heap(w http.ResponseWriter, _ *http.Request) {
	// Set Content Type assuming WriteHeapProfile will work,
	// because if it does it starts writing.
	w.Header().Set("Content-Type", "application/octet-stream")
	if err := pprof.WriteHeapProfile(w); err != nil {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintf(w, "Could not dump heap: %s\n", err)
		return
	}
}
