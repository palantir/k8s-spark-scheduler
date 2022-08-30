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
	"crypto/tls"
	"io"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"github.com/palantir/go-encrypted-config-value/encryptedconfigvalue"
	"github.com/palantir/pkg/metrics"
	"github.com/palantir/pkg/signals"
	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/conjure/witchcraft/api/logging"
	"github.com/palantir/witchcraft-go-logging/wlog"
	"github.com/palantir/witchcraft-go-logging/wlog/auditlog/audit2log"
	"github.com/palantir/witchcraft-go-logging/wlog/diaglog/diag1log"
	"github.com/palantir/witchcraft-go-logging/wlog/evtlog/evt2log"
	"github.com/palantir/witchcraft-go-logging/wlog/extractor"
	"github.com/palantir/witchcraft-go-logging/wlog/metriclog/metric1log"
	"github.com/palantir/witchcraft-go-logging/wlog/reqlog/req2log"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-logging/wlog/trclog/trc1log"
	"github.com/palantir/witchcraft-go-logging/wlog/wapp"
	"github.com/palantir/witchcraft-go-server/config"
	"github.com/palantir/witchcraft-go-server/status"
	refreshablehealth "github.com/palantir/witchcraft-go-server/status/health/refreshable"
	"github.com/palantir/witchcraft-go-server/witchcraft/refreshable"
	"github.com/palantir/witchcraft-go-server/wrouter"
	"github.com/palantir/witchcraft-go-server/wrouter/whttprouter"
	"github.com/palantir/witchcraft-go-tracing/wtracing"
	"github.com/palantir/witchcraft-go-tracing/wzipkin"
	"gopkg.in/yaml.v2"

	// Use zap as logger implementation: witchcraft-based applications are opinionated about the logging implementation used
	_ "github.com/palantir/witchcraft-go-logging/wlog-zap"
)

type Server struct {
	// handlers specifies any custom HTTP handlers that should be used by the server. The provided handlers are invoked
	// in order after the built-in handlers (which provide things such as panic handling). The context in the request
	// will have the appropriate loggers and logger parameters set.
	handlers []wrouter.RequestHandlerMiddleware

	// useSelfSignedServerCertificate specifies whether the server uses a dynamically generated self-signed certificate
	// for TLS. No verification mechanism is provided for the self-signed certificate, so clients can only connect to a
	// server using this mode in an untrusted manner. As such, this option should only be used in very specialized
	// scenarios such as tests or in an environment where the server is exposed in a way that the connection to it can
	// be trusted based on other external mechanisms (in the latter scenario, using HTTPS with an unverified certificate
	// still provides the benefit that the traffic itself is encrypted).
	//
	// If false, the key material at the paths specified in serverConfig.CertFile and serverConfig.KeyFile is used.
	useSelfSignedServerCertificate bool

	// manages storing and retrieving server state (idle, initializing, running)
	stateManager serverStateManager

	// specifies the io.Writer to which goroutine dump will be written if a SIGQUIT is received while the server is
	// running. If nil, os.Stdout is used as the default. If the value is ioutil.Discard, then no plaintext output will
	// be emitted. A diagnostic.1 line is logged unless disableSigQuitHandler is true.
	sigQuitHandlerWriter io.Writer

	// if true, disables the default behavior of emitting a goroutine dump on SIGQUIT signals.
	disableSigQuitHandler bool

	// if true, disables the default behavior of shutting down the server on SIGTERM and SIGINT signals.
	disableShutdownSignalHandler bool

	// provides the bytes for the install configuration for the server. If nil, a default configuration provider that
	// reads the file at "var/conf/install.yml" is used.
	installConfigProvider ConfigBytesProvider

	// a function that provides the refreshable.Refreshable that provides the bytes for the runtime configuration for
	// the server. The ctx provided to the function is valid for the lifetime of the server. If nil, uses a function
	// that returns a default file-based Refreshable that reads the file at "var/conf/runtime.yml". The value of the
	// Refreshable is "[]byte", where the byte slice is the contents of the runtime configuration file.
	runtimeConfigProvider func(ctx context.Context) (refreshable.Refreshable, error)

	// specifies the source used to provide the readiness information for the server. If nil, a default value that uses
	// the server's status is used.
	readinessSource status.Source

	// specifies the source used to provide the liveness information for the server. If nil, a default value that uses
	// the server's status is used.
	livenessSource status.Source

	// specifies the sources that are used to determine the health of this service
	healthCheckSources []status.HealthCheckSource

	// specifies the handlers to invoke upon health status changes. The LoggingHealthStatusChangeHandler is added by default.
	healthStatusChangeHandlers []status.HealthStatusChangeHandler

	// provides the RouterImpl used by the server (and management server if it is separate). If nil, a default function
	// that returns a new whttprouter is used.
	routerImplProvider func() wrouter.RouterImpl

	// called on server initialization before the server starts. Is provided with a context that is active for the
	// duration of the server lifetime, the server router (which can be used to register endpoints), the unmarshaled
	// install configuration and the refreshable runtime configuration.
	//
	// If this function returns an error, the server is not started and the error is returned.
	initFn InitFunc

	// installConfigStruct is a concrete struct used to determine the type into which the install configuration bytes
	// are unmarshaled. If nil, a default value of config.Install{} is used.
	installConfigStruct interface{}

	// runtimeConfigStruct is a concrete struct used to determine the type into which the runtime configuration bytes
	// are unmarshaled. If nil, a default value of config.Runtime{} is used.
	runtimeConfigStruct interface{}

	// provides the encrypted-config-value key that is used to decrypt encrypted values in configuration. If nil, a
	// default provider that reads the key from the file at "var/conf/encrypted-config-value.key" is used.
	ecvKeyProvider ECVKeyProvider

	// if true, then Go runtime metrics will not be recorded. If false, Go runtime metrics will be recorded at a
	// collection interval that matches the metric emit interval specified in the install configuration (or every 60
	// seconds if an interval is not specified in configuration).
	disableGoRuntimeMetrics bool

	// metricsBlacklist specifies the set of metrics that should not be emitted by the metric logger.
	metricsBlacklist map[string]struct{}

	// metricTypeValuesBlacklist specifies the values for a metric type that should be omitted from metric output. For
	// example, if the map is set to {"timer":{"5m":{}}}, then the value for "5m" will be omitted from all timer metric
	// output. If nil, the default value is the map returned by defaultMetricTypeValuesBlacklist().
	metricTypeValuesBlacklist map[string]map[string]struct{}

	// specifies the TLS client authentication mode used by the server. If not specified, the default value is
	// tls.NoClientCert.
	clientAuth tls.ClientAuthType

	// specifies the value used for the "origin" field for service logs. If not specified, the default value is set to
	// be the package from which "Start" was called.
	svcLogOrigin *string

	// specifies that the service.1 logger should use the call site for the origin field.
	// See docs on svc1log.OriginFromCallLine for details.
	svcLogOriginFromCallLine bool

	// applicationTraceSampler is the function that is used to determine whether or not a trace should be sampled.
	// This applies to routes registered under the application port and the context passed to the initialize function
	// If nil, the default behavior is to sample every trace.
	applicationTraceSampler wtracing.Sampler

	// managementTraceSampler is the function that is used to determine whether or not a trace should be sampled.
	// This applies to routes registered under the management port
	// If nil, the default behavior is to sample no traces.
	managementTraceSampler wtracing.Sampler

	// disableKeepAlives disables keep-alives.
	disableKeepAlives bool

	// configYAMLUnmarshalFn is the function used to unmarshal YAML configuration. By default, this is yaml.Unmarshal.
	// If WithStrictUnmarshalConfig is called, this is set to yaml.UnmarshalStrict.
	configYAMLUnmarshalFn func(in []byte, out interface{}) (err error)

	// request logger configuration

	// idsExtractor specifies the extractor used to extract identifiers (such as UID, SID, TokenID) from requests for
	// request logging and middleware. If nil, uses extractor.NewDefaultIDsExtractor().
	idsExtractor     extractor.IDsFromRequest
	safePathParams   []string
	safeQueryParams  []string
	safeHeaderParams []string

	// loggerStdoutWriter specifies the io.Writer that is written to if the loggers are in a mode that specifies that
	// they should write to Stdout. If nil, os.Stdout is used by default.
	loggerStdoutWriter io.Writer

	// loggers
	svcLogger    svc1log.Logger
	evtLogger    evt2log.Logger
	auditLogger  audit2log.Logger
	metricLogger metric1log.Logger
	trcLogger    trc1log.Logger
	diagLogger   diag1log.Logger
	reqLogger    req2log.Logger

	// the http.Server for the main server
	httpServer *http.Server

	// allows the server to wait until Close() or Shutdown() return prior to returning from Start()
	shutdownFinished sync.WaitGroup
}

// InitFunc is a function type used to initialize a server. ctx is a context configured with loggers and is valid for
// the duration of the server. Refer to the documentation of InitInfo for its fields.
//
// If the returned cleanup function is non-nil, it is deferred and run on server shutdown. If the returned error is
// non-nil, the server will not start and will return the error.
type InitFunc func(ctx context.Context, info InitInfo) (cleanup func(), rErr error)

type InitInfo struct {
	// Router is a ConfigurableRouter that implements wrouter.Router for the server. It can be
	// used to register endpoints on the server and to configure things such as health, readiness and liveness sources and
	// any middleware (note that any values set using router will override any values previously set on the server).
	Router ConfigurableRouter

	// InstallConfig the install configuration. Its type is determined by the struct provided to the
	// "WithInstallConfigType" function (the default is config.Install).
	InstallConfig interface{}

	// RuntimeConfig is a refreshable that contains the initial runtime configuration. The type returned by the
	// refreshable is determined by the struct provided to the "WithRuntimeConfigType" function (the default is
	// config.Runtime).
	RuntimeConfig refreshable.Refreshable

	// ShutdownServer gracefully closes the server, waiting for any in-flight requests to finish (or the context to be cancelled).
	// When the InitFunc is executed, the server is not yet started. This will most often be useful if launching a goroutine which
	// requires access to shutdown the server in some error condition.
	ShutdownServer func(context.Context) error
}

// ConfigurableRouter is a wrouter.Router that provides additional support for configuring things such as health,
// readiness, liveness and middleware.
type ConfigurableRouter interface {
	wrouter.Router

	WithHealth(healthSources ...status.HealthCheckSource) *Server
	WithReadiness(readiness status.Source) *Server
	WithLiveness(liveness status.Source) *Server
}

const defaultSampleRate = 0.01

// NewServer returns a new uninitialized server.
func NewServer() *Server {
	return &Server{}
}

// WithInitFunc configures the server to use the provided setup function to set up its initial state.
func (s *Server) WithInitFunc(initFn InitFunc) *Server {
	s.initFn = initFn
	return s
}

// WithInstallConfigType configures the server to use the type of the provided struct as the type for the install
// configuration. The YAML representation of the install configuration is unmarshaled into a newly created struct that
// has the same type as the provided struct, so the provided struct should either embed or be compatible with
// config.Install.
func (s *Server) WithInstallConfigType(installConfigStruct interface{}) *Server {
	s.installConfigStruct = installConfigStruct
	return s
}

// WithRuntimeConfigType configures the server to use the type of the provided struct as the type for the runtime
// configuration. The YAML representation of the runtime configuration is unmarshaled into a newly created struct that
// has the same type as the provided struct, so the provided struct should either embed or be compatible with
// config.Runtime.
func (s *Server) WithRuntimeConfigType(runtimeConfigStruct interface{}) *Server {
	s.runtimeConfigStruct = runtimeConfigStruct
	return s
}

// WithInstallConfig configures the server to use the provided install configuration. The provided install configuration
// must support being marshaled as YAML.
func (s *Server) WithInstallConfig(installConfigStruct interface{}) *Server {
	s.installConfigProvider = cfgBytesProviderFn(func() ([]byte, error) {
		return yaml.Marshal(installConfigStruct)
	})
	return s
}

// WithInstallConfigFromFile configures the server to read the install configuration from the file at the specified
// path.
func (s *Server) WithInstallConfigFromFile(fpath string) *Server {
	s.installConfigProvider = cfgBytesProviderFn(func() ([]byte, error) {
		return ioutil.ReadFile(fpath)
	})
	return s
}

// WithInstallConfigProvider configures the server to use the install configuration obtained by reading the bytes from
// the specified ConfigBytesProvider.
func (s *Server) WithInstallConfigProvider(p ConfigBytesProvider) *Server {
	s.installConfigProvider = p
	return s
}

// WithRuntimeConfig configures the server to use the provided runtime configuration. The provided runtime configuration
// must support being marshaled as YAML.
func (s *Server) WithRuntimeConfig(in interface{}) *Server {
	s.runtimeConfigProvider = func(_ context.Context) (refreshable.Refreshable, error) {
		runtimeCfgYAML, err := yaml.Marshal(in)
		if err != nil {
			return nil, err
		}
		return refreshable.NewDefaultRefreshable(runtimeCfgYAML), nil
	}
	return s
}

// WithRuntimeConfigProvider configures the server to use the provided Refreshable as its runtime configuration. The
// value provided by the refreshable must be the byte slice for the runtime configuration.
func (s *Server) WithRuntimeConfigProvider(r refreshable.Refreshable) *Server {
	s.runtimeConfigProvider = func(_ context.Context) (refreshable.Refreshable, error) {
		return r, nil
	}
	return s
}

// WithRuntimeConfigFromFile configures the server to use the file at the provided path as its runtime configuration.
// The server will create a refreshable.Refreshable using the file at the provided path (and will thus live-reload the
// configuration based on updates to the file).
func (s *Server) WithRuntimeConfigFromFile(fpath string) *Server {
	s.runtimeConfigProvider = func(ctx context.Context) (refreshable.Refreshable, error) {
		return refreshable.NewFileRefreshable(ctx, fpath)
	}
	return s
}

// WithSelfSignedCertificate configures the server to use a dynamically generated self-signed certificate for its TLS
// authentication. Because there is no way to verify the certificate used by the server, this option is typically only
// used in tests or very specialized circumstances where the connection to the server can be verified/authenticated
// using separate external mechanisms.
func (s *Server) WithSelfSignedCertificate() *Server {
	s.useSelfSignedServerCertificate = true
	return s
}

// WithECVKeyFromFile configures the server to use the ECV key in the file at the specified path as the ECV key for
// decrypting ECV values in configuration.
func (s *Server) WithECVKeyFromFile(fPath string) *Server {
	s.ecvKeyProvider = ECVKeyFromFile(fPath)
	return s
}

// WithECVKeyProvider configures the server to use the ECV key provided by the specified provider as the ECV key for
// decrypting ECV values in configuration.
func (s *Server) WithECVKeyProvider(ecvProvider ECVKeyProvider) *Server {
	s.ecvKeyProvider = ecvProvider
	return s
}

// WithClientAuth configures the server to use the specified client authentication type for its TLS connections.
func (s *Server) WithClientAuth(clientAuth tls.ClientAuthType) *Server {
	s.clientAuth = clientAuth
	return s
}

// WithHealth configures the server to use the specified health check sources to report the server's health. If multiple
// healthSource's results have the same key, the result from the latest entry in healthSources will be used. These
// results are combined with the server's built-in health source, which uses the `SERVER_STATUS` key.
func (s *Server) WithHealth(healthSources ...status.HealthCheckSource) *Server {
	s.healthCheckSources = healthSources
	return s
}

// WithReadiness configures the server to use the specified source to report readiness.
func (s *Server) WithReadiness(readiness status.Source) *Server {
	s.readinessSource = readiness
	return s
}

// WithLiveness configures the server to use the specified source to report liveness.
func (s *Server) WithLiveness(liveness status.Source) *Server {
	s.livenessSource = liveness
	return s
}

// WithOrigin configures the server to use the specified origin.
func (s *Server) WithOrigin(origin string) *Server {
	s.svcLogOrigin = &origin
	return s
}

// WithOriginFromCallLine configures the server to use the svc1log.OriginFromCallLine parameter.
func (s *Server) WithOriginFromCallLine() *Server {
	s.svcLogOriginFromCallLine = true
	return s
}

// WithMiddleware configures the server to use the specified middleware. The provided middleware is added to any other
// specified middleware.
func (s *Server) WithMiddleware(middleware wrouter.RequestHandlerMiddleware) *Server {
	s.handlers = append(s.handlers, middleware)
	return s
}

// WithRouterImplProvider configures the server to use the specified routerImplProvider to provide router
// implementations.
func (s *Server) WithRouterImplProvider(routerImplProvider func() wrouter.RouterImpl) *Server {
	s.routerImplProvider = routerImplProvider
	return s
}

// WithTraceSampler configures the server's application trace log tracer to use the specified traceSampler function to make a
// determination on whether or not a trace should be sampled (if such a decision needs to be made).
func (s *Server) WithTraceSampler(traceSampler wtracing.Sampler) *Server {
	s.applicationTraceSampler = traceSampler
	return s
}

// WithTraceSamplerRate is a convenience function for creating an application traceSampler based off a sample rate
func (s *Server) WithTraceSamplerRate(sampleRate float64) *Server {
	return s.WithTraceSampler(traceSamplerFromSampleRate(sampleRate))
}

// WithManagementTraceSampler configures the server's management trace log tracer to use the specified traceSampler function to make a
// determination on whether or not a trace should be sampled (if such a decision needs to be made).
func (s *Server) WithManagementTraceSampler(traceSampler wtracing.Sampler) *Server {
	s.managementTraceSampler = traceSampler
	return s
}

// WithManagementTraceSamplerRate is a convenience function for creating a management traceSampler based off a sample rate
func (s *Server) WithManagementTraceSamplerRate(sampleRate float64) *Server {
	return s.WithManagementTraceSampler(traceSamplerFromSampleRate(sampleRate))
}

// WithSigQuitHandlerWriter sets the output for the goroutine dump on SIGQUIT.
func (s *Server) WithSigQuitHandlerWriter(w io.Writer) *Server {
	s.sigQuitHandlerWriter = w
	return s
}

// WithDisableSigQuitHandler disables the server's enabled-by-default goroutine dump on SIGQUIT.
func (s *Server) WithDisableSigQuitHandler() *Server {
	s.disableSigQuitHandler = true
	return s
}

// WithDisableShutdownSignalHandler disables the server's enabled-by-default shutdown on SIGTERM and SIGINT.
func (s *Server) WithDisableShutdownSignalHandler() *Server {
	s.disableShutdownSignalHandler = true
	return s
}

// WithDisableKeepAlives disables keep-alives on the server by calling SetKeepAlivesEnabled(false) on the http.Server
// used by the server. Note that this setting is only applied to the main server -- if the management server is separate
// from the main server, this setting is not applied to the management server. Refer to the documentation for
// SetKeepAlivesEnabled in http.Server for more information on when a server may want to use this setting.
func (s *Server) WithDisableKeepAlives() *Server {
	s.disableKeepAlives = true
	return s
}

// WithStrictUnmarshalConfig configures the server to use the provided strict unmarshal configuration.
func (s *Server) WithStrictUnmarshalConfig() *Server {
	s.configYAMLUnmarshalFn = yaml.UnmarshalStrict
	return s
}

// WithDisableGoRuntimeMetrics disables the server's enabled-by-default collection of runtime memory statistics.
func (s *Server) WithDisableGoRuntimeMetrics() *Server {
	s.disableGoRuntimeMetrics = true
	return s
}

// WithMetricsBlacklist sets the metric blacklist to the provided set of metrics. The provided metrics should be the
// name of the metric (for example, "server.response.size"). The blacklist only supports blacklisting at the metric
// level: blacklisting an individual metric value (such as "server.response.size.count") will not have any effect. The
// provided input is copied.
func (s *Server) WithMetricsBlacklist(blacklist map[string]struct{}) *Server {
	metricsBlacklist := make(map[string]struct{})
	for k, v := range blacklist {
		metricsBlacklist[k] = v
	}
	s.metricsBlacklist = metricsBlacklist
	return s
}

// WithMetricTypeValuesBlacklist sets the value of the metric type value blacklist to be the same as the provided value
// (the content is copied).
func (s *Server) WithMetricTypeValuesBlacklist(blacklist map[string]map[string]struct{}) *Server {
	newBlacklist := make(map[string]map[string]struct{}, len(blacklist))
	for k, v := range blacklist {
		newVal := make(map[string]struct{}, len(v))
		for kk := range v {
			newVal[kk] = struct{}{}
		}
		newBlacklist[k] = newVal
	}
	s.metricTypeValuesBlacklist = newBlacklist
	return s
}

// WithLoggerStdoutWriter configures the writer that loggers will write to IF they are configured to write to STDOUT.
// This configuration is typically only used in specialized scenarios (for example, to write logger output to an
// in-memory buffer rather than Stdout for tests).
func (s *Server) WithLoggerStdoutWriter(loggerStdoutWriter io.Writer) *Server {
	s.loggerStdoutWriter = loggerStdoutWriter
	return s
}

// WithHealthStatusChangeHandlers configures the health status change handlers that are called whenever the configured HealthCheckSource
// returns a health status with differing check states.
func (s *Server) WithHealthStatusChangeHandlers(handlers ...status.HealthStatusChangeHandler) *Server {
	s.healthStatusChangeHandlers = append(s.healthStatusChangeHandlers, handlers...)
	return s
}

const (
	defaultMetricEmitFrequency = time.Second * 60

	ecvKeyPath        = "var/conf/encrypted-config-value.key"
	installConfigPath = "var/conf/install.yml"
	runtimeConfigPath = "var/conf/runtime.yml"

	runtimeConfigReloadCheckType = "CONFIG_RELOAD"
)

// Start begins serving HTTPS traffic and blocks until s.Close() or s.Shutdown() return.
// Errors are logged via s.svcLogger before being returned.
// Panics are recovered; in the case of a recovered panic, Start will log and return
// a non-nil error containing the recovered object (overwriting any existing error).
func (s *Server) Start() (rErr error) {
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				rErr = err
			} else {
				rErr = werror.Error("panic recovered", werror.UnsafeParam("recovered", r))
			}

			if s.svcLogger == nil {
				// If we have not yet initialized our loggers, use default configuration as best-effort.
				s.initLoggers(false, wlog.InfoLevel, metrics.DefaultMetricsRegistry)
			}

			s.svcLogger.Error("panic recovered", svc1log.SafeParam("stack", diag1log.ThreadDumpV1FromGoroutines(debug.Stack())), svc1log.Stacktrace(rErr))
		}
	}()
	defer func() {
		if rErr != nil {
			if s.svcLogger == nil {
				// If we have not yet initialized our loggers, use default configuration as best-effort.
				s.initLoggers(false, wlog.InfoLevel, metrics.DefaultMetricsRegistry)
			}
			s.svcLogger.Error(rErr.Error(), svc1log.Stacktrace(rErr))
		}
	}()

	// Set state to "initializing". Fails if current state is not "idle" (ensures that this instance is not being run
	// concurrently).
	if err := s.stateManager.Start(); err != nil {
		return err
	}
	// Reset state if server terminated without calling s.Close() or s.Shutdown()
	defer func() {
		if s.State() != ServerIdle {
			s.stateManager.setState(ServerIdle)
		}
	}()

	// set provider for ECV key
	if s.ecvKeyProvider == nil {
		s.ecvKeyProvider = ECVKeyFromFile(ecvKeyPath)
	}

	// if config unmarshal function is not set, default to yaml.Unmarshal
	if s.configYAMLUnmarshalFn == nil {
		s.configYAMLUnmarshalFn = yaml.Unmarshal
	}

	// load install configuration
	baseInstallCfg, fullInstallCfg, err := s.initInstallConfig()
	if err != nil {
		return err
	}

	if s.idsExtractor == nil {
		s.idsExtractor = extractor.NewDefaultIDsExtractor()
	}

	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

	// initialize metrics. Note that loggers have not been initialized or associated with ctx
	metricsRegistry, metricsDeferFn, err := s.initMetrics(ctx, baseInstallCfg)
	if err != nil {
		return err
	}
	defer metricsDeferFn()
	ctx = metrics.WithRegistry(ctx, metricsRegistry)

	// initialize loggers
	s.initLoggers(baseInstallCfg.UseConsoleLog, wlog.InfoLevel, metricsRegistry)

	// add loggers to context
	ctx = s.withLoggers(ctx)

	// load runtime configuration
	baseRefreshableRuntimeCfg, refreshableRuntimeCfg, configReloadHealthCheckSource, err := s.initRuntimeConfig(ctx)
	if err != nil {
		return err
	}

	if loggerCfg := baseRefreshableRuntimeCfg.CurrentBaseRuntimeConfig().LoggerConfig; loggerCfg != nil {
		s.svcLogger.SetLevel(loggerCfg.Level)
	}

	if s.routerImplProvider == nil {
		s.routerImplProvider = func() wrouter.RouterImpl {
			return whttprouter.New()
		}
	}

	// initialize routers
	router, mgmtRouter := s.initRouters(baseInstallCfg)

	// add middleware
	s.addMiddleware(router.RootRouter(), metricsRegistry, s.getApplicationTracingOptions(baseInstallCfg))
	if mgmtRouter != router {
		// add middleware to management router as well if it is distinct
		s.addMiddleware(mgmtRouter.RootRouter(), metricsRegistry, s.getManagementTracingOptions(baseInstallCfg))
	}

	// handle built-in runtime config changes
	unsubscribe := baseRefreshableRuntimeCfg.Map(func(in interface{}) interface{} {
		return in.(config.Runtime).LoggerConfig
	}).Subscribe(func(in interface{}) {
		if loggerCfg := in.(*config.LoggerConfig); loggerCfg != nil {
			s.svcLogger.SetLevel(loggerCfg.Level)
		}
	})
	defer unsubscribe()

	s.initStackTraceHandler(ctx)
	s.initShutdownSignalHandler(ctx)

	// wait for s.Close() or s.Shutdown() to return if called
	defer s.shutdownFinished.Wait()

	if s.initFn != nil {
		traceReporter := wtracing.NewNoopReporter()
		if s.trcLogger != nil {
			traceReporter = s.trcLogger
		}
		tracer, err := wzipkin.NewTracer(traceReporter, s.getApplicationTracingOptions(baseInstallCfg)...)
		if err != nil {
			return err
		}
		ctx = wtracing.ContextWithTracer(ctx, tracer)

		svc1log.FromContext(ctx).Debug("Running server initialization function.")
		cleanupFn, err := s.initFn(
			ctx,
			InitInfo{
				Router: &configurableRouterImpl{
					Router: newMultiRouterImpl(router, mgmtRouter),
					Server: s,
				},
				InstallConfig:  fullInstallCfg,
				RuntimeConfig:  refreshableRuntimeCfg,
				ShutdownServer: s.Shutdown,
			},
		)
		if err != nil {
			return err
		}
		if cleanupFn != nil {
			defer cleanupFn()
		}
	}

	// add routes for health, liveness and readiness. Must be done after initFn to ensure that any
	// health/liveness/readiness configuration updated by initFn is applied. Includes the
	// configReloadHealthCheckSource, which is always appended to s.healthCheckSources.
	if err := s.addRoutes(mgmtRouter, baseRefreshableRuntimeCfg, configReloadHealthCheckSource); err != nil {
		return err
	}

	// only create and start a separate management http server if management port is explicitly specified and differs
	// from the main server port
	if mgmtPort := baseInstallCfg.Server.ManagementPort; mgmtPort != 0 && baseInstallCfg.Server.Port != mgmtPort {
		mgmtStart, mgmtShutdown, err := s.newMgmtServer(baseInstallCfg.ProductName, baseInstallCfg.Server, mgmtRouter.RootRouter())
		if err != nil {
			return err
		}

		// start management server in its own goroutine
		go wapp.RunWithRecoveryLogging(ctx, func(ctx context.Context) {
			if err := mgmtStart(); err != nil {
				svc1log.FromContext(ctx).Error("management server failed", svc1log.Stacktrace(err))
			}
		})
		defer func() {
			if err := mgmtShutdown(ctx); err != nil {
				svc1log.FromContext(ctx).Error("management server failed to shutdown", svc1log.Stacktrace(err))
			}
		}()
	}

	httpServer, svrStart, _, err := s.newServer(baseInstallCfg.ProductName, baseInstallCfg.Server, router.RootRouter())
	if err != nil {
		return err
	}

	s.httpServer = httpServer
	if s.disableKeepAlives {
		s.httpServer.SetKeepAlivesEnabled(false)
	}

	s.stateManager.setState(ServerRunning)
	return svrStart()
}

func (s *Server) withLoggers(ctx context.Context) context.Context {
	ctx = svc1log.WithLogger(ctx, s.svcLogger)
	ctx = evt2log.WithLogger(ctx, s.evtLogger)
	ctx = metric1log.WithLogger(ctx, s.metricLogger)
	ctx = trc1log.WithLogger(ctx, s.trcLogger)
	ctx = audit2log.WithLogger(ctx, s.auditLogger)
	ctx = diag1log.WithLogger(ctx, s.diagLogger)
	return ctx
}

type configurableRouterImpl struct {
	wrouter.Router
	*Server
}

func (s *Server) initInstallConfig() (config.Install, interface{}, error) {
	if s.installConfigProvider == nil {
		// if install config provider is not specified, use a file-based one
		s.installConfigProvider = cfgBytesProviderFn(func() ([]byte, error) {
			return ioutil.ReadFile(installConfigPath)
		})
	}

	cfgBytes, err := s.installConfigProvider.LoadBytes()
	if err != nil {
		return config.Install{}, nil, werror.Wrap(err, "Failed to load install configuration bytes")
	}
	cfgBytes, err = s.decryptConfigBytes(cfgBytes)
	if err != nil {
		return config.Install{}, nil, werror.Wrap(err, "Failed to decrypt install configuration bytes")
	}

	var baseInstallCfg config.Install
	if err := yaml.Unmarshal(cfgBytes, &baseInstallCfg); err != nil {
		return config.Install{}, nil, werror.Wrap(err, "Failed to unmarshal install base configuration YAML")
	}

	installConfigStruct := s.installConfigStruct
	if installConfigStruct == nil {
		installConfigStruct = config.Install{}
	}
	specificInstallCfg := reflect.New(reflect.TypeOf(installConfigStruct)).Interface()

	if err := s.configYAMLUnmarshalFn(cfgBytes, *&specificInstallCfg); err != nil {
		return config.Install{}, nil, werror.Wrap(err, "Failed to unmarshal install specific configuration YAML")
	}
	return baseInstallCfg, reflect.Indirect(reflect.ValueOf(specificInstallCfg)).Interface(), nil
}

func (s *Server) initRuntimeConfig(ctx context.Context) (rBaseCfg refreshableBaseRuntimeConfig, rCfg refreshable.Refreshable, hcSrc status.HealthCheckSource, rErr error) {
	if s.runtimeConfigProvider == nil {
		// if runtime provider is not specified, use a file-based one
		s.runtimeConfigProvider = func(ctx context.Context) (refreshable.Refreshable, error) {
			return refreshable.NewFileRefreshable(ctx, runtimeConfigPath)
		}
	}

	runtimeConfigProvider, err := s.runtimeConfigProvider(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	runtimeConfigProvider = runtimeConfigProvider.Map(func(cfgBytesVal interface{}) interface{} {
		cfgBytes, err := s.decryptConfigBytes(cfgBytesVal.([]byte))
		if err != nil {
			s.svcLogger.Warn("Failed to decrypt encrypted runtime configuration", svc1log.Stacktrace(err))
		}
		return cfgBytes
	})

	validatedRuntimeConfig, err := refreshable.NewValidatingRefreshable(
		runtimeConfigProvider,
		func(cfgBytesVal interface{}) error {
			runtimeConfigStruct := s.runtimeConfigStruct
			if runtimeConfigStruct == nil {
				runtimeConfigStruct = config.Runtime{}
			}
			runtimeCfg := reflect.New(reflect.TypeOf(runtimeConfigStruct)).Interface()
			return s.configYAMLUnmarshalFn(cfgBytesVal.([]byte), *&runtimeCfg)
		})
	if err != nil {
		return nil, nil, nil, err
	}

	validatingRefreshableHealthCheckSource := refreshablehealth.NewValidatingRefreshableHealthCheckSource(
		runtimeConfigReloadCheckType,
		*validatedRuntimeConfig)

	baseRuntimeConfig := newRefreshableBaseRuntimeConfig(validatedRuntimeConfig.Map(func(cfgBytesVal interface{}) interface{} {
		var runtimeCfg config.Runtime
		if err := s.configYAMLUnmarshalFn(cfgBytesVal.([]byte), &runtimeCfg); err != nil {
			s.svcLogger.Error("Failed to unmarshal runtime configuration", svc1log.Stacktrace(err))
		}
		return runtimeCfg
	}))

	runtimeConfig := validatedRuntimeConfig.Map(func(cfgBytesVal interface{}) interface{} {
		runtimeConfigStruct := s.runtimeConfigStruct
		if runtimeConfigStruct == nil {
			runtimeConfigStruct = config.Runtime{}
		}
		runtimeCfg := reflect.New(reflect.TypeOf(runtimeConfigStruct)).Interface()
		if err := s.configYAMLUnmarshalFn(cfgBytesVal.([]byte), *&runtimeCfg); err != nil {
			// this should not happen unless there is a bug in Witchcraft because configuration has already been
			// processed by unmarshalYAMLFn without issue at this stage
			panic("Failed to unmarshal runtime configuration")
		}
		return reflect.Indirect(reflect.ValueOf(runtimeCfg)).Interface()
	})

	return baseRuntimeConfig, runtimeConfig, validatingRefreshableHealthCheckSource, nil
}

func (s *Server) initStackTraceHandler(ctx context.Context) {
	if s.disableSigQuitHandler {
		return
	}

	stackTraceHandler := func(stackTraceOutput []byte) error {
		if s.diagLogger != nil {
			s.diagLogger.Diagnostic(logging.NewDiagnosticFromThreadDump(diag1log.ThreadDumpV1FromGoroutines(stackTraceOutput)))
		}
		if s.sigQuitHandlerWriter != nil {
			if _, err := s.sigQuitHandlerWriter.Write(stackTraceOutput); err != nil {
				return err
			}
		}
		return nil
	}
	errHandler := func(err error) {
		if s.svcLogger != nil && err != nil {
			s.svcLogger.Error("Failed to dump goroutines", svc1log.Stacktrace(err))
		}
	}

	signals.RegisterStackTraceHandlerOnSignals(ctx, stackTraceHandler, errHandler, syscall.SIGQUIT)
}

func (s *Server) initShutdownSignalHandler(ctx context.Context) {
	if s.disableShutdownSignalHandler {
		return
	}

	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, syscall.SIGTERM, syscall.SIGINT)

	go wapp.RunWithRecoveryLogging(ctx, func(ctx context.Context) {
		<-shutdownSignal
		if err := s.Shutdown(ctx); err != nil {
			s.svcLogger.Warn("Failed to gracefully shutdown server.", svc1log.Stacktrace(err))
		}
	})
}

// Running returns true if the server is in the "running" state (as opposed to "idle" or "initializing"), false
// otherwise.
func (s *Server) Running() bool {
	return s.stateManager.Running()
}

// State returns the state of the current server (idle, initializing or running).
func (s *Server) State() ServerState {
	return s.stateManager.State()
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.shutdownFinished.Add(1)
	defer s.shutdownFinished.Done()

	s.svcLogger.Info("Shutting down server")
	return stopServer(s, func(svr *http.Server) error {
		return svr.Shutdown(ctx)
	})
}

func (s *Server) Close() error {
	s.shutdownFinished.Add(1)
	defer s.shutdownFinished.Done()

	s.svcLogger.Info("Closing server")
	return stopServer(s, func(svr *http.Server) error {
		return svr.Close()
	})
}

func (s *Server) decryptConfigBytes(cfgBytes []byte) ([]byte, error) {
	if !encryptedconfigvalue.ContainsEncryptedConfigValueStringVars(cfgBytes) {
		// Nothing to do
		return cfgBytes, nil
	}
	if s.ecvKeyProvider == nil {
		return cfgBytes, werror.Error("No encryption key provider configured but config contains encrypted values")
	}
	ecvKey, err := s.ecvKeyProvider.Load()
	if err != nil {
		return cfgBytes, err
	}
	if ecvKey == nil {
		return cfgBytes, werror.Error("No encryption key configured but config contains encrypted values")
	}
	return encryptedconfigvalue.DecryptAllEncryptedValueStringVars(cfgBytes, *ecvKey), nil
}

func stopServer(s *Server, stopper func(s *http.Server) error) error {
	if s.State() != ServerRunning {
		return werror.Error("server is not running")
	}
	s.stateManager.setState(ServerIdle)
	return stopper(s.httpServer)
}

func (s *Server) getApplicationTracingOptions(install config.Install) []wtracing.TracerOption {
	return getTracingOptions(s.applicationTraceSampler, install, traceSamplerFromSampleRate(defaultSampleRate), install.Server.Port, install.TraceSampleRate)
}

func (s *Server) getManagementTracingOptions(install config.Install) []wtracing.TracerOption {
	return getTracingOptions(s.managementTraceSampler, install, neverSample, install.Server.ManagementPort, install.ManagementTraceSampleRate)
}

func getTracingOptions(configuredSampler wtracing.Sampler, install config.Install, fallbackSampler wtracing.Sampler, port int, sampleRate *float64) []wtracing.TracerOption {
	endpoint := &wtracing.Endpoint{
		ServiceName: install.ProductName,
		Port:        uint16(port),
	}
	if parsedIP := net.ParseIP(install.Server.Address); len(parsedIP) > 0 {
		if parsedIP.To4() != nil {
			endpoint.IPv4 = parsedIP
		} else {
			endpoint.IPv6 = parsedIP
		}
	}
	return []wtracing.TracerOption{
		wtracing.WithLocalEndpoint(endpoint),
		getSamplingTraceOption(configuredSampler, fallbackSampler, sampleRate),
	}
}

func getSamplingTraceOption(configuredSampler wtracing.Sampler, fallbackSampler wtracing.Sampler, sampleRate *float64) wtracing.TracerOption {
	if configuredSampler != nil {
		return wtracing.WithSampler(configuredSampler)
	} else if sampleRate != nil {
		return wtracing.WithSampler(traceSamplerFromSampleRate(*sampleRate))
	}
	return wtracing.WithSampler(fallbackSampler)
}

func traceSamplerFromSampleRate(sampleRate float64) wtracing.Sampler {
	if sampleRate <= 0 {
		return neverSample
	}
	if sampleRate >= 1 {
		return alwaysSample
	}
	boundary := uint64(sampleRate * float64(math.MaxUint64)) // does not overflow because we already checked bounds
	return func(id uint64) bool {
		return id < boundary
	}
}

func neverSample(id uint64) bool { return false }

func alwaysSample(id uint64) bool { return true }
