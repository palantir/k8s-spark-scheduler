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

package req2log

import (
	"io"
	"net/http"
	"time"

	"github.com/palantir/witchcraft-go-logging/wlog"
	"github.com/palantir/witchcraft-go-logging/wlog/extractor"
)

const (
	TypeValue = "request.2"

	methodKey       = "method"
	protocolKey     = "protocol"
	pathKey         = "path"
	paramsKey       = "params"
	statusKey       = "status"
	requestSizeKey  = "requestSize"
	responseSizeKey = "responseSize"
	durationKey     = "duration"
	traceIDKey      = wlog.TraceIDKey
)

// Logger creates a request log entry based on the provided information.
type Logger interface {
	Request(req Request)

	RequestParamPerms
}

type RequestParamPerms interface {
	PathParamPerms() ParamPerms
	QueryParamPerms() ParamPerms
	HeaderParamPerms() ParamPerms
}

// Request represents an HTTP request that has been (or is about to be) completed. Contains information on the request
// such as the request itself, the status code of the response, etc.
type Request struct {
	// Request is the *http.Request associated with the event.
	Request *http.Request
	// RouteInfo contains the path template and path parameter values for the request.
	RouteInfo RouteInfo
	// ResponseStatus is the status code of the response to the request.
	ResponseStatus int
	// ResponseSize is the size of the response to the request.
	ResponseSize int64
	// Duration is the total time it took to process the request.
	Duration time.Duration
	// PathParamPerms determines the path parameters that are safe and forbidden for logging.
	PathParamPerms ParamPerms
	// QueryParamPerms determines the query parameters that are safe and forbidden for logging.
	QueryParamPerms ParamPerms
	// HeaderParamPerms determines the header parameters that are safe and forbidden for logging.
	HeaderParamPerms ParamPerms
}

type RouteInfo struct {
	Template   string
	PathParams map[string]string
}

func New(w io.Writer, params ...LoggerCreatorParam) Logger {
	return NewFromCreator(w, wlog.DefaultLoggerProvider().NewLogger, params...)
}

func NewFromCreator(w io.Writer, creator wlog.LoggerCreator, params ...LoggerCreatorParam) Logger {
	loggerBuilder := &defaultLoggerBuilder{
		loggerCreator: creator,
		idsExtractor:  extractor.NewDefaultIDsExtractor(),
	}
	for _, p := range params {
		p.apply(loggerBuilder)
	}
	return loggerBuilder.build(w)
}

type defaultLoggerBuilder struct {
	loggerCreator wlog.LoggerCreator
	idsExtractor  extractor.IDsFromRequest

	safePathParams      []string
	forbiddenPathParams []string

	safeQueryParams      []string
	forbiddenQueryParams []string

	safeHeaderParams      []string
	forbiddenHeaderParams []string
}

func (b *defaultLoggerBuilder) build(w io.Writer) *defaultLogger {
	defaultParams := DefaultRequestParamPerms()
	return &defaultLogger{
		logger:           b.loggerCreator(w),
		idsExtractor:     b.idsExtractor,
		pathParamPerms:   CombinedParamPerms(defaultParams.PathParamPerms(), NewParamPerms(b.safePathParams, b.forbiddenPathParams)),
		queryParamPerms:  CombinedParamPerms(defaultParams.QueryParamPerms(), NewParamPerms(b.safeQueryParams, b.forbiddenQueryParams)),
		headerParamPerms: CombinedParamPerms(defaultParams.HeaderParamPerms(), NewParamPerms(b.safeHeaderParams, b.forbiddenHeaderParams)),
	}
}
