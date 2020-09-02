// Copyright (c) 2020 Palantir Technologies. All rights reserved.
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

package metricloggers

import (
	"github.com/palantir/pkg/metrics"
	"github.com/palantir/witchcraft-go-logging/wlog/reqlog/req2log"
)

var _ req2log.Logger = (*req2Logger)(nil)

type req2Logger struct {
	logger   req2log.Logger
	recorder metricRecorder
}

func NewReq2Logger(logger req2log.Logger, registry metrics.Registry) req2log.Logger {
	return &req2Logger{
		logger:   logger,
		recorder: newMetricRecorder(registry, req2log.TypeValue),
	}
}

func (r *req2Logger) Request(req req2log.Request) {
	r.logger.Request(req)
	r.recorder.RecordSLSLog()
}

func (r *req2Logger) PathParamPerms() req2log.ParamPerms {
	return r.logger.PathParamPerms()
}

func (r *req2Logger) QueryParamPerms() req2log.ParamPerms {
	return r.logger.QueryParamPerms()
}

func (r *req2Logger) HeaderParamPerms() req2log.ParamPerms {
	return r.logger.HeaderParamPerms()
}
