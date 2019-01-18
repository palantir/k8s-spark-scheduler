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
	"github.com/palantir/witchcraft-go-logging/wlog"
	"github.com/palantir/witchcraft-go-logging/wlog/extractor"
)

type LoggerCreatorParam interface {
	apply(*defaultLoggerBuilder)
}

type loggerCreatorParamFunc func(*defaultLoggerBuilder)

func (f loggerCreatorParamFunc) apply(logger *defaultLoggerBuilder) {
	f(logger)
}

func Creator(creator wlog.LoggerCreator) LoggerCreatorParam {
	return loggerCreatorParamFunc(func(logger *defaultLoggerBuilder) {
		logger.loggerCreator = creator
	})
}

func Extractor(extractor extractor.IDsFromRequest) LoggerCreatorParam {
	return loggerCreatorParamFunc(func(logger *defaultLoggerBuilder) {
		logger.idsExtractor = extractor
	})
}

func SafePathParams(safeParams ...string) LoggerCreatorParam {
	return loggerCreatorParamFunc(func(logger *defaultLoggerBuilder) {
		logger.safePathParams = append(logger.safePathParams, safeParams...)
	})
}

func ForbiddenPathParams(forbiddenParams ...string) LoggerCreatorParam {
	return loggerCreatorParamFunc(func(logger *defaultLoggerBuilder) {
		logger.forbiddenPathParams = append(logger.forbiddenPathParams, forbiddenParams...)
	})
}

func SafeQueryParams(safeParams ...string) LoggerCreatorParam {
	return loggerCreatorParamFunc(func(logger *defaultLoggerBuilder) {
		logger.safeQueryParams = append(logger.safeQueryParams, safeParams...)
	})
}

func ForbiddenQueryParams(forbiddenParams ...string) LoggerCreatorParam {
	return loggerCreatorParamFunc(func(logger *defaultLoggerBuilder) {
		logger.forbiddenQueryParams = append(logger.forbiddenQueryParams, forbiddenParams...)
	})
}

func SafeHeaderParams(safeParams ...string) LoggerCreatorParam {
	return loggerCreatorParamFunc(func(logger *defaultLoggerBuilder) {
		logger.safeHeaderParams = append(logger.safeHeaderParams, safeParams...)
	})
}

func ForbiddenHeaderParams(forbiddenParams ...string) LoggerCreatorParam {
	return loggerCreatorParamFunc(func(logger *defaultLoggerBuilder) {
		logger.forbiddenHeaderParams = append(logger.forbiddenHeaderParams, forbiddenParams...)
	})
}
