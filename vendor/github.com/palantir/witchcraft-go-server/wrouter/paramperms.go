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

package wrouter

import (
	"strings"
)

type RouteParamPerms interface {
	PathParamPerms() ParamPerms
	QueryParamPerms() ParamPerms
	HeaderParamPerms() ParamPerms
}

type requestParamPermsImpl struct {
	pathParamPerms   ParamPerms
	queryParamPerms  ParamPerms
	headerParamPerms ParamPerms
}

func (r *requestParamPermsImpl) PathParamPerms() ParamPerms {
	return r.pathParamPerms
}

func (r *requestParamPermsImpl) QueryParamPerms() ParamPerms {
	return r.queryParamPerms
}

func (r *requestParamPermsImpl) HeaderParamPerms() ParamPerms {
	return r.headerParamPerms
}

type ParamPerms interface {
	// Safe returns true if the parameter with the provided name is safe to log. Case-insensitive.
	Safe(paramName string) bool
	// Forbidden returns true if the provided parameter is forbidden from being logged (that is, it should not be logged
	// at all, even as an unsafe parameter). Case-insensitive.
	Forbidden(paramName string) bool
}

type mapParamPerms struct {
	safe      map[string]struct{}
	forbidden map[string]struct{}
}

func newSafeParamPerms(params ...string) ParamPerms {
	return newParamPerms(params, nil)
}

func newForbiddenParamPerms(params ...string) ParamPerms {
	return newParamPerms(nil, params)
}

func newParamPerms(safeParams []string, forbiddenParams []string) ParamPerms {
	m := &mapParamPerms{
		safe:      make(map[string]struct{}),
		forbidden: make(map[string]struct{}),
	}
	for _, p := range forbiddenParams {
		m.forbidden[strings.ToLower(p)] = struct{}{}
	}
	for _, p := range safeParams {
		k := strings.ToLower(p)
		if _, ok := m.forbidden[k]; ok {
			continue
		}
		m.safe[k] = struct{}{}
	}
	return m
}

func (m *mapParamPerms) Safe(paramName string) bool {
	if m.Forbidden(paramName) {
		return false
	}
	_, ok := m.safe[strings.ToLower(paramName)]
	return ok
}

func (m *mapParamPerms) Forbidden(paramName string) bool {
	_, ok := m.forbidden[strings.ToLower(paramName)]
	return ok
}

func NewCombinedParamPerms(paramPerms ...ParamPerms) ParamPerms {
	return combinedParamPerms(paramPerms)
}

type combinedParamPerms []ParamPerms

func (c combinedParamPerms) Safe(paramName string) bool {
	if c.Forbidden(paramName) {
		return false
	}
	for _, currPerms := range c {
		if currPerms == nil {
			continue
		}
		if currPerms.Safe(paramName) {
			return true
		}
	}
	return false
}

func (c combinedParamPerms) Forbidden(paramName string) bool {
	for _, currPerms := range c {
		if currPerms == nil {
			continue
		}
		if currPerms.Forbidden(paramName) {
			return true
		}
	}
	return false
}
