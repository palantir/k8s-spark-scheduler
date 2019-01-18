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

package whttprouter

import (
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"
	"github.com/palantir/witchcraft-go-server/wrouter"
)

// New returns a wrouter.RouterImpl backed by a new httprouter.Router configured using the provided parameters.
func New(params ...Param) wrouter.RouterImpl {
	r := httprouter.New()
	for _, p := range params {
		p.apply(r)
	}
	return (*router)(r)
}

type Param interface {
	apply(*httprouter.Router)
}

type paramFunc func(*httprouter.Router)

func (f paramFunc) apply(r *httprouter.Router) {
	f(r)
}

func RedirectTrailingSlash(redirect bool) Param {
	return paramFunc(func(r *httprouter.Router) {
		r.RedirectTrailingSlash = redirect
	})
}

func RedirectFixedPath(redirect bool) Param {
	return paramFunc(func(r *httprouter.Router) {
		r.RedirectFixedPath = redirect
	})
}

func HandleMethodNotAllowed(notAllowed bool) Param {
	return paramFunc(func(r *httprouter.Router) {
		r.HandleMethodNotAllowed = notAllowed
	})
}

func HandleOPTIONS(handle bool) Param {
	return paramFunc(func(r *httprouter.Router) {
		r.HandleOPTIONS = handle
	})
}

type router httprouter.Router

func (r *router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	(*httprouter.Router)(r).ServeHTTP(w, req)
}

func (r *router) Register(method string, pathSegments []wrouter.PathSegment, handler http.Handler) {
	(*httprouter.Router)(r).Handler(method, r.convertPathParams(pathSegments), handler)
}

func (r *router) PathParams(req *http.Request, pathVarNames []string) map[string]string {
	_, vars, _ := (*httprouter.Router)(r).Lookup(req.Method, req.URL.Path)
	if len(vars) == 0 {
		return nil
	}
	params := make(map[string]string)
	for i := range vars {
		// strip preceding forward slashes for trailing match path params. httprouter is implemented such that,
		// for a trailing parameter for the form "/{param*}", the path "/var/foo/bar.txt" will match with a
		// value of "/var/foo/bar.txt". However, the contract of zappermux stipulates that the match should be
		// of the form "var/foo/bar.txt", so strip the preceding slash to fulfill this contract.
		params[vars[i].Key] = strings.TrimPrefix(vars[i].Value, "/")
	}
	return params
}

func (r *router) convertPathParams(pathSegments []wrouter.PathSegment) string {
	pathParts := make([]string, len(pathSegments))
	for i, segment := range pathSegments {
		switch segment.Type {
		case wrouter.PathParamSegment:
			pathParts[i] = ":" + segment.Value
		case wrouter.TrailingPathParamSegment:
			pathParts[i] = "*" + segment.Value
		default:
			pathParts[i] = segment.Value
		}
	}
	return "/" + strings.Join(pathParts, "/")
}
