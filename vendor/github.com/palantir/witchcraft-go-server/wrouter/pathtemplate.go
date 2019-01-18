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
	"fmt"
	"regexp"
	"strings"
)

type PathTemplate interface {
	Template() string
	Segments() []PathSegment
}

type pathTemplateImpl struct {
	rawTemplate string
	segments    []PathSegment
}

func (p *pathTemplateImpl) Template() string {
	return p.rawTemplate
}

func (p *pathTemplateImpl) Segments() []PathSegment {
	return p.segments
}

var (
	fullPathRegExp           = regexp.MustCompile(`^/[a-zA-Z0-9_{}/.\-*]*$`)
	pathParamMatcher         = regexp.MustCompile(`^\{([a-zA-Z0-9]+)(\*?)}$`)
	regularPathSegmentRegExp = regexp.MustCompile(`^[a-zA-Z0-9\-_]+$`)
)

// NewPathTemplate creates a new PathTemplate using the provided path. The provided path must be of the following form:
//
// * Must start with '/'
// * Must contain only alphanumeric characters and '_', '{', '}', '/', '.', '-', and '*'
// * Path parameters must be of the form "{paramName}", where paramName must be an alphanumeric string
// * Path parameters that occur at the end of a path can take the form "{paramName*}", in which case the "*" signifies
//   that the parameter is a trailing path parameter
// * For trailing path parameters, the value of the parameter will be the string that occurs after the final '/' that
//   precedes the variable
//   * For example, a route registered with path "/pkg/{pkgPath*}" matched against the request
//     "/pkg/product/1.0.0/package.tgz" will result in a path parameter value of "product/1.0.0/package.tgz"
func NewPathTemplate(in string) (PathTemplate, error) {
	segments, err := toPathSegments(in)
	if err != nil {
		return nil, err
	}
	return &pathTemplateImpl{
		rawTemplate: in,
		segments:    segments,
	}, nil
}

func toPathSegments(path string) ([]PathSegment, error) {
	if !fullPathRegExp.MatchString(path) {
		return nil, fmt.Errorf("path %q must match regexp %s", path, fullPathRegExp)
	}
	// trim first '/'
	pathParts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	foundPathParams := make(map[string]struct{})
	pathSegments := make([]PathSegment, len(pathParts))
	for i, pathPart := range pathParts {
		switch {
		case pathPart == "":
			if i != len(pathParts)-1 {
				// segment must not be empty unless it is the last one
				return nil, fmt.Errorf("segment at index %d of path %s with segments %v was empty", i, path, pathParts)
			}
			pathSegments[i] = PathSegment{Type: LiteralSegment}

		case pathPart[0] == '{':
			matches := pathParamMatcher.FindStringSubmatch(pathPart)
			if matches == nil {
				return nil, fmt.Errorf("invalid path param %s in path %s", pathPart, path)
			}
			if _, ok := foundPathParams[matches[1]]; ok {
				return nil, fmt.Errorf("path param %q appears more than once in path %s", matches[1], path)
			}
			foundPathParams[matches[1]] = struct{}{}

			segmentType := PathParamSegment
			if isTrailingMatchParam(matches) {
				if i < len(pathParts)-1 {
					return nil, fmt.Errorf("trailing match path param %s does not appear at end of path %s", pathPart, path)
				}
				segmentType = TrailingPathParamSegment
			}
			pathSegments[i] = PathSegment{Value: matches[1], Type: segmentType}
		case !regularPathSegmentRegExp.MatchString(pathPart):
			return nil, fmt.Errorf("segment %s of path %s includes special characters", pathPart, path)
		default:
			pathSegments[i] = PathSegment{Value: pathPart, Type: LiteralSegment}
		}
	}
	return pathSegments, nil
}

// One-liner just for readability.
func isTrailingMatchParam(pathParamMatches []string) bool {
	return pathParamMatches[2] == "*"
}
