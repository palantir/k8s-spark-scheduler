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

package errors

import (
	"fmt"
	"reflect"
)

var registry = map[string]reflect.Type{}

var errorInterfaceType = reflect.TypeOf((*Error)(nil)).Elem()

// RegisterErrorType registers an error name and its go type in a global registry.
// The type should be a struct type whose pointer implements Error.
// Panics if name is already registered or *type does not implement Error.
func RegisterErrorType(name string, typ reflect.Type) {
	if existing, exists := registry[name]; exists {
		panic(fmt.Sprintf("ErrorName %v already registered as %v", name, existing))
	}
	if ptr := reflect.PtrTo(typ); !ptr.Implements(errorInterfaceType) {
		panic(fmt.Sprintf("Error type %v does not implement errors.Error interface", ptr))
	}
	registry[name] = typ
}
