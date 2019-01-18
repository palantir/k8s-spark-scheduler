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

package refreshable

type Refreshable interface {
	// Current returns the most recent value of this Refreshable.
	Current() interface{}

	// Subscribe subscribes to changes of this Refreshable. The provided function is called with the value of Current()
	// whenever the value changes.
	Subscribe(consumer func(interface{})) (unsubscribe func())

	// Map returns a new Refreshable based on the current one that handles updates based on the current Refreshable.
	Map(func(interface{}) interface{}) Refreshable
}
