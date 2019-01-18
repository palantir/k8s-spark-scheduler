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

type String interface {
	Refreshable
	CurrentString() string
}

func NewString(in Refreshable) String {
	return refreshableTyped{
		Refreshable: in,
	}
}

type Int interface {
	Refreshable
	CurrentInt() int
}

func NewInt(in Refreshable) Int {
	return refreshableTyped{
		Refreshable: in,
	}
}

type Bool interface {
	Refreshable
	CurrentBool() bool
}

func NewBool(in Refreshable) Bool {
	return refreshableTyped{
		Refreshable: in,
	}
}

type refreshableTyped struct {
	Refreshable
}

func (rt refreshableTyped) CurrentString() string {
	return rt.Current().(string)
}

func (rt refreshableTyped) CurrentInt() int {
	return rt.Current().(int)
}

func (rt refreshableTyped) CurrentBool() bool {
	return rt.Current().(bool)
}
