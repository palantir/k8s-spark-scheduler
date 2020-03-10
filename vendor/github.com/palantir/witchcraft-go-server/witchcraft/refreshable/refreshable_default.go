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

import (
	"reflect"
	"sync"
	"sync/atomic"

	werror "github.com/palantir/witchcraft-go-error"
)

type DefaultRefreshable struct {
	typ     reflect.Type
	current *atomic.Value

	sync.Mutex  // protects subscribers
	subscribers []*func(interface{})
}

func NewDefaultRefreshable(val interface{}) *DefaultRefreshable {
	current := atomic.Value{}
	current.Store(val)

	return &DefaultRefreshable{
		current: &current,
		typ:     reflect.TypeOf(val),
	}
}

func (d *DefaultRefreshable) Update(val interface{}) error {
	d.Lock()
	defer d.Unlock()

	if valType := reflect.TypeOf(val); valType != d.typ {
		return werror.Error("value of Refreshable must is not the correct type",
			werror.SafeParam("refreshableType", d.typ),
			werror.SafeParam("providedType", valType))
	}

	if reflect.DeepEqual(d.current.Load(), val) {
		return nil
	}
	d.current.Store(val)

	for _, sub := range d.subscribers {
		(*sub)(val)
	}
	return nil
}

func (d *DefaultRefreshable) Current() interface{} {
	return d.current.Load()
}

func (d *DefaultRefreshable) Subscribe(consumer func(interface{})) (unsubscribe func()) {
	d.Lock()
	defer d.Unlock()

	consumerFnPtr := &consumer
	d.subscribers = append(d.subscribers, consumerFnPtr)
	return func() {
		d.unsubscribe(consumerFnPtr)
	}
}

func (d *DefaultRefreshable) unsubscribe(consumerFnPtr *func(interface{})) {
	d.Lock()
	defer d.Unlock()

	matchIdx := -1
	for idx, currSub := range d.subscribers {
		if currSub == consumerFnPtr {
			matchIdx = idx
			break
		}
	}
	if matchIdx != -1 {
		d.subscribers = append(d.subscribers[:matchIdx], d.subscribers[matchIdx+1:]...)
	}
}

func (d *DefaultRefreshable) Map(mapFn func(interface{}) interface{}) Refreshable {
	newRefreshable := NewDefaultRefreshable(mapFn(d.Current()))
	d.Subscribe(func(updatedVal interface{}) {
		_ = newRefreshable.Update(mapFn(updatedVal))
	})
	return newRefreshable
}
