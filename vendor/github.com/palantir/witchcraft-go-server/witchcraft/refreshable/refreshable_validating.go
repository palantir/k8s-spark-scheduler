// Copyright (c) 2019 Palantir Technologies. All rights reserved.
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
	"sync/atomic"

	werror "github.com/palantir/witchcraft-go-error"
)

type ValidatingRefreshable struct {
	Refreshable

	validatedRefreshable Refreshable
	lastValidateErr      *atomic.Value
}

// this is needed to be able to store the absence of an error in an atomic.Value
type errorWrapper struct {
	err error
}

func (v *ValidatingRefreshable) Current() interface{} {
	return v.validatedRefreshable.Current()
}

func (v *ValidatingRefreshable) Subscribe(consumer func(interface{})) (unsubscribe func()) {
	return v.validatedRefreshable.Subscribe(consumer)
}

func (v *ValidatingRefreshable) Map(mapFn func(interface{}) interface{}) Refreshable {
	return v.validatedRefreshable.Map(mapFn)
}

func (v *ValidatingRefreshable) LastValidateErr() error {
	return v.lastValidateErr.Load().(errorWrapper).err
}

// NewValidatingRefreshable returns a new Refreshable whose current value is the latest value that passes the provided
// validatingFn successfully. This returns an error if the current value of the passed in Refreshable does not pass the
// validatingFn or if the validatingFn or Refreshable are nil.
func NewValidatingRefreshable(origRefreshable Refreshable, validatingFn func(interface{}) error) (*ValidatingRefreshable, error) {
	if validatingFn == nil {
		return nil, werror.Error("failed to create validating Refreshable because the validating function was nil")
	}

	if origRefreshable == nil {
		return nil, werror.Error("failed to create validating Refreshable because the passed in Refreshable was nil")
	}

	currentVal := origRefreshable.Current()
	if err := validatingFn(currentVal); err != nil {
		return nil, werror.Wrap(err, "failed to create validating Refreshable because initial value could not be validated")
	}

	validatedRefreshable := NewDefaultRefreshable(currentVal)

	var lastValidateErr atomic.Value
	lastValidateErr.Store(errorWrapper{})
	v := ValidatingRefreshable{
		validatedRefreshable: validatedRefreshable,
		lastValidateErr:      &lastValidateErr,
	}

	_ = origRefreshable.Subscribe(func(i interface{}) {
		if err := validatingFn(i); err != nil {
			v.lastValidateErr.Store(errorWrapper{err})
			return
		}

		if err := validatedRefreshable.Update(i); err != nil {
			v.lastValidateErr.Store(errorWrapper{err})
			return
		}

		v.lastValidateErr.Store(errorWrapper{})
	})
	return &v, nil
}
