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

package conjuretype

import (
	"encoding/json"
	"fmt"
)

const (
	safeIntVal = (int64(1) << 53)
	minVal     = -safeIntVal + 1
	maxVal     = safeIntVal - 1
)

type SafeLong int64

func NewSafeLong(val int64) (SafeLong, error) {
	if val < minVal || val > maxVal {
		return 0, fmt.Errorf("%d is not a valid value for a SafeLong as it is not safely representable in Javascript: must be between %d and %d", val, minVal, maxVal)
	}
	return SafeLong(val), nil
}

func (s *SafeLong) UnmarshalJSON(b []byte) error {
	var val int64
	if err := json.Unmarshal(b, &val); err != nil {
		return err
	}

	newVal, err := NewSafeLong(val)
	if err != nil {
		return err
	}
	*s = newVal

	return nil
}

func (s SafeLong) MarshalJSON() ([]byte, error) {
	if int64(s) < minVal || int64(s) > maxVal {
		return nil, fmt.Errorf("%d is not a valid value for a SafeLong as it is not safely representable in Javascript: must be between %d and %d", s, minVal, maxVal)
	}
	return json.Marshal(int64(s))
}
