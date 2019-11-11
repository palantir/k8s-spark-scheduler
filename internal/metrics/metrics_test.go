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

package metrics

import (
	"testing"
)

func Test_crossZoneShuffleTransfer(t *testing.T) {
	type args struct {
		numPodsPerZone map[string]int
		totalNumPods   int
	}
	tests := []struct {
		name     string
		args     args
		expected int64
	}{{
		name: "Should be 0 if everything is in a single zone",
		args: args{
			numPodsPerZone: map[string]int{"zone1": 5},
			totalNumPods:   5,
		},
		expected: 0,
	}, {
		name: "Should be 50% if there are 2 zones with 1 pod each",
		args: args{
			numPodsPerZone: map[string]int{"zone1": 1, "zone2": 1},
			totalNumPods:   2,
		},
		expected: 50,
	}, {
		name: "Should be 50% if there are 2 zones with 3 pods each",
		args: args{
			numPodsPerZone: map[string]int{"zone1": 3, "zone2": 3},
			totalNumPods:   6,
		},
		expected: 50,
	}, {
		name: "Should be 75% if there are 4 zones with 1 pod each",
		args: args{
			numPodsPerZone: map[string]int{"zone1": 1, "zone2": 1, "zone3": 1, "zone4": 1},
			totalNumPods:   4,
		},
		expected: 75,
	}, {
		name: "Should be 48% if there are 2 zones with 2 and 3 pods each",
		args: args{
			numPodsPerZone: map[string]int{"zone1": 2, "zone2": 3},
			totalNumPods:   5,
		},
		expected: 48,
	},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if actual := crossZoneShuffleTransfer(tt.args.numPodsPerZone, tt.args.totalNumPods); actual != tt.expected {
				t.Errorf("crossZoneShuffleTransfer() = %v, want %v", actual, tt.expected)
			}
		})
	}
}
