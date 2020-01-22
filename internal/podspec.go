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

package internal

import (
	v1 "k8s.io/api/core/v1"
)

// FindInstanceGroupFromPodSpec extracts the instance group from a Pod spec.
func FindInstanceGroupFromPodSpec(podSpec v1.PodSpec, instanceGroupLabel string) (instanceGroup string, success bool) {
	for _, nodeSelectorTerm := range podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		for _, matchExpression := range nodeSelectorTerm.MatchExpressions {
			if matchExpression.Key == instanceGroupLabel {
				if len(matchExpression.Values) == 1 {
					return matchExpression.Values[0], true
				}
			}
		}
	}
	return "", false
}
