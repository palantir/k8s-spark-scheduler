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
	"context"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type TestPod v1.Pod

func (t TestPod) withCondition(conditionType v1.PodConditionType, time time.Time) TestPod {
	t.Status.Conditions = append(t.Status.Conditions, v1.PodCondition{
		Type:               conditionType,
		Status:             v1.ConditionTrue,
		LastTransitionTime: metav1.NewTime(time),
	})
	return t
}

func createPod(instanceGroup, instanceGroupTagLabel, sparkRole string, creationTimeStamp time.Time) TestPod {
	return TestPod{
		ObjectMeta: metav1.ObjectMeta{
			Labels:            map[string]string{sparkRoleLabel: sparkRole},
			CreationTimestamp: metav1.NewTime(creationTimeStamp),
		},
		Spec: v1.PodSpec{
			NodeSelector: map[string]string{instanceGroupTagLabel: instanceGroup},
		},
		Status: v1.PodStatus{
			Conditions: []v1.PodCondition{},
		},
	}
}

func podTags(instanceGroup, instanceGroupTagLabel, sparkRole string, conditionType v1.PodConditionType) PodTags {
	ctx := context.Background()
	return PodTags{
		InstanceGroupTag(ctx, instanceGroup),
		SparkRoleTag(ctx, sparkRole),
		lifecycleTags[conditionType],
	}
}

func TestMarkTimes(t *testing.T) {
	now := time.Now()
	pods := []TestPod{
		createPod("a", "resource_channel", "driver", now.Add(-10*time.Second)).
			withCondition(v1.PodScheduled, now.Add(-5*time.Second)),
		createPod("a", "resource_channel", "driver", now.Add(-15*time.Second)).
			withCondition(v1.PodScheduled, now.Add(-5*time.Second)).
			withCondition(v1.PodInitialized, now.Add(-5*time.Second)).
			withCondition(v1.PodReady, now),
		createPod("a", "resource_channel", "executor", now.Add(-5*time.Second)),
	}

	expected := map[PodTags]struct {
		count int64
		max   int64
	}{
		podTags("a", "resource_channel", "driver", v1.PodScheduled): {
			max: 10e9,
		},
		podTags("a", "resource_channel", "driver", v1.PodReady): {
			max: 5e9,
		},
		podTags("a", "resource_channel", "driver", v1.PodInitialized): {
			max:   5e9,
			count: 1,
		},
		podTags("a", "resource_channel", "executor", v1.PodScheduled): {
			max:   5e9,
			count: 1,
		},
	}

	histograms := PodHistograms{}
	for _, pod := range pods {
		actualPod := v1.Pod(pod)
		histograms.MarkTimes(context.Background(), &actualPod, "resource_channel", now)
	}

	for key, expectedValues := range expected {
		actual := histograms[key]
		if expectedValues.count != actual.Counter.Count() {
			t.Fatalf("mismatch count in key %v, expected %v, got %v", key, expectedValues.count, actual.Counter.Count())
		}
		if expectedValues.max != actual.Histogram.Max() {
			t.Fatalf("mismatch count in key %v, expected %v, got %v", key, expectedValues.count, actual.Histogram.Max())
		}
	}
}
