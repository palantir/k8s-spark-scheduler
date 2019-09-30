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

package extender

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func createResources(cpu, memory int64) *resources.Resources {
	return &resources.Resources{
		CPU:    *resource.NewQuantity(cpu, resource.DecimalSI),
		Memory: *resource.NewQuantity(memory, resource.BinarySI),
	}
}

func TestSparkResources(t *testing.T) {
	tests := []struct {
		name                         string
		pod                          v1.Pod
		expectedApplicationResources *sparkApplicationResources
	}{{
		name: "parses pod annotations into resources",
		pod: v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					DriverCPU:      "1",
					DriverMemory:   "2432Mi",
					ExecutorCPU:    "2",
					ExecutorMemory: "6758Mi",
					ExecutorCount:  "2",
				},
			},
		},
		expectedApplicationResources: &sparkApplicationResources{
			driverResources:   createResources(1, 2432*1024*1024),
			executorResources: createResources(2, 6758*1024*1024),
			executorCount:     2,
		},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			applicationResources, err := sparkResources(context.Background(), &test.pod)
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			if !applicationResources.driverResources.Eq(test.expectedApplicationResources.driverResources) {
				t.Fatalf("driverResources are not equal, expected: %v, got: %v",
					test.expectedApplicationResources.driverResources, applicationResources.driverResources)
			}
			if !applicationResources.executorResources.Eq(test.expectedApplicationResources.executorResources) {
				t.Fatalf("executorResources are not equal, expected: %v, got: %v",
					test.expectedApplicationResources.executorResources, applicationResources.executorResources)
			}
			if applicationResources.executorCount != test.expectedApplicationResources.executorCount {
				t.Fatalf("executorCount are not equal, expected: %v, got: %v",
					test.expectedApplicationResources.executorCount, applicationResources.executorCount)
			}
		})
	}
}

func createPod(seconds int64, uid, instanceGroupLabel, instanceGroup string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:               types.UID(uid),
			CreationTimestamp: metav1.NewTime(time.Unix(seconds, 0)),
		},
		Spec: v1.PodSpec{
			NodeSelector: map[string]string{instanceGroupLabel: instanceGroup},
		},
	}
}

func TestIsEarliest(t *testing.T) {
	tests := []struct {
		name   string
		pod    *v1.Pod
		pods   []*v1.Pod
		result []string
	}{{
		name: "selects earliest unassigned",
		pod:  createPod(100, "1", "instance-group-label", "instance-group-foobar"),
		pods: []*v1.Pod{
			createPod(101, "3", "instance-group-label", "instance-group-foobar"),
			createPod(150, "2", "instance-group-label", "instance-group-foobar"),
			createPod(100, "1", "instance-group-label", "instance-group-foobar")},
		result: []string{},
	}, {
		name:   "selects if earliest and not in cache",
		pod:    createPod(100, "1", "instance-group-label", "instance-group-foobar"),
		pods:   []*v1.Pod{createPod(101, "2", "instance-group-label", "instance-group-foobar")},
		result: []string{},
	}, {
		name: "does not select when not earliest",
		pod:  createPod(100, "1", "instance-group-label", "instance-group-foobar"),
		pods: []*v1.Pod{
			createPod(101, "3", "instance-group-label", "instance-group-foobar"),
			createPod(99, "2", "instance-group-label", "instance-group-foobar"),
			createPod(100, "1", "instance-group-label", "instance-group-foobar")},
		result: []string{"2"},
	}, {
		name: "does not select when not earliest and not in cache",
		pod:  createPod(100, "1", "instance-group-label", "instance-group-foobar"),
		pods: []*v1.Pod{
			createPod(99, "3", "instance-group-label", "instance-group-foobar"),
			createPod(101, "2", "instance-group-label", "instance-group-foobar")},
		result: []string{"3"},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			drivers := filterToEarliestAndSort(test.pod, test.pods, "instance-group-label")
			uids := make([]string, 0, len(drivers))
			for _, d := range drivers {
				uids = append(uids, string(d.UID))
			}
			if !reflect.DeepEqual(uids, test.result) {
				t.Fatalf("expected: %v, got: %v", test.result, uids)
			}
		})
	}
}
