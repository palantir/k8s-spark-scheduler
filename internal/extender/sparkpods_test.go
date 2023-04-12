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
	"github.com/palantir/k8s-spark-scheduler/internal/common"
	internaltypes "github.com/palantir/k8s-spark-scheduler/internal/types"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func createResources(cpu, memory, nvidiaGPU int64) *resources.Resources {
	return &resources.Resources{
		CPU:       *resource.NewQuantity(cpu, resource.DecimalSI),
		Memory:    *resource.NewQuantity(memory, resource.BinarySI),
		NvidiaGPU: *resource.NewQuantity(nvidiaGPU, resource.DecimalSI),
	}
}

func TestSparkResources(t *testing.T) {
	tests := []struct {
		name                         string
		pod                          v1.Pod
		expectedApplicationResources *internaltypes.SparkApplicationResources
	}{{
		name: "parses static allocation pod annotations into resources",
		pod: v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					common.DriverCPU:          "1",
					common.DriverMemory:       "2432Mi",
					common.DriverNvidiaGPUs:   "1",
					common.ExecutorCPU:        "2",
					common.ExecutorMemory:     "6758Mi",
					common.ExecutorNvidiaGPUs: "1",
					common.ExecutorCount:      "2",
				},
			},
		},
		expectedApplicationResources: &internaltypes.SparkApplicationResources{
			DriverResources:   createResources(1, 2432*1024*1024, 1),
			ExecutorResources: createResources(2, 6758*1024*1024, 1),
			MinExecutorCount:  2,
			MaxExecutorCount:  2,
		},
	}, {
		name: "parses dynamic allocation pod annotations into resources",
		pod: v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					common.DriverCPU:                "1",
					common.DriverMemory:             "2432Mi",
					common.DriverNvidiaGPUs:         "1",
					common.ExecutorCPU:              "2",
					common.ExecutorMemory:           "6758Mi",
					common.ExecutorNvidiaGPUs:       "1",
					common.DynamicAllocationEnabled: "true",
					common.DAMinExecutorCount:       "2",
					common.DAMaxExecutorCount:       "5",
				},
			},
		},
		expectedApplicationResources: &internaltypes.SparkApplicationResources{
			DriverResources:   createResources(1, 2432*1024*1024, 1),
			ExecutorResources: createResources(2, 6758*1024*1024, 1),
			MinExecutorCount:  2,
			MaxExecutorCount:  5,
		},
	}, {
		name: "parses static allocation pod annotations into resources when no gpu annotation is present",
		pod: v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					common.DriverCPU:      "1",
					common.DriverMemory:   "2432Mi",
					common.ExecutorCPU:    "2",
					common.ExecutorMemory: "6758Mi",
					common.ExecutorCount:  "2",
				},
			},
		},
		expectedApplicationResources: &internaltypes.SparkApplicationResources{
			DriverResources:   createResources(1, 2432*1024*1024, 0),
			ExecutorResources: createResources(2, 6758*1024*1024, 0),
			MinExecutorCount:  2,
			MaxExecutorCount:  2,
		},
	},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			applicationResources, err := sparkResources(context.Background(), &test.pod)
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			cacheQuantities(applicationResources.DriverResources)
			cacheQuantities(test.expectedApplicationResources.DriverResources)
			cacheQuantities(applicationResources.ExecutorResources)
			cacheQuantities(test.expectedApplicationResources.ExecutorResources)
			if !applicationResources.DriverResources.Eq(test.expectedApplicationResources.DriverResources) {
				t.Fatalf("driverResources are not equal, expected: %v, got: %v",
					test.expectedApplicationResources.DriverResources, applicationResources.DriverResources)
			}
			if !applicationResources.ExecutorResources.Eq(test.expectedApplicationResources.ExecutorResources) {
				t.Fatalf("executorResources are not equal, expected: %v, got: %v",
					test.expectedApplicationResources.ExecutorResources, applicationResources.ExecutorResources)
			}
			if applicationResources.MinExecutorCount != test.expectedApplicationResources.MinExecutorCount {
				t.Fatalf("minExecutorCount not equal to ExecutorCount in static allocation, expected: %v, got: %v",
					test.expectedApplicationResources.MinExecutorCount, applicationResources.MinExecutorCount)
			}
			if applicationResources.MaxExecutorCount != test.expectedApplicationResources.MaxExecutorCount {
				t.Fatalf("maxExecutorCount not equal to ExecutorCount in static allocation, expected: %v, got: %v",
					test.expectedApplicationResources.MaxExecutorCount, applicationResources.MaxExecutorCount)
			}
		})
	}
}

func cacheQuantities(resources *resources.Resources) {
	_ = resources.CPU.String()
	_ = resources.Memory.String()
	_ = resources.NvidiaGPU.String()
}

func createPod(seconds int64, uid, instanceGroupLabel, instanceGroup string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:               types.UID(uid),
			CreationTimestamp: metav1.NewTime(time.Unix(seconds, 0)),
		},
		Spec: v1.PodSpec{
			Affinity: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchExpressions: []v1.NodeSelectorRequirement{
									{
										Key:      instanceGroupLabel,
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{instanceGroup},
									},
								},
							},
						},
					},
				},
			},
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
