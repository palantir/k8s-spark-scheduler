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
	demandapi "github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/scaler/v1alpha2"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"reflect"
	"testing"
)

var testResource = createResources(1, 2432*1024*1024, 1)

var testResources = &sparkApplicationResources{
	driverResources:   testResource,
	executorResources: testResource,
	minExecutorCount:  0,
	maxExecutorCount:  0,
}
var testPod = &v1.Pod{
	ObjectMeta: metav1.ObjectMeta{
		Name:      "test-name",
		Namespace: "test-namespace",
	},
}

func Test_demandResourcesForApplication(t *testing.T) {
	type args struct {
		driverPod            *v1.Pod
		applicationResources *sparkApplicationResources
	}
	var tests = []struct {
		name string
		args args
		want []demandapi.DemandUnit
	}{
		{
			name: "Demand created for application contains pod to deduplicate against",
			args: args{
				driverPod:            testPod,
				applicationResources: testResources,
			},
			want: []demandapi.DemandUnit{
				{
					Resources: demandapi.ResourceList{
						demandapi.ResourceCPU:       testResources.driverResources.CPU,
						demandapi.ResourceMemory:    testResources.driverResources.Memory,
						demandapi.ResourceNvidiaGPU: testResources.driverResources.NvidiaGPU,
					},
					Count:               1,
					PodNamesByNamespace: map[string][]string{"test-namespace": {"test-name"}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := demandResourcesForApplication(tt.args.driverPod, tt.args.applicationResources); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("demandResourcesForApplication() = %v, want %v", got, tt.want)
			}
		})
	}
}
