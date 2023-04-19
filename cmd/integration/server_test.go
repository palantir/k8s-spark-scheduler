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

package integration

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta2"
	ssclientset "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/fake"
	"github.com/palantir/k8s-spark-scheduler/cmd"
	schedulerconfig "github.com/palantir/k8s-spark-scheduler/config"
	"github.com/palantir/k8s-spark-scheduler/internal/binpacker"
	"github.com/palantir/k8s-spark-scheduler/internal/common"
	"github.com/palantir/witchcraft-go-server/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	schedulerapi "k8s.io/kube-scheduler/extender/v1"
)

// Test_StaticCompaction tests that an existing static driver pod and its executors and be re-scheduled
func Test_StaticCompaction(t *testing.T) {
	rr := v1beta2.ResourceReservation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "appID1",
			Namespace: "podNamespace",
		},
		Spec: v1beta2.ResourceReservationSpec{
			Reservations: map[string]v1beta2.Reservation{
				"driver": {
					Node: "n1",
					Resources: map[string]*resource.Quantity{
						"cpu":    ToResource(resource.MustParse("1")),
						"memory": ToResource(resource.MustParse("1024Mi")),
					},
				},
				"executor-1": {
					Node: "n1",
					Resources: map[string]*resource.Quantity{
						"cpu":    ToResource(resource.MustParse("2")),
						"memory": ToResource(resource.MustParse("4096Mi")),
					},
				},
				"executor-2": {
					Node: "n1",
					Resources: map[string]*resource.Quantity{
						"cpu":    ToResource(resource.MustParse("2")),
						"memory": ToResource(resource.MustParse("4096Mi")),
					},
				},
			},
		},
		Status: v1beta2.ResourceReservationStatus{
			Pods: map[string]string{
				"driver":     "my-pod-driver",
				"executor-1": "my-pod-executor-1",
				"executor-2": "missing-pod-1",
			},
		},
	}
	driverPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-pod-driver",
			Namespace: "podNamespace",
			Labels: map[string]string{
				common.SparkRoleLabel:  common.Driver,
				common.SparkAppIDLabel: "appID1",
			},
			Annotations: map[string]string{
				common.DriverCPU:      "1",
				common.DriverMemory:   "1024Mi",
				common.ExecutorCPU:    "2",
				common.ExecutorMemory: "4096Mi",
				common.ExecutorCount:  "2",
			},
		},
		Spec: v1.PodSpec{
			NodeName:      "n1",
			SchedulerName: common.SparkSchedulerName,
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}
	existingNode := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "n1",
			Labels: map[string]string{
				v1.LabelTopologyZone:          "zone1",
				v1.LabelFailureDomainBetaZone: "zone1",
				"resource_channel":            "",
			},
		},
		Status: v1.NodeStatus{
			Capacity: map[v1.ResourceName]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("28192Mi"),
			},
			Allocatable: map[v1.ResourceName]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("28192Mi"),
			},
			Conditions: []v1.NodeCondition{
				{
					Type:   v1.NodeReady,
					Status: v1.ConditionTrue,
				},
			},
		},
	}

	executor1Pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-pod-executor-1",
			Namespace: "podNamespace",
			Labels: map[string]string{
				common.SparkRoleLabel:  common.Executor,
				common.SparkAppIDLabel: "appID1",
			},
		},
		Spec: v1.PodSpec{
			NodeName: "n1",
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}
	allClients := cmd.AllClient{
		APIExtensionsClient:  apiextensionsfake.NewSimpleClientset(),
		SparkSchedulerClient: ssclientset.NewSimpleClientset(&rr),
		KubeClient:           k8sfake.NewSimpleClientset(existingNode, driverPod, executor1Pod),
	}

	installConfig := schedulerconfig.Install{
		Install: config.Install{
			UseConsoleLog: true,
		},
		ShouldScheduleDynamicallyAllocatedExecutorsInSameAZ: true,
		BinpackAlgo: binpacker.SingleAzMinimalFragmentation,
	}
	testSetup := SetUpServer(context.Background(), t, installConfig, allClients)
	ctx := testSetup.ctx
	defer testSetup.cleanup()
	nodeNames := []string{existingNode.Name}
	args := schedulerapi.ExtenderArgs{
		Pod: &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "new-podName",
				Namespace: "podNamespace",
				Labels: map[string]string{
					common.SparkRoleLabel:  common.Executor,
					common.SparkAppIDLabel: "appID1",
				},
			},
			Spec: v1.PodSpec{
				Affinity: GetAffinityForInstanceGroup("resource_channel", "desiredInstanceGroup"),
			},
		},
		NodeNames: &nodeNames,
	}
	// Ensure we return the correct node
	filterResult := testSetup.ref.Predicate(ctx, args)
	returnedNodeNames := *filterResult.NodeNames
	assert.Equal(t, 1, len(returnedNodeNames))
	assert.Equal(t, "n1", returnedNodeNames[0])
	// And that we updated the reservation

	checkTheReservation := func() bool {
		existingRR, err := allClients.SparkSchedulerClient.SparkschedulerV1beta2().ResourceReservations(rr.Namespace).Get(ctx, rr.Name, metav1.GetOptions{})
		require.NoError(t, err)
		fmt.Println(existingRR.Status.Pods)
		return reflect.DeepEqual(existingRR.Status.Pods, map[string]string{
			"driver":     "my-pod-driver",
			"executor-1": "my-pod-executor-1",
			"executor-2": "new-podName",
		})
	}
	waitForCondition(ctx, t, checkTheReservation)
}
