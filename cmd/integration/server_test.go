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
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta2"
	"github.com/palantir/k8s-spark-scheduler/internal/extender"
	"testing"
	"time"

	demandapi "github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/scaler/v1alpha2"
	ssclientset "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/fake"
	"github.com/palantir/k8s-spark-scheduler/cmd"
	config2 "github.com/palantir/k8s-spark-scheduler/config"
	"github.com/palantir/k8s-spark-scheduler/internal/common"
	"github.com/palantir/witchcraft-go-server/config"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	extensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	schedulerapi "k8s.io/kube-scheduler/extender/v1"
)

func Test_InitServerWithClients(t *testing.T) {
	allClients := cmd.AllClient{
		APIExtensionsClient:  extensionsfake.NewSimpleClientset(getReadyCRDs()...),
		SparkSchedulerClient: ssclientset.NewSimpleClientset(),
		KubeClient:           k8sfake.NewSimpleClientset(),
	}

	installConfig := config2.Install{
		Install: config.Install{
			UseConsoleLog: true,
		},
	}
	testSetup := setUpServer(context.Background(), t, installConfig, allClients)
	ctx := testSetup.ctx
	defer testSetup.cleanup()
	var nodeNames []string
	args := schedulerapi.ExtenderArgs{
		Pod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "podName",
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
					common.ExecutorCount:  "4",
				},
			},
			Spec: corev1.PodSpec{
				Affinity: &corev1.Affinity{
					NodeAffinity: &corev1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{
								{
									MatchExpressions: []corev1.NodeSelectorRequirement{
										{
											Key:      "resource_channel",
											Operator: "",
											Values:   []string{"desiredInstanceGroup"},
										},
									},
								},
							},
						},
					},
				},
			},
			Status: corev1.PodStatus{},
		},
		NodeNames: &nodeNames,
	}
	testSetup.ref.Predicate(ctx, args)
	waitForCondition(ctx, t, func() bool {
		demandList, err := allClients.SparkSchedulerClient.ScalerV1alpha2().Demands("").List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		return len(demandList.Items) == 1
	})
	demandList, err := allClients.SparkSchedulerClient.ScalerV1alpha2().Demands("").List(ctx, metav1.ListOptions{})
	assert.NoError(t, err)
	item := demandList.Items[0]
	assert.Equal(t, demandapi.Demand{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demand-podName",
			Namespace: "podNamespace",
			Labels: map[string]string{
				"spark-app-id": "appID1",
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         "v1",
					Kind:               "Pod",
					Name:               "podName",
					Controller:         getBool(true),
					BlockOwnerDeletion: getBool(true),
				},
			},
		},
		Spec: demandapi.DemandSpec{
			Units: []demandapi.DemandUnit{
				{
					Resources: demandapi.ResourceList{
						demandapi.ResourceCPU:       resource.MustParse("1"),
						demandapi.ResourceMemory:    resource.MustParse("1024Mi"),
						demandapi.ResourceNvidiaGPU: resource.Quantity{},
					},
					Count:               1,
					PodNamesByNamespace: map[string][]string{"podNamespace": {"podName"}},
				},
				{
					Resources: demandapi.ResourceList{
						demandapi.ResourceCPU:       resource.MustParse("2"),
						demandapi.ResourceMemory:    resource.MustParse("4096Mi"),
						demandapi.ResourceNvidiaGPU: resource.Quantity{},
					},
					Count: 4,
				},
			},
			InstanceGroup: "desiredInstanceGroup",
		},
	}, item)
}

func Test_PartialFit(t *testing.T) {
	existingNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "n1",
			Labels: map[string]string{
				corev1.LabelTopologyZone:          "zone1",
				corev1.LabelFailureDomainBetaZone: "zone1",
				"resource_channel":                "desiredInstanceGroup",
			},
		},
		Status: corev1.NodeStatus{
			Capacity: map[corev1.ResourceName]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("28192Mi"),
			},
			Allocatable: map[corev1.ResourceName]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("28192Mi"),
			},
			Conditions: []corev1.NodeCondition{
				{
					Type:   corev1.NodeReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	allClients := cmd.AllClient{
		APIExtensionsClient:  extensionsfake.NewSimpleClientset(getReadyCRDs()...),
		SparkSchedulerClient: ssclientset.NewSimpleClientset(),
		KubeClient:           k8sfake.NewSimpleClientset(existingNode),
	}

	installConfig := config2.Install{
		Install: config.Install{
			UseConsoleLog: true,
		},
		ShouldScheduleDynamicallyAllocatedExecutorsInSameAZ: true,
		BinpackAlgo: extender.SingleAzMinimalFragmentation,
	}
	testSetup := setUpServer(context.Background(), t, installConfig, allClients)
	ctx := testSetup.ctx
	defer testSetup.cleanup()
	nodeNames := []string{existingNode.Name}
	args := schedulerapi.ExtenderArgs{
		Pod: &corev1.Pod{
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
					common.ExecutorCPU:    "10",
					common.ExecutorMemory: "96Mi",
					common.ExecutorCount:  "4",
				},
			},
			Spec: corev1.PodSpec{
				SchedulerName: common.SparkSchedulerName,
				Affinity: &corev1.Affinity{
					NodeAffinity: &corev1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{
								{
									MatchExpressions: []corev1.NodeSelectorRequirement{
										{
											Key:      "resource_channel",
											Operator: corev1.NodeSelectorOpIn,
											Values:   []string{"desiredInstanceGroup"},
										},
									},
								},
							},
						},
					},
				},
			},
			Status: corev1.PodStatus{},
		},
		NodeNames: &nodeNames,
	}
	v := testSetup.ref.Predicate(ctx, args)
	fmt.Println(v.NodeNames)
}

func Test_OrphanReservation(t *testing.T) {
	rr := v1beta2.ResourceReservation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "appID2",
			Namespace: "podNamespace",
		},
		Spec: v1beta2.ResourceReservationSpec{
			Reservations: map[string]v1beta2.Reservation{
				"missing-driver": {
					Node: "n1",
					Resources: map[string]*resource.Quantity{
						"cpu":    toResource(resource.MustParse("1000")),
						"memory": toResource(resource.MustParse("1024Mi")),
					},
				},
				"executor-1": {
					Node: "n1",
					Resources: map[string]*resource.Quantity{
						"cpu":    toResource(resource.MustParse("2")),
						"memory": toResource(resource.MustParse("4096Mi")),
					},
				},
			},
		},
		Status: v1beta2.ResourceReservationStatus{
			Pods: map[string]string{
				"missing-driver":     "my-pod-driver",
				"missing-executor-1": "my-pod-executor-1",
			},
		},
	}
	existingNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "n1",
			Labels: map[string]string{
				corev1.LabelTopologyZone:          "zone1",
				corev1.LabelFailureDomainBetaZone: "zone1",
				"resource_channel":                "desiredInstanceGroup",
			},
		},
		Status: corev1.NodeStatus{
			Capacity: map[corev1.ResourceName]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("28192Mi"),
			},
			Allocatable: map[corev1.ResourceName]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("28192Mi"),
			},
			Conditions: []corev1.NodeCondition{
				{
					Type:   corev1.NodeReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	allClients := cmd.AllClient{
		APIExtensionsClient:  extensionsfake.NewSimpleClientset(getReadyCRDs()...),
		SparkSchedulerClient: ssclientset.NewSimpleClientset(&rr),
		KubeClient:           k8sfake.NewSimpleClientset(existingNode),
	}

	installConfig := config2.Install{
		Install: config.Install{
			UseConsoleLog: true,
		},
		ShouldScheduleDynamicallyAllocatedExecutorsInSameAZ: true,
		BinpackAlgo: extender.SingleAzMinimalFragmentation,
	}
	testSetup := setUpServer(context.Background(), t, installConfig, allClients)
	ctx := testSetup.ctx
	defer testSetup.cleanup()
	nodeNames := []string{existingNode.Name}
	args := schedulerapi.ExtenderArgs{
		Pod: &corev1.Pod{
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
					common.ExecutorCount:  "1",
				},
			},
			Spec: corev1.PodSpec{
				SchedulerName: common.SparkSchedulerName,
				Affinity: &corev1.Affinity{
					NodeAffinity: &corev1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{
								{
									MatchExpressions: []corev1.NodeSelectorRequirement{
										{
											Key:      "resource_channel",
											Operator: corev1.NodeSelectorOpIn,
											Values:   []string{"desiredInstanceGroup"},
										},
									},
								},
							},
						},
					},
				},
			},
			Status: corev1.PodStatus{},
		},
		NodeNames: &nodeNames,
	}
	v := testSetup.ref.Predicate(ctx, args)
	fmt.Println(v.NodeNames)
}

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
						"cpu":    toResource(resource.MustParse("1")),
						"memory": toResource(resource.MustParse("1024Mi")),
					},
				},
				"executor-1": {
					Node: "n1",
					Resources: map[string]*resource.Quantity{
						"cpu":    toResource(resource.MustParse("2")),
						"memory": toResource(resource.MustParse("4096Mi")),
					},
				},
				"executor-2": {
					Node: "n1",
					Resources: map[string]*resource.Quantity{
						"cpu":    toResource(resource.MustParse("2")),
						"memory": toResource(resource.MustParse("4096Mi")),
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
	driverPod := &corev1.Pod{
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
		Spec: corev1.PodSpec{
			NodeName:      "n1",
			SchedulerName: common.SparkSchedulerName,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
	existingNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "n1",
			Labels: map[string]string{
				corev1.LabelTopologyZone:          "zone1",
				corev1.LabelFailureDomainBetaZone: "zone1",
				"resource_channel":                "",
			},
		},
		Status: corev1.NodeStatus{
			Capacity: map[corev1.ResourceName]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("28192Mi"),
			},
			Allocatable: map[corev1.ResourceName]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("28192Mi"),
			},
			Conditions: []corev1.NodeCondition{
				{
					Type:   corev1.NodeReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	executor1Pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-pod-executor-1",
			Namespace: "podNamespace",
			Labels: map[string]string{
				common.SparkRoleLabel:  common.Executor,
				common.SparkAppIDLabel: "appID1",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "n1",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
	allClients := cmd.AllClient{
		APIExtensionsClient:  extensionsfake.NewSimpleClientset(getReadyCRDs()...),
		SparkSchedulerClient: ssclientset.NewSimpleClientset(&rr),
		KubeClient:           k8sfake.NewSimpleClientset(existingNode, driverPod, executor1Pod),
	}

	installConfig := config2.Install{
		Install: config.Install{
			UseConsoleLog: true,
		},
		ShouldScheduleDynamicallyAllocatedExecutorsInSameAZ: true,
		BinpackAlgo: extender.SingleAzMinimalFragmentation,
	}
	testSetup := setUpServer(context.Background(), t, installConfig, allClients)
	ctx := testSetup.ctx
	defer testSetup.cleanup()
	nodeNames := []string{existingNode.Name}
	args := schedulerapi.ExtenderArgs{
		Pod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "new-podName",
				Namespace: "podNamespace",
				Labels: map[string]string{
					common.SparkRoleLabel:  common.Executor,
					common.SparkAppIDLabel: "appID1",
				},
				Annotations: map[string]string{
					common.ExecutorCPU:    "2",
					common.ExecutorMemory: "4096Mi",
					common.ExecutorCount:  "4",
				},
			},
			Spec: corev1.PodSpec{
				Affinity: &corev1.Affinity{
					NodeAffinity: &corev1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{
								{
									MatchExpressions: []corev1.NodeSelectorRequirement{
										{
											Key:      "resource_channel",
											Operator: "",
											Values:   []string{"desiredInstanceGroup"},
										},
									},
								},
							},
						},
					},
				},
			},
			Status: corev1.PodStatus{},
		},
		NodeNames: &nodeNames,
	}
	testSetup.ref.Predicate(ctx, args)
}

func Test_DynamicDoubleCounting(t *testing.T) {
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
						"cpu":    toResource(resource.MustParse("1")),
						"memory": toResource(resource.MustParse("1024Mi")),
					},
				},
			},
		},
		Status: v1beta2.ResourceReservationStatus{
			Pods: map[string]string{
				"driver": "my-pod-driver",
			},
		},
	}
	driverPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-pod-driver",
			Namespace: "podNamespace",
			UID:       "uid-0",
			Labels: map[string]string{
				common.SparkRoleLabel:  common.Driver,
				common.SparkAppIDLabel: "appID1",
			},
			Annotations: map[string]string{
				common.DriverCPU:                "1",
				common.DriverMemory:             "1024Mi",
				common.ExecutorCPU:              "2",
				common.ExecutorMemory:           "4096Mi",
				common.DAMinExecutorCount:       "0",
				common.DAMaxExecutorCount:       "2",
				common.DynamicAllocationEnabled: "true",
			},
		},
		Spec: corev1.PodSpec{
			NodeName:      "n1",
			SchedulerName: common.SparkSchedulerName,
			Containers: []corev1.Container{
				{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("1"),
							corev1.ResourceMemory: resource.MustParse("1024Mi"),
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
	existingNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "n1",
			Labels: map[string]string{
				corev1.LabelTopologyZone:          "zone1",
				corev1.LabelFailureDomainBetaZone: "zone1",
				"resource_channel":                "",
			},
		},
		Status: corev1.NodeStatus{
			Capacity: map[corev1.ResourceName]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("28192Mi"),
			},
			Allocatable: map[corev1.ResourceName]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("28192Mi"),
			},
			Conditions: []corev1.NodeCondition{
				{
					Type:   corev1.NodeReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	executor1Pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-pod-executor-1",
			Namespace: "podNamespace",
			UID:       "uid-1",
			Labels: map[string]string{
				common.SparkRoleLabel:  common.Executor,
				common.SparkAppIDLabel: "appID1",
			},
		},
		Spec: corev1.PodSpec{
			NodeName:      "n1",
			SchedulerName: common.SparkSchedulerName,
			Containers: []corev1.Container{
				{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("2"),
							corev1.ResourceMemory: resource.MustParse("4096Mi"),
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
	allClients := cmd.AllClient{
		APIExtensionsClient:  extensionsfake.NewSimpleClientset(getReadyCRDs()...),
		SparkSchedulerClient: ssclientset.NewSimpleClientset(&rr),
		KubeClient:           k8sfake.NewSimpleClientset(existingNode, driverPod, executor1Pod),
	}

	installConfig := config2.Install{
		Install: config.Install{
			UseConsoleLog: true,
		},
		ShouldScheduleDynamicallyAllocatedExecutorsInSameAZ: true,
		BinpackAlgo: extender.SingleAzMinimalFragmentation,
	}
	testSetup := setUpServer(context.Background(), t, installConfig, allClients)
	ctx := testSetup.ctx
	defer testSetup.cleanup()
	nodeNames := []string{existingNode.Name}
	args := schedulerapi.ExtenderArgs{
		Pod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "new-podName",
				Namespace: "podNamespace",
				UID:       "uid-2",
				Labels: map[string]string{
					common.SparkRoleLabel:  common.Executor,
					common.SparkAppIDLabel: "appID1",
				},
				Annotations: map[string]string{
					common.ExecutorCPU:    "2",
					common.ExecutorMemory: "4096Mi",
					common.ExecutorCount:  "4",
				},
			},
			Spec: corev1.PodSpec{
				Affinity: &corev1.Affinity{
					NodeAffinity: &corev1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{
								{
									MatchExpressions: []corev1.NodeSelectorRequirement{
										{
											Key:      "resource_channel",
											Operator: "",
											Values:   []string{"desiredInstanceGroup"},
										},
									},
								},
							},
						},
					},
				},
			},
			Status: corev1.PodStatus{},
		},
		NodeNames: &nodeNames,
	}
	testSetup.ref.Predicate(ctx, args)
}

func Test_DynamicCompaction(t *testing.T) {
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
						"cpu":    toResource(resource.MustParse("1")),
						"memory": toResource(resource.MustParse("1024Mi")),
					},
				},
				"executor-1": {
					Node: "n1",
					Resources: map[string]*resource.Quantity{
						"cpu":    toResource(resource.MustParse("2")),
						"memory": toResource(resource.MustParse("4096Mi")),
					},
				},
				"executor-2": {
					Node: "n1",
					Resources: map[string]*resource.Quantity{
						"cpu":    toResource(resource.MustParse("2")),
						"memory": toResource(resource.MustParse("4096Mi")),
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
	driverPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-pod-driver",
			Namespace: "podNamespace",
			Labels: map[string]string{
				common.SparkRoleLabel:  common.Driver,
				common.SparkAppIDLabel: "appID1",
			},
			Annotations: map[string]string{
				common.DriverCPU:                "1",
				common.DriverMemory:             "1024Mi",
				common.ExecutorCPU:              "2",
				common.ExecutorMemory:           "4096Mi",
				common.DAMinExecutorCount:       "2",
				common.DAMaxExecutorCount:       "4",
				common.DynamicAllocationEnabled: "true",
			},
		},
		Spec: corev1.PodSpec{
			NodeName:      "n1",
			SchedulerName: common.SparkSchedulerName,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
	existingNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "n1",
			Labels: map[string]string{
				corev1.LabelTopologyZone:          "zone1",
				corev1.LabelFailureDomainBetaZone: "zone1",
				"resource_channel":                "",
			},
		},
		Status: corev1.NodeStatus{
			Capacity: map[corev1.ResourceName]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("28192Mi"),
			},
			Allocatable: map[corev1.ResourceName]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("28192Mi"),
			},
			Conditions: []corev1.NodeCondition{
				{
					Type:   corev1.NodeReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	executor1Pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-pod-executor-1",
			Namespace: "podNamespace",
			Labels: map[string]string{
				common.SparkRoleLabel:  common.Executor,
				common.SparkAppIDLabel: "appID1",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "n1",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
	allClients := cmd.AllClient{
		APIExtensionsClient:  extensionsfake.NewSimpleClientset(getReadyCRDs()...),
		SparkSchedulerClient: ssclientset.NewSimpleClientset(&rr),
		KubeClient:           k8sfake.NewSimpleClientset(existingNode, driverPod, executor1Pod),
	}

	installConfig := config2.Install{
		Install: config.Install{
			UseConsoleLog: true,
		},
		ShouldScheduleDynamicallyAllocatedExecutorsInSameAZ: true,
		BinpackAlgo: extender.SingleAzMinimalFragmentation,
	}
	testSetup := setUpServer(context.Background(), t, installConfig, allClients)
	ctx := testSetup.ctx
	defer testSetup.cleanup()
	nodeNames := []string{} //existingNode.Name}
	args := schedulerapi.ExtenderArgs{
		Pod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "new-podName",
				Namespace: "podNamespace",
				Labels: map[string]string{
					common.SparkRoleLabel:  common.Executor,
					common.SparkAppIDLabel: "appID1",
				},
				Annotations: map[string]string{
					common.ExecutorCPU:    "2",
					common.ExecutorMemory: "4096Mi",
					common.ExecutorCount:  "4",
				},
			},
			Spec: corev1.PodSpec{
				Affinity: &corev1.Affinity{
					NodeAffinity: &corev1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{
								{
									MatchExpressions: []corev1.NodeSelectorRequirement{
										{
											Key:      "resource_channel",
											Operator: "",
											Values:   []string{"desiredInstanceGroup"},
										},
									},
								},
							},
						},
					},
				},
			},
			Status: corev1.PodStatus{},
		},
		NodeNames: &nodeNames,
	}
	testSetup.ref.Predicate(ctx, args)
}

func Test_InitServerWithClientsDy(t *testing.T) {
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
						"cpu":    toResource(resource.MustParse("1")),
						"memory": toResource(resource.MustParse("1024Mi")),
					},
				},
				"executor-1": {
					Node: "n1",
					Resources: map[string]*resource.Quantity{
						"cpu":    toResource(resource.MustParse("2")),
						"memory": toResource(resource.MustParse("4096Mi")),
					},
				},
			},
		},
		Status: v1beta2.ResourceReservationStatus{
			Pods: map[string]string{
				"driver":     "my-pod-driver",
				"executor-1": "my-pod-executor-1",
			},
		},
	}
	driverPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-pod-driver",
			Namespace: "podNamespace",
			Labels: map[string]string{
				common.SparkRoleLabel:  common.Driver,
				common.SparkAppIDLabel: "appID1",
			},
			Annotations: map[string]string{
				common.DriverCPU:                "1",
				common.DriverMemory:             "1024Mi",
				common.ExecutorCPU:              "2",
				common.ExecutorMemory:           "4096Mi",
				common.DAMinExecutorCount:       "1",
				common.DAMaxExecutorCount:       "2",
				common.DynamicAllocationEnabled: "true",
			},
		},
		Spec: corev1.PodSpec{
			NodeName:      "n1",
			SchedulerName: common.SparkSchedulerName,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
	existingNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "n1",
			Labels: map[string]string{
				corev1.LabelTopologyZone:          "zone1",
				corev1.LabelFailureDomainBetaZone: "zone1",
				"resource_channel":                "",
			},
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{
					Type:   corev1.NodeReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	newNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "n2",
			Labels: map[string]string{
				corev1.LabelTopologyZone:          "zone1",
				corev1.LabelFailureDomainBetaZone: "zone1",
				"resource_channel":                "",
			},
		},
		Status: corev1.NodeStatus{
			Capacity: map[corev1.ResourceName]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("8192Mi"),
			},
			Allocatable: map[corev1.ResourceName]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("8192Mi"),
			},
			Conditions: []corev1.NodeCondition{
				{
					Type:   corev1.NodeReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	executor1Pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-pod-executor-1",
			Namespace: "podNamespace",
			Labels: map[string]string{
				common.SparkRoleLabel:  common.Executor,
				common.SparkAppIDLabel: "appID1",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "n1",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
	allClients := cmd.AllClient{
		APIExtensionsClient:  extensionsfake.NewSimpleClientset(getReadyCRDs()...),
		SparkSchedulerClient: ssclientset.NewSimpleClientset(&rr),
		KubeClient:           k8sfake.NewSimpleClientset(existingNode, newNode, driverPod, executor1Pod),
	}

	installConfig := config2.Install{
		Install: config.Install{
			UseConsoleLog: true,
		},
		ShouldScheduleDynamicallyAllocatedExecutorsInSameAZ: true,
		BinpackAlgo: extender.SingleAzMinimalFragmentation,
	}
	testSetup := setUpServer(context.Background(), t, installConfig, allClients)
	ctx := testSetup.ctx
	defer testSetup.cleanup()
	nodeNames := []string{newNode.Name}
	args := schedulerapi.ExtenderArgs{
		Pod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "podName",
				Namespace: "podNamespace",
				Labels: map[string]string{
					common.SparkRoleLabel:  common.Executor,
					common.SparkAppIDLabel: "appID1",
				},
				Annotations: map[string]string{
					common.ExecutorCPU:    "2",
					common.ExecutorMemory: "4096Mi",
					common.ExecutorCount:  "4",
				},
			},
			Spec: corev1.PodSpec{
				Affinity: &corev1.Affinity{
					NodeAffinity: &corev1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{
								{
									MatchExpressions: []corev1.NodeSelectorRequirement{
										{
											Key:      "resource_channel",
											Operator: "",
											Values:   []string{"desiredInstanceGroup"},
										},
									},
								},
							},
						},
					},
				},
			},
			Status: corev1.PodStatus{},
		},
		NodeNames: &nodeNames,
	}
	testSetup.ref.Predicate(ctx, args)
	time.Sleep(time.Hour)
}
