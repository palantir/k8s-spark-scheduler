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
	demandapi "github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/scaler/v1alpha2"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta1"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta2"
	ssclientset "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/fake"
	"github.com/palantir/k8s-spark-scheduler/cmd"
	config2 "github.com/palantir/k8s-spark-scheduler/config"
	"github.com/palantir/k8s-spark-scheduler/internal/common"
	"github.com/palantir/witchcraft-go-server/config"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	extensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	schedulerapi "k8s.io/kube-scheduler/extender/v1"
	"testing"
)

func Test_InitServerWithClients(t *testing.T) {
	crd := v1beta2.ResourceReservationCustomResourceDefinition(&v1.WebhookClientConfig{}, v1beta1.ResourceReservationCustomResourceDefinitionVersion())
	crd.Status = v1.CustomResourceDefinitionStatus{
		Conditions: []v1.CustomResourceDefinitionCondition{
			{
				Type:   v1.Established,
				Status: v1.ConditionTrue,
			},
		},
	}
	demandCRD := demandapi.DemandCustomResourceDefinition(nil)
	demandCRD.Status = v1.CustomResourceDefinitionStatus{
		Conditions: []v1.CustomResourceDefinitionCondition{
			{
				Type:   v1.Established,
				Status: v1.ConditionTrue,
			},
		},
	}
	allClients := cmd.AllClient{
		ApiExtensionsClient:  extensionsfake.NewSimpleClientset(crd, demandCRD),
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
