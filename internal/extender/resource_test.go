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
	"fmt"
	ssclientset "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/fake"
	ssinformers "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/informers/externalversions"
	sscache "github.com/palantir/k8s-spark-scheduler/internal/cache"
	"github.com/palantir/witchcraft-go-logging/wlog"
	_ "github.com/palantir/witchcraft-go-logging/wlog-zap"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	v1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	schedulerapi "k8s.io/kubernetes/plugin/pkg/scheduler/api"
	"os"
	"testing"
)

const (
	namespace       = "namespace"
	resourceChannel = "example"
)

func TestScheduler(t *testing.T) {
	ctx := newLoggingContext()

	node1 := node("node1")
	node2 := node("node2")
	nodeNames := []string{node1.Name, node2.Name}
	podsToSchedule := sparkApplicationPods("2-executor-app", 2)

	fakeKubeClient := fake.NewSimpleClientset(
		&node1,
		&node2,
		&podsToSchedule[0],
		&podsToSchedule[1],
		&podsToSchedule[2])

	extender, podStore := newExtender(ctx, t, fakeKubeClient)

	for i := 0; i < 3; i++ {
		assertSuccessfulSchedule(
			ctx,
			t,
			extender,
			podsToSchedule[i],
			nodeNames,
			"There should be enough capacity to schedule the full application")
	}

	newExecutor := podsToSchedule[1]
	newExecutor.Name = "newly-requested-exec"
	assertFailedSchedule(
		ctx,
		t,
		extender,
		newExecutor,
		nodeNames,
		"Since all reservations are bound, a new executor should not be scheduled")

	terminatePod(t, podStore, podsToSchedule[1])

	assertSuccessfulSchedule(
		ctx,
		t,
		extender,
		newExecutor,
		nodeNames,
		"Because an executor is terminated, the new request can replace its reservation")
}

func newExtender(ctx context.Context, t *testing.T, fakeKubeClient *fake.Clientset) (*SparkSchedulerExtender, cache.Store) {
	fakeSchedulerClient := ssclientset.NewSimpleClientset()
	fakeAPIExtensionsClient := apiextensionsfake.NewSimpleClientset()
	kubeInformerFactory := informers.NewSharedInformerFactory(fakeKubeClient, 0)
	nodeInformer := kubeInformerFactory.Core().V1().Nodes()
	podInformer := kubeInformerFactory.Core().V1().Pods()

	sparkSchedulerInformerFactory := ssinformers.NewSharedInformerFactory(fakeSchedulerClient, 0)
	resourceReservationInformerBeta := sparkSchedulerInformerFactory.Sparkscheduler().V1beta1().ResourceReservations()

	go func() {
		kubeInformerFactory.Start(ctx.Done())
	}()
	go func() {
		sparkSchedulerInformerFactory.Start(ctx.Done())
	}()

	cache.WaitForCacheSync(
		ctx.Done(),
		nodeInformer.Informer().HasSynced,
		podInformer.Informer().HasSynced,
		resourceReservationInformerBeta.Informer().HasSynced)

	resourceReservationCache, err := sscache.NewResourceReservationCache(
		resourceReservationInformerBeta,
		fakeSchedulerClient.SparkschedulerV1beta1(),
	)
	if err != nil {
		t.Fatal("Error constructing resource reservation cache")
	}

	demandCache := sscache.NewSafeDemandCache(
		sparkSchedulerInformerFactory,
		fakeAPIExtensionsClient,
		fakeSchedulerClient.ScalerV1alpha1(),
	)
	if err != nil {
		t.Fatal("Error constructing demand cache")
	}

	overheadComputer := NewOverheadComputer(
		ctx,
		podInformer.Lister(),
		resourceReservationCache,
		nodeInformer.Lister(),
	)

	isFIFO := true
	binpacker := SelectBinpacker(tightlyPack)

	return NewExtender(
		nodeInformer.Lister(),
		NewSparkPodLister(podInformer.Lister()),
		resourceReservationCache,
		fakeKubeClient.CoreV1(),
		demandCache,
		fakeAPIExtensionsClient,
		isFIFO,
		binpacker,
		overheadComputer,
	), podInformer.Informer().GetStore()
}

func terminatePod(t *testing.T, podStore cache.Store, pod v1.Pod) {
	termination := v1.ContainerStateTerminated{
		ExitCode: 1,
	}
	pod.Status.ContainerStatuses = []v1.ContainerStatus{
		v1.ContainerStatus{
			State: v1.ContainerState{
				Terminated: &termination,
			},
		},
	}

	err := podStore.Update(&pod)
	if err != nil {
		t.Fatal("Failed to terminate pod using fake pod store")
	}
}

func node(name string) v1.Node {
	return v1.Node{
		TypeMeta: metav1.TypeMeta{
			Kind: "node",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "namespace",
			Labels: map[string]string{
				"resource_channel": "batch-medium-priority",
			},
			Annotations: map[string]string{},
		},
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:    *resource.NewQuantity(8, resource.DecimalSI),
				v1.ResourceMemory: *resource.NewQuantity(8*1024*1024*1024, resource.BinarySI),
			},
		},
	}
}

func sparkApplicationPods(sparkApplicationID string, numExecutors int) []v1.Pod {
	pods := make([]v1.Pod, 1+numExecutors)
	pods[0] = v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind: "pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-driver",
			Namespace: namespace,
			Labels: map[string]string{
				"spark-role":   "driver",
				"spark-app-id": sparkApplicationID,
			},
			Annotations: map[string]string{
				"spark-driver-cpu":     "1",
				"spark-driver-mem":     "1",
				"spark-executor-cpu":   "1",
				"spark-executor-mem":   "1",
				"spark-executor-count": fmt.Sprintf("%d", numExecutors),
			},
		},
		Spec: v1.PodSpec{
			NodeSelector: map[string]string{
				"resource_channel": "batch-medium-priority",
			},
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	for i := 0; i < numExecutors; i++ {
		pods[i+1] = v1.Pod{
			TypeMeta: metav1.TypeMeta{
				Kind: "pod",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("spark-exec-%d", i),
				Namespace: namespace,
				Labels: map[string]string{
					"spark-role":   "executor",
					"spark-app-id": sparkApplicationID,
				},
			},
			Spec: v1.PodSpec{
				NodeSelector: map[string]string{
					"resource_channel": resourceChannel,
				},
			},
			Status: v1.PodStatus{
				Phase: v1.PodPending,
			},
		}
	}

	return pods
}

func assertSuccessfulSchedule(ctx context.Context, t *testing.T, extender *SparkSchedulerExtender, pod v1.Pod, nodeNames []string, errorDetails string) {
	result := extender.Predicate(ctx, schedulerapi.ExtenderArgs{
		Pod:       pod,
		NodeNames: &nodeNames,
	})
	if result.NodeNames == nil {
		t.Errorf("Scheduling should succeed: %s", errorDetails)
	}
}

func assertFailedSchedule(ctx context.Context, t *testing.T, extender *SparkSchedulerExtender, pod v1.Pod, nodeNames []string, errorDetails string) {
	result := extender.Predicate(ctx, schedulerapi.ExtenderArgs{
		Pod:       pod,
		NodeNames: &nodeNames,
	})
	if result.NodeNames != nil {
		t.Errorf("Scheduling should not succeed: %s", errorDetails)
	}
}

func newLoggingContext() context.Context {
	logger := svc1log.New(os.Stdout, wlog.DebugLevel)
	ctx := context.Background()
	ctx = svc1log.WithLogger(ctx, logger)
	return ctx
}
