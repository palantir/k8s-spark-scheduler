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

package extendertest

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta2"
	ssclientset "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/fake"
	ssinformers "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/informers/externalversions"
	"github.com/palantir/k8s-spark-scheduler/config"
	sscache "github.com/palantir/k8s-spark-scheduler/internal/cache"
	"github.com/palantir/k8s-spark-scheduler/internal/crd"
	"github.com/palantir/k8s-spark-scheduler/internal/extender"
	"github.com/palantir/k8s-spark-scheduler/internal/metrics"
	"github.com/palantir/k8s-spark-scheduler/internal/sort"
	"github.com/palantir/witchcraft-go-logging/wlog"
	"github.com/palantir/witchcraft-go-logging/wlog/evtlog/evt2log"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	v1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	schedulerapi "k8s.io/kube-scheduler/extender/v1"
)

const (
	namespace       = "namespace"
	resourceChannel = "example"
)

// Harness is an extension of an extender with in-memory k8s stores
type Harness struct {
	Extender                 *extender.SparkSchedulerExtender
	UnschedulablePodMarker   *extender.UnschedulablePodMarker
	PodStore                 cache.Store
	NodeStore                cache.Store
	ResourceReservationCache *sscache.ResourceReservationCache
	SoftReservationStore     *sscache.SoftReservationStore
	Ctx                      context.Context
}

// NewTestExtender returns a new extender test harness, initialized with the provided k8s objects
func NewTestExtender(binpackAlgo string, objects ...runtime.Object) (*Harness, error) {
	wlog.SetDefaultLoggerProvider(wlog.NewNoopLoggerProvider()) // suppressing Witchcraft warning log about logger provider
	ctx := newLoggingContext()

	installConfig := config.Install{}
	fakeKubeClient := fake.NewSimpleClientset(objects...)
	fakeSchedulerClient := ssclientset.NewSimpleClientset()
	fakeAPIExtensionsClient := apiextensionsfake.NewSimpleClientset()
	kubeInformerFactory := informers.NewSharedInformerFactory(fakeKubeClient, 0)
	nodeInformerInterface := kubeInformerFactory.Core().V1().Nodes()
	nodeInformer := nodeInformerInterface.Informer()
	nodeLister := nodeInformerInterface.Lister()

	podInformerInterface := kubeInformerFactory.Core().V1().Pods()
	podInformer := podInformerInterface.Informer()
	podLister := podInformerInterface.Lister()

	sparkSchedulerInformerFactory := ssinformers.NewSharedInformerFactory(fakeSchedulerClient, 0)
	resourceReservationInformerInterface := sparkSchedulerInformerFactory.Sparkscheduler().V1beta2().ResourceReservations()
	resourceReservationInformer := resourceReservationInformerInterface.Informer()

	instanceGroupLabel := "resource_channel"

	go func() {
		kubeInformerFactory.Start(ctx.Done())
	}()
	go func() {
		sparkSchedulerInformerFactory.Start(ctx.Done())
	}()

	cache.WaitForCacheSync(
		ctx.Done(),
		nodeInformer.HasSynced,
		podInformer.HasSynced,
		resourceReservationInformer.HasSynced)

	resourceReservationCache, err := sscache.NewResourceReservationCache(
		ctx,
		resourceReservationInformerInterface,
		fakeSchedulerClient.SparkschedulerV1beta2(),
		installConfig.AsyncClientConfig,
	)
	if err != nil {
		return nil, err
	}

	lazyDemandInformer := crd.NewLazyDemandInformer(
		sparkSchedulerInformerFactory,
		fakeAPIExtensionsClient,
		time.Minute,
	)
	demandCache := sscache.NewSafeDemandCache(
		lazyDemandInformer,
		fakeSchedulerClient.ScalerV1alpha2(),
		installConfig.AsyncClientConfig,
	)
	softReservationStore := sscache.NewSoftReservationStore(ctx, podInformerInterface)

	sparkPodLister := extender.NewSparkPodLister(podLister, instanceGroupLabel)
	resourceReservationManager := extender.NewResourceReservationManager(ctx, resourceReservationCache, softReservationStore, sparkPodLister, podInformerInterface)

	overheadComputer := extender.NewOverheadComputer(
		ctx,
		podInformerInterface,
		resourceReservationManager,
		nodeLister,
	)

	isFIFO := true
	fifoConfig := config.FifoConfig{}
	binpacker := extender.SelectBinpacker(binpackAlgo)
	shouldScheduleDynamicallyAllocatedExecutorsInSameAZ := true

	wasteMetricsReporter := metrics.NewWasteMetricsReporter(ctx, instanceGroupLabel)

	sparkSchedulerExtender := extender.NewExtender(
		nodeLister,
		sparkPodLister,
		resourceReservationCache,
		softReservationStore,
		resourceReservationManager,
		fakeKubeClient.CoreV1(),
		demandCache,
		fakeAPIExtensionsClient,
		isFIFO,
		fifoConfig,
		binpacker,
		shouldScheduleDynamicallyAllocatedExecutorsInSameAZ,
		overheadComputer,
		instanceGroupLabel,
		sort.NewNodeSorter(nil, nil),
		wasteMetricsReporter,
	)

	unschedulablePodMarker := extender.NewUnschedulablePodMarker(
		nodeLister,
		podLister,
		fakeKubeClient.CoreV1(),
		overheadComputer,
		binpacker,
		installConfig.UnschedulablePodTimeoutDuration)

	return &Harness{
		Extender:                 sparkSchedulerExtender,
		UnschedulablePodMarker:   unschedulablePodMarker,
		PodStore:                 podInformer.GetStore(),
		NodeStore:                nodeInformer.GetStore(),
		ResourceReservationCache: resourceReservationCache,
		SoftReservationStore:     softReservationStore,
		Ctx:                      ctx,
	}, nil
}

// Schedule calls the extender's Predicate method for the given pod and nodes
func (h *Harness) Schedule(t *testing.T, pod v1.Pod, nodeNames []string) *schedulerapi.ExtenderFilterResult {
	extenderPredicateResult := h.Extender.Predicate(h.Ctx, schedulerapi.ExtenderArgs{
		Pod:       &pod,
		NodeNames: &nodeNames,
	})
	if extenderPredicateResult.NodeNames != nil && len(*extenderPredicateResult.NodeNames) > 0 {
		pod.Spec.NodeName = (*extenderPredicateResult.NodeNames)[0]
		pod.Status.Phase = v1.PodRunning
		err := h.PodStore.Update(&pod)
		if err != nil {
			t.Errorf("failed to update pod in store after schedule")
		}
	}
	return extenderPredicateResult
}

// TerminatePod terminates an existing pod
func (h *Harness) TerminatePod(pod v1.Pod) error {
	termination := v1.ContainerStateTerminated{
		ExitCode: 1,
	}
	pod.Status.ContainerStatuses = []v1.ContainerStatus{
		{
			State: v1.ContainerState{
				Terminated: &termination,
			},
		},
	}

	return h.PodStore.Update(&pod)
}

// AssertSuccessfulSchedule tries to schedule the provided pods and fails the test if not successful
func (h *Harness) AssertSuccessfulSchedule(t *testing.T, pod v1.Pod, nodeNames []string, errorDetails string) {
	result := h.Schedule(t, pod, nodeNames)
	if result.NodeNames == nil {
		t.Errorf("Scheduling should succeed: %s", errorDetails)
	}
}

// AssertSuccessfulScheduleOnNode tries to schedule the provided pods and fails if the pod isn't schedule on the expected node
func (h *Harness) AssertSuccessfulScheduleOnNode(t *testing.T, pod v1.Pod, nodeNames []string, expectedNode string, errorDetails string) {
	result := h.Schedule(t, pod, nodeNames)
	if result.NodeNames == nil {
		t.Errorf("Scheduling should succeed: %s", errorDetails)
	}
	if len(*result.NodeNames) != 1 || (*result.NodeNames)[0] != expectedNode {
		t.Errorf("Scheduled pod on the wrong node: %s", errorDetails)
	}
}

// AssertFailedSchedule tries to schedule the provided pods and fails the test if successful
func (h *Harness) AssertFailedSchedule(t *testing.T, pod v1.Pod, nodeNames []string, errorDetails string) {
	result := h.Schedule(t, pod, nodeNames)
	if result.NodeNames != nil {
		t.Errorf("Scheduling should not succeed: %s", errorDetails)
	}
}

// NewNode creates a new dummy node with the given name
func NewNode(name string, zone string) v1.Node {
	return v1.Node{
		TypeMeta: metav1.TypeMeta{
			Kind: "node",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"resource_channel":                  "batch-medium-priority",
				"com.palantir.rubix/instance-group": "batch-medium-priority",
				"test":                              "something",
				v1.LabelTopologyZone:                zone,
			},
			Annotations: map[string]string{},
		},
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:            *resource.NewQuantity(8, resource.DecimalSI),
				v1.ResourceMemory:         *resource.NewQuantity(8*1024*1024*1024, resource.BinarySI),
				v1beta2.ResourceNvidiaGPU: *resource.NewQuantity(1, resource.DecimalSI),
			},
			Conditions: []v1.NodeCondition{
				{
					Type:   v1.NodeReady,
					Status: v1.ConditionTrue,
				},
			},
		},
	}
}

// StaticAllocationSparkPods returns a list of pods corresponding to a Spark Application with 1 driver and numExecutors executors
// with the proper static allocation annotations set
func StaticAllocationSparkPods(sparkApplicationID string, numExecutors int) []v1.Pod {
	return StaticAllocationSparkPodsWithSizes(sparkApplicationID, numExecutors, "1", "1", "1", "1")
}

// StaticAllocationSparkPodsWithSizes behaves like StaticAllocationSparkPods except that it allows the caller to
// configure pod sizes
func StaticAllocationSparkPodsWithSizes(
	sparkApplicationID string,
	numExecutors int,
	driverMem, driverCPU, executorMem, executorCPU string,
) []v1.Pod {
	driverAnnotations := map[string]string{
		"spark-driver-cpu":            driverCPU,
		"spark-driver-mem":            driverMem,
		"spark-driver-nvidia.com/gpu": "1",
		"spark-executor-cpu":          executorCPU,
		"spark-executor-mem":          executorMem,
		"spark-executor-count":        fmt.Sprintf("%d", numExecutors),
	}
	return sparkApplicationPods(sparkApplicationID, driverAnnotations, numExecutors)
}

// StaticAllocationSparkPodsWithExecutorGPUs returns a list of pods corresponding to a Spark Application with 1 driver and numExecutors executors
// with the proper static allocation annotations set, executors also request one gpu
func StaticAllocationSparkPodsWithExecutorGPUs(sparkApplicationID string, numExecutors int) []v1.Pod {
	driverAnnotations := map[string]string{
		"spark-driver-cpu":              "1",
		"spark-driver-mem":              "1",
		"spark-driver-nvidia.com/gpu":   "1",
		"spark-executor-cpu":            "1",
		"spark-executor-mem":            "1",
		"spark-executor-nvidia.com/gpu": "1",
		"spark-executor-count":          fmt.Sprintf("%d", numExecutors),
	}
	return sparkApplicationPods(sparkApplicationID, driverAnnotations, numExecutors)
}

// DynamicAllocationSparkPods returns a list of pods corresponding to a Spark Application with 1 driver and maxExecutors executors
// with the proper dynamic allocation annotations set for min and max executor counts
func DynamicAllocationSparkPods(sparkApplicationID string, minExecutors int, maxExecutors int) []v1.Pod {
	return DynamicAllocationSparkPodsWithSizes(sparkApplicationID, minExecutors, maxExecutors, "1", "1", "1", "1")
}

// DynamicAllocationSparkPodsWithSizes behaves like DynamicAllocationSparkPods except that it allows the caller to
// configure pod sizes
func DynamicAllocationSparkPodsWithSizes(
	sparkApplicationID string,
	minExecutors int,
	maxExecutors int,
	driverMem, driverCPU, executorMem, executorCPU string,
) []v1.Pod {
	driverAnnotations := map[string]string{
		"spark-driver-cpu":                            driverCPU,
		"spark-driver-mem":                            driverMem,
		"spark-driver-nvidia.com/gpu":                 "1",
		"spark-executor-cpu":                          executorCPU,
		"spark-executor-mem":                          executorMem,
		"spark-dynamic-allocation-enabled":            "true",
		"spark-dynamic-allocation-min-executor-count": fmt.Sprintf("%d", minExecutors),
		"spark-dynamic-allocation-max-executor-count": fmt.Sprintf("%d", maxExecutors),
	}
	return sparkApplicationPods(sparkApplicationID, driverAnnotations, maxExecutors)
}

// sparkApplicationPods returns a list of pods corresponding to a Spark Application
func sparkApplicationPods(sparkApplicationID string, driverAnnotations map[string]string, maxExecutorCount int) []v1.Pod {
	pods := make([]v1.Pod, 1+maxExecutorCount)
	pods[0] = v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind: "pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      sparkApplicationID + "-spark-driver",
			Namespace: namespace,
			Labels: map[string]string{
				"spark-role":   "driver",
				"spark-app-id": sparkApplicationID,
			},
			Annotations: driverAnnotations,
		},
		Spec: v1.PodSpec{
			Affinity: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchExpressions: []v1.NodeSelectorRequirement{
									{
										Key:      "resource_channel",
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{"batch-medium-priority"},
									},
									{
										Key:      "com.palantir.rubix/instance-group",
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{"batch-medium-priority"},
									},
								},
							},
						},
					},
				},
			},
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	for i := 0; i < maxExecutorCount; i++ {
		pods[i+1] = v1.Pod{
			TypeMeta: metav1.TypeMeta{
				Kind: "pod",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-spark-exec-%d", sparkApplicationID, i),
				Namespace: namespace,
				Labels: map[string]string{
					"spark-role":   "executor",
					"spark-app-id": sparkApplicationID,
				},
			},
			Spec: v1.PodSpec{
				Affinity: &v1.Affinity{
					NodeAffinity: &v1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      "resource_channel",
											Operator: v1.NodeSelectorOpIn,
											Values:   []string{resourceChannel},
										},
									},
								},
							},
						},
					},
				},
			},
			Status: v1.PodStatus{
				Phase: v1.PodPending,
			},
		}
	}

	return pods
}

func newLoggingContext() context.Context {
	ctx := context.Background()
	logger := svc1log.New(os.Stdout, wlog.DebugLevel)
	ctx = svc1log.WithLogger(ctx, logger)
	evtlogger := evt2log.New(os.Stdout)
	ctx = evt2log.WithLogger(ctx, evtlogger)
	return ctx
}
