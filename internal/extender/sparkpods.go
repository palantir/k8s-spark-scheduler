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
	"sort"
	"strconv"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	corelisters "k8s.io/client-go/listers/core/v1"
)

const (
	// SparkSchedulerName is the name of the kube-scheduler instance that talks with the extender
	SparkSchedulerName = "spark-scheduler"
	// SparkRoleLabel represents the label key for the spark-role of a pod
	SparkRoleLabel = "spark-role"
	// SparkAppIDLabel represents the label key for the spark application ID on a pod
	SparkAppIDLabel = "spark-app-id" // TODO(onursatici): change this to a spark specific label when spark has one
	// Driver represents the label key for a pod that identifies the pod as a spark driver
	Driver = "driver"
	// Executor represents the label key for a pod that identifies the pod as a spark executor
	Executor = "executor"
)

const (
	// DriverCPU represents the key of an annotation that describes how much CPU a spark driver requires
	DriverCPU = "spark-driver-cpu"
	// DriverMemory represents the key of an annotation that describes how much memory a spark driver requires
	DriverMemory = "spark-driver-mem"
	// ExecutorCPU represents the key of an annotation that describes how much cpu a spark executor requires
	ExecutorCPU = "spark-executor-cpu"
	// ExecutorMemory represents the key of an annotation that describes how much memory a spark executor requires
	ExecutorMemory = "spark-executor-mem"
	// DynamicAllocationEnabled sets whether dynamic allocation is enabled for this spark application (false by default)
	DynamicAllocationEnabled = "spark-dynamic-allocation-enabled"
	// ExecutorCount represents the key of an annotation that describes how many executors a spark application requires (required if DynamicAllocationEnabled is false)
	ExecutorCount = "spark-executor-count"
	// DAMinExecutorCount represents the lower bound on the number of executors a spark application requires if dynamic allocation is enabled (required if DynamicAllocationEnabled is true)
	DAMinExecutorCount = "spark-dynamic-allocation-min-executor-count"
	// DAMaxExecutorCount represents the upper bound on the number of executors a spark application can have if dynamic allocation is enabled (required if DynamicAllocationEnabled is true)
	DAMaxExecutorCount = "spark-dynamic-allocation-max-executor-count"
)

type sparkApplicationResources struct {
	driverResources   *resources.Resources
	executorResources *resources.Resources
	minExecutorCount  int
	maxExecutorCount  int
}

// SparkPodLister is a PodLister which can also list drivers per node selector
type SparkPodLister struct {
	corelisters.PodLister
	instanceGroupLabel string
}

// NewSparkPodLister creates and initializes a SparkPodLister
func NewSparkPodLister(delegate corelisters.PodLister, instanceGroupLabel string) *SparkPodLister {
	return &SparkPodLister{delegate, instanceGroupLabel}
}

// ListEarlierDrivers lists earlier driver than the given driver that has the same node selectors
func (s SparkPodLister) ListEarlierDrivers(driver *v1.Pod) ([]*v1.Pod, error) {
	selector := labels.Set(map[string]string{SparkRoleLabel: Driver}).AsSelector()
	drivers, err := s.List(selector)
	if err != nil {
		return nil, err
	}
	return filterToEarliestAndSort(driver, drivers, s.instanceGroupLabel), nil
}

func filterToEarliestAndSort(driver *v1.Pod, allDrivers []*v1.Pod, instanceGroupLabel string) []*v1.Pod {
	earlierDrivers := make([]*v1.Pod, 0, 10)
	for _, p := range allDrivers {
		// add only unscheduled drivers with the same instance group and targeted to the same scheduler
		if len(p.Spec.NodeName) == 0 &&
			p.Spec.SchedulerName == driver.Spec.SchedulerName &&
			p.Spec.NodeSelector[instanceGroupLabel] == driver.Spec.NodeSelector[instanceGroupLabel] &&
			p.CreationTimestamp.Before(&driver.CreationTimestamp) &&
			p.DeletionTimestamp == nil {
			earlierDrivers = append(earlierDrivers, p)
		}
	}
	sort.Slice(earlierDrivers, func(i, j int) bool {
		return earlierDrivers[i].CreationTimestamp.Before(&earlierDrivers[j].CreationTimestamp)
	})
	return earlierDrivers
}

func sparkResources(ctx context.Context, pod *v1.Pod) (*sparkApplicationResources, error) {
	parsedResources := map[string]resource.Quantity{}
	dynamicAllocationEnabled := false
	if daLabel, ok := pod.Annotations[DynamicAllocationEnabled]; ok {
		da, err := strconv.ParseBool(daLabel)
		if err != nil {
			return nil, fmt.Errorf("annotation DynamicAllocationEnabled could not be parsed as a boolean")
		}
		dynamicAllocationEnabled = da
	}

	for _, a := range []string{DriverCPU, DriverMemory, ExecutorCPU, ExecutorMemory, ExecutorCount, DAMinExecutorCount, DAMaxExecutorCount} {
		value, ok := pod.Annotations[a]
		if !ok {
			switch {
			case dynamicAllocationEnabled == false && a == ExecutorCount:
				return nil, fmt.Errorf("annotation ExecutorCount is required when DynamicAllocationEnabled is false")
			case dynamicAllocationEnabled == true && (a == DAMinExecutorCount || a == DAMaxExecutorCount):
				return nil, fmt.Errorf("annotation %v is required when DynamicAllocationEnabled is true", a)
			case a == ExecutorCount || a == DAMinExecutorCount || a == DAMaxExecutorCount:
				continue
			}
			return nil, fmt.Errorf("annotation %v is missing from driver", a)
		}
		quantity, err := resource.ParseQuantity(value)
		if err != nil {
			return nil, fmt.Errorf("annotation %v does not have a parseable value %v", a, value)
		}
		parsedResources[a] = quantity
	}

	var minExecutorCount int
	var maxExecutorCount int
	if dynamicAllocationEnabled {
		// justification for casting to int from int64: executor count is small (<1000)
		parsedMinExecutorCount := parsedResources[DAMinExecutorCount]
		parsedMaxExecutorCount := parsedResources[DAMaxExecutorCount]
		minExecutorCount = int(parsedMinExecutorCount.Value())
		maxExecutorCount = int(parsedMaxExecutorCount.Value())
	} else {
		parsedExecutorCount := parsedResources[ExecutorCount]
		minExecutorCount = int(parsedExecutorCount.Value())
		maxExecutorCount = int(parsedExecutorCount.Value())
	}

	driverResources := &resources.Resources{
		CPU:    parsedResources[DriverCPU],
		Memory: parsedResources[DriverMemory],
	}
	executorResources := &resources.Resources{
		CPU:    parsedResources[ExecutorCPU],
		Memory: parsedResources[ExecutorMemory],
	}
	return &sparkApplicationResources{driverResources, executorResources, minExecutorCount, maxExecutorCount}, nil
}

func sparkResourceUsage(driverResources, executorResources *resources.Resources, driverNode string, executorNodes []string) resources.NodeGroupResources {
	res := resources.NodeGroupResources{}
	res[driverNode] = driverResources
	for _, n := range executorNodes {
		res[n] = executorResources
	}
	return res
}

func (s SparkPodLister) getDriverPod(ctx context.Context, executor *v1.Pod) (*v1.Pod, error) {
	selector := labels.Set(map[string]string{SparkAppIDLabel: executor.Labels[SparkAppIDLabel], SparkRoleLabel: Driver}).AsSelector()
	driver, err := s.Pods(executor.Namespace).List(selector)
	if err != nil || len(driver) != 1 {
		return nil, err
	}
	return driver[0], nil
}