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

package demands

import (
	"context"
	"encoding/json"

	demandapi "github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/scaler/v1alpha2"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"github.com/palantir/k8s-spark-scheduler/internal"
	"github.com/palantir/k8s-spark-scheduler/internal/binpacker"
	"github.com/palantir/k8s-spark-scheduler/internal/cache"
	"github.com/palantir/k8s-spark-scheduler/internal/common"
	"github.com/palantir/k8s-spark-scheduler/internal/common/utils"
	"github.com/palantir/k8s-spark-scheduler/internal/events"
	"github.com/palantir/k8s-spark-scheduler/internal/types"
	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Manager holds the types of demand operations that are available
type Manager interface {
	DeleteDemandIfExists(ctx context.Context, pod *v1.Pod, source string)
	CreateDemandForApplicationInAnyZone(ctx context.Context, driverPod *v1.Pod, applicationResources *types.SparkApplicationResources)
	CreateDemandForExecutorInAnyZone(ctx context.Context, executorPod *v1.Pod, executorResources *resources.Resources)
	CreateDemandForExecutorInSpecificZone(ctx context.Context, executorPod *v1.Pod, executorResources *resources.Resources, zone *demandapi.Zone)
}

type defaultManager struct {
	demands            *cache.SafeDemandCache
	binpacker          *binpacker.Binpacker
	instanceGroupLabel string
}

// NewDefaultManager creates the default implementation of the Manager
func NewDefaultManager(
	demands *cache.SafeDemandCache,
	binpacker *binpacker.Binpacker,
	instanceGroupLabel string) Manager {
	return &defaultManager{
		demands:            demands,
		binpacker:          binpacker,
		instanceGroupLabel: instanceGroupLabel,
	}
}

func (d *defaultManager) CreateDemandForExecutorInAnyZone(ctx context.Context, executorPod *v1.Pod, executorResources *resources.Resources) {
	d.CreateDemandForExecutorInSpecificZone(ctx, executorPod, executorResources, nil)
}

func (d *defaultManager) CreateDemandForExecutorInSpecificZone(ctx context.Context, executorPod *v1.Pod, executorResources *resources.Resources, zone *demandapi.Zone) {
	if !d.demands.CRDExists() {
		return
	}
	units := []demandapi.DemandUnit{
		{
			Count: 1,
			Resources: demandapi.ResourceList{
				demandapi.ResourceCPU:       executorResources.CPU,
				demandapi.ResourceMemory:    executorResources.Memory,
				demandapi.ResourceNvidiaGPU: executorResources.NvidiaGPU,
			},
			PodNamesByNamespace: map[string][]string{
				executorPod.Namespace: {executorPod.Name},
			},
		},
	}
	d.createDemand(ctx, executorPod, units, zone)
}

func (d *defaultManager) CreateDemandForApplicationInAnyZone(ctx context.Context, driverPod *v1.Pod, applicationResources *types.SparkApplicationResources) {
	if !d.demands.CRDExists() {
		return
	}
	d.createDemand(ctx, driverPod, demandResourcesForApplication(driverPod, applicationResources), nil)
}

func (d *defaultManager) createDemand(ctx context.Context, pod *v1.Pod, demandUnits []demandapi.DemandUnit, zone *demandapi.Zone) {
	instanceGroup, ok := internal.FindInstanceGroupFromPodSpec(pod.Spec, d.instanceGroupLabel)
	if !ok {
		svc1log.FromContext(ctx).Error("No instanceGroup label exists. Cannot map to InstanceGroup. Skipping demand object",
			svc1log.SafeParam("expectedLabel", d.instanceGroupLabel))
		return
	}

	newDemand, err := d.newDemand(pod, instanceGroup, demandUnits, zone)
	if err != nil {
		svc1log.FromContext(ctx).Error("failed to construct demand object", svc1log.Stacktrace(err))
		return
	}
	err = d.doCreateDemand(ctx, newDemand)
	if err != nil {
		svc1log.FromContext(ctx).Error("failed to create demand", svc1log.Stacktrace(err))
		return
	}
}

func (d *defaultManager) doCreateDemand(ctx context.Context, newDemand *demandapi.Demand) error {
	demandObjectBytes, err := json.Marshal(newDemand)
	if err != nil {
		return werror.Wrap(err, "failed to marshal demand object")
	}
	svc1log.FromContext(ctx).Info("Creating demand object", svc1log.SafeParams(internal.DemandSafeParamsFromObj(newDemand)), svc1log.SafeParam("demandObjectBytes", string(demandObjectBytes)))
	err = d.demands.Create(newDemand)
	if err != nil {
		_, ok := d.demands.Get(newDemand.Namespace, newDemand.Name)
		if ok {
			svc1log.FromContext(ctx).Info("demand object already exists for pod so no action will be taken")
			return nil
		}
	}
	events.EmitDemandCreated(ctx, newDemand)
	return err
}

func (d *defaultManager) removeDemandIfExists(ctx context.Context, pod *v1.Pod) {
	d.DeleteDemandIfExists(ctx, pod, "SparkSchedulerExtender")
}

// DeleteDemandIfExists removes a demand object if it exists, and emits an event tagged by the source of the deletion
func (d *defaultManager) DeleteDemandIfExists(ctx context.Context, pod *v1.Pod, source string) {
	if !d.demands.CRDExists() {
		return
	}
	demandName := utils.DemandName(pod)
	if demand, ok := d.demands.Get(pod.Namespace, demandName); ok {
		// there is no harm in the demand being deleted elsewhere in between the two calls.
		d.demands.Delete(pod.Namespace, demandName)
		svc1log.FromContext(ctx).Info("Removed demand object for pod", svc1log.SafeParams(internal.DemandSafeParams(demandName, pod.Namespace)))
		events.EmitDemandDeleted(ctx, demand, source)
	}
}

func (d *defaultManager) newDemand(pod *v1.Pod, instanceGroup string, units []demandapi.DemandUnit, zone *demandapi.Zone) (*demandapi.Demand, error) {
	appID, ok := pod.Labels[common.SparkAppIDLabel]
	if !ok {
		return nil, werror.Error("pod did not contain expected label for AppID", werror.SafeParam("expectedLabel", common.SparkAppIDLabel))
	}
	demandName := utils.DemandName(pod)
	return &demandapi.Demand{
		ObjectMeta: metav1.ObjectMeta{
			Name:      demandName,
			Namespace: pod.Namespace,
			Labels: map[string]string{
				common.SparkAppIDLabel: appID,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(pod, types.PodGroupVersionKind),
			},
		},
		Spec: demandapi.DemandSpec{
			InstanceGroup:               instanceGroup,
			Units:                       units,
			EnforceSingleZoneScheduling: d.binpacker.IsSingleAz,
			Zone:                        zone,
		},
	}, nil
}

func demandResourcesForApplication(driverPod *v1.Pod, applicationResources *types.SparkApplicationResources) []demandapi.DemandUnit {
	demandUnits := []demandapi.DemandUnit{
		{
			Count: 1,
			Resources: demandapi.ResourceList{
				demandapi.ResourceCPU:       applicationResources.DriverResources.CPU,
				demandapi.ResourceMemory:    applicationResources.DriverResources.Memory,
				demandapi.ResourceNvidiaGPU: applicationResources.DriverResources.NvidiaGPU,
			},
			// By specifying the pod driver pod here, we don't duplicate the resources of the pod with the created demand
			PodNamesByNamespace: map[string][]string{
				driverPod.Namespace: {driverPod.Name},
			},
		},
	}
	if applicationResources.MinExecutorCount > 0 {
		demandUnits = append(demandUnits, demandapi.DemandUnit{
			Count: applicationResources.MinExecutorCount,
			Resources: demandapi.ResourceList{
				demandapi.ResourceCPU:       applicationResources.ExecutorResources.CPU,
				demandapi.ResourceMemory:    applicationResources.ExecutorResources.Memory,
				demandapi.ResourceNvidiaGPU: applicationResources.ExecutorResources.NvidiaGPU,
			},
		})
	}
	return demandUnits
}
