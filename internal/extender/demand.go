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
	"encoding/json"
	"time"

	demandapi "github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/scaler/v1alpha1"
	demandclient "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/typed/scaler/v1alpha1"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"github.com/palantir/k8s-spark-scheduler/internal"
	"github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
)

const (
	instanceGroupNodeSelector = "resource_channel"
	instanceGroupLabel        = "instance-group"
	maxRetries                = 3
)

const (
	podDemandCreated v1.PodConditionType = "PodDemandCreated"
)

var (
	demandCreatedCondition = &v1.PodCondition{
		Type:   podDemandCreated,
		Status: v1.ConditionTrue,
	}
)

func (s *SparkSchedulerExtender) updatePodStatus(ctx context.Context, pod *v1.Pod, condition *v1.PodCondition) {
	if !podutil.UpdatePodCondition(&pod.Status, demandCreatedCondition) {
		svc1log.FromContext(ctx).Info("pod condition for demand creation already exist")
		return
	}
	_, err := s.coreClient.Pods(pod.Namespace).UpdateStatus(pod)
	if err != nil {
		svc1log.FromContext(ctx).Warn("pod condition update failed", svc1log.SafeParam("reason", err.Error()))
	}
}

func (s *SparkSchedulerExtender) createDemandForExecutor(ctx context.Context, executorPod *v1.Pod, executorResources *resources.Resources) error {
	if !s.demandCRDInitialized.Load() {
		return nil
	}
	units := []demandapi.DemandUnit{
		{
			Count:  1,
			CPU:    executorResources.CPU,
			Memory: executorResources.Memory,
		},
	}
	newDemand, err := newDemand(executorPod, units)
	if err != nil {
		return err
	}
	err = s.createDemand(ctx, newDemand)
	if err != nil {
		return err
	}
	s.updatePodStatus(ctx, executorPod, demandCreatedCondition)
	return nil
}

func (s *SparkSchedulerExtender) createDemandForApplication(ctx context.Context, driverPod *v1.Pod, applicationResources *SparkApplicationResources) error {
	if !s.demandCRDInitialized.Load() {
		return nil
	}
	newDemand, err := newDemand(driverPod, demandResources(applicationResources))
	if err != nil {
		return err
	}
	err = s.createDemand(ctx, newDemand)
	if err != nil {
		return err
	}
	s.updatePodStatus(ctx, driverPod, demandCreatedCondition)
	return nil
}

func (s *SparkSchedulerExtender) createDemand(ctx context.Context, newDemand *demandapi.Demand) error {
	_, ok, err := checkForExistingDemand(ctx, newDemand.Namespace, newDemand.Name, s.demandClient)
	if err != nil {
		return err
	}
	if ok {
		svc1log.FromContext(ctx).Info("demand object already exists for pod so no action will be taken")
		return nil
	}
	demandObjectBytes, err := json.Marshal(newDemand)
	if err != nil {
		return werror.Wrap(err, "failed to marshal demand object")
	}
	svc1log.FromContext(ctx).Info("Creating demand object", svc1log.SafeParams(internal.DemandSafeParamsFromObj(newDemand)), svc1log.SafeParam("demandObjectBytes", string(demandObjectBytes)))
	return createDemandResource(ctx, newDemand, s.demandClient)
}

// removeDemandIfExists removes a demand object if it exists. Returns whether or not the demand was removed.
func (s *SparkSchedulerExtender) removeDemandIfExists(ctx context.Context, pod *v1.Pod) {
	if !s.demandCRDInitialized.Load() {
		return
	}
	demandName := demandResourceName(pod)
	removed, err := doRemoveDemandIfExists(ctx, pod.Namespace, demandName, s.demandClient)
	if err != nil {
		svc1log.FromContext(ctx).Info("Failed to remove existing demand resource - continuing anyways",
			svc1log.Stacktrace(err))
	} else if removed {
		svc1log.FromContext(ctx).Info("Removed demand object because capacity exists for pod", svc1log.SafeParams(internal.DemandSafeParams(demandName, pod.Namespace)))
	}
}

func (s *SparkSchedulerExtender) checkDemandCRDExists(ctx context.Context) bool {
	_, ready, err := checkCRDExists(demandapi.DemandCustomResourceDefinitionName(), s.apiExtensionsClient)
	if err != nil {
		svc1log.FromContext(ctx).Info("failed to determine if demand CRD exists", svc1log.Stacktrace(err))
		return false
	}
	if ready {
		svc1log.FromContext(ctx).Info("demand CRD has been initialized. Demand resources can now be created")
		s.demandCRDInitialized.Store(true)
	}
	return ready
}

func newDemand(pod *v1.Pod, units []demandapi.DemandUnit) (*demandapi.Demand, error) {
	instanceGroup, ok := pod.Spec.NodeSelector[instanceGroupNodeSelector]
	if !ok {
		return nil, werror.Error("No resource_channel label exists. Cannot map to InstanceGroup. Skipping demand object")
	}
	appID, ok := pod.Labels[SparkAppIDLabel]
	if !ok {
		return nil, werror.Error("pod did not contain expected label for AppID", werror.SafeParam("expectedLabel", SparkAppIDLabel))
	}
	demandName := demandResourceName(pod)
	return &demandapi.Demand{
		ObjectMeta: metav1.ObjectMeta{
			Name:      demandName,
			Namespace: pod.Namespace,
			Labels: map[string]string{
				SparkAppIDLabel:    appID,
				instanceGroupLabel: instanceGroup,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(pod, podGroupVersionKind),
			},
		},
		Spec: demandapi.DemandSpec{
			InstanceGroup: instanceGroup,
			Units:         units,
		},
	}, nil
}

func createDemandResource(ctx context.Context, demand *demandapi.Demand, client demandclient.ScalerV1alpha1Interface) error {
	safeParams := internal.DemandSafeParamsFromObj(demand)
	var err error
	for i := 0; i < maxRetries; i++ {
		_, err = client.Demands(demand.Namespace).Create(demand)
		switch {
		case err == nil:
			return nil
		case isRetryableError(err):
			if i == maxRetries-1 {
				return werror.Wrap(err, "failed to update demand object after multiple retries",
					werror.SafeParams(safeParams))
			}
			svc1log.FromContext(ctx).Info("attempt to update demand failed due to retryable error. Attempting to retry",
				svc1log.SafeParam("attempt", i+1),
				svc1log.SafeParams(safeParams),
				svc1log.SafeParam("error", err.Error()))
			time.Sleep(time.Second * 3)
		default:
			// error isn't retryable. Immediately give up
			break
		}
	}
	return werror.Wrap(err, "Failed to update demand resource", werror.SafeParams(safeParams))
}

func doRemoveDemandIfExists(ctx context.Context, namespace, name string, client demandclient.ScalerV1alpha1Interface) (bool, error) {
	demand, exists, err := checkForExistingDemand(ctx, namespace, name, client)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	safeParams := internal.DemandSafeParamsFromObj(demand)
	for i := 0; i < maxRetries; i++ {
		err = client.Demands(demand.Namespace).Delete(name, nil)
		switch {
		case err == nil:
			return true, nil
		case errors.IsGone(err):
			return true, nil
		case isRetryableError(err):
			if i == maxRetries-1 {
				return false, werror.Wrap(err, "failed to delete demand object after multiple retries",
					werror.SafeParams(safeParams))
			}
			svc1log.FromContext(ctx).Info("attempt to delete demand failed due to retryable error. Attempting to retry",
				svc1log.SafeParam("attempt", i+1),
				svc1log.SafeParams(safeParams),
				svc1log.SafeParam("error", err.Error()))
			time.Sleep(time.Second * 3)
		default:
			// error isn't retryable. Immediately give up
			break
		}
	}
	return false, werror.Wrap(err, "Failed to delete demand resource", werror.SafeParams(safeParams))
}

func checkForExistingDemand(ctx context.Context, namespace, demandName string, client demandclient.ScalerV1alpha1Interface) (*demandapi.Demand, bool, error) {
	safeParams := map[string]interface{}{
		"demandName": demandName,
		"namespace":  namespace,
	}
	existingDemand, err := client.Demands(namespace).Get(demandName, metav1.GetOptions{})

	switch {
	case err == nil:
		return existingDemand, true, nil
	case errors.IsNotFound(err):
		return nil, false, nil
	default:
		return nil, false, werror.Wrap(err, "Failed to check if demand object exists", werror.SafeParams(safeParams))
	}

}

func demandResources(applicationResources *SparkApplicationResources) []demandapi.DemandUnit {
	return []demandapi.DemandUnit{
		{
			Count:  1,
			CPU:    applicationResources.driverResources.CPU,
			Memory: applicationResources.driverResources.Memory,
		},
		{
			Count:  applicationResources.executorCount,
			CPU:    applicationResources.executorResources.CPU,
			Memory: applicationResources.executorResources.Memory,
		},
	}
}

func demandResourceName(pod *v1.Pod) string {
	return "demand-" + pod.Name
}

func isRetryableError(err error) bool {
	return errors.IsServerTimeout(err) || errors.IsServiceUnavailable(err) ||
		errors.IsTooManyRequests(err) || errors.IsTimeout(err)
}
