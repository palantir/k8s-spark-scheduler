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
	"fmt"
	"reflect"
	"time"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta1"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"github.com/palantir/witchcraft-go-error"
	"k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

var podGroupVersionKind = v1.SchemeGroupVersion.WithKind("Pod")

func checkCRDExists(crdName string, clientset apiextensionsclientset.Interface) (*apiextensionsv1beta1.CustomResourceDefinition, bool, error) {
	crd, err := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Get(crdName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	for _, cond := range crd.Status.Conditions {
		if cond.Type == apiextensionsv1beta1.Established && cond.Status == apiextensionsv1beta1.ConditionTrue {
			return crd, true, nil
		}
	}
	return crd, false, nil
}

func getStorageVersion(crd *apiextensionsv1beta1.CustomResourceDefinition) string {
	for _, crdVersion := range crd.Spec.Versions {
		if crdVersion.Storage {
			return crdVersion.Name
		}
	}
	return crd.Spec.Version
}

func verifyCRD(existing, desired *apiextensionsv1beta1.CustomResourceDefinition) bool {
	return getStorageVersion(existing) == getStorageVersion(desired) && reflect.DeepEqual(existing.Annotations, desired.Annotations)
}

// EnsureResourceReservationsCRD is responsible for creating and ensuring the ResourceReservation CRD
// is created
func EnsureResourceReservationsCRD(clientset apiextensionsclientset.Interface, annotations map[string]string) error {
	crd := v1beta1.ResourceReservationCustomResourceDefinition()
	if crd.Annotations == nil {
		crd.Annotations = make(map[string]string)
	}
	for k, v := range annotations {
		crd.Annotations[k] = v
	}
	existing, ready, err := checkCRDExists(crd.Name, clientset)
	if err != nil {
		return werror.Wrap(err, "Failed to get CRD")
	}
	if ready && verifyCRD(existing, crd) {
		return nil
	}
	_, err = clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Create(crd)
	if err != nil {
		if errors.IsAlreadyExists(err) {
			existing, getErr := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Get(crd.Name, metav1.GetOptions{})
			if getErr != nil {
				return werror.Wrap(getErr, "Failed to get existing CRD")
			}
			copyCrd := crd.DeepCopy()
			copyCrd.ResourceVersion = existing.ResourceVersion
			_, updateErr := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Update(copyCrd)
			if updateErr != nil {
				return werror.Wrap(updateErr, "Failed to update CRD")
			}
		} else {
			return werror.Wrap(err, "Failed to create CRD")
		}
	}

	err = wait.Poll(500*time.Millisecond, 60*time.Second, func() (bool, error) {
		existing, ready, err := checkCRDExists(crd.Name, clientset)
		if err != nil {
			return false, err
		}
		return ready && verifyCRD(existing, v1beta1.ResourceReservationCustomResourceDefinition()), nil
	})

	if err != nil {
		deleteErr := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Delete(crd.Name, nil)
		if deleteErr != nil {
			return werror.Wrap(deleteErr, err.Error())
		}
		return werror.Wrap(err, "Create CRD was successful but failed to ensure its existence")
	}
	return nil
}

func newResourceReservation(driverNode string, executorNodes []string, driver *v1.Pod, driverResources, executorResources *resources.Resources) *v1beta1.ResourceReservation {
	reservations := make(map[string]v1beta1.Reservation, len(executorNodes)+1)
	reservations["driver"] = v1beta1.Reservation{
		Node:   driverNode,
		CPU:    driverResources.CPU,
		Memory: driverResources.Memory,
	}
	for idx, nodeName := range executorNodes {
		reservations[fmt.Sprintf("executor-%d", idx+1)] = v1beta1.Reservation{
			Node:   nodeName,
			CPU:    executorResources.CPU,
			Memory: executorResources.Memory,
		}
	}
	return &v1beta1.ResourceReservation{
		ObjectMeta: metav1.ObjectMeta{
			Name:            driver.Labels[SparkAppIDLabel],
			Namespace:       driver.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(driver, podGroupVersionKind)},
			Labels: map[string]string{
				v1beta1.AppIDLabel: driver.Labels[SparkAppIDLabel],
			},
		},
		Spec: v1beta1.ResourceReservationSpec{
			Reservations: reservations,
		},
		Status: v1beta1.ResourceReservationStatus{
			Pods: map[string]string{"driver": driver.Name},
		},
	}
}
