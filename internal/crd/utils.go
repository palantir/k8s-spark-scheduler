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

package crd

import (
	"context"
	"reflect"
	"time"

	werror "github.com/palantir/witchcraft-go-error"
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

// CheckCRDExists checks if the given crd exists and is established
func CheckCRDExists(ctx context.Context, crdName string, clientset apiextensionsclientset.Interface) (*v1.CustomResourceDefinition, bool, error) {
	crd, err := clientset.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, crdName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	for _, cond := range crd.Status.Conditions {
		if cond.Type == v1.Established && cond.Status == v1.ConditionTrue {
			return crd, true, nil
		}
	}
	return crd, false, nil
}

func getStorageVersion(crd *v1.CustomResourceDefinition) string {
	for _, crdVersion := range crd.Spec.Versions {
		if crdVersion.Storage {
			return crdVersion.Name
		}
	}
	return crd.Spec.Versions[0].Name
}

func emptyIfNil(x map[string]string) map[string]string {
	if x == nil {
		return map[string]string{}
	}
	return x
}

func verifyCRD(existing, desired *v1.CustomResourceDefinition) bool {
	return reflect.DeepEqual(existing.Spec.Versions, desired.Spec.Versions) && reflect.DeepEqual(emptyIfNil(existing.Annotations), emptyIfNil(desired.Annotations))
}

// EnsureResourceReservationsCRD is responsible for creating and ensuring the ResourceReservation CRD
// is created, it ensures that both v1beta1 and v1beta2 exist.
func EnsureResourceReservationsCRD(ctx context.Context, clientset apiextensionsclientset.Interface, annotations map[string]string, crd *v1.CustomResourceDefinition) error {
	if crd.Annotations == nil {
		crd.Annotations = make(map[string]string)
	}
	for k, v := range annotations {
		crd.Annotations[k] = v
	}
	existing, ready, err := CheckCRDExists(ctx, crd.Name, clientset)
	if err != nil {
		return werror.Wrap(err, "Failed to get CRD")
	}
	if ready && verifyCRD(existing, crd) {
		return nil
	}
	_, err = clientset.ApiextensionsV1().CustomResourceDefinitions().Create(context.Background(), crd, metav1.CreateOptions{})
	if err != nil {
		if errors.IsAlreadyExists(err) {
			existing, getErr := clientset.ApiextensionsV1().CustomResourceDefinitions().Get(context.Background(), crd.Name, metav1.GetOptions{})
			if getErr != nil {
				return werror.Wrap(getErr, "Failed to get existing CRD")
			}
			copyCrd := crd.DeepCopy()
			copyCrd.ResourceVersion = existing.ResourceVersion
			_, updateErr := clientset.ApiextensionsV1().CustomResourceDefinitions().Update(context.Background(), copyCrd, metav1.UpdateOptions{})
			if updateErr != nil {
				return werror.Wrap(updateErr, "Failed to update CRD")
			}
		} else {
			return werror.Wrap(err, "Failed to create CRD")
		}
	}

	err = wait.Poll(500*time.Millisecond, 60*time.Second, func() (bool, error) {
		existing, ready, err := CheckCRDExists(ctx, crd.Name, clientset)
		if err != nil {
			return false, err
		}
		return ready && verifyCRD(existing, crd), nil
	})

	if err != nil {
		deleteErr := clientset.ApiextensionsV1().CustomResourceDefinitions().Delete(context.Background(), crd.Name, metav1.DeleteOptions{})
		if deleteErr != nil {
			return werror.Wrap(deleteErr, err.Error())
		}
		return werror.Wrap(err, "Create CRD was successful but failed to ensure its existence")
	}
	return nil
}
