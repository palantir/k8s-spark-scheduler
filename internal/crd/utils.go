package crd

import (
	"reflect"
	"time"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta1"
	werror "github.com/palantir/witchcraft-go-error"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

func CheckCRDExists(crdName string, clientset apiextensionsclientset.Interface) (*apiextensionsv1beta1.CustomResourceDefinition, bool, error) {
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
	existing, ready, err := CheckCRDExists(crd.Name, clientset)
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
		existing, ready, err := CheckCRDExists(crd.Name, clientset)
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
