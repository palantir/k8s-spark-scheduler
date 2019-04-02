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

package v1beta1

import (
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const resourceReservationCRDName = ResourceReservationPlural + "." + GroupName

var resourceReservationDefinition = &apiextensionsv1beta1.CustomResourceDefinition{
	ObjectMeta: metav1.ObjectMeta{
		Name: resourceReservationCRDName,
	},
	Spec: apiextensionsv1beta1.CustomResourceDefinitionSpec{
		Group:   GroupName,
		Version: "v1beta1",
		Versions: []apiextensionsv1beta1.CustomResourceDefinitionVersion{
			{
				Name:    "v1beta1",
				Served:  true,
				Storage: true,
			},
		},
		Scope: apiextensionsv1beta1.NamespaceScoped,
		Names: apiextensionsv1beta1.CustomResourceDefinitionNames{
			Plural:     ResourceReservationPlural,
			Kind:       "ResourceReservation",
			ShortNames: []string{"rr"},
			Categories: []string{"all"},
		},
		AdditionalPrinterColumns: []apiextensionsv1beta1.CustomResourceColumnDefinition{{
			Name:        "driver",
			Type:        "string",
			JSONPath:    ".status.driverPod",
			Description: "Pod name of the driver",
		}},
	},
}

// ResourceReservationCustomResourceDefinition returns the CRD definition for resource reservations
func ResourceReservationCustomResourceDefinition() *apiextensionsv1beta1.CustomResourceDefinition {
	return resourceReservationDefinition.DeepCopy()
}
