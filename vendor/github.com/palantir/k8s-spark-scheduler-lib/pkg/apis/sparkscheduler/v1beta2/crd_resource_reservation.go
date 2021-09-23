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

package v1beta2

import (
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler"
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var v1beta2VersionDefinition = v1.CustomResourceDefinitionVersion{
	Name:    "v1beta2",
	Served:  true,
	Storage: true,
	AdditionalPrinterColumns: []v1.CustomResourceColumnDefinition{{
		Name:        "driver",
		Type:        "string",
		JSONPath:    ".status.pods.driver",
		Description: "Pod name of the driver",
	}},
	Schema: &v1.CustomResourceValidation{
		OpenAPIV3Schema: &v1.JSONSchemaProps{
			Type:     "object",
			Required: []string{"spec", "metadata"},
			Properties: map[string]v1.JSONSchemaProps{
				"status": {
					Type:     "object",
					Required: []string{"pods"},
					Properties: map[string]v1.JSONSchemaProps{
						"pods": {
							Type: "object",
							AdditionalProperties: &v1.JSONSchemaPropsOrBool{
								Schema: &v1.JSONSchemaProps{
									Type: "string",
								},
							},
						},
					},
				},
				"spec": {
					Type:     "object",
					Required: []string{"reservations"},
					Properties: map[string]v1.JSONSchemaProps{
						"reservations": {
							Type: "object",
							AdditionalProperties: &v1.JSONSchemaPropsOrBool{
								Schema: &v1.JSONSchemaProps{
									Type:     "object",
									Required: []string{"node", "resources"},
									Properties: map[string]v1.JSONSchemaProps{
										"node": {
											Type: "string",
										},
										"resources": {
											Type: "object",
											AdditionalProperties: &v1.JSONSchemaPropsOrBool{
												Schema: &v1.JSONSchemaProps{Type: "string"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	},
}

var resourceReservationDefinition = &v1.CustomResourceDefinition{
	ObjectMeta: metav1.ObjectMeta{
		Name: sparkscheduler.ResourceReservationCRDName,
	},
	Spec: v1.CustomResourceDefinitionSpec{
		Group: sparkscheduler.GroupName,
		Versions: []v1.CustomResourceDefinitionVersion{
			v1beta2VersionDefinition,
		},
		Scope: v1.NamespaceScoped,
		Names: v1.CustomResourceDefinitionNames{
			Plural:     sparkscheduler.ResourceReservationPlural,
			Kind:       "ResourceReservation",
			ShortNames: []string{"rr"},
			Categories: []string{"all"},
		},
		Conversion: &v1.CustomResourceConversion{
			Strategy: v1.WebhookConverter,
			Webhook: &v1.WebhookConversion{
				ConversionReviewVersions: []string{"v1", "v1beta1"},
				ClientConfig:             nil,
			},
		},
	},
}

// ResourceReservationCustomResourceDefinition returns the CRD definition for resource reservations
func ResourceReservationCustomResourceDefinition(webhook *v1.WebhookClientConfig, supportedVersions ...v1.CustomResourceDefinitionVersion) *v1.CustomResourceDefinition {
	resourceReservation := resourceReservationDefinition.DeepCopy()
	resourceReservation.Spec.Conversion.Webhook.ClientConfig = webhook
	for i := range supportedVersions {
		supportedVersions[i].Storage = false
	}
	resourceReservation.Spec.Versions = append(resourceReservation.Spec.Versions, supportedVersions...)
	return resourceReservation
}
