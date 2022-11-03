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

package v1alpha2

import (
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	pluralName                 = "demands"
	demandGroupVersionResource = SchemeGroupVersion.WithResource(pluralName) // k8s requires this must be plural name
	demandGroupResource        = demandGroupVersionResource.GroupResource()
	oneFloat                   = float64(1)
	oneInt                     = int64(1)
	v1alpha2VersionDefinition  = v1.CustomResourceDefinitionVersion{
		Name:    SchemeGroupVersion.Version,
		Served:  true,
		Storage: true,
		Subresources: &v1.CustomResourceSubresources{
			Status: &v1.CustomResourceSubresourceStatus{},
		},
		Schema: &v1.CustomResourceValidation{
			OpenAPIV3Schema: &v1.JSONSchemaProps{
				Type:     "object",
				Required: []string{"spec", "metadata"},
				Properties: map[string]v1.JSONSchemaProps{
					"status": {
						Type:     "object",
						Required: []string{"phase"},
						Properties: map[string]v1.JSONSchemaProps{
							"phase": {
								Type: "string",
								Enum: getAllowedDemandPhasesEnum(),
							},
							"last-transition-time": {
								Type:     "string",
								Format:   "date-time",
								Nullable: true,
							},
							"fulfilled-zone": {
								Type:     "string",
								Nullable: true,
							},
						},
					},
					"spec": {
						Type:     "object",
						Required: []string{"units", "instance-group"},
						Properties: map[string]v1.JSONSchemaProps{
							"instance-group": {
								Type:      "string",
								MinLength: &oneInt,
							},
							"is-long-lived": {
								Type: "boolean",
							},
							"enforce-single-zone-scheduling": {
								Type: "boolean",
							},
							"zone": {
								Type: "string",
							},
							"units": {
								Type: "array",
								Items: &v1.JSONSchemaPropsOrArray{
									Schema: &v1.JSONSchemaProps{
										Type:     "object",
										Required: []string{"count", "resources"},
										Properties: map[string]v1.JSONSchemaProps{
											"resources": {
												Type: "object",
												Properties: map[string]v1.JSONSchemaProps{
													string(ResourceCPU):       {Type: "string", MinLength: &oneInt},
													string(ResourceMemory):    {Type: "string", MinLength: &oneInt},
													string(ResourceNvidiaGPU): {Type: "string", MinLength: &oneInt},
												},
											},
											"count":                  {Type: "integer", Minimum: &oneFloat},
											"pod-names-by-namespace": {Type: "object"},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		AdditionalPrinterColumns: []v1.CustomResourceColumnDefinition{{
			Name:        "status",
			Type:        "string",
			JSONPath:    ".status.phase",
			Description: "The phase of the Demand request",
		}, {
			Name:        "instance group",
			Type:        "string",
			JSONPath:    `.spec.instance-group`,
			Description: "The instance group for the Demand request",
		}, {
			Name:        "long lived",
			Type:        "boolean",
			JSONPath:    ".spec.is-long-lived",
			Description: "The lifecycle description of the Demand request",
		}, {
			Name:        "single zone",
			Type:        "boolean",
			JSONPath:    ".spec.enforce-single-zone-scheduling",
			Description: "The zone distribution description of the Demand request",
		}, {
			Name:        "zone",
			Type:        "string",
			JSONPath:    ".spec.zone",
			Description: "The zone where the demand should be fulfilled if specified",
		}, {
			Name:        "fulfilled zone",
			Type:        "boolean",
			JSONPath:    ".status.fulfilled-zone",
			Description: "The zone scaled to satisfy the single zone Demand request",
		}, {
			Name:        "units",
			Type:        "string",
			JSONPath:    ".spec.units",
			Description: "The units of the Demand request",
			Priority:    1,
		}},
	}
	demandDefinition = v1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: DemandCustomResourceDefinitionName(),
		},
		Spec: v1.CustomResourceDefinitionSpec{
			Group: SchemeGroupVersion.Group,
			Versions: []v1.CustomResourceDefinitionVersion{
				v1alpha2VersionDefinition,
			},
			Scope: v1.NamespaceScoped,
			Names: v1.CustomResourceDefinitionNames{
				Plural:     pluralName,
				Singular:   "demand",
				Kind:       "Demand",
				ShortNames: []string{"dem"},
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
)

// DemandCustomResourceDefinition returns the CustomResourceDefinition for the demand resource.
// Webhook provided has to support conversions between v1alpha2 and all the versions in the
// supportedVersions array.
func DemandCustomResourceDefinition(webhook *v1.WebhookClientConfig, supportedVersions ...v1.CustomResourceDefinitionVersion) *v1.CustomResourceDefinition {
	demand := demandDefinition.DeepCopy()
	demand.Spec.Conversion.Webhook.ClientConfig = webhook
	for i := range supportedVersions {
		supportedVersions[i].Storage = false
	}
	demand.Spec.Versions = append(demand.Spec.Versions, supportedVersions...)
	return demand
}

// DemandCustomResourceDefinitionName returns the demand resource name as a string
func DemandCustomResourceDefinitionName() string {
	return (&demandGroupResource).String()
}

// DemandGroupVersionResource returns the schema.GroupVersionResource for the demand resource
func DemandGroupVersionResource() schema.GroupVersionResource {
	return demandGroupVersionResource
}

func getAllowedDemandPhasesEnum() []v1.JSON {
	var json []v1.JSON
	for _, phase := range AllDemandPhases {
		json = append(json, v1.JSON{Raw: []byte("\"" + phase + "\"")})
	}
	return json
}
