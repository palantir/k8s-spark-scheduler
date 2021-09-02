// limitations under the License.

package v1beta2

import (
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const resourceReservationCRDName = ResourceReservationPlural + "." + GroupName

var v1beta2VersionDefinition = v1.CustomResourceDefinitionVersion{
	Name:    "v1beta2",
	Served:  true,
	Storage: true,
	AdditionalPrinterColumns: []v1.CustomResourceColumnDefinition{{
		Name:        "driver",
		Type:        "string",
		JSONPath:    ".status.driverPod",
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
									Required: []string{"node", "cpu", "memory", "nvidia.com/gpu"},
									Properties: map[string]v1.JSONSchemaProps{
										"node": {
											Type: "string",
										},
										"cpu": {
											Type: "string",
										},
										"memory": {
											Type: "string",
										},
										"nvidia.com/gpu": {
											Type: "string",
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
		Name: resourceReservationCRDName,
	},
	Spec: v1.CustomResourceDefinitionSpec{
		Group: GroupName,
		Versions: []v1.CustomResourceDefinitionVersion{
			v1beta2VersionDefinition,
		},
		Scope: v1.NamespaceScoped,
		Names: v1.CustomResourceDefinitionNames{
			Plural:     ResourceReservationPlural,
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

// ResourceReservationCustomResourceDefinition returns the CRD definition for resource reservations
func ResourceReservationCustomResourceDefinitionBase() *v1.CustomResourceDefinition {
	return resourceReservationDefinition.DeepCopy()
}
