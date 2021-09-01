// limitations under the License.

package v1beta2

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
		Version: "v1beta2",
		Versions: []apiextensionsv1beta1.CustomResourceDefinitionVersion{
			{
				Name:    "v1beta2",
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
		Conversion: &apiextensionsv1beta1.CustomResourceConversion{
			Strategy:                 apiextensionsv1beta1.WebhookConverter,
			ConversionReviewVersions: []string{"v1", "v1beta1"},
			WebhookClientConfig:      nil,
		},
	},
}

// ResourceReservationCustomResourceDefinition returns the CRD definition for resource reservations
func ResourceReservationCustomResourceDefinition(webhook *apiextensionsv1beta1.WebhookClientConfig, supportedVersions ...apiextensionsv1beta1.CustomResourceDefinitionVersion) *apiextensionsv1beta1.CustomResourceDefinition {
	resourceReservation := resourceReservationDefinition.DeepCopy()
	resourceReservation.Spec.Conversion.WebhookClientConfig = webhook
	for i := range supportedVersions {
		supportedVersions[i].Storage = false
	}
	resourceReservation.Spec.Versions = append(resourceReservation.Spec.Versions, supportedVersions...)
	return resourceReservation
}
