// Code generated by client-gen. DO NOT EDIT.

package versioned

import (
	"fmt"
	"net/http"

	scalerv1alpha1 "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/typed/scaler/v1alpha1"
	scalerv1alpha2 "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/typed/scaler/v1alpha2"
	sparkschedulerv1beta1 "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/typed/sparkscheduler/v1beta1"
	sparkschedulerv1beta2 "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/typed/sparkscheduler/v1beta2"
	discovery "k8s.io/client-go/discovery"
	rest "k8s.io/client-go/rest"
	flowcontrol "k8s.io/client-go/util/flowcontrol"
)

type Interface interface {
	Discovery() discovery.DiscoveryInterface
	ScalerV1alpha1() scalerv1alpha1.ScalerV1alpha1Interface
	ScalerV1alpha2() scalerv1alpha2.ScalerV1alpha2Interface
	SparkschedulerV1beta1() sparkschedulerv1beta1.SparkschedulerV1beta1Interface
	SparkschedulerV1beta2() sparkschedulerv1beta2.SparkschedulerV1beta2Interface
}

// Clientset contains the clients for groups. Each group has exactly one
// version included in a Clientset.
type Clientset struct {
	*discovery.DiscoveryClient
	scalerV1alpha1        *scalerv1alpha1.ScalerV1alpha1Client
	scalerV1alpha2        *scalerv1alpha2.ScalerV1alpha2Client
	sparkschedulerV1beta1 *sparkschedulerv1beta1.SparkschedulerV1beta1Client
	sparkschedulerV1beta2 *sparkschedulerv1beta2.SparkschedulerV1beta2Client
}

// ScalerV1alpha1 retrieves the ScalerV1alpha1Client
func (c *Clientset) ScalerV1alpha1() scalerv1alpha1.ScalerV1alpha1Interface {
	return c.scalerV1alpha1
}

// ScalerV1alpha2 retrieves the ScalerV1alpha2Client
func (c *Clientset) ScalerV1alpha2() scalerv1alpha2.ScalerV1alpha2Interface {
	return c.scalerV1alpha2
}

// SparkschedulerV1beta1 retrieves the SparkschedulerV1beta1Client
func (c *Clientset) SparkschedulerV1beta1() sparkschedulerv1beta1.SparkschedulerV1beta1Interface {
	return c.sparkschedulerV1beta1
}

// SparkschedulerV1beta2 retrieves the SparkschedulerV1beta2Client
func (c *Clientset) SparkschedulerV1beta2() sparkschedulerv1beta2.SparkschedulerV1beta2Interface {
	return c.sparkschedulerV1beta2
}

// Discovery retrieves the DiscoveryClient
func (c *Clientset) Discovery() discovery.DiscoveryInterface {
	if c == nil {
		return nil
	}
	return c.DiscoveryClient
}

// NewForConfig creates a new Clientset for the given config.
// If config's RateLimiter is not set and QPS and Burst are acceptable,
// NewForConfig will generate a rate-limiter in configShallowCopy.
// NewForConfig is equivalent to NewForConfigAndClient(c, httpClient),
// where httpClient was generated with rest.HTTPClientFor(c).
func NewForConfig(c *rest.Config) (*Clientset, error) {
	configShallowCopy := *c

	if configShallowCopy.UserAgent == "" {
		configShallowCopy.UserAgent = rest.DefaultKubernetesUserAgent()
	}

	// share the transport between all clients
	httpClient, err := rest.HTTPClientFor(&configShallowCopy)
	if err != nil {
		return nil, err
	}

	return NewForConfigAndClient(&configShallowCopy, httpClient)
}

// NewForConfigAndClient creates a new Clientset for the given config and http client.
// Note the http client provided takes precedence over the configured transport values.
// If config's RateLimiter is not set and QPS and Burst are acceptable,
// NewForConfigAndClient will generate a rate-limiter in configShallowCopy.
func NewForConfigAndClient(c *rest.Config, httpClient *http.Client) (*Clientset, error) {
	configShallowCopy := *c
	if configShallowCopy.RateLimiter == nil && configShallowCopy.QPS > 0 {
		if configShallowCopy.Burst <= 0 {
			return nil, fmt.Errorf("burst is required to be greater than 0 when RateLimiter is not set and QPS is set to greater than 0")
		}
		configShallowCopy.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(configShallowCopy.QPS, configShallowCopy.Burst)
	}

	var cs Clientset
	var err error
	cs.scalerV1alpha1, err = scalerv1alpha1.NewForConfigAndClient(&configShallowCopy, httpClient)
	if err != nil {
		return nil, err
	}
	cs.scalerV1alpha2, err = scalerv1alpha2.NewForConfigAndClient(&configShallowCopy, httpClient)
	if err != nil {
		return nil, err
	}
	cs.sparkschedulerV1beta1, err = sparkschedulerv1beta1.NewForConfigAndClient(&configShallowCopy, httpClient)
	if err != nil {
		return nil, err
	}
	cs.sparkschedulerV1beta2, err = sparkschedulerv1beta2.NewForConfigAndClient(&configShallowCopy, httpClient)
	if err != nil {
		return nil, err
	}

	cs.DiscoveryClient, err = discovery.NewDiscoveryClientForConfigAndClient(&configShallowCopy, httpClient)
	if err != nil {
		return nil, err
	}
	return &cs, nil
}

// NewForConfigOrDie creates a new Clientset for the given config and
// panics if there is an error in the config.
func NewForConfigOrDie(c *rest.Config) *Clientset {
	cs, err := NewForConfig(c)
	if err != nil {
		panic(err)
	}
	return cs
}

// New creates a new Clientset for the given RESTClient.
func New(c rest.Interface) *Clientset {
	var cs Clientset
	cs.scalerV1alpha1 = scalerv1alpha1.New(c)
	cs.scalerV1alpha2 = scalerv1alpha2.New(c)
	cs.sparkschedulerV1beta1 = sparkschedulerv1beta1.New(c)
	cs.sparkschedulerV1beta2 = sparkschedulerv1beta2.New(c)

	cs.DiscoveryClient = discovery.NewDiscoveryClient(c)
	return &cs
}
