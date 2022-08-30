module github.com/palantir/k8s-spark-scheduler

go 1.18

require (
	github.com/palantir/go-metrics v1.1.1
	github.com/palantir/k8s-spark-scheduler-lib v0.5.0
	github.com/palantir/pkg/cobracli v1.0.1
	github.com/palantir/pkg/metrics v1.2.0
	github.com/palantir/pkg/retry v1.1.1
	github.com/palantir/pkg/signals v1.0.1
	github.com/palantir/witchcraft-go-error v1.5.0
	github.com/palantir/witchcraft-go-logging v1.17.0
	github.com/palantir/witchcraft-go-server v1.30.0
	github.com/spf13/cobra v1.2.1
	go.uber.org/atomic v1.7.0
	k8s.io/api v0.21.9
	k8s.io/apiextensions-apiserver v0.21.9
	k8s.io/apimachinery v0.21.9
	k8s.io/client-go v0.21.9
	k8s.io/kube-scheduler v0.0.0
	k8s.io/kubernetes v1.21.9
	sigs.k8s.io/controller-runtime v0.6.4
)

require (
	golang.org/x/sys v0.0.0-20220823224334-20c2bfdbfe24 // indirect
	golang.org/x/text v0.3.7 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace (
	// k8s.io/apiserver has transitive dependency on "naming" package of google.golang.org/grpc module, which isn't
	// available in newer versions of grpc.
	google.golang.org/grpc => google.golang.org/grpc v1.26.0
	// k8s.io/kubernetes sets these to v0.0.0, replace them with the current
	// k8s.io/kubernetes version to be able to depend on it.
	k8s.io/api => k8s.io/api v0.21.9
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.21.9
	k8s.io/apimachinery => k8s.io/apimachinery v0.21.9
	k8s.io/apiserver => k8s.io/apiserver v0.21.9
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.21.9
	k8s.io/client-go => k8s.io/client-go v0.21.9
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.21.9
	k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.21.9
	k8s.io/code-generator => k8s.io/code-generator v0.21.9
	k8s.io/component-base => k8s.io/component-base v0.21.9
	k8s.io/component-helpers => k8s.io/component-helpers v0.21.9
	k8s.io/controller-manager => k8s.io/controller-manager v0.21.9
	k8s.io/cri-api => k8s.io/cri-api v0.21.9
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.21.9
	k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.21.9
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.21.9
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.21.9
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.21.9
	k8s.io/kubectl => k8s.io/kubectl v0.21.9
	k8s.io/kubelet => k8s.io/kubelet v0.21.9
	k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.21.9
	k8s.io/metrics => k8s.io/metrics v0.21.9
	k8s.io/mount-utils => k8s.io/mount-utils v0.21.9
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.21.9
)
