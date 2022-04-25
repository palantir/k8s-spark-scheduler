module github.com/palantir/k8s-spark-scheduler

go 1.14

require (
	github.com/golang/groupcache v0.0.0-20191227052852-215e87163ea7 // indirect
	github.com/palantir/go-metrics v1.1.0
	github.com/palantir/k8s-spark-scheduler-lib v0.4.0
	github.com/palantir/pkg/cobracli v1.0.1
	github.com/palantir/pkg/metrics v1.0.1
	github.com/palantir/pkg/retry v1.1.1
	github.com/palantir/pkg/signals v1.0.1
	github.com/palantir/witchcraft-go-error v1.4.3
	github.com/palantir/witchcraft-go-logging v1.7.0
	github.com/palantir/witchcraft-go-server v1.27.0
	github.com/prometheus/client_golang v1.1.0 // indirect
	github.com/prometheus/common v0.8.0 // indirect
	github.com/spf13/cobra v0.0.5
	go.uber.org/atomic v1.6.0
	google.golang.org/genproto v0.0.0-20200115191322-ca5a22157cba // indirect
	k8s.io/api v0.18.8
	k8s.io/apiextensions-apiserver v0.18.8
	k8s.io/apimachinery v0.18.8
	k8s.io/client-go v0.18.8
	k8s.io/kube-scheduler v0.0.0
	k8s.io/kubernetes v1.18.8
	sigs.k8s.io/controller-runtime v0.6.4
)

// k8s.io/kubernetes sets these to v0.0.0, replace them with the current
// k8s.io/kubernetes version to be able to depend on it.
replace (
	k8s.io/api => k8s.io/api v0.18.8
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.18.8
	k8s.io/apimachinery => k8s.io/apimachinery v0.18.8
	k8s.io/apiserver => k8s.io/apiserver v0.18.8
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.18.8
	k8s.io/client-go => k8s.io/client-go v0.18.8
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.18.8
	k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.18.8
	k8s.io/code-generator => k8s.io/code-generator v0.18.8
	k8s.io/component-base => k8s.io/component-base v0.18.8
	k8s.io/cri-api => k8s.io/cri-api v0.18.8
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.18.8
	k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.18.8
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.18.8
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.18.8
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.18.8
	k8s.io/kubectl => k8s.io/kubectl v0.18.8
	k8s.io/kubelet => k8s.io/kubelet v0.18.8
	k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.18.8
	k8s.io/metrics => k8s.io/metrics v0.18.8
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.18.8
)
