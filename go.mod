module github.com/palantir/k8s-spark-scheduler

go 1.18

require (
	github.com/palantir/go-metrics v1.1.1
	github.com/palantir/k8s-spark-scheduler-lib v0.9.0
	github.com/palantir/pkg/cobracli v1.0.1
	github.com/palantir/pkg/metrics v1.2.0
	github.com/palantir/pkg/retry v1.1.1
	github.com/palantir/pkg/signals v1.0.1
	github.com/palantir/witchcraft-go-error v1.5.0
	github.com/palantir/witchcraft-go-logging v1.17.0
	github.com/palantir/witchcraft-go-server v1.30.0
	github.com/spf13/cobra v1.2.1
	go.uber.org/atomic v1.7.0
	k8s.io/api v0.22.15
	k8s.io/apiextensions-apiserver v0.22.15
	k8s.io/apimachinery v0.22.15
	k8s.io/client-go v0.22.15
	k8s.io/kube-scheduler v0.0.0
	k8s.io/kubernetes v1.22.15
	sigs.k8s.io/controller-runtime v0.6.4
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/evanphx/json-patch v4.9.0+incompatible // indirect
	github.com/go-logr/logr v0.4.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/go-cmp v0.5.6 // indirect
	github.com/google/gofuzz v1.1.0 // indirect
	github.com/googleapis/gnostic v0.4.1 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/imdario/mergo v0.3.9 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/json-iterator/go v1.1.11 // indirect
	github.com/julienschmidt/httprouter v1.3.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/nmiyake/pkg/errorstringer v1.0.0 // indirect
	github.com/openzipkin/zipkin-go v0.2.2 // indirect
	github.com/palantir/conjure-go-runtime/v2 v2.5.0 // indirect
	github.com/palantir/go-encrypted-config-value v1.1.0 // indirect
	github.com/palantir/pkg v1.0.1 // indirect
	github.com/palantir/pkg/datetime v1.0.1 // indirect
	github.com/palantir/pkg/safejson v1.0.1 // indirect
	github.com/palantir/pkg/safelong v1.0.1 // indirect
	github.com/palantir/pkg/safeyaml v1.0.1 // indirect
	github.com/palantir/pkg/tlsconfig v1.0.1 // indirect
	github.com/palantir/pkg/transform v1.0.0 // indirect
	github.com/palantir/pkg/uuid v1.0.1 // indirect
	github.com/palantir/witchcraft-go-params v1.2.0 // indirect
	github.com/palantir/witchcraft-go-tracing v1.4.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.17.0 // indirect
	golang.org/x/net v0.0.0-20211209124913-491a49abca63 // indirect
	golang.org/x/oauth2 v0.0.0-20210402161424-2e8d93401602 // indirect
	golang.org/x/sys v0.0.0-20220823224334-20c2bfdbfe24 // indirect
	golang.org/x/term v0.0.0-20210220032956-6a3ed077a48d // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/protobuf v1.26.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/apiserver v0.22.15 // indirect
	k8s.io/component-base v0.22.15 // indirect
	k8s.io/component-helpers v0.22.15 // indirect
	k8s.io/klog/v2 v2.9.0 // indirect
	k8s.io/kube-openapi v0.0.0-20211110012726-3cc51fd1e909 // indirect
	k8s.io/utils v0.0.0-20210521133846-da695404a2bc // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.2.1 // indirect
	sigs.k8s.io/yaml v1.2.0 // indirect
)

replace (
	// k8s.io/apiserver has transitive dependency on "naming" package of google.golang.org/grpc module, which isn't
	// available in newer versions of grpc.
	google.golang.org/grpc => google.golang.org/grpc v1.26.0
	// k8s.io/kubernetes sets these to v0.0.0, replace them with the current
	// k8s.io/kubernetes version to be able to depend on it.
	k8s.io/api => k8s.io/api v0.22.15
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.22.15
	k8s.io/apimachinery => k8s.io/apimachinery v0.22.15
	k8s.io/apiserver => k8s.io/apiserver v0.22.15
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.22.15
	k8s.io/client-go => k8s.io/client-go v0.22.15
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.22.15
	k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.22.15
	k8s.io/code-generator => k8s.io/code-generator v0.22.15
	k8s.io/component-base => k8s.io/component-base v0.22.15
	k8s.io/component-helpers => k8s.io/component-helpers v0.22.15
	k8s.io/controller-manager => k8s.io/controller-manager v0.22.15
	k8s.io/cri-api => k8s.io/cri-api v0.22.15
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.22.15
	k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.22.15
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.22.15
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.22.15
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.22.15
	k8s.io/kubectl => k8s.io/kubectl v0.22.15
	k8s.io/kubelet => k8s.io/kubelet v0.22.15
	k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.22.15
	k8s.io/metrics => k8s.io/metrics v0.22.15
	k8s.io/mount-utils => k8s.io/mount-utils v0.22.15
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.22.15
)
