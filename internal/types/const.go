package types

import v1 "k8s.io/api/core/v1"

var PodGroupVersionKind = v1.SchemeGroupVersion.WithKind("Pod")
