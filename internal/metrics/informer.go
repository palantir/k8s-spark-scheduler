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

package metrics

import (
	"context"
	"time"

	"github.com/palantir/pkg/metrics"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	v1 "k8s.io/api/core/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientcache "k8s.io/client-go/tools/cache"
)

type informerDelayMetrics struct {
	ctx context.Context
}

// RegisterInformerDelayMetrics registers an event handler to the pod informer to report delays
func RegisterInformerDelayMetrics(ctx context.Context, informer coreinformers.PodInformer) {
	idm := &informerDelayMetrics{
		ctx: ctx,
	}
	informer.Informer().AddEventHandler(
		clientcache.ResourceEventHandlerFuncs{
			AddFunc: idm.onPodAdd,
		})
}

func (idm *informerDelayMetrics) onPodAdd(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		svc1log.FromContext(idm.ctx).Error("failed to parse obj as pod")
		return
	}
	now := time.Now()
	metrics.FromContext(idm.ctx).Histogram(podInformerDelay).Update(now.Sub(pod.CreationTimestamp.Time).Nanoseconds())
}
