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

package extender

import (
	"context"

	"github.com/palantir/k8s-spark-scheduler/internal/cache"
	"github.com/palantir/k8s-spark-scheduler/internal/common/utils"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	v1 "k8s.io/api/core/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientcache "k8s.io/client-go/tools/cache"
)

// DemandGC is a background pod event handler which deletes any demand we have previously created for a pod when a pod gets scheduled.
// We also delete demands elsewhere in the extender when we schedule the pod, but those can miss some demands due to race conditions.
type DemandGC struct {
	demandCache *cache.SafeDemandCache
	ctx         context.Context
}

// StartDemandGC initializes the DemandGC which handles events in the background
func StartDemandGC(ctx context.Context, podInformer coreinformers.PodInformer, demandCache *cache.SafeDemandCache) {
	dgc := &DemandGC{
		demandCache: demandCache,
		ctx:         ctx,
	}

	podInformer.Informer().AddEventHandler(
		clientcache.FilteringResourceEventHandler{
			FilterFunc: utils.IsSparkSchedulerPod,
			Handler: clientcache.ResourceEventHandlerFuncs{
				UpdateFunc: dgc.onPodUpdate,
			},
		},
	)
}

func (dgc *DemandGC) onPodUpdate(oldObj interface{}, newObj interface{}) {
	oldPod, ok := oldObj.(*v1.Pod)
	if !ok {
		svc1log.FromContext(dgc.ctx).Error("failed to parse oldObj as pod")
		return
	}
	newPod, ok := newObj.(*v1.Pod)
	if !ok {
		svc1log.FromContext(dgc.ctx).Error("failed to parse newObj as pod")
		return
	}

	if !dgc.isPodScheduled(oldPod) && dgc.isPodScheduled(newPod) {
		DeleteDemandIfExists(dgc.ctx, dgc.demandCache, newPod, "DemandGC")
	}
}

func (dgc *DemandGC) isPodScheduled(pod *v1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == v1.PodScheduled && cond.Status == v1.ConditionTrue {
			return true
		}
	}
	return false
}
