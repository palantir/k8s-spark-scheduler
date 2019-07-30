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

package cache

import (
	"context"

	"github.com/palantir/k8s-spark-scheduler/internal/cache/store"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	rest "k8s.io/client-go/rest"
)

type object interface {
	metav1.Object
	runtime.Object
}

type asyncClient struct {
	client             rest.Interface
	resourceName       string
	emptyObjectCreator func() object
	queue              store.ShardedUniqueQueue
	objectStore        store.ObjectStore
}

func (as *asyncClient) Run(ctx context.Context) {
	for _, q := range as.queue.GetConsumers() {
		go as.runWorker(ctx, q)
	}
}

func (as *asyncClient) runWorker(ctx context.Context, requests <-chan func() store.Request) {
	for {
		select {
		case <-ctx.Done():
			return
		case requestGetter := <-requests:
			r := requestGetter()
			switch r.Type {
			case store.CreateRequestType:
				as.doCreate(requestCtx(ctx, r.Key, "create"), r.Key)
			case store.UpdateRequestType:
				as.doUpdate(requestCtx(ctx, r.Key, "update"), r.Key)
			case store.DeleteRequestType:
				as.doDelete(requestCtx(ctx, r.Key, "delete"), r.Key)
			}
		}
	}
}

func (as *asyncClient) doCreate(ctx context.Context, key store.Key) {
	obj, ok := as.objectStore.Get(key)
	if !ok {
		svc1log.FromContext(ctx).Info("Ignoring request for deleted object")
		return
	}
	result := as.emptyObjectCreator()
	err := as.client.Post().
		Namespace(obj.GetNamespace()).
		Resource(as.resourceName).
		Body(obj).
		Do().
		Into(result)
	switch {
	case err == nil:
		as.objectStore.OverrideResourceVersionIfNewer(ctx, obj)
	case isRetryableError(ctx, err):
		as.queue.AddIfAbsent(store.CreateRequest(obj))
	default:
		logAndIgnore(ctx, err)
	}
}

func (as *asyncClient) doUpdate(ctx context.Context, key store.Key) {
	obj, ok := as.objectStore.Get(key)
	if !ok {
		svc1log.FromContext(ctx).Info("Ignoring request for deleted object")
		return
	}

	result := as.emptyObjectCreator()
	err := as.client.Put().
		Namespace(obj.GetNamespace()).
		Resource(as.resourceName).
		Name(obj.GetName()).
		Body(obj).
		Do().
		Into(result)
	if err != nil {
		as.objectStore.OverrideResourceVersionIfNewer(ctx, obj)
	}
	switch {
	case err == nil:
		as.objectStore.OverrideResourceVersionIfNewer(ctx, obj)
	case isRetryableError(ctx, err):
		as.queue.AddIfAbsent(store.UpdateRequest(obj))
	case errors.IsConflict(err):
		svc1log.FromContext(ctx).Warn("got conflict, will retry", svc1log.Stacktrace(err))
		as.queue.AddIfAbsent(store.UpdateRequest(obj))
	default:
		logAndIgnore(ctx, err)
	}
}

func (as *asyncClient) doDelete(ctx context.Context, key store.Key) {
	err := as.client.Delete().
		Namespace(key.Namespace).
		Resource(as.resourceName).
		Name(key.Name).
		Do().
		Error()
	if err != nil {
		as.objectStore.Delete(key)
	}
	switch {
	case err == nil:
		as.objectStore.Delete(key)
	case isRetryableError(ctx, err):
		as.queue.AddIfAbsent(store.Request{Key: key, Type: store.DeleteRequestType})
	default:
		logAndIgnore(ctx, err)
	}
}

func isRetryableError(ctx context.Context, err error) bool {
	isRetryable := errors.IsServerTimeout(err) || errors.IsServiceUnavailable(err) ||
		errors.IsTooManyRequests(err) || errors.IsTimeout(err)
	if isRetryable {
		svc1log.FromContext(ctx).Warn("got retryable error, will retry", svc1log.Stacktrace(err))
	}
	return isRetryable
}

func logAndIgnore(ctx context.Context, err error) {
	svc1log.FromContext(ctx).Error("got non retryable error, giving up", svc1log.Stacktrace(err))
}

func requestCtx(ctx context.Context, key store.Key, requestType string) context.Context {
	return svc1log.WithLoggerParams(ctx, svc1log.SafeParams(store.KeySafeParams(key)), svc1log.SafeParam("requestType", requestType))
}
