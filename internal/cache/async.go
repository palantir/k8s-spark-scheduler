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

func (ac *asyncClient) Run(ctx context.Context) {
	for _, q := range ac.queue.GetConsumers() {
		go ac.runWorker(ctx, q)
	}
}

func (ac *asyncClient) runWorker(ctx context.Context, requests <-chan func() store.Request) {
	for {
		select {
		case <-ctx.Done():
			return
		case requestGetter := <-requests:
			r := requestGetter()
			switch r.Type {
			case store.CreateRequestType:
				ac.doCreate(requestCtx(ctx, r.Key, "create"), r.Key)
			case store.UpdateRequestType:
				ac.doUpdate(requestCtx(ctx, r.Key, "update"), r.Key)
			case store.DeleteRequestType:
				ac.doDelete(requestCtx(ctx, r.Key, "delete"), r.Key)
			}
		}
	}
}

func (ac *asyncClient) doCreate(ctx context.Context, key store.Key) {
	obj, ok := ac.objectStore.Get(key)
	if !ok {
		svc1log.FromContext(ctx).Info("Ignoring request for deleted object")
		return
	}
	result := ac.emptyObjectCreator()
	err := ac.client.Post().
		Namespace(obj.GetNamespace()).
		Resource(ac.resourceName).
		Body(obj).
		Do().
		Into(result)
	switch {
	case err == nil:
		ac.objectStore.OverrideResourceVersionIfNewer(ctx, obj)
	case isRetryableError(err):
		svc1log.FromContext(ctx).Warn("got retryable error, will retry", svc1log.Stacktrace(err))
		ac.queue.AddIfAbsent(store.CreateRequest(obj))
	default:
		logNonRetryableError(ctx, err)
	}
}

func (ac *asyncClient) doUpdate(ctx context.Context, key store.Key) {
	obj, ok := ac.objectStore.Get(key)
	if !ok {
		svc1log.FromContext(ctx).Info("Ignoring request for deleted object")
		return
	}

	result := ac.emptyObjectCreator()
	err := ac.client.Put().
		Namespace(obj.GetNamespace()).
		Resource(ac.resourceName).
		Name(obj.GetName()).
		Body(obj).
		Do().
		Into(result)
	if err != nil {
		ac.objectStore.OverrideResourceVersionIfNewer(ctx, obj)
	}
	switch {
	case err == nil:
		ac.objectStore.OverrideResourceVersionIfNewer(ctx, obj)
	case isRetryableError(err):
		svc1log.FromContext(ctx).Warn("got retryable error, will retry", svc1log.Stacktrace(err))
		ac.queue.AddIfAbsent(store.UpdateRequest(obj))
	case errors.IsConflict(err):
		svc1log.FromContext(ctx).Warn("got conflict, will retry", svc1log.Stacktrace(err))
		ac.queue.AddIfAbsent(store.UpdateRequest(obj))
	default:
		logNonRetryableError(ctx, err)
	}
}

func (ac *asyncClient) doDelete(ctx context.Context, key store.Key) {
	err := ac.client.Delete().
		Namespace(key.Namespace).
		Resource(ac.resourceName).
		Name(key.Name).
		Do().
		Error()
	if err != nil {
		ac.objectStore.Delete(key)
	}
	switch {
	case err == nil:
		ac.objectStore.Delete(key)
	case isRetryableError(err):
		svc1log.FromContext(ctx).Warn("got retryable error, will retry", svc1log.Stacktrace(err))
		ac.queue.AddIfAbsent(store.Request{Key: key, Type: store.DeleteRequestType})
	default:
		logNonRetryableError(ctx, err)
	}
}

func isRetryableError(err error) bool {
	return errors.IsServerTimeout(err) || errors.IsServiceUnavailable(err) ||
		errors.IsTooManyRequests(err) || errors.IsTimeout(err)
}

func logNonRetryableError(ctx context.Context, err error) {
	svc1log.FromContext(ctx).Error("got non retryable error, giving up", svc1log.Stacktrace(err))
}

func requestCtx(ctx context.Context, key store.Key, requestType string) context.Context {
	return svc1log.WithLoggerParams(ctx, svc1log.SafeParams(store.KeySafeParams(key)), svc1log.SafeParam("requestType", requestType))
}
