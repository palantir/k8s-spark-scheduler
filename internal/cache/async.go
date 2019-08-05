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
	"regexp"

	"github.com/palantir/k8s-spark-scheduler/internal/cache/store"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	namespaceTerminatingPattern = regexp.MustCompile(`unable to create new content in namespace .* because it is being terminated`)
)

// Client is a generic representation of a kube client
// that acts on metav1.Object, asyncClient can be used
// for multiple k8s resources.
type Client interface {
	Create(metav1.Object) (metav1.Object, error)
	Update(metav1.Object) (metav1.Object, error)
	Delete(namespace, name string) error
	Get(namespace, name string) (metav1.Object, error)
}

type asyncClient struct {
	client      Client
	queue       store.ShardedUniqueQueue
	objectStore store.ObjectStore
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
	result, err := ac.client.Create(obj)
	switch {
	case err == nil:
		ac.objectStore.OverrideResourceVersionIfNewer(ctx, result)
	case isRetryableError(err):
		svc1log.FromContext(ctx).Warn("got retryable error, will retry", svc1log.Stacktrace(err))
		ac.queue.AddIfAbsent(store.CreateRequest(obj))
	case isNamespaceTerminating(err):
		svc1log.FromContext(ctx).Info("can not create object because its namespace is being terminated")
		ac.objectStore.Delete(key)
	default:
		logNonRetryableError(ctx, err)
		ac.objectStore.Delete(key)
	}
}

func (ac *asyncClient) doUpdate(ctx context.Context, key store.Key) {
	obj, ok := ac.objectStore.Get(key)
	if !ok {
		svc1log.FromContext(ctx).Info("Ignoring request for deleted object")
		return
	}

	result, err := ac.client.Update(obj)
	switch {
	case err == nil:
		ac.objectStore.OverrideResourceVersionIfNewer(ctx, result)
	case isRetryableError(err):
		svc1log.FromContext(ctx).Warn("got retryable error, will retry", svc1log.Stacktrace(err))
		ac.queue.AddIfAbsent(store.UpdateRequest(obj))
	case errors.IsConflict(err):
		svc1log.FromContext(ctx).Warn("got conflict, will try updating resource version", svc1log.Stacktrace(err))
		newObj, getErr := ac.client.Get(key.Namespace, key.Name)
		switch {
		case getErr == nil:
			ac.objectStore.OverrideResourceVersionIfNewer(ctx, newObj)
			ac.doUpdate(ctx, key)
		case isRetryableError(getErr):
			svc1log.FromContext(ctx).Warn("got retryable error, will retry", svc1log.Stacktrace(getErr))
			ac.queue.AddIfAbsent(store.UpdateRequest(obj))
		default:
			logNonRetryableError(ctx, err)
		}
	default:
		logNonRetryableError(ctx, err)
	}
}

func (ac *asyncClient) doDelete(ctx context.Context, key store.Key) {
	err := ac.client.Delete(key.Namespace, key.Name)
	switch {
	case err == nil:
		return
	case isRetryableError(err):
		svc1log.FromContext(ctx).Warn("got retryable error, will retry", svc1log.Stacktrace(err))
		ac.queue.AddIfAbsent(store.Request{Key: key, Type: store.DeleteRequestType})
	case errors.IsNotFound(err):
		svc1log.FromContext(ctx).Info("object already deleted")
	default:
		logNonRetryableError(ctx, err)
	}
}

func isRetryableError(err error) bool {
	return errors.IsServerTimeout(err) || errors.IsServiceUnavailable(err) ||
		errors.IsTooManyRequests(err) || errors.IsTimeout(err)
}

func logNonRetryableError(ctx context.Context, err error) {
	svc1log.FromContext(ctx).Error("got non retryable error", svc1log.Stacktrace(err))
}

func requestCtx(ctx context.Context, key store.Key, requestType string) context.Context {
	return svc1log.WithLoggerParams(ctx, svc1log.SafeParams(store.KeySafeParams(key)), svc1log.SafeParam("requestType", requestType))
}

func isNamespaceTerminating(err error) bool {
	return errors.IsForbidden(err) && namespaceTerminatingPattern.FindString(err.Error()) != ""
}
