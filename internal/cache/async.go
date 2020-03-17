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

	"github.com/palantir/k8s-spark-scheduler/config"
	"github.com/palantir/k8s-spark-scheduler/internal/cache/store"
	"github.com/palantir/pkg/metrics"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	namespaceTerminatingPattern = regexp.MustCompile(`unable to create new content in namespace .* because it is being terminated`)
	namespaceNotFoundPattern    = regexp.MustCompile(`namespaces .* not found`)
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
	config      config.AsyncClientConfig
	metrics     *AsyncClientMetrics
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
				ac.doCreate(requestCtx(ctx, r.Key, "create"), r)
			case store.UpdateRequestType:
				ac.doUpdate(requestCtx(ctx, r.Key, "update"), r)
			case store.DeleteRequestType:
				ac.doDelete(requestCtx(ctx, r.Key, "delete"), r)
			}
		}
	}
}

func (ac *asyncClient) doCreate(ctx context.Context, r store.Request) {
	obj, ok := ac.objectStore.Get(r.Key)
	if !ok {
		svc1log.FromContext(ctx).Info("Ignoring request for deleted object")
		return
	}
	ac.metrics.MarkRequest(ctx)
	result, err := ac.client.Create(obj)
	switch {
	case err == nil:
		ac.objectStore.OverrideResourceVersionIfNewer(result)
	case isNamespaceTerminating(err):
		svc1log.FromContext(ctx).Info("can not create object because its namespace is being terminated")
		ac.objectStore.Delete(r.Key)
	default:
		didRetry := ac.maybeRetryRequest(ctx, r, err)
		if !didRetry {
			ac.objectStore.Delete(r.Key)
		}
	}
}

func (ac *asyncClient) doUpdate(ctx context.Context, r store.Request) {
	obj, ok := ac.objectStore.Get(r.Key)
	if !ok {
		svc1log.FromContext(ctx).Info("Ignoring request for deleted object")
		return
	}

	ac.metrics.MarkRequest(ctx)
	result, err := ac.client.Update(obj)
	switch {
	case err == nil:
		ac.objectStore.OverrideResourceVersionIfNewer(result)
	case errors.IsConflict(err):
		svc1log.FromContext(ctx).Warn("got conflict, will try updating resource version", svc1log.Stacktrace(err))
		newObj, getErr := ac.client.Get(r.Key.Namespace, r.Key.Name)
		switch {
		case getErr == nil:
			ac.objectStore.OverrideResourceVersionIfNewer(newObj)
			ac.doUpdate(ctx, store.UpdateRequest(newObj))
		default:
			ac.maybeRetryRequest(ctx, r, getErr)
		}
	default:
		ac.maybeRetryRequest(ctx, r, err)
	}
}

func (ac *asyncClient) doDelete(ctx context.Context, r store.Request) {
	ac.metrics.MarkRequest(ctx)
	err := ac.client.Delete(r.Key.Namespace, r.Key.Name)
	switch {
	case err == nil:
		return
	case errors.IsNotFound(err):
		svc1log.FromContext(ctx).Info("object already deleted")
	default:
		ac.maybeRetryRequest(ctx, r, err)
	}
}

func (ac *asyncClient) maybeRetryRequest(ctx context.Context, r store.Request, err error) bool {
	if r.RetryCount >= ac.config.MaxRetryCount() {
		svc1log.FromContext(ctx).Error("max retry count reached, dropping request", svc1log.Stacktrace(err))
		ac.metrics.MarkRequestDropped(ctx)
		return false
	}
	svc1log.FromContext(ctx).Warn("got retryable error, will retry", svc1log.SafeParam("retryCount", r.RetryCount), svc1log.Stacktrace(err))
	ac.metrics.MarkRequestRetry(ctx)
	ac.queue.AddIfAbsent(r.WithIncrementedRetryCount())
	return true
}

func requestCtx(ctx context.Context, key store.Key, requestType string) context.Context {
	return svc1log.WithLoggerParams(ctx, ObjectSafeParams(key.Namespace, key.Name), svc1log.SafeParam("requestType", requestType))
}

func isNamespaceTerminating(err error) bool {
	return (errors.IsForbidden(err) && namespaceTerminatingPattern.FindString(err.Error()) != "") ||
		(errors.IsNotFound(err) && namespaceNotFoundPattern.FindString(err.Error()) != "")
}

const (
	asyncClientRequest         = "foundry.spark.scheduler.async.request.count"
	asyncClientRetries         = "foundry.spark.scheduler.async.request.retries.count"
	asyncClientDroppedRequests = "foundry.spark.scheduler.async.request.dropped.count"
	objectTypeTagKey           = "objectType"
)

// AsyncClientMetrics emits metrics on retries and failures of the internal async client calls to the api server
// TODO: Move this to the metrics package after a refactor of that package to avoid cyclical imports
type AsyncClientMetrics struct {
	ObjectTypeTag string
}

// MarkRequest marks that a request to the api server is being made
func (acm *AsyncClientMetrics) MarkRequest(ctx context.Context) {
	metrics.FromContext(ctx).Counter(asyncClientRequest, metrics.MustNewTag(objectTypeTagKey, acm.ObjectTypeTag)).Inc(1)
}

// MarkRequestRetry marks that a request to the api server failed and is being retried
func (acm *AsyncClientMetrics) MarkRequestRetry(ctx context.Context) {
	metrics.FromContext(ctx).Counter(asyncClientRetries, metrics.MustNewTag(objectTypeTagKey, acm.ObjectTypeTag)).Inc(1)
}

// MarkRequestDropped marks that a request to the api server failed and is not going to be retried because it reached the maximum number of retries
func (acm *AsyncClientMetrics) MarkRequestDropped(ctx context.Context) {
	metrics.FromContext(ctx).Counter(asyncClientDroppedRequests, metrics.MustNewTag(objectTypeTagKey, acm.ObjectTypeTag)).Inc(1)
}
