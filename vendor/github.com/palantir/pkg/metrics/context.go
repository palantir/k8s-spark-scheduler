// Copyright (c) 2018 Palantir Technologies. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package metrics

import (
	"context"
)

type mContextKey string

const (
	registryKey = mContextKey("metrics-registry")
	tagsKey     = mContextKey("metrics-tags")
)

var DefaultMetricsRegistry = NewRootMetricsRegistry()

func WithRegistry(ctx context.Context, registry Registry) context.Context {
	return context.WithValue(ctx, registryKey, registry)
}

func FromContext(ctx context.Context) Registry {
	registry, ok := ctx.Value(registryKey).(Registry)
	if !ok {
		registry = DefaultMetricsRegistry
	}
	rootRegistry, ok := registry.(*rootRegistry)
	if !ok {
		return registry
	}
	tagsContainer, ok := ctx.Value(tagsKey).(*tagsContainer)
	if !ok {
		return registry
	}
	return &childRegistry{
		root: rootRegistry,
		tags: tagsContainer.Tags,
	}
}

// AddTags adds the provided tags to the provided context. If the provided context already has tag storage, the new tags
// are appended to the storage in the existing context (which mutates the content of the context) and returns the same
// context. If the provided context does not store any tags, this call creates and returns a new context that has tag
// storage and stores the provided tags. If the provided context has any tags already set on it, the provided tags are
// appended to them. This function does not perform any de-duplication (that is, if a tag in the provided tags has the
// same key as an existing one, it will still be appended). Note that tags are shared
func AddTags(ctx context.Context, tags ...Tag) context.Context {
	if tagsContainer, ok := ctx.Value(tagsKey).(*tagsContainer); ok && tagsContainer != nil {
		tagsContainer.Tags = append(tagsContainer.Tags, tags...)
		return ctx
	}
	return context.WithValue(ctx, tagsKey, &tagsContainer{
		Tags: tags,
	})
}

// TagsFromContext returns the tags stored on the provided context. May be nil if no tags have been set on the context.
func TagsFromContext(ctx context.Context) Tags {
	if tagsContainer, ok := ctx.Value(tagsKey).(*tagsContainer); ok {
		return tagsContainer.Tags
	}
	return nil
}

type tagsContainer struct {
	Tags Tags
}
