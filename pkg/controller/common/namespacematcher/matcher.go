// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

// Package namespacematcher provides a shared service that evaluates a label
// selector against a Namespace's current labels via the controller-runtime
// cache. It is used by the dynamic namespace-selector mode to decide, per
// event, whether a resource belongs to a namespace ECK currently manages.
package namespacematcher

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// Matcher decides whether a namespace currently matches the operator's
// configured namespaceSelector. A disabled Matcher matches every namespace
// (legacy / static-resolution modes).
type Matcher struct {
	cache    cache.Cache
	selector labels.Selector
	enabled  bool
}

// New returns a Matcher. When enabled is false the Matcher acts as a no-op
// (Matches always returns true) so callers can wire it unconditionally.
func New(c cache.Cache, sel labels.Selector, enabled bool) *Matcher {
	return &Matcher{cache: c, selector: sel, enabled: enabled}
}

// Enabled reports whether the matcher is actively filtering. Callers that
// need to skip work entirely in legacy mode can use this to short-circuit.
func (m *Matcher) Enabled() bool {
	if m == nil {
		return false
	}
	return m.enabled
}

// Selector returns the configured label selector. It is nil when the
// matcher is disabled.
func (m *Matcher) Selector() labels.Selector {
	if m == nil {
		return nil
	}
	return m.selector
}

// Matches returns true if the namespace's current labels match the
// configured selector. Cluster-scoped events (empty namespace) always
// match. When the matcher is disabled, every call returns true.
//
// On cache miss the function returns false: we would rather drop an event
// for a namespace we cannot resolve than reconcile resources outside our
// configured scope. The Namespace flip-state controller re-enqueues CRs
// when a namespace becomes visible/matching, so dropped early events are
// retried.
func (m *Matcher) Matches(ctx context.Context, ns string) bool {
	if m == nil || !m.enabled {
		return true
	}
	if ns == "" {
		return true
	}
	var nsObj corev1.Namespace
	if err := m.cache.Get(ctx, client.ObjectKey{Name: ns}, &nsObj); err != nil {
		return false
	}
	return m.selector.Matches(labels.Set(nsObj.Labels))
}
 
// MatchesLabels evaluates the selector against the given label set without
// touching the cache. Useful in the Namespace flip-state controller where
// the post-change labels are already on the event object.
func (m *Matcher) MatchesLabels(lbls map[string]string) bool {
	if m == nil || !m.enabled {
		return true
	}
	return m.selector.Matches(labels.Set(lbls))
}

// Predicate returns a controller-runtime predicate that admits events whose
// object lives in a currently-matching namespace. Cluster-scoped objects
// (empty namespace) are always admitted.
func Predicate(m *Matcher) predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		if obj == nil {
			return true
		}
		return m.Matches(context.Background(), obj.GetNamespace())
	})
}
