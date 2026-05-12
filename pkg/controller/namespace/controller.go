// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

// Package namespace contains the flip-state controller used in dynamic
// namespaceSelector mode. It watches Namespace objects, and whenever a
// namespace's match-state against the configured selector changes, it
// re-enqueues every ECK CR in that namespace by bumping an annotation.
// The bump triggers the per-kind controllers via their namespace-filtered
// predicates, so newly-matching namespaces get their CRs reconciled and
// newly-unmatching ones get filtered out at the next event.
package namespace

import (
	"context"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	agentv1alpha1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/agent/v1alpha1"
	apmv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/apm/v1"
	autoopsv1alpha1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/autoops/v1alpha1"
	autoscalingv1alpha1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/autoscaling/v1alpha1"
	beatv1beta1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/beat/v1beta1"
	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	entv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/enterprisesearch/v1"
	kbv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/kibana/v1"
	logstashv1alpha1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/logstash/v1alpha1"
	emsv1alpha1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/maps/v1alpha1"
	eprv1alpha1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/packageregistry/v1alpha1"
	policyv1alpha1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/stackconfigpolicy/v1alpha1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/namespacematcher"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/operator"
	ulog "github.com/elastic/cloud-on-k8s/v3/pkg/utils/log"
)

const (
	controllerName = "namespace-controller"

	// FlipAnnotationKey is bumped on every ECK CR in a namespace whose
	// match-state has just changed. Per-kind controllers see the resulting
	// Update event through their namespace-filtered predicates.
	FlipAnnotationKey = "eck.k8s.elastic.co/last-namespace-flip"
)

// eckListKinds is the set of ECK CR list kinds we re-enqueue when a
// namespace flips. Kinds whose CRDs are not installed in the cluster are
// transparently skipped at runtime.
func eckListKinds() []client.ObjectList {
	return []client.ObjectList{
		&esv1.ElasticsearchList{},
		&kbv1.KibanaList{},
		&apmv1.ApmServerList{},
		&entv1.EnterpriseSearchList{},
		&beatv1beta1.BeatList{},
		&agentv1alpha1.AgentList{},
		&logstashv1alpha1.LogstashList{},
		&emsv1alpha1.ElasticMapsServerList{},
		&policyv1alpha1.StackConfigPolicyList{},
		&autoscalingv1alpha1.ElasticsearchAutoscalerList{},
		&autoopsv1alpha1.AutoOpsAgentPolicyList{},
		&eprv1alpha1.PackageRegistryList{},
	}
}

// Add registers the namespace flip-state controller with the manager. It is
// only meaningful when the matcher is enabled (dynamic mode); the caller
// should skip registration otherwise.
func Add(mgr manager.Manager, params operator.Parameters) error {
	if !params.NamespaceMatcher.Enabled() {
		return nil
	}
	r := &reconciler{
		client:  mgr.GetClient(),
		matcher: params.NamespaceMatcher,
		state:   make(map[string]bool),
	}
	c, err := common.NewController(mgr, controllerName, r, params)
	if err != nil {
		return err
	}
	return c.Watch(source.Kind(mgr.GetCache(), &corev1.Namespace{}, &handler.TypedEnqueueRequestForObject[*corev1.Namespace]{}))
}

type reconciler struct {
	client  client.Client
	matcher *namespacematcher.Matcher

	mu    sync.Mutex
	state map[string]bool // namespace name -> last known match result
}

func (r *reconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log := ulog.FromContext(ctx).WithValues("namespace", request.Name)

	var ns corev1.Namespace
	if err := r.client.Get(ctx, types.NamespacedName{Name: request.Name}, &ns); err != nil {
		if apierrors.IsNotFound(err) {
			r.forget(request.Name)
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	isMatching := r.matcher.MatchesLabels(ns.Labels)
	wasMatching, known := r.swap(request.Name, isMatching)
	if known && wasMatching == isMatching {
		return reconcile.Result{}, nil
	}

	log.Info("namespace match-state changed", "matches", isMatching, "previously_known", known)

	enqueued, err := r.bumpAllECKResources(ctx, request.Name)
	if err != nil {
		return reconcile.Result{}, err
	}
	log.Info("re-enqueued ECK resources for namespace flip", "count", enqueued)
	return reconcile.Result{}, nil
}

func (r *reconciler) swap(ns string, isMatching bool) (prev, known bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	prev, known = r.state[ns]
	r.state[ns] = isMatching
	return prev, known
}

func (r *reconciler) forget(ns string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.state, ns)
}

// bumpAllECKResources lists every ECK CR kind in the given namespace and
// patches a no-op annotation on each one, so the per-kind controllers
// observe an Update event through their namespace-filtered predicates and
// reconcile accordingly.
func (r *reconciler) bumpAllECKResources(ctx context.Context, namespace string) (int, error) {
	stamp := time.Now().UTC().Format(time.RFC3339Nano)
	total := 0
	for _, list := range eckListKinds() {
		if err := r.client.List(ctx, list, client.InNamespace(namespace)); err != nil {
			// Tolerate missing CRDs (some ECK CRDs are optional installs):
			// skip kinds whose APIs aren't registered in this cluster.
			if meta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
				continue
			}
			return total, err
		}
		bumped, err := r.bumpList(ctx, list, stamp)
		if err != nil {
			return total, err
		}
		total += bumped
	}
	return total, nil
}

func (r *reconciler) bumpList(ctx context.Context, list client.ObjectList, stamp string) (int, error) {
	items, err := meta.ExtractList(list)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, item := range items {
		obj, ok := item.(client.Object)
		if !ok {
			continue
		}
		patch := client.MergeFrom(obj.DeepCopyObject().(client.Object))
		anns := obj.GetAnnotations()
		if anns == nil {
			anns = map[string]string{}
		}
		anns[FlipAnnotationKey] = stamp
		obj.SetAnnotations(anns)
		if err := r.client.Patch(ctx, obj, patch); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return count, err
		}
		count++
	}
	return count, nil
}

var _ reconcile.Reconciler = (*reconciler)(nil)
