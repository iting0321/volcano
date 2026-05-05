/*
Copyright 2018 The Kubernetes Authors.
Copyright 2018-2025 The Volcano Authors.

Modifications made by Volcano authors:
- Added hierarchical queue support with weight and hierarchy configuration
- Enhanced queue management with reclaimable resource controls
- Migrated to v1beta1 API from v1alpha1/v1alpha2 for improved stability

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package api

import (
	"k8s.io/apimachinery/pkg/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"volcano.sh/apis/pkg/apis/scheduling"
	schedulingscheme "volcano.sh/apis/pkg/apis/scheduling/scheme"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	queueutil "volcano.sh/volcano/pkg/queue"
)

// QueueID is UID type, serves as unique ID for each queue
type QueueID types.UID

// QueueInfo will have all details about queue
type QueueInfo struct {
	UID  QueueID
	Name string
	// Scope is cluster or namespace.
	Scope string
	// Namespace is only set for NamespaceQueue-backed queue infos.
	Namespace string
	// SourceName is the original queue resource name before canonicalization.
	SourceName string

	Weight int32

	// Weights is a list of slash sperated float numbers.
	// Each of them is a weight corresponding the
	// hierarchy level.
	Weights string
	// Hierarchy is a list of node name along the
	// path from the root to the node itself.
	Hierarchy string

	Queue *scheduling.Queue
	// NamespaceQueue is set when this queue info originates from a NamespaceQueue resource.
	NamespaceQueue *v1beta1.NamespaceQueue
}

// NewQueueInfo creates new queueInfo object
func NewQueueInfo(queue *scheduling.Queue) *QueueInfo {
	effective := queue.DeepCopy()
	effective.Name = queueutil.ClusterKey(queue.Name)
	if effective.Spec.Parent != "" {
		effective.Spec.Parent = queueutil.ClusterKey(effective.Spec.Parent)
	}

	return &QueueInfo{
		UID:        QueueID(effective.Name),
		Name:       effective.Name,
		Scope:      queueutil.ClusterQueueScope,
		SourceName: queue.Name,

		Weight:    effective.Spec.Weight,
		Hierarchy: effective.Annotations[v1beta1.KubeHierarchyAnnotationKey],
		Weights:   effective.Annotations[v1beta1.KubeHierarchyWeightAnnotationKey],

		Queue: effective,
	}
}

// NewNamespaceQueueInfo creates a queueInfo from a namespaced queue by translating it into an effective scheduler Queue.
func NewNamespaceQueueInfo(namespaceQueue *v1beta1.NamespaceQueue) *QueueInfo {
	canonicalName := queueutil.NamespaceKey(namespaceQueue.Namespace, namespaceQueue.Name)
	effectiveSpec := scheduling.QueueSpec{}
	effectiveStatus := scheduling.QueueStatus{}
	_ = schedulingscheme.Scheme.Convert(&namespaceQueue.Spec, &effectiveSpec, nil)
	_ = schedulingscheme.Scheme.Convert(&namespaceQueue.Status, &effectiveStatus, nil)
	effective := &scheduling.Queue{
		TypeMeta: namespaceQueue.TypeMeta,
		ObjectMeta: metav1.ObjectMeta{
			Name:              canonicalName,
			Namespace:         namespaceQueue.Namespace,
			ResourceVersion:   namespaceQueue.ResourceVersion,
			Generation:        namespaceQueue.Generation,
			Labels:            namespaceQueue.Labels,
			Annotations:       namespaceQueue.Annotations,
			CreationTimestamp: namespaceQueue.CreationTimestamp,
		},
		Spec:   effectiveSpec,
		Status: effectiveStatus,
	}
	if effective.Spec.Parent != "" {
		effective.Spec.Parent = queueutil.NamespaceKey(namespaceQueue.Namespace, effective.Spec.Parent)
	}

	return &QueueInfo{
		UID:            QueueID(canonicalName),
		Name:           canonicalName,
		Scope:          queueutil.NamespaceQueueScope,
		Namespace:      namespaceQueue.Namespace,
		SourceName:     namespaceQueue.Name,
		Weight:         effective.Spec.Weight,
		Hierarchy:      effective.Annotations[v1beta1.KubeHierarchyAnnotationKey],
		Weights:        effective.Annotations[v1beta1.KubeHierarchyWeightAnnotationKey],
		Queue:          effective,
		NamespaceQueue: namespaceQueue,
	}
}

// Clone is used to clone queueInfo object
func (q *QueueInfo) Clone() *QueueInfo {
	return &QueueInfo{
		UID:        q.UID,
		Name:       q.Name,
		Scope:      q.Scope,
		Namespace:  q.Namespace,
		SourceName: q.SourceName,
		Weight:     q.Weight,
		Hierarchy:  q.Hierarchy,
		Weights:    q.Weights,
		Queue:      q.Queue.DeepCopy(),
		NamespaceQueue: func() *v1beta1.NamespaceQueue {
			if q.NamespaceQueue == nil {
				return nil
			}
			return q.NamespaceQueue.DeepCopy()
		}(),
	}
}

// Reclaimable return whether queue is reclaimable
func (q *QueueInfo) Reclaimable() bool {
	if q == nil {
		return false
	}

	if q.Queue == nil {
		return false
	}

	if q.Queue.Spec.Reclaimable == nil {
		return true
	}

	return *q.Queue.Spec.Reclaimable
}
