/*
Copyright 2026 The Volcano Authors.

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

package queue

import (
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	schedulinglister "volcano.sh/apis/pkg/client/listers/scheduling/v1beta1"
)

type ResolvedReference struct {
	Key            string
	Scope          string
	Namespace      string
	Name           string
	Queue          *schedulingv1beta1.Queue
	NamespaceQueue *schedulingv1beta1.NamespaceQueue
}

// AsQueue returns a queue-shaped view of the resolved reference so callers can
// reuse the existing queue-based logic for namespace queues.
func (r *ResolvedReference) AsQueue() *schedulingv1beta1.Queue {
	if r == nil {
		return nil
	}
	if r.Queue != nil {
		return r.Queue
	}
	if r.NamespaceQueue == nil {
		return nil
	}

	return &schedulingv1beta1.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name:        NamespaceKey(r.Namespace, r.Name),
			Namespace:   r.Namespace,
			Labels:      r.NamespaceQueue.Labels,
			Annotations: r.NamespaceQueue.Annotations,
		},
		Spec:   r.NamespaceQueue.Spec,
		Status: r.NamespaceQueue.Status,
	}
}

// Resolve finds a NamespaceQueue in the workload namespace first and falls back to a cluster Queue.
func Resolve(namespace, name string, queueLister schedulinglister.QueueLister, namespaceQueueLister schedulinglister.NamespaceQueueLister) (*ResolvedReference, error) {
	if namespaceQueueLister != nil && namespace != "" {
		namespaceQueue, err := namespaceQueueLister.NamespaceQueues(namespace).Get(name)
		if err == nil {
			return &ResolvedReference{
				Key:            NamespaceKey(namespace, name),
				Scope:          NamespaceQueueScope,
				Namespace:      namespace,
				Name:           name,
				NamespaceQueue: namespaceQueue,
			}, nil
		}
		if err != nil && !apierrors.IsNotFound(err) {
			return nil, err
		}
	}

	if queueLister == nil {
		return nil, fmt.Errorf("queue %s not found", name)
	}

	queue, err := queueLister.Get(name)
	if err != nil {
		return nil, err
	}

	return &ResolvedReference{
		Key:       ClusterKey(name),
		Scope:     ClusterQueueScope,
		Name:      name,
		Queue:     queue,
		Namespace: "",
	}, nil
}
