/*
Copyright 2019 The Volcano Authors.

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
	"context"
	"encoding/json"
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/bus/v1alpha1"
	busv1alpha1 "volcano.sh/apis/pkg/apis/bus/v1alpha1"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	v1beta1apply "volcano.sh/apis/pkg/client/applyconfiguration/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/controllers/apis"
	"volcano.sh/volcano/pkg/controllers/metrics"
	"volcano.sh/volcano/pkg/controllers/queue/state"
	queueutil "volcano.sh/volcano/pkg/queue"
)

const (
	ClosedByParentAnnotationKey        = "volcano.sh/closed-by-parent"
	ClosedByParentAnnotationTrueValue  = "true"
	ClosedByParentAnnotationFalseValue = "false"
)

type queueStatusAdapter interface {
	key() string
	currentStatus() *schedulingv1beta1.QueueStatus
	syncStatus(status *schedulingv1beta1.QueueStatus, podGroups []string)
	openStatus(status *schedulingv1beta1.QueueStatus)
	closeStatus(status *schedulingv1beta1.QueueStatus, podGroups []string)
	persistStatus(status *schedulingv1beta1.QueueStatus) error
}

type queueStatusAdapterFuncs struct {
	queueKey  string
	getStatus func() *schedulingv1beta1.QueueStatus
	syncFn    func(status *schedulingv1beta1.QueueStatus, podGroups []string)
	openFn    func(status *schedulingv1beta1.QueueStatus)
	closeFn   func(status *schedulingv1beta1.QueueStatus, podGroups []string)
	persistFn func(status *schedulingv1beta1.QueueStatus) error
}

func (a *queueStatusAdapterFuncs) key() string {
	return a.queueKey
}

func (a *queueStatusAdapterFuncs) currentStatus() *schedulingv1beta1.QueueStatus {
	return a.getStatus()
}

func (a *queueStatusAdapterFuncs) syncStatus(status *schedulingv1beta1.QueueStatus, podGroups []string) {
	a.syncFn(status, podGroups)
}

func (a *queueStatusAdapterFuncs) openStatus(status *schedulingv1beta1.QueueStatus) {
	a.openFn(status)
}

func (a *queueStatusAdapterFuncs) closeStatus(status *schedulingv1beta1.QueueStatus, podGroups []string) {
	a.closeFn(status, podGroups)
}

func (a *queueStatusAdapterFuncs) persistStatus(status *schedulingv1beta1.QueueStatus) error {
	return a.persistFn(status)
}

func resetQueuePodGroupCounters(status *schedulingv1beta1.QueueStatus) {
	status.Pending = 0
	status.Running = 0
	status.Unknown = 0
	status.Inqueue = 0
	status.Completed = 0
}

func (c *queuecontroller) buildQueueStatus(queueKey string, baseStatus *schedulingv1beta1.QueueStatus) (*schedulingv1beta1.QueueStatus, []string, error) {
	podGroups := c.getPodGroups(queueKey)
	queueStatus := baseStatus.DeepCopy()
	resetQueuePodGroupCounters(queueStatus)

	for _, pgKey := range podGroups {
		// Ignore error here, it cannot occur.
		ns, name, _ := cache.SplitMetaNamespaceKey(pgKey)

		pg, err := c.pgLister.PodGroups(ns).Get(name)
		if err != nil {
			if !errors.IsNotFound(err) {
				return nil, nil, err
			}

			klog.V(4).Infof("The podGroup %s is not found, skip it and continue to sync cache", pgKey)
			c.pgMutex.Lock()
			delete(c.podGroups[queueKey], pgKey)
			c.pgMutex.Unlock()
			continue
		}

		switch pg.Status.Phase {
		case schedulingv1beta1.PodGroupPending:
			queueStatus.Pending++
		case schedulingv1beta1.PodGroupRunning:
			queueStatus.Running++
		case schedulingv1beta1.PodGroupUnknown:
			queueStatus.Unknown++
		case schedulingv1beta1.PodGroupInqueue:
			queueStatus.Inqueue++
		case schedulingv1beta1.PodGroupCompleted:
			queueStatus.Completed++
		}
	}

	return queueStatus, podGroups, nil
}

func (c *queuecontroller) syncQueueStatus(adapter queueStatusAdapter) error {
	queueStatus, podGroups, err := c.buildQueueStatus(adapter.key(), adapter.currentStatus())
	if err != nil {
		return err
	}

	adapter.syncStatus(queueStatus, podGroups)
	metrics.UpdateQueueMetrics(adapter.key(), queueStatus)

	return adapter.persistStatus(queueStatus)
}

func (c *queuecontroller) openQueueStatus(adapter queueStatusAdapter) error {
	queueStatus := adapter.currentStatus().DeepCopy()
	adapter.openStatus(queueStatus)
	return adapter.persistStatus(queueStatus)
}

func (c *queuecontroller) closeQueueStatus(adapter queueStatusAdapter) error {
	queueStatus := adapter.currentStatus().DeepCopy()
	adapter.closeStatus(queueStatus, c.getPodGroups(adapter.key()))
	return adapter.persistStatus(queueStatus)
}

func (c *queuecontroller) syncQueue(queue *schedulingv1beta1.Queue, updateStateFn state.UpdateQueueStatusFn) error {
	klog.V(4).Infof("Begin to sync queue %s.", queue.Name)
	defer klog.V(4).Infof("End sync queue %s.", queue.Name)

	// add parent queue if parent not specified
	queue, err := c.updateQueueParent(queue)
	if err != nil {
		return err
	}

	var newQueue *schedulingv1beta1.Queue
	adapter := &queueStatusAdapterFuncs{
		queueKey: queueutil.ClusterKey(queue.Name),
		getStatus: func() *schedulingv1beta1.QueueStatus {
			return queue.Status.DeepCopy()
		},
		syncFn: func(status *schedulingv1beta1.QueueStatus, podGroups []string) {
			if updateStateFn != nil {
				updateStateFn(status, podGroups)
				return
			}
			status.State = queue.Status.State
		},
		openFn: func(status *schedulingv1beta1.QueueStatus) {
			if updateStateFn != nil {
				updateStateFn(status, nil)
			}
		},
		closeFn: func(status *schedulingv1beta1.QueueStatus, podGroups []string) {
			if updateStateFn != nil {
				updateStateFn(status, podGroups)
			}
		},
		persistFn: func(status *schedulingv1beta1.QueueStatus) error {
			newQueue = queue.DeepCopy()
			if status.State == queue.Status.State {
				return nil
			}

			queueStatusApply := v1beta1apply.QueueStatus().WithState(status.State)
			queueApply := v1beta1apply.Queue(queue.Name).WithStatus(queueStatusApply)
			var applyErr error
			if newQueue, applyErr = c.vcClient.SchedulingV1beta1().Queues().ApplyStatus(context.TODO(), queueApply, metav1.ApplyOptions{FieldManager: controllerName}); applyErr != nil {
				klog.Errorf("Update queue state from %s to %s failed for %v", queue.Status.State, status.State, applyErr)
				return applyErr
			}
			return nil
		},
	}

	if err := c.syncQueueStatus(adapter); err != nil {
		return err
	}
	if newQueue == nil {
		newQueue = queue.DeepCopy()
	}

	return c.syncHierarchicalQueue(newQueue)
}

func (c *queuecontroller) openQueue(queue *schedulingv1beta1.Queue, updateStateFn state.UpdateQueueStatusFn) error {
	klog.V(4).Infof("Begin to open queue %s.", queue.Name)

	if queue.Status.State != schedulingv1beta1.QueueStateOpen {
		err := c.openHierarchicalQueue(queue)
		if err != nil {
			return err
		}
	}

	newQueue := queue.DeepCopy()
	err := c.openQueueStatus(&queueStatusAdapterFuncs{
		queueKey: queueutil.ClusterKey(queue.Name),
		getStatus: func() *schedulingv1beta1.QueueStatus {
			return queue.Status.DeepCopy()
		},
		syncFn: func(status *schedulingv1beta1.QueueStatus, podGroups []string) {},
		openFn: func(status *schedulingv1beta1.QueueStatus) {
			if updateStateFn != nil {
				updateStateFn(status, nil)
			}
		},
		closeFn: func(status *schedulingv1beta1.QueueStatus, podGroups []string) {},
		persistFn: func(status *schedulingv1beta1.QueueStatus) error {
			newQueue.Status = *status
			if queue.Status.State == status.State {
				return nil
			}

			queueStatusApply := v1beta1apply.QueueStatus().WithState(status.State)
			queueApply := v1beta1apply.Queue(queue.Name).WithStatus(queueStatusApply)
			if _, err := c.vcClient.SchedulingV1beta1().Queues().ApplyStatus(context.TODO(), queueApply, metav1.ApplyOptions{FieldManager: controllerName}); err != nil {
				c.recorder.Event(newQueue, v1.EventTypeWarning, string(v1alpha1.OpenQueueAction),
					fmt.Sprintf("Update queue status from %s to %s failed for %v",
						queue.Status.State, newQueue.Status.State, err))
				return err
			}
			return nil
		},
	})
	if err != nil {
		return err
	}

	_, err = c.updateQueueAnnotation(queue, ClosedByParentAnnotationKey, ClosedByParentAnnotationFalseValue)
	return err
}

func (c *queuecontroller) closeQueue(queue *schedulingv1beta1.Queue, updateStateFn state.UpdateQueueStatusFn) error {
	klog.V(4).Infof("Begin to close queue %s.", queue.Name)

	if queue.Status.State != schedulingv1beta1.QueueStateClosed && queue.Status.State != schedulingv1beta1.QueueStateClosing {
		continued, err := c.closeHierarchicalQueue(queue)
		if !continued {
			return err
		}
	}

	newQueue := queue.DeepCopy()
	err := c.closeQueueStatus(&queueStatusAdapterFuncs{
		queueKey: queueutil.ClusterKey(queue.Name),
		getStatus: func() *schedulingv1beta1.QueueStatus {
			return queue.Status.DeepCopy()
		},
		syncFn: func(status *schedulingv1beta1.QueueStatus, podGroups []string) {},
		openFn: func(status *schedulingv1beta1.QueueStatus) {},
		closeFn: func(status *schedulingv1beta1.QueueStatus, podGroups []string) {
			if updateStateFn != nil {
				updateStateFn(status, podGroups)
			}
		},
		persistFn: func(status *schedulingv1beta1.QueueStatus) error {
			newQueue.Status = *status
			if queue.Status.State == status.State {
				return nil
			}

			queueStatusApply := v1beta1apply.QueueStatus().WithState(status.State)
			queueApply := v1beta1apply.Queue(queue.Name).WithStatus(queueStatusApply)
			if _, err := c.vcClient.SchedulingV1beta1().Queues().ApplyStatus(context.TODO(), queueApply, metav1.ApplyOptions{FieldManager: controllerName}); err != nil {
				c.recorder.Event(newQueue, v1.EventTypeWarning, string(v1alpha1.CloseQueueAction),
					fmt.Sprintf("Close queue failed for %v", err))
				return err
			}
			c.recorder.Event(newQueue, v1.EventTypeNormal, string(v1alpha1.CloseQueueAction), "Close queue succeed")
			return nil
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *queuecontroller) handleNamespaceQueue(req *apis.Request, queue *schedulingv1beta1.NamespaceQueue) error {
	klog.V(4).Infof("Begin execute %s action for namespace queue %s/%s, current status %s", req.Action, queue.Namespace, queue.Name, queue.Status.State)
	switch req.Action {
	case busv1alpha1.OpenQueueAction:
		return c.openNamespaceQueue(queue)
	case busv1alpha1.CloseQueueAction:
		return c.closeNamespaceQueue(queue)
	default:
		return c.syncNamespaceQueue(queue)
	}
}

func (c *queuecontroller) syncNamespaceQueue(queue *schedulingv1beta1.NamespaceQueue) error {
	if err := c.syncQueueStatus(&queueStatusAdapterFuncs{
		queueKey: queueutil.NamespaceKey(queue.Namespace, queue.Name),
		getStatus: func() *schedulingv1beta1.QueueStatus {
			return queue.Status.DeepCopy()
		},
		syncFn: func(status *schedulingv1beta1.QueueStatus, podGroups []string) {
			if queue.Status.State == schedulingv1beta1.QueueStateClosed && len(podGroups) > 0 {
				status.State = schedulingv1beta1.QueueStateClosing
			} else if queue.Status.State == schedulingv1beta1.QueueStateClosing && len(podGroups) == 0 {
				status.State = schedulingv1beta1.QueueStateClosed
			} else if queue.Status.State == "" {
				status.State = schedulingv1beta1.QueueStateOpen
			} else {
				status.State = queue.Status.State
			}
		},
		openFn: func(status *schedulingv1beta1.QueueStatus) {
			status.State = schedulingv1beta1.QueueStateOpen
		},
		closeFn: func(status *schedulingv1beta1.QueueStatus, podGroups []string) {
			if len(podGroups) == 0 {
				status.State = schedulingv1beta1.QueueStateClosed
				return
			}
			status.State = schedulingv1beta1.QueueStateClosing
		},
		persistFn: func(status *schedulingv1beta1.QueueStatus) error {
			if equality.Semantic.DeepEqual(queue.Status, *status) {
				return nil
			}

			newQueue := queue.DeepCopy()
			newQueue.Status = *status
			_, err := c.vcClient.SchedulingV1beta1().NamespaceQueues(queue.Namespace).UpdateStatus(context.TODO(), newQueue, metav1.UpdateOptions{})
			return err
		},
	}); err != nil {
		return err
	}

	return c.syncHierarchicalNamespaceQueue(queue)
}

func (c *queuecontroller) openNamespaceQueue(queue *schedulingv1beta1.NamespaceQueue) error {
	if queue.Status.State != schedulingv1beta1.QueueStateOpen {
		err := c.openHierarchicalNamespaceQueue(queue)
		if err != nil {
			return err
		}
	}

	err := c.openQueueStatus(&queueStatusAdapterFuncs{
		queueKey: queueutil.NamespaceKey(queue.Namespace, queue.Name),
		getStatus: func() *schedulingv1beta1.QueueStatus {
			return queue.Status.DeepCopy()
		},
		syncFn:  func(status *schedulingv1beta1.QueueStatus, podGroups []string) {},
		openFn:  func(status *schedulingv1beta1.QueueStatus) { status.State = schedulingv1beta1.QueueStateOpen },
		closeFn: func(status *schedulingv1beta1.QueueStatus, podGroups []string) {},
		persistFn: func(status *schedulingv1beta1.QueueStatus) error {
			if equality.Semantic.DeepEqual(queue.Status, *status) {
				return nil
			}

			newQueue := queue.DeepCopy()
			newQueue.Status = *status
			_, err := c.vcClient.SchedulingV1beta1().NamespaceQueues(queue.Namespace).UpdateStatus(context.TODO(), newQueue, metav1.UpdateOptions{})
			return err
		},
	})
	if err != nil {
		return err
	}

	_, err = c.updateNamespaceQueueAnnotation(queue, ClosedByParentAnnotationKey, ClosedByParentAnnotationFalseValue)
	return err
}

func (c *queuecontroller) closeNamespaceQueue(queue *schedulingv1beta1.NamespaceQueue) error {
	if queue.Status.State != schedulingv1beta1.QueueStateClosed && queue.Status.State != schedulingv1beta1.QueueStateClosing {
		continued, err := c.closeHierarchicalNamespaceQueue(queue)
		if !continued {
			return err
		}
	}

	return c.closeQueueStatus(&queueStatusAdapterFuncs{
		queueKey: queueutil.NamespaceKey(queue.Namespace, queue.Name),
		getStatus: func() *schedulingv1beta1.QueueStatus {
			return queue.Status.DeepCopy()
		},
		syncFn: func(status *schedulingv1beta1.QueueStatus, podGroups []string) {},
		openFn: func(status *schedulingv1beta1.QueueStatus) {},
		closeFn: func(status *schedulingv1beta1.QueueStatus, podGroups []string) {
			if len(podGroups) == 0 {
				status.State = schedulingv1beta1.QueueStateClosed
				return
			}
			status.State = schedulingv1beta1.QueueStateClosing
		},
		persistFn: func(status *schedulingv1beta1.QueueStatus) error {
			if equality.Semantic.DeepEqual(queue.Status, *status) {
				return nil
			}

			newQueue := queue.DeepCopy()
			newQueue.Status = *status
			_, err := c.vcClient.SchedulingV1beta1().NamespaceQueues(queue.Namespace).UpdateStatus(context.TODO(), newQueue, metav1.UpdateOptions{})
			return err
		},
	})
}

// sync the state between parent and child queues
func (c *queuecontroller) syncHierarchicalQueue(queue *schedulingv1beta1.Queue) error {
	if queue.Name == "root" {
		return nil
	}

	parentQueue, err := c.queueLister.Get(queue.Spec.Parent)
	if err != nil {
		klog.Errorf("Failed to get parent queue of Queue %s: %v.", queue.Name, err)
		return err
	}

	switch parentQueue.Status.State {
	case schedulingv1beta1.QueueStateClosed, schedulingv1beta1.QueueStateClosing:
		// consider the case where the open queue is updated and the parent queue is in the closed/closing state.
		if queue.Status.State != schedulingv1beta1.QueueStateClosed && queue.Status.State != schedulingv1beta1.QueueStateClosing {
			_, err = c.updateQueueAnnotation(queue, ClosedByParentAnnotationKey, ClosedByParentAnnotationTrueValue)
			if err != nil {
				klog.Errorf("Failed to patch annotation of Queue %s: %v.", queue.Name, err)
				return err
			}

			req := &apis.Request{
				QueueName: queue.Name,
				Action:    busv1alpha1.CloseQueueAction,
			}

			c.enqueue(req)
			klog.V(3).Infof("Closing queue %s because its parent queue %s is closing or closed.", queue.Name, parentQueue.Name)
		}
	case schedulingv1beta1.QueueStateOpen:
		if queue.Status.State == schedulingv1beta1.QueueStateClosed || queue.Status.State == schedulingv1beta1.QueueStateClosing {
			// consider the scenario where the parent queue of a queue, which has transitioned to a closed or closing state due to the closure of its parent queue, is updated, and the state of the updated parent queue is open.
			if queue.Annotations[ClosedByParentAnnotationKey] == ClosedByParentAnnotationTrueValue {
				req := &apis.Request{
					QueueName: queue.Name,
					Action:    busv1alpha1.OpenQueueAction,
				}

				c.enqueue(req)
				klog.V(3).Infof("Opening queue %s because its parent queue %s is opened.", queue.Name, parentQueue.Name)
			}
		}
	}

	return nil
}

func (c *queuecontroller) syncHierarchicalNamespaceQueue(queue *schedulingv1beta1.NamespaceQueue) error {
	if queue.Name == "root" || queue.Spec.Parent == "" || queue.Spec.Parent == "root" {
		return nil
	}

	parentQueue, err := c.namespaceQueueLister.NamespaceQueues(queue.Namespace).Get(queue.Spec.Parent)
	if err != nil {
		klog.Errorf("Failed to get parent queue of NamespaceQueue %s/%s: %v.", queue.Namespace, queue.Name, err)
		return err
	}

	switch parentQueue.Status.State {
	case schedulingv1beta1.QueueStateClosed, schedulingv1beta1.QueueStateClosing:
		if queue.Status.State != schedulingv1beta1.QueueStateClosed && queue.Status.State != schedulingv1beta1.QueueStateClosing {
			_, err = c.updateNamespaceQueueAnnotation(queue, ClosedByParentAnnotationKey, ClosedByParentAnnotationTrueValue)
			if err != nil {
				klog.Errorf("Failed to patch annotation of NamespaceQueue %s/%s: %v.", queue.Namespace, queue.Name, err)
				return err
			}

			req := &apis.Request{
				QueueName: queueutil.NamespaceKey(queue.Namespace, queue.Name),
				Action:    busv1alpha1.CloseQueueAction,
			}

			c.enqueue(req)
			klog.V(3).Infof("Closing namespace queue %s/%s because its parent queue %s is closing or closed.", queue.Namespace, queue.Name, parentQueue.Name)
		}
	case schedulingv1beta1.QueueStateOpen:
		if queue.Status.State == schedulingv1beta1.QueueStateClosed || queue.Status.State == schedulingv1beta1.QueueStateClosing {
			if queue.Annotations[ClosedByParentAnnotationKey] == ClosedByParentAnnotationTrueValue {
				req := &apis.Request{
					QueueName: queueutil.NamespaceKey(queue.Namespace, queue.Name),
					Action:    busv1alpha1.OpenQueueAction,
				}

				c.enqueue(req)
				klog.V(3).Infof("Opening namespace queue %s/%s because its parent queue %s is opened.", queue.Namespace, queue.Name, parentQueue.Name)
			}
		}
	}

	return nil
}

func (c *queuecontroller) openHierarchicalQueue(queue *schedulingv1beta1.Queue) error {
	if queue.Spec.Parent != "" && queue.Spec.Parent != "root" {
		parentQueue, err := c.queueLister.Get(queue.Spec.Parent)
		if err != nil {
			return fmt.Errorf("Failed to get parent queue %s of queue %s: %v", queue.Spec.Parent, queue.Name, err)
		}
		if parentQueue.Status.State == schedulingv1beta1.QueueStateClosing || parentQueue.Status.State == schedulingv1beta1.QueueStateClosed {
			// the parent queue may be being opened, and it may take a few attempts to open the child queue.
			return fmt.Errorf("Failed to open queue %s because its parent queue %s is closing or closed. Open the parent queue first.", queue.Name, queue.Spec.Parent)
		}
	}

	queueList, err := c.queueLister.List(labels.Everything())
	if err != nil {
		return err
	}

	for _, childQueue := range queueList {
		if childQueue.Spec.Parent == queue.Name && len(childQueue.Annotations) > 0 && childQueue.Annotations[ClosedByParentAnnotationKey] == ClosedByParentAnnotationTrueValue {
			req := &apis.Request{
				QueueName: childQueue.Name,
				Action:    busv1alpha1.OpenQueueAction,
			}

			c.enqueue(req)
			klog.Infof("Opening queue %s because its parent queue %s is opened", childQueue.Name, queue.Name)
		}
	}
	return nil
}

func (c *queuecontroller) openHierarchicalNamespaceQueue(queue *schedulingv1beta1.NamespaceQueue) error {
	if queue.Spec.Parent != "" && queue.Spec.Parent != "root" {
		parentQueue, err := c.namespaceQueueLister.NamespaceQueues(queue.Namespace).Get(queue.Spec.Parent)
		if err != nil {
			return fmt.Errorf("failed to get parent queue %s of namespace queue %s/%s: %v", queue.Spec.Parent, queue.Namespace, queue.Name, err)
		}
		if parentQueue.Status.State == schedulingv1beta1.QueueStateClosing || parentQueue.Status.State == schedulingv1beta1.QueueStateClosed {
			return fmt.Errorf("failed to open namespace queue %s/%s because its parent queue %s is closing or closed. Open the parent queue first", queue.Namespace, queue.Name, queue.Spec.Parent)
		}
	}

	queueList, err := c.namespaceQueueLister.NamespaceQueues(queue.Namespace).List(labels.Everything())
	if err != nil {
		return err
	}

	for _, childQueue := range queueList {
		if childQueue.Spec.Parent == queue.Name && len(childQueue.Annotations) > 0 && childQueue.Annotations[ClosedByParentAnnotationKey] == ClosedByParentAnnotationTrueValue {
			req := &apis.Request{
				QueueName: queueutil.NamespaceKey(childQueue.Namespace, childQueue.Name),
				Action:    busv1alpha1.OpenQueueAction,
			}

			c.enqueue(req)
			klog.Infof("Opening namespace queue %s/%s because its parent queue %s/%s is opened", childQueue.Namespace, childQueue.Name, queue.Namespace, queue.Name)
		}
	}
	return nil
}

func (c *queuecontroller) closeHierarchicalQueue(queue *schedulingv1beta1.Queue) (bool, error) {
	if queue.Name == "root" {
		klog.Errorf("Root queue cannot be closed")
		return false, nil
	}

	queueList, err := c.queueLister.List(labels.Everything())
	if err != nil {
		return false, err
	}

	for _, childQueue := range queueList {
		if childQueue.Spec.Parent != queue.Name {
			continue
		}
		if childQueue.Status.State != schedulingv1beta1.QueueStateClosed && childQueue.Status.State != schedulingv1beta1.QueueStateClosing {
			_, err = c.updateQueueAnnotation(childQueue, ClosedByParentAnnotationKey, ClosedByParentAnnotationTrueValue)
			if err != nil {
				return false, fmt.Errorf("Failed to update annotations of queue %s: %v", childQueue.Name, err)
			}
			req := &apis.Request{
				QueueName: childQueue.Name,
				Action:    busv1alpha1.CloseQueueAction,
			}

			c.enqueue(req)
			klog.V(3).Infof("Closing child queue %s because its parent queue %s is closing or closed.", childQueue.Name, queue.Name)
		}
	}

	return true, nil
}

func (c *queuecontroller) closeHierarchicalNamespaceQueue(queue *schedulingv1beta1.NamespaceQueue) (bool, error) {
	if queue.Name == "root" {
		klog.Errorf("Root namespace queue cannot be closed")
		return false, nil
	}

	queueList, err := c.namespaceQueueLister.NamespaceQueues(queue.Namespace).List(labels.Everything())
	if err != nil {
		return false, err
	}

	for _, childQueue := range queueList {
		if childQueue.Spec.Parent != queue.Name {
			continue
		}
		if childQueue.Status.State != schedulingv1beta1.QueueStateClosed && childQueue.Status.State != schedulingv1beta1.QueueStateClosing {
			_, err = c.updateNamespaceQueueAnnotation(childQueue, ClosedByParentAnnotationKey, ClosedByParentAnnotationTrueValue)
			if err != nil {
				return false, fmt.Errorf("failed to update annotations of namespace queue %s/%s: %v", childQueue.Namespace, childQueue.Name, err)
			}
			req := &apis.Request{
				QueueName: queueutil.NamespaceKey(childQueue.Namespace, childQueue.Name),
				Action:    busv1alpha1.CloseQueueAction,
			}

			c.enqueue(req)
			klog.V(3).Infof("Closing child namespace queue %s/%s because its parent queue %s/%s is closing or closed.", childQueue.Namespace, childQueue.Name, queue.Namespace, queue.Name)
		}
	}

	return true, nil
}

func (c *queuecontroller) updateQueueParent(queue *schedulingv1beta1.Queue) (*schedulingv1beta1.Queue, error) {
	if queue.Name == "root" || len(queue.Spec.Parent) > 0 {
		return queue, nil
	}

	var patch = []patchOperation{
		{
			Op:    "add",
			Path:  "/spec/parent",
			Value: "root",
		},
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return nil, err
	}

	return c.vcClient.SchedulingV1beta1().Queues().Patch(context.TODO(), queue.Name, types.JSONPatchType, patchBytes, metav1.PatchOptions{})
}

func (c *queuecontroller) updateQueueAnnotation(queue *schedulingv1beta1.Queue, key string, value string) (*schedulingv1beta1.Queue, error) {
	if len(queue.Annotations) > 0 && queue.Annotations[key] == value {
		return queue, nil
	}

	var patch []patchOperation
	if len(queue.Annotations) == 0 {
		patch = append(patch, patchOperation{
			Op:   "replace",
			Path: "/metadata/annotations",
			Value: map[string]string{
				key: value,
			},
		})
	} else {
		patch = append(patch, patchOperation{
			Op:    "replace",
			Path:  fmt.Sprintf("/metadata/annotations/%s", strings.ReplaceAll(key, "/", "~1")),
			Value: value,
		})
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return nil, err
	}

	return c.vcClient.SchedulingV1beta1().Queues().Patch(context.TODO(), queue.Name, types.JSONPatchType, patchBytes, metav1.PatchOptions{})
}

func (c *queuecontroller) updateNamespaceQueueAnnotation(queue *schedulingv1beta1.NamespaceQueue, key string, value string) (*schedulingv1beta1.NamespaceQueue, error) {
	if len(queue.Annotations) > 0 && queue.Annotations[key] == value {
		return queue, nil
	}

	var patch []patchOperation
	if len(queue.Annotations) == 0 {
		patch = append(patch, patchOperation{
			Op:   "replace",
			Path: "/metadata/annotations",
			Value: map[string]string{
				key: value,
			},
		})
	} else {
		patch = append(patch, patchOperation{
			Op:    "replace",
			Path:  fmt.Sprintf("/metadata/annotations/%s", strings.ReplaceAll(key, "/", "~1")),
			Value: value,
		})
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return nil, err
	}

	return c.vcClient.SchedulingV1beta1().NamespaceQueues(queue.Namespace).Patch(context.TODO(), queue.Name, types.JSONPatchType, patchBytes, metav1.PatchOptions{})
}
