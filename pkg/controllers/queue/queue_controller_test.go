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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	vcclient "volcano.sh/apis/pkg/client/clientset/versioned/fake"
	informerfactory "volcano.sh/apis/pkg/client/informers/externalversions"
	"volcano.sh/volcano/pkg/controllers/apis"
	"volcano.sh/volcano/pkg/controllers/framework"
	"volcano.sh/volcano/pkg/controllers/queue/state"
	queueutil "volcano.sh/volcano/pkg/queue"
)

func newFakeController() *queuecontroller {
	KubeBatchClientSet := vcclient.NewSimpleClientset()
	KubeClientSet := kubeclient.NewSimpleClientset()

	vcSharedInformers := informerfactory.NewSharedInformerFactory(KubeBatchClientSet, 0)

	controller := &queuecontroller{}
	opt := framework.ControllerOption{
		VolcanoClient:           KubeBatchClientSet,
		KubeClient:              KubeClientSet,
		VCSharedInformerFactory: vcSharedInformers,
	}

	controller.Initialize(&opt)

	return controller
}

func addNamespaceQueue(t *testing.T, c *queuecontroller, queue *schedulingv1beta1.NamespaceQueue) {
	t.Helper()

	_, err := c.vcClient.SchedulingV1beta1().NamespaceQueues(queue.Namespace).Create(context.TODO(), queue, metav1.CreateOptions{})
	assert.NoError(t, err)
	err = c.namespaceQueueInformer.Informer().GetStore().Add(queue.DeepCopy())
	assert.NoError(t, err)
}

func addClusterQueue(t *testing.T, c *queuecontroller, queue *schedulingv1beta1.Queue) {
	t.Helper()

	_, err := c.vcClient.SchedulingV1beta1().Queues().Create(context.TODO(), queue, metav1.CreateOptions{})
	assert.NoError(t, err)
	err = c.queueInformer.Informer().GetStore().Add(queue.DeepCopy())
	assert.NoError(t, err)
}

func TestAddQueue(t *testing.T) {
	testCases := []struct {
		Name        string
		queue       *schedulingv1beta1.Queue
		ExpectValue int
	}{
		{
			Name: "AddQueue",
			queue: &schedulingv1beta1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name: "c1",
				},
				Spec: schedulingv1beta1.QueueSpec{
					Weight: 1,
				},
			},
			ExpectValue: 1,
		},
	}

	for i, testcase := range testCases {
		c := newFakeController()

		c.addQueue(testcase.queue)

		if testcase.ExpectValue != c.queue.Len() {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, c.queue.Len())
		}
	}
}

func TestDeleteQueue(t *testing.T) {
	testCases := []struct {
		Name        string
		queue       *schedulingv1beta1.Queue
		ExpectValue bool
	}{
		{
			Name: "DeleteQueue",
			queue: &schedulingv1beta1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name: "c1",
				},
				Spec: schedulingv1beta1.QueueSpec{
					Weight: 1,
				},
			},
			ExpectValue: false,
		},
	}

	for i, testcase := range testCases {
		c := newFakeController()
		queueKey := queueutil.ClusterKey(testcase.queue.Name)
		c.podGroups[queueKey] = make(map[string]struct{})

		c.deleteQueue(testcase.queue)

		if _, ok := c.podGroups[queueKey]; ok != testcase.ExpectValue {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, ok)
		}
	}

}

func TestAddPodGroup(t *testing.T) {
	namespace := "c1"

	testCases := []struct {
		Name        string
		podGroup    *schedulingv1beta1.PodGroup
		ExpectValue int
	}{
		{
			Name: "addpodgroup",
			podGroup: &schedulingv1beta1.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pg1",
					Namespace: namespace,
				},
				Spec: schedulingv1beta1.PodGroupSpec{
					Queue: "c1",
				},
			},
			ExpectValue: 1,
		},
	}

	for i, testcase := range testCases {
		c := newFakeController()
		queueKey := queueutil.ClusterKey(testcase.podGroup.Spec.Queue)

		c.addPodGroup(testcase.podGroup)

		if testcase.ExpectValue != c.queue.Len() {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, c.queue.Len())
		}
		if testcase.ExpectValue != len(c.podGroups[queueKey]) {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, len(c.podGroups[queueKey]))
		}
	}

}

func TestDeletePodGroup(t *testing.T) {
	namespace := "c1"

	testCases := []struct {
		Name        string
		podGroup    *schedulingv1beta1.PodGroup
		ExpectValue bool
	}{
		{
			Name: "deletepodgroup",
			podGroup: &schedulingv1beta1.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pg1",
					Namespace: namespace,
				},
				Spec: schedulingv1beta1.PodGroupSpec{
					Queue: "c1",
				},
			},
			ExpectValue: false,
		},
	}

	for i, testcase := range testCases {
		c := newFakeController()
		queueKey := queueutil.ClusterKey(testcase.podGroup.Spec.Queue)

		key, _ := cache.MetaNamespaceKeyFunc(testcase.podGroup)
		c.podGroups[queueKey] = make(map[string]struct{})
		c.podGroups[queueKey][key] = struct{}{}

		c.deletePodGroup(testcase.podGroup)
		if _, ok := c.podGroups[queueKey][key]; ok != testcase.ExpectValue {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, ok)
		}
	}
}

func TestUpdatePodGroup(t *testing.T) {
	namespace := "c1"

	testCases := []struct {
		Name        string
		podGroupold *schedulingv1beta1.PodGroup
		podGroupnew *schedulingv1beta1.PodGroup
		ExpectValue int
	}{
		{
			Name: "updatepodgroup",
			podGroupold: &schedulingv1beta1.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pg1",
					Namespace: namespace,
				},
				Spec: schedulingv1beta1.PodGroupSpec{
					Queue: "c1",
				},
				Status: schedulingv1beta1.PodGroupStatus{
					Phase: schedulingv1beta1.PodGroupPending,
				},
			},
			podGroupnew: &schedulingv1beta1.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pg1",
					Namespace: namespace,
				},
				Spec: schedulingv1beta1.PodGroupSpec{
					Queue: "c1",
				},
				Status: schedulingv1beta1.PodGroupStatus{
					Phase: schedulingv1beta1.PodGroupRunning,
				},
			},
			ExpectValue: 1,
		},
	}

	for i, testcase := range testCases {
		c := newFakeController()

		c.updatePodGroup(testcase.podGroupold, testcase.podGroupnew)

		if testcase.ExpectValue != c.queue.Len() {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, c.queue.Len())
		}
	}
}

func TestSyncQueue(t *testing.T) {
	testCases := []struct {
		Name                  string
		queue                 *schedulingv1beta1.Queue
		updateStatusFnFactory func(queue *schedulingv1beta1.Queue) state.UpdateQueueStatusFn
		ExpectState           schedulingv1beta1.QueueState
	}{
		{
			Name: "From empty state to open",
			queue: &schedulingv1beta1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name: "root",
				},
				Status: schedulingv1beta1.QueueStatus{
					State: "",
				},
			},
			ExpectState: schedulingv1beta1.QueueStateOpen,
			updateStatusFnFactory: func(queue *schedulingv1beta1.Queue) state.UpdateQueueStatusFn {
				return func(status *schedulingv1beta1.QueueStatus, podGroupList []string) {
					if len(queue.Status.State) == 0 {
						status.State = schedulingv1beta1.QueueStateOpen
					}
				}
			},
		},
		{
			Name: "From open to close",
			queue: &schedulingv1beta1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name: "root",
				},
				Status: schedulingv1beta1.QueueStatus{
					State: schedulingv1beta1.QueueStateOpen,
				},
			},
			ExpectState: schedulingv1beta1.QueueStateClosed,
			updateStatusFnFactory: func(queue *schedulingv1beta1.Queue) state.UpdateQueueStatusFn {
				return func(status *schedulingv1beta1.QueueStatus, podGroupList []string) {
					status.State = schedulingv1beta1.QueueStateClosed
				}
			},
		},
	}

	for _, testcase := range testCases {
		c := newFakeController()

		addClusterQueue(t, c, testcase.queue)

		updateStatusFn := testcase.updateStatusFnFactory(testcase.queue)
		err := c.syncQueue(testcase.queue, updateStatusFn)
		assert.NoError(t, err)

		item, err := c.vcClient.SchedulingV1beta1().Queues().Get(context.TODO(), testcase.queue.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, testcase.ExpectState, item.Status.State)
	}
}

func TestNamespaceQueueStateTransitions(t *testing.T) {
	testCases := []struct {
		name          string
		queue         *schedulingv1beta1.NamespaceQueue
		podGroups     []string
		action        func(*queuecontroller, *schedulingv1beta1.NamespaceQueue) error
		expectedState schedulingv1beta1.QueueState
	}{
		{
			name: "sync empty state to open",
			queue: &schedulingv1beta1.NamespaceQueue{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "ns1",
					Name:      "q1",
				},
			},
			action: func(c *queuecontroller, queue *schedulingv1beta1.NamespaceQueue) error {
				return c.syncNamespaceQueue(queue)
			},
			expectedState: schedulingv1beta1.QueueStateOpen,
		},
		{
			name: "close namespace queue with running podgroups",
			queue: &schedulingv1beta1.NamespaceQueue{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "ns1",
					Name:      "q2",
				},
				Status: schedulingv1beta1.QueueStatus{
					State: schedulingv1beta1.QueueStateOpen,
				},
			},
			podGroups: []string{"ns1/pg1"},
			action: func(c *queuecontroller, queue *schedulingv1beta1.NamespaceQueue) error {
				return c.closeNamespaceQueue(queue)
			},
			expectedState: schedulingv1beta1.QueueStateClosing,
		},
		{
			name: "close namespace queue without podgroups",
			queue: &schedulingv1beta1.NamespaceQueue{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "ns1",
					Name:      "q3",
				},
				Status: schedulingv1beta1.QueueStatus{
					State: schedulingv1beta1.QueueStateOpen,
				},
			},
			action: func(c *queuecontroller, queue *schedulingv1beta1.NamespaceQueue) error {
				return c.closeNamespaceQueue(queue)
			},
			expectedState: schedulingv1beta1.QueueStateClosed,
		},
	}

	for _, testcase := range testCases {
		c := newFakeController()

		addNamespaceQueue(t, c, testcase.queue)

		queueKey := queueutil.NamespaceKey(testcase.queue.Namespace, testcase.queue.Name)
		c.podGroups[queueKey] = make(map[string]struct{}, len(testcase.podGroups))
		for _, pgKey := range testcase.podGroups {
			c.podGroups[queueKey][pgKey] = struct{}{}
		}

		err := testcase.action(c, testcase.queue)
		assert.NoError(t, err)

		item, err := c.vcClient.SchedulingV1beta1().NamespaceQueues(testcase.queue.Namespace).Get(context.TODO(), testcase.queue.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, testcase.expectedState, item.Status.State)
	}
}

func TestNamespaceQueueHierarchy(t *testing.T) {
	t.Run("open child blocked by closed parent", func(t *testing.T) {
		c := newFakeController()
		parent := &schedulingv1beta1.NamespaceQueue{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns1",
				Name:      "parent",
			},
			Status: schedulingv1beta1.QueueStatus{
				State: schedulingv1beta1.QueueStateClosed,
			},
		}
		child := &schedulingv1beta1.NamespaceQueue{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns1",
				Name:      "child",
			},
			Spec: schedulingv1beta1.QueueSpec{
				Parent: "parent",
			},
			Status: schedulingv1beta1.QueueStatus{
				State: schedulingv1beta1.QueueStateClosed,
			},
		}

		addNamespaceQueue(t, c, parent)
		addNamespaceQueue(t, c, child)

		err := c.openNamespaceQueue(child)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Open the parent queue first")
	})

	t.Run("close parent cascades child close request", func(t *testing.T) {
		c := newFakeController()
		parent := &schedulingv1beta1.NamespaceQueue{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns1",
				Name:      "parent",
			},
			Status: schedulingv1beta1.QueueStatus{
				State: schedulingv1beta1.QueueStateOpen,
			},
		}
		child := &schedulingv1beta1.NamespaceQueue{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns1",
				Name:      "child",
			},
			Spec: schedulingv1beta1.QueueSpec{
				Parent: "parent",
			},
			Status: schedulingv1beta1.QueueStatus{
				State: schedulingv1beta1.QueueStateOpen,
			},
		}

		addNamespaceQueue(t, c, parent)
		addNamespaceQueue(t, c, child)

		err := c.closeNamespaceQueue(parent)
		assert.NoError(t, err)
		assert.Equal(t, 1, c.queue.Len())

		req, shutdown := c.queue.Get()
		assert.False(t, shutdown)
		assert.Equal(t, queueutil.NamespaceKey("ns1", "child"), req.QueueName)
		assert.Equal(t, "CloseQueue", string(req.Action))
		c.queue.Done(req)

		item, err := c.vcClient.SchedulingV1beta1().NamespaceQueues("ns1").Get(context.TODO(), "child", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, ClosedByParentAnnotationTrueValue, item.Annotations[ClosedByParentAnnotationKey])
	})

	t.Run("sync child enqueues close when parent is closed", func(t *testing.T) {
		c := newFakeController()
		parent := &schedulingv1beta1.NamespaceQueue{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns1",
				Name:      "parent",
			},
			Status: schedulingv1beta1.QueueStatus{
				State: schedulingv1beta1.QueueStateClosed,
			},
		}
		child := &schedulingv1beta1.NamespaceQueue{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns1",
				Name:      "child",
			},
			Spec: schedulingv1beta1.QueueSpec{
				Parent: "parent",
			},
			Status: schedulingv1beta1.QueueStatus{
				State: schedulingv1beta1.QueueStateOpen,
			},
		}

		addNamespaceQueue(t, c, parent)
		addNamespaceQueue(t, c, child)

		err := c.syncNamespaceQueue(child)
		assert.NoError(t, err)
		assert.Equal(t, 1, c.queue.Len())

		req, shutdown := c.queue.Get()
		assert.False(t, shutdown)
		assert.Equal(t, queueutil.NamespaceKey("ns1", "child"), req.QueueName)
		assert.Equal(t, "CloseQueue", string(req.Action))
		c.queue.Done(req)
	})

	t.Run("sync child enqueues reopen when parent reopens", func(t *testing.T) {
		c := newFakeController()
		parent := &schedulingv1beta1.NamespaceQueue{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns1",
				Name:      "parent",
			},
			Status: schedulingv1beta1.QueueStatus{
				State: schedulingv1beta1.QueueStateOpen,
			},
		}
		child := &schedulingv1beta1.NamespaceQueue{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   "ns1",
				Name:        "child",
				Annotations: map[string]string{ClosedByParentAnnotationKey: ClosedByParentAnnotationTrueValue},
			},
			Spec: schedulingv1beta1.QueueSpec{
				Parent: "parent",
			},
			Status: schedulingv1beta1.QueueStatus{
				State: schedulingv1beta1.QueueStateClosed,
			},
		}

		addNamespaceQueue(t, c, parent)
		addNamespaceQueue(t, c, child)

		err := c.syncNamespaceQueue(child)
		assert.NoError(t, err)
		assert.Equal(t, 1, c.queue.Len())

		req, shutdown := c.queue.Get()
		assert.False(t, shutdown)
		assert.Equal(t, queueutil.NamespaceKey("ns1", "child"), req.QueueName)
		assert.Equal(t, "OpenQueue", string(req.Action))
		c.queue.Done(req)
	})

	t.Run("open child clears closed-by-parent annotation", func(t *testing.T) {
		c := newFakeController()
		parent := &schedulingv1beta1.NamespaceQueue{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns1",
				Name:      "parent",
			},
			Status: schedulingv1beta1.QueueStatus{
				State: schedulingv1beta1.QueueStateOpen,
			},
		}
		child := &schedulingv1beta1.NamespaceQueue{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   "ns1",
				Name:        "child",
				Annotations: map[string]string{ClosedByParentAnnotationKey: ClosedByParentAnnotationTrueValue},
			},
			Spec: schedulingv1beta1.QueueSpec{
				Parent: "parent",
			},
			Status: schedulingv1beta1.QueueStatus{
				State: schedulingv1beta1.QueueStateClosed,
			},
		}

		addNamespaceQueue(t, c, parent)
		addNamespaceQueue(t, c, child)

		err := c.openNamespaceQueue(child)
		assert.NoError(t, err)

		item, err := c.vcClient.SchedulingV1beta1().NamespaceQueues("ns1").Get(context.TODO(), "child", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, schedulingv1beta1.QueueStateOpen, item.Status.State)
		assert.Equal(t, ClosedByParentAnnotationFalseValue, item.Annotations[ClosedByParentAnnotationKey])
	})
}

func TestProcessNextWorkItem(t *testing.T) {
	testCases := []struct {
		Name        string
		ExpectValue int32
	}{
		{
			Name:        "processNextWorkItem",
			ExpectValue: 0,
		},
	}

	for i, testcase := range testCases {
		c := newFakeController()
		c.queue.Add(&apis.Request{JobName: "test"})
		bVal := c.processNextWorkItem()
		fmt.Println("The value of boolean is ", bVal)
		if c.queue.Len() != 0 {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, c.queue.Len())
		}
	}
}
