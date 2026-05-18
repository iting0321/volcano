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

package util

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	busv1alpha1 "volcano.sh/apis/pkg/apis/bus/v1alpha1"
	"volcano.sh/apis/pkg/apis/helpers"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	queueutil "volcano.sh/volcano/pkg/queue"
)

func CreateNamespaceQueue(ctx *TestContext, queue *schedulingv1beta1.NamespaceQueue) *schedulingv1beta1.NamespaceQueue {
	created, err := ctx.Vcclient.SchedulingV1beta1().NamespaceQueues(queue.Namespace).Create(context.TODO(), queue, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred(), "failed to create namespace queue %s/%s", queue.Namespace, queue.Name)
	time.Sleep(3 * time.Second)
	return created
}

func GetNamespaceQueue(ctx *TestContext, namespace, name string) *schedulingv1beta1.NamespaceQueue {
	queue, err := ctx.Vcclient.SchedulingV1beta1().NamespaceQueues(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	Expect(err).NotTo(HaveOccurred(), "failed to get namespace queue %s/%s", namespace, name)
	return queue
}

func UpdateNamespaceQueueStatus(ctx *TestContext, namespace, name string, state schedulingv1beta1.QueueState) {
	queue := GetNamespaceQueue(ctx, namespace, name)
	queue.Status.State = state
	_, err := ctx.Vcclient.SchedulingV1beta1().NamespaceQueues(namespace).UpdateStatus(context.TODO(), queue, metav1.UpdateOptions{})
	Expect(err).NotTo(HaveOccurred(), "failed to update namespace queue status %s/%s", namespace, name)
}

func WaitNamespaceQueueState(ctx *TestContext, namespace, name string, state schedulingv1beta1.QueueState) error {
	return wait.Poll(100*time.Millisecond, TenMinute, func() (bool, error) {
		queue, err := ctx.Vcclient.SchedulingV1beta1().NamespaceQueues(namespace).Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return queue.Status.State == state, nil
	})
}

func CreateNamespaceQueueCommand(ctx *TestContext, commandNamespace, queueNamespace, queueName string, action busv1alpha1.Action) {
	queue := GetNamespaceQueue(ctx, queueNamespace, queueName)
	targetRef := &metav1.OwnerReference{
		APIVersion: schedulingv1beta1.SchemeGroupVersion.String(),
		Kind:       helpers.V1beta1QueueKind.Kind,
		Name:       queueutil.NamespaceKey(queue.Namespace, queue.Name),
		UID:        queue.UID,
	}

	cmd := &busv1alpha1.Command{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("%s-%s-", queue.Name, action),
			Namespace:    commandNamespace,
		},
		TargetObject: targetRef,
		Action:       string(action),
	}

	_, err := ctx.Vcclient.BusV1alpha1().Commands(commandNamespace).Create(context.TODO(), cmd, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred(), "failed to create command for namespace queue %s/%s", queueNamespace, queueName)
}

func DeleteNamespaceQueue(ctx *TestContext, namespace, name string) {
	err := ctx.Vcclient.SchedulingV1beta1().NamespaceQueues(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
	Expect(err).NotTo(HaveOccurred(), "failed to delete namespace queue %s/%s", namespace, name)
}

func NewNamespaceQueue(namespace, name string, parent string, deserved v1.ResourceList) *schedulingv1beta1.NamespaceQueue {
	return &schedulingv1beta1.NamespaceQueue{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Spec: schedulingv1beta1.QueueSpec{
			Weight:   1,
			Parent:   parent,
			Deserved: deserved,
		},
	}
}
