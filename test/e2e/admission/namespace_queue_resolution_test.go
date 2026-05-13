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

package admission

import (
	"context"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/test/e2e/util"
)

var _ = ginkgo.Describe("NamespaceQueue Resolution E2E Test", func() {
	ginkgo.It("Should prefer namespace queue over cluster queue for job admission", func() {
		testCtx := util.InitTestContext(util.Options{})
		defer util.CleanupTestContext(testCtx)

		clusterQueue := &schedulingv1beta1.Queue{
			ObjectMeta: metav1.ObjectMeta{Name: "research"},
			Spec:       schedulingv1beta1.QueueSpec{Weight: 1},
			Status:     schedulingv1beta1.QueueStatus{State: schedulingv1beta1.QueueStateClosed},
		}
		_, err := testCtx.Vcclient.SchedulingV1beta1().Queues().Create(context.TODO(), clusterQueue, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		util.CreateNamespaceQueue(testCtx, util.NewNamespaceQueue(testCtx.Namespace, "research", "", nil))
		err = util.WaitNamespaceQueueState(testCtx, testCtx.Namespace, "research", schedulingv1beta1.QueueStateOpen)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		job := createBaseJob("namespace-queue-job", testCtx.Namespace)
		job.Spec.Queue = "research"
		_, err = testCtx.Vcclient.BatchV1alpha1().Jobs(testCtx.Namespace).Create(context.TODO(), job, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("Should reject job when namespace queue is closed even if cluster queue with same name is open", func() {
		testCtx := util.InitTestContext(util.Options{})
		defer util.CleanupTestContext(testCtx)

		clusterQueue := &schedulingv1beta1.Queue{
			ObjectMeta: metav1.ObjectMeta{Name: "research"},
			Spec:       schedulingv1beta1.QueueSpec{Weight: 1},
			Status:     schedulingv1beta1.QueueStatus{State: schedulingv1beta1.QueueStateOpen},
		}
		_, err := testCtx.Vcclient.SchedulingV1beta1().Queues().Create(context.TODO(), clusterQueue, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		util.CreateNamespaceQueue(testCtx, util.NewNamespaceQueue(testCtx.Namespace, "research", "", nil))
		util.UpdateNamespaceQueueStatus(testCtx, testCtx.Namespace, "research", schedulingv1beta1.QueueStateClosed)

		job := createBaseJob("closed-namespace-queue-job", testCtx.Namespace)
		job.Spec.Queue = "research"
		_, err = testCtx.Vcclient.BatchV1alpha1().Jobs(testCtx.Namespace).Create(context.TODO(), job, metav1.CreateOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("can only submit job to queue with state `Open`"))
		gomega.Expect(err.Error()).To(gomega.ContainSubstring(testCtx.Namespace + "/research"))
	})

	ginkgo.It("Should reject job submission to non-leaf namespace queue", func() {
		testCtx := util.InitTestContext(util.Options{})
		defer util.CleanupTestContext(testCtx)

		util.CreateNamespaceQueue(testCtx, util.NewNamespaceQueue(testCtx.Namespace, "parent", "", nil))
		util.CreateNamespaceQueue(testCtx, util.NewNamespaceQueue(testCtx.Namespace, "child", "parent", nil))

		job := createBaseJob("non-leaf-namespace-queue-job", testCtx.Namespace)
		job.Spec.Queue = "parent"
		_, err := testCtx.Vcclient.BatchV1alpha1().Jobs(testCtx.Namespace).Create(context.TODO(), job, metav1.CreateOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("can only submit job to leaf queue"))
		gomega.Expect(err.Error()).To(gomega.ContainSubstring(testCtx.Namespace + "/parent"))
	})

	ginkgo.It("Should prefer namespace queue over cluster queue for podgroup admission", func() {
		testCtx := util.InitTestContext(util.Options{})
		defer util.CleanupTestContext(testCtx)

		clusterQueue := &schedulingv1beta1.Queue{
			ObjectMeta: metav1.ObjectMeta{Name: "shared"},
			Spec:       schedulingv1beta1.QueueSpec{Weight: 1},
			Status:     schedulingv1beta1.QueueStatus{State: schedulingv1beta1.QueueStateClosed},
		}
		_, err := testCtx.Vcclient.SchedulingV1beta1().Queues().Create(context.TODO(), clusterQueue, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		util.CreateNamespaceQueue(testCtx, util.NewNamespaceQueue(testCtx.Namespace, "shared", "", nil))

		pg := &schedulingv1beta1.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pg-namespace-preferred",
				Namespace: testCtx.Namespace,
			},
			Spec: schedulingv1beta1.PodGroupSpec{
				Queue:     "shared",
				MinMember: 1,
			},
		}

		_, err = testCtx.Vcclient.SchedulingV1beta1().PodGroups(testCtx.Namespace).Create(context.TODO(), pg, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("Should reject podgroup when namespace queue is closed even if cluster queue with same name is open", func() {
		testCtx := util.InitTestContext(util.Options{})
		defer util.CleanupTestContext(testCtx)

		clusterQueue := &schedulingv1beta1.Queue{
			ObjectMeta: metav1.ObjectMeta{Name: "shared"},
			Spec:       schedulingv1beta1.QueueSpec{Weight: 1},
			Status:     schedulingv1beta1.QueueStatus{State: schedulingv1beta1.QueueStateOpen},
		}
		_, err := testCtx.Vcclient.SchedulingV1beta1().Queues().Create(context.TODO(), clusterQueue, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		util.CreateNamespaceQueue(testCtx, util.NewNamespaceQueue(testCtx.Namespace, "shared", "", nil))
		util.UpdateNamespaceQueueStatus(testCtx, testCtx.Namespace, "shared", schedulingv1beta1.QueueStateClosed)

		pg := &schedulingv1beta1.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pg-namespace-closed",
				Namespace: testCtx.Namespace,
			},
			Spec: schedulingv1beta1.PodGroupSpec{
				Queue:     "shared",
				MinMember: 1,
			},
		}

		_, err = testCtx.Vcclient.SchedulingV1beta1().PodGroups(testCtx.Namespace).Create(context.TODO(), pg, metav1.CreateOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("can only submit PodGroup to queue with state `Open`"))
		gomega.Expect(err.Error()).To(gomega.ContainSubstring(testCtx.Namespace + "/shared"))
	})
})
