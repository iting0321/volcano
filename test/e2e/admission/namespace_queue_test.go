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

var _ = ginkgo.Describe("NamespaceQueue Admission E2E Test", func() {
	ginkgo.It("Should apply default reclaimable and weight for namespace queue", func() {
		testCtx := util.InitTestContext(util.Options{})
		defer util.CleanupTestContext(testCtx)

		queue := &schedulingv1beta1.NamespaceQueue{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: testCtx.Namespace,
				Name:      "defaults-nq",
			},
		}

		created, err := testCtx.Vcclient.SchedulingV1beta1().NamespaceQueues(testCtx.Namespace).Create(context.TODO(), queue, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(created.Spec.Weight).To(gomega.Equal(int32(1)))
		gomega.Expect(created.Spec.Reclaimable).NotTo(gomega.BeNil())
		gomega.Expect(*created.Spec.Reclaimable).To(gomega.BeTrue())
	})

	ginkgo.It("Should allow valid namespace queue creation", func() {
		testCtx := util.InitTestContext(util.Options{})
		defer util.CleanupTestContext(testCtx)

		queue := util.NewNamespaceQueue(testCtx.Namespace, "valid-nq", "", nil)
		_, err := testCtx.Vcclient.SchedulingV1beta1().NamespaceQueues(testCtx.Namespace).Create(context.TODO(), queue, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("Should reject reserved namespace queue name root", func() {
		testCtx := util.InitTestContext(util.Options{})
		defer util.CleanupTestContext(testCtx)

		queue := util.NewNamespaceQueue(testCtx.Namespace, "root", "", nil)
		_, err := testCtx.Vcclient.SchedulingV1beta1().NamespaceQueues(testCtx.Namespace).Create(context.TODO(), queue, metav1.CreateOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("namespace queue name `root` is reserved"))
	})

	ginkgo.It("Should reject namespace queue creation with missing parent", func() {
		testCtx := util.InitTestContext(util.Options{})
		defer util.CleanupTestContext(testCtx)

		queue := util.NewNamespaceQueue(testCtx.Namespace, "child-nq", "missing-parent", nil)
		_, err := testCtx.Vcclient.SchedulingV1beta1().NamespaceQueues(testCtx.Namespace).Create(context.TODO(), queue, metav1.CreateOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("failed to get parent namespace queue"))
	})

	ginkgo.It("Should reject deleting open namespace queue", func() {
		testCtx := util.InitTestContext(util.Options{})
		defer util.CleanupTestContext(testCtx)

		util.CreateNamespaceQueue(testCtx, util.NewNamespaceQueue(testCtx.Namespace, "open-nq", "", nil))
		err := testCtx.Vcclient.SchedulingV1beta1().NamespaceQueues(testCtx.Namespace).Delete(context.TODO(), "open-nq", metav1.DeleteOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("only queue with state `Closed` can be deleted"))
	})

	ginkgo.It("Should reject deleting closed namespace queue with child queues", func() {
		testCtx := util.InitTestContext(util.Options{})
		defer util.CleanupTestContext(testCtx)

		util.CreateNamespaceQueue(testCtx, util.NewNamespaceQueue(testCtx.Namespace, "parent-nq", "", nil))
		util.CreateNamespaceQueue(testCtx, util.NewNamespaceQueue(testCtx.Namespace, "child-nq", "parent-nq", nil))
		util.UpdateNamespaceQueueStatus(testCtx, testCtx.Namespace, "parent-nq", schedulingv1beta1.QueueStateClosed)

		err := testCtx.Vcclient.SchedulingV1beta1().NamespaceQueues(testCtx.Namespace).Delete(context.TODO(), "parent-nq", metav1.DeleteOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("still has 1 child queues"))
	})

	ginkgo.It("Should allow deleting closed leaf namespace queue", func() {
		testCtx := util.InitTestContext(util.Options{})
		defer util.CleanupTestContext(testCtx)

		util.CreateNamespaceQueue(testCtx, util.NewNamespaceQueue(testCtx.Namespace, "leaf-nq", "", nil))
		util.UpdateNamespaceQueueStatus(testCtx, testCtx.Namespace, "leaf-nq", schedulingv1beta1.QueueStateClosed)

		err := testCtx.Vcclient.SchedulingV1beta1().NamespaceQueues(testCtx.Namespace).Delete(context.TODO(), "leaf-nq", metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})
})
