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

package jobseq

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	busv1alpha1 "volcano.sh/apis/pkg/apis/bus/v1alpha1"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	e2eutil "volcano.sh/volcano/test/e2e/util"
)

var _ = Describe("NamespaceQueue E2E Test", func() {
	var testCtx *e2eutil.TestContext

	JustAfterEach(func() {
		e2eutil.DumpTestContextIfFailed(testCtx, CurrentSpecReport())
	})

	It("NamespaceQueue Command Close And Open With State Check", func() {
		testCtx = e2eutil.InitTestContext(e2eutil.Options{})
		DeferCleanup(e2eutil.CleanupTestContext, testCtx)

		e2eutil.CreateNamespaceQueue(testCtx, e2eutil.NewNamespaceQueue(testCtx.Namespace, "nq1", "", nil))

		By("Close namespace queue command check")
		e2eutil.CreateNamespaceQueueCommand(testCtx, e2eutil.DefaultQueue, testCtx.Namespace, "nq1", busv1alpha1.CloseQueueAction)
		err := e2eutil.WaitNamespaceQueueState(testCtx, testCtx.Namespace, "nq1", schedulingv1beta1.QueueStateClosed)
		Expect(err).NotTo(HaveOccurred(), "wait for closed namespace queue failed")

		By("Open namespace queue command check")
		e2eutil.CreateNamespaceQueueCommand(testCtx, e2eutil.DefaultQueue, testCtx.Namespace, "nq1", busv1alpha1.OpenQueueAction)
		err = e2eutil.WaitNamespaceQueueState(testCtx, testCtx.Namespace, "nq1", schedulingv1beta1.QueueStateOpen)
		Expect(err).NotTo(HaveOccurred(), "wait for reopened namespace queue failed")
	})

	It("NamespaceQueue Parent Child Lifecycle Should Stay Namespace Local", func() {
		testCtx = e2eutil.InitTestContext(e2eutil.Options{})
		DeferCleanup(e2eutil.CleanupTestContext, testCtx)

		e2eutil.CreateNamespaceQueue(testCtx, e2eutil.NewNamespaceQueue(testCtx.Namespace, "parent", "", nil))
		e2eutil.CreateNamespaceQueue(testCtx, e2eutil.NewNamespaceQueue(testCtx.Namespace, "child", "parent", nil))

		By("Closing parent closes child")
		e2eutil.CreateNamespaceQueueCommand(testCtx, e2eutil.DefaultQueue, testCtx.Namespace, "parent", busv1alpha1.CloseQueueAction)
		err := e2eutil.WaitNamespaceQueueState(testCtx, testCtx.Namespace, "parent", schedulingv1beta1.QueueStateClosed)
		Expect(err).NotTo(HaveOccurred(), "wait for parent queue close failed")
		err = e2eutil.WaitNamespaceQueueState(testCtx, testCtx.Namespace, "child", schedulingv1beta1.QueueStateClosed)
		Expect(err).NotTo(HaveOccurred(), "wait for child queue close failed")

		By("Opening child while parent is closed should keep child closed")
		e2eutil.CreateNamespaceQueueCommand(testCtx, e2eutil.DefaultQueue, testCtx.Namespace, "child", busv1alpha1.OpenQueueAction)
		Consistently(func() schedulingv1beta1.QueueState {
			return e2eutil.GetNamespaceQueue(testCtx, testCtx.Namespace, "child").Status.State
		}, "5s", "500ms").Should(Equal(schedulingv1beta1.QueueStateClosed))

		By("Opening parent then child should succeed")
		e2eutil.CreateNamespaceQueueCommand(testCtx, e2eutil.DefaultQueue, testCtx.Namespace, "parent", busv1alpha1.OpenQueueAction)
		err = e2eutil.WaitNamespaceQueueState(testCtx, testCtx.Namespace, "parent", schedulingv1beta1.QueueStateOpen)
		Expect(err).NotTo(HaveOccurred(), "wait for parent queue reopen failed")

		e2eutil.CreateNamespaceQueueCommand(testCtx, e2eutil.DefaultQueue, testCtx.Namespace, "child", busv1alpha1.OpenQueueAction)
		err = e2eutil.WaitNamespaceQueueState(testCtx, testCtx.Namespace, "child", schedulingv1beta1.QueueStateOpen)
		Expect(err).NotTo(HaveOccurred(), "wait for child queue reopen failed")
	})

	It("NamespaceQueue Status Should Track PodGroup Counts", func() {
		testCtx = e2eutil.InitTestContext(e2eutil.Options{})
		DeferCleanup(e2eutil.CleanupTestContext, testCtx)

		e2eutil.CreateNamespaceQueue(testCtx, e2eutil.NewNamespaceQueue(testCtx.Namespace, "tracked", "", nil))

		pg := &schedulingv1beta1.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "tracked-pg",
				Namespace: testCtx.Namespace,
			},
			Spec: schedulingv1beta1.PodGroupSpec{
				Queue:     "tracked",
				MinMember: 1,
			},
		}
		_, err := testCtx.Vcclient.SchedulingV1beta1().PodGroups(testCtx.Namespace).Create(context.TODO(), pg, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() int32 {
			return e2eutil.GetNamespaceQueue(testCtx, testCtx.Namespace, "tracked").Status.Pending
		}, "30s", "1s").Should(BeNumerically(">=", 1))
	})
})
