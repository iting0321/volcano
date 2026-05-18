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
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	busv1alpha1 "volcano.sh/apis/pkg/apis/bus/v1alpha1"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	queueutil "volcano.sh/volcano/pkg/queue"

	"github.com/spf13/cobra"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestOperateQueue(t *testing.T) {
	clusterQueueResponse := v1beta1.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-queue",
			UID:  "cluster-uid",
		},
	}
	namespaceQueueResponse := v1beta1.NamespaceQueue{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-namespace-queue",
			Namespace: "test-ns",
			UID:       "namespace-uid",
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/apis/scheduling.volcano.sh/v1beta1/queues/test-queue":
			_ = json.NewEncoder(w).Encode(clusterQueueResponse)
		case r.Method == http.MethodGet && r.URL.Path == "/apis/scheduling.volcano.sh/v1beta1/namespaces/test-ns/namespacequeues/test-namespace-queue":
			_ = json.NewEncoder(w).Encode(namespaceQueueResponse)
		case r.Method == http.MethodPatch && r.URL.Path == "/apis/scheduling.volcano.sh/v1beta1/queues/test-queue":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("failed to read cluster queue patch body: %v", err)
			}
			if string(body) != `{"spec":{"weight":3}}` {
				t.Fatalf("unexpected cluster queue patch body: %s", string(body))
			}
			_ = json.NewEncoder(w).Encode(clusterQueueResponse)
		case r.Method == http.MethodPatch && r.URL.Path == "/apis/scheduling.volcano.sh/v1beta1/namespaces/test-ns/namespacequeues/test-namespace-queue":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("failed to read namespace queue patch body: %v", err)
			}
			if string(body) != `{"spec":{"weight":3}}` {
				t.Fatalf("unexpected namespace queue patch body: %s", string(body))
			}
			_ = json.NewEncoder(w).Encode(namespaceQueueResponse)
		case r.Method == http.MethodPost && r.URL.Path == "/apis/bus.volcano.sh/v1alpha1/namespaces/default/commands":
			var cmd busv1alpha1.Command
			if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
				t.Fatalf("failed to decode command: %v", err)
			}

			switch cmd.TargetObject.Name {
			case "test-queue":
				if cmd.TargetObject.Kind != "Queue" {
					t.Fatalf("unexpected cluster queue target kind: %s", cmd.TargetObject.Kind)
				}
			case queueutil.NamespaceKey("test-ns", "test-namespace-queue"):
				if cmd.TargetObject.Kind != "Queue" {
					t.Fatalf("unexpected namespace queue target kind: %s", cmd.TargetObject.Kind)
				}
				if len(cmd.OwnerReferences) != 0 {
					t.Fatalf("namespace queue command should not set owner references")
				}
			default:
				t.Fatalf("unexpected command target name: %s", cmd.TargetObject.Name)
			}

			_ = json.NewEncoder(w).Encode(cmd)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	operateQueueFlags.Master = server.URL
	testCases := []struct {
		Name        string
		QueueName   string
		Namespace   string
		Weight      int32
		Action      string
		ExpectValue error
	}{
		{
			Name:        "Normal Case Operate Cluster Queue Succeed, Action close",
			QueueName:   "test-queue",
			Action:      ActionClose,
			ExpectValue: nil,
		},
		{
			Name:        "Normal Case Operate Cluster Queue Succeed, Action open",
			QueueName:   "test-queue",
			Action:      ActionOpen,
			ExpectValue: nil,
		},
		{
			Name:        "Normal Case Operate Cluster Queue Succeed, Update Weight",
			QueueName:   "test-queue",
			Action:      ActionUpdate,
			Weight:      3,
			ExpectValue: nil,
		},
		{
			Name:        "Normal Case Operate Namespace Queue Succeed, Action close",
			QueueName:   "test-namespace-queue",
			Namespace:   "test-ns",
			Action:      ActionClose,
			ExpectValue: nil,
		},
		{
			Name:        "Normal Case Operate Namespace Queue Succeed, Action open",
			QueueName:   "test-namespace-queue",
			Namespace:   "test-ns",
			Action:      ActionOpen,
			ExpectValue: nil,
		},
		{
			Name:        "Normal Case Operate Namespace Queue Succeed, Update Weight",
			QueueName:   "test-namespace-queue",
			Namespace:   "test-ns",
			Action:      ActionUpdate,
			Weight:      3,
			ExpectValue: nil,
		},
		{
			Name:      "Abnormal Case Update Queue Failed For Invalid Weight",
			QueueName: "abnormal-case-invalid-weight",
			Action:    ActionUpdate,
			ExpectValue: fmt.Errorf("when %s queue %s, weight must be specified, "+
				"the value must be greater than 0", ActionUpdate, "abnormal-case-invalid-weight"),
		},
		{
			Name:        "Abnormal Case Operate Queue Failed For Name Not Specified",
			QueueName:   "",
			ExpectValue: fmt.Errorf("queue name must be specified"),
		},
		{
			Name:        "Abnormal Case Operate Queue Failed For Action null",
			QueueName:   "abnormal-case-null-action",
			Action:      "",
			ExpectValue: fmt.Errorf("action can not be null"),
		},
		{
			Name:      "Abnormal Case Operate Queue Failed For Action Invalid",
			QueueName: "abnormal-case-invalid-action",
			Action:    "invalid",
			ExpectValue: fmt.Errorf("action %s invalid, valid actions are %s, %s and %s",
				"invalid", ActionOpen, ActionClose, ActionUpdate),
		},
	}

	for _, testCase := range testCases {
		operateQueueFlags.Name = testCase.QueueName
		operateQueueFlags.Namespace = testCase.Namespace
		operateQueueFlags.Action = testCase.Action
		operateQueueFlags.Weight = testCase.Weight

		err := OperateQueue(context.TODO())
		if false == reflect.DeepEqual(err, testCase.ExpectValue) {
			t.Errorf("Case '%s' failed, expected: '%v', got '%v'", testCase.Name, testCase.ExpectValue, err)
		}
	}
}

func TestInitOperateFlags(t *testing.T) {
	var cmd cobra.Command
	InitOperateFlags(&cmd)

	if cmd.Flag("name") == nil {
		t.Errorf("Could not find the flag name")
	}
	if cmd.Flag("namespace") == nil {
		t.Errorf("Could not find the flag namespace")
	}
	if cmd.Flag("weight") == nil {
		t.Errorf("Could not find the flag weight")
	}
	if cmd.Flag("action") == nil {
		t.Errorf("Could not find the flag action")
	}
}
