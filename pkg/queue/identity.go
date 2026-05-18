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
	"strings"
)

const (
	ClusterQueueScope   = "cluster"
	NamespaceQueueScope = "namespace"
)

// ClusterKey returns the canonical key for a cluster-scoped queue.
func ClusterKey(name string) string {
	return fmt.Sprintf("%s:%s", ClusterQueueScope, name)
}

// NamespaceKey returns the canonical key for a namespace-scoped queue.
func NamespaceKey(namespace, name string) string {
	return fmt.Sprintf("%s:%s/%s", NamespaceQueueScope, namespace, name)
}

// SyntheticRootKey returns the canonical key of the synthetic root queue for a namespace.
func SyntheticRootKey(namespace string) string {
	return NamespaceKey(namespace, "root")
}

// IsNamespaceKey reports whether the key refers to a NamespaceQueue.
func IsNamespaceKey(key string) bool {
	return strings.HasPrefix(key, NamespaceQueueScope+":")
}

// IsClusterKey reports whether the key refers to a cluster Queue.
func IsClusterKey(key string) bool {
	return strings.HasPrefix(key, ClusterQueueScope+":")
}

// ParseClusterKey returns the queue name from a cluster queue key.
func ParseClusterKey(key string) (string, bool) {
	if !IsClusterKey(key) {
		return "", false
	}
	return strings.TrimPrefix(key, ClusterQueueScope+":"), true
}

// ParseNamespaceKey returns namespace and queue name from a namespace queue key.
func ParseNamespaceKey(key string) (string, string, bool) {
	if !IsNamespaceKey(key) {
		return "", "", false
	}
	trimmed := strings.TrimPrefix(key, NamespaceQueueScope+":")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}
