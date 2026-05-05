# How To Use NamespaceQueue

`NamespaceQueue` is a namespace-scoped queue resource for Volcano.

It allows tenants to manage queue configuration inside their own namespace without requiring cluster-wide permission on `Queue`.

## Behavior

- A workload resolves its queue reference in this order:
  1. `NamespaceQueue` in the workload namespace
  2. cluster-scoped `Queue`
- Existing users of cluster `Queue` do not need to change manifests.
- `NamespaceQueue` uses the same queue fields as `Queue`, including `capability`, `deserved`, `guarantee`, `priority`, `reclaimable`, and `parent`.

## Create a NamespaceQueue

```yaml
apiVersion: scheduling.volcano.sh/v1beta1
kind: NamespaceQueue
metadata:
  name: team-a
  namespace: tenant-a
spec:
  weight: 1
  reclaimable: true
  deserved:
    cpu: "4"
    memory: 16Gi
  capability:
    cpu: "8"
    memory: 32Gi
```

Apply it with:

```bash
kubectl apply -f namespacequeue.yaml
```

## Reference it from workloads

Jobs and PodGroups keep using the existing queue field or annotation.

```yaml
spec:
  queue: team-a
```

or

```yaml
metadata:
  annotations:
    scheduling.volcano.sh/queue-name: team-a
```

If a `NamespaceQueue` named `team-a` exists in the workload namespace, Volcano uses it. Otherwise, Volcano falls back to the cluster `Queue` named `team-a`.

## Notes

- `NamespaceQueue` named `root` is reserved.
- Parent relationships for `NamespaceQueue` are namespace-local.
- Queue status is updated through Volcano queue reconciliation in the same way as cluster queues.
