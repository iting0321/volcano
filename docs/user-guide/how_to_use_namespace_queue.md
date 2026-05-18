# How to Use NamespaceQueue

`NamespaceQueue` is a namespace-scoped Volcano queue. It lets tenants manage queue capacity and hierarchy inside their own namespace without cluster-wide `Queue` permissions.

`NamespaceQueue` uses the same queue fields as `Queue`, including `weight`, `capability`, `deserved`, `guarantee`, `priority`, `reclaimable`, and `parent`.

## Queue Resolution

Workloads keep using the existing queue name fields. NamespaceQueue does not add a new workload field or require namespace-qualified queue names.

For a workload in namespace `team-a` with queue name `training`, Volcano resolves the queue in this order:

1. `NamespaceQueue` `team-a/training`
2. Cluster-scoped `Queue` `training`

This keeps existing cluster `Queue` users compatible. A `NamespaceQueue` only takes precedence inside its own namespace.

## Create

```yaml
apiVersion: scheduling.volcano.sh/v1beta1
kind: NamespaceQueue
metadata:
  namespace: team-a
  name: training
spec:
  weight: 6
  parent: research
  deserved:
    cpu: "4"
    memory: 16Gi
  capability:
    cpu: "8"
    memory: 32Gi
```

```bash
kubectl apply -f namespacequeue.yaml
```

## Use from Workloads

Reference the queue by its plain name. Do not include the namespace in the workload queue value.

```yaml
spec:
  queue: training
```

If the workload runs in namespace `team-a`, Volcano uses `NamespaceQueue` `team-a/training` when it exists. Otherwise, it falls back to cluster `Queue` `training`.

## Manage

Use the existing queue command with `--namespace` to operate on a `NamespaceQueue`:

```bash
vcctl queue operate --namespace team-a --name training --action close
vcctl queue operate --namespace team-a --name training --action open
vcctl queue operate --namespace team-a --name training --action update --weight 2
```

Without `--namespace`, the command operates on a cluster-scoped `Queue`.

## Compatibility and Migration

Existing workloads do not need manifest changes. Keep using the same plain queue name as before.

To migrate a tenant from a cluster `Queue` to a namespace-local queue:

1. Create a `NamespaceQueue` in the tenant namespace with the same name as the existing cluster `Queue`.
2. Leave Job, PodGroup, and Pod manifests unchanged.
3. New and re-resolved workloads in that namespace use the `NamespaceQueue`.
4. Other namespaces continue using their own matching `NamespaceQueue`, or the cluster `Queue` fallback.
5. To roll back, delete the `NamespaceQueue`; workloads with the same queue name fall back to the cluster `Queue`.

Migration is namespace-by-namespace and does not require changing workload queue references.

## Constraints

- `NamespaceQueue` named `root` is reserved.
- Parent relationships for `NamespaceQueue` are namespace-local.
- Workloads can only be submitted to leaf queues.
- Queue status is reconciled by Volcano in the same way as cluster queues.
