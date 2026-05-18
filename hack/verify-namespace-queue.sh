#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-unit}"
NAMESPACE="${NAMESPACE:-namespace-queue-e2e}"

cd "${ROOT_DIR}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

wait_jsonpath() {
  local resource="$1"
  local jsonpath="$2"
  local expected="$3"
  local attempts="${4:-30}"

  local current=""
  for _ in $(seq 1 "${attempts}"); do
    current="$(kubectl get ${resource} -o jsonpath="${jsonpath}" 2>/dev/null || true)"
    if [[ "${current}" == "${expected}" ]]; then
      return 0
    fi
    sleep 2
  done

  echo "timed out waiting for ${resource} ${jsonpath}=${expected}, current=${current}" >&2
  return 1
}

expect_apply_failure() {
  local pattern="$1"
  local manifest="$2"
  local output

  set +e
  output="$(kubectl apply -f - 2>&1 <<<"${manifest}")"
  local rc=$?
  set -e

  if [[ ${rc} -eq 0 ]]; then
    echo "expected apply failure but command succeeded" >&2
    return 1
  fi
  if [[ "${output}" != *"${pattern}"* ]]; then
    echo "apply failed, but output did not contain expected pattern: ${pattern}" >&2
    echo "${output}" >&2
    return 1
  fi
}

expect_create_failure() {
  local manifest="$1"
  local pattern="$2"
  local output

  set +e
  output="$(kubectl create -f - 2>&1 <<<"${manifest}")"
  local rc=$?
  set -e

  if [[ ${rc} -eq 0 ]]; then
    echo "expected create failure but command succeeded" >&2
    return 1
  fi
  if [[ "${output}" != *"${pattern}"* ]]; then
    echo "create failed, but output did not contain expected pattern: ${pattern}" >&2
    echo "${output}" >&2
    return 1
  fi
}

run_unit() {
  echo "[namespace-queue] running targeted unit tests"
  go test ./pkg/webhooks/admission/queues/validate -run 'TestAdmitNamespaceQueues|TestValidateNamespaceQueue'
  go test ./pkg/controllers/queue -run 'TestNamespaceQueue'
  go test ./pkg/cli/queue -run 'TestOperateQueue|TestInitOperateFlags'
}

run_live() {
  local vcctl_cmd
  vcctl_cmd="${VCCTL_CMD:-go run ./cmd/cli}"

  require_cmd kubectl
  kubectl version --client >/dev/null
  kubectl get crd namespacequeues.scheduling.volcano.sh >/dev/null
  kubectl get crd podgroups.scheduling.volcano.sh >/dev/null
  kubectl get crd jobs.batch.volcano.sh >/dev/null
  kubectl -n volcano-system get deploy volcano-controllers >/dev/null
  kubectl -n volcano-system get deploy volcano-scheduler >/dev/null
  kubectl -n volcano-system get deploy volcano-admission >/dev/null 2>&1 || \
    kubectl -n volcano-system get deploy volcano-webhook-manager >/dev/null

  echo "[namespace-queue] using namespace ${NAMESPACE}"
  kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

  echo "[namespace-queue] cleanup leftovers from any previous failed run"
  kubectl delete job.batch.volcano.sh parent-job -n "${NAMESPACE}" --ignore-not-found=true || true
  kubectl delete podgroup pg-open-child pg-closed-child -n "${NAMESPACE}" --ignore-not-found=true || true
  if kubectl get namespacequeue invalid-child -n "${NAMESPACE}" >/dev/null 2>&1; then
    ${vcctl_cmd} queue operate --namespace "${NAMESPACE}" --name invalid-child --action close || true
    sleep 2
    kubectl delete namespacequeue invalid-child -n "${NAMESPACE}" --ignore-not-found=true || true
  fi
  kubectl delete queue child --ignore-not-found=true

  echo "[namespace-queue] create cluster queue with same name to verify namespace resolution precedence"
  cat <<EOF | kubectl apply -f -
apiVersion: scheduling.volcano.sh/v1beta1
kind: Queue
metadata:
  name: child
spec:
  weight: 1
EOF
  ${vcctl_cmd} queue operate --name child --action close
  wait_jsonpath "queue/child" '{.status.state}' 'Closed'

  echo "[namespace-queue] create namespace queue hierarchy"
  cat <<EOF | kubectl apply -f -
apiVersion: scheduling.volcano.sh/v1beta1
kind: NamespaceQueue
metadata:
  name: parent
  namespace: ${NAMESPACE}
spec:
  weight: 1
  deserved:
    cpu: "8"
  capability:
    cpu: "8"
---
apiVersion: scheduling.volcano.sh/v1beta1
kind: NamespaceQueue
metadata:
  name: child
  namespace: ${NAMESPACE}
spec:
  weight: 1
  parent: parent
  deserved:
    cpu: "2"
  capability:
    cpu: "4"
EOF

  wait_jsonpath "namespacequeue/parent -n ${NAMESPACE}" '{.status.state}' 'Open'
  wait_jsonpath "namespacequeue/child -n ${NAMESPACE}" '{.status.state}' 'Open'

  echo "[namespace-queue] invalid child must be rejected by queue validating webhook"
  expect_apply_failure "exceeds its ancestor's capability" "$(cat <<EOF
apiVersion: scheduling.volcano.sh/v1beta1
kind: NamespaceQueue
metadata:
  name: invalid-child
  namespace: ${NAMESPACE}
spec:
  weight: 1
  parent: parent
  capability:
    cpu: "16"
EOF
)"

  echo "[namespace-queue] submitting a Job to parent must fail because it is not a leaf queue"
  expect_create_failure "$(cat <<EOF
apiVersion: batch.volcano.sh/v1alpha1
kind: Job
metadata:
  name: parent-job
  namespace: ${NAMESPACE}
spec:
  minAvailable: 1
  schedulerName: volcano
  queue: parent
  tasks:
  - replicas: 1
    name: main
    template:
      spec:
        restartPolicy: Never
        containers:
        - name: main
          image: busybox:1.36
          command: ["sh","-c","sleep 300"]
EOF
)" "can only submit job to leaf queue"

  echo "[namespace-queue] PodGroup should resolve to namespace queue child, not the closed cluster queue child"
  cat <<EOF | kubectl create -f -
apiVersion: scheduling.volcano.sh/v1beta1
kind: PodGroup
metadata:
  name: pg-open-child
  namespace: ${NAMESPACE}
spec:
  minMember: 1
  queue: child
EOF

  echo "[namespace-queue] update child weight via webhook path"
  kubectl patch namespacequeue child -n "${NAMESPACE}" --type merge -p '{"spec":{"weight":2}}'
  wait_jsonpath "namespacequeue/child -n ${NAMESPACE}" '{.spec.weight}' '2'

  echo "[namespace-queue] remove accepted PodGroup so close can converge to Closed"
  kubectl delete podgroup pg-open-child -n "${NAMESPACE}" --ignore-not-found=true

  echo "[namespace-queue] close via vcctl and verify admission starts rejecting submissions"
  ${vcctl_cmd} queue operate --namespace "${NAMESPACE}" --name child --action close
  wait_jsonpath "namespacequeue/child -n ${NAMESPACE}" '{.status.state}' 'Closed'
  expect_create_failure "$(cat <<EOF
apiVersion: scheduling.volcano.sh/v1beta1
kind: PodGroup
metadata:
  name: pg-closed-child
  namespace: ${NAMESPACE}
spec:
  minMember: 1
  queue: child
EOF
)" "state \`Open\`"

  echo "[namespace-queue] reopen via vcctl and verify spec update via vcctl"
  ${vcctl_cmd} queue operate --namespace "${NAMESPACE}" --name child --action open
  wait_jsonpath "namespacequeue/child -n ${NAMESPACE}" '{.status.state}' 'Open'

  ${vcctl_cmd} queue operate --namespace "${NAMESPACE}" --name child --action update --weight 3
  wait_jsonpath "namespacequeue/child -n ${NAMESPACE}" '{.spec.weight}' '3'

  echo "[namespace-queue] close parent so delete validation can reach child-exists protection"
  ${vcctl_cmd} queue operate --namespace "${NAMESPACE}" --name parent --action close
  wait_jsonpath "namespacequeue/parent -n ${NAMESPACE}" '{.status.state}' 'Closed'
  wait_jsonpath "namespacequeue/child -n ${NAMESPACE}" '{.status.state}' 'Closed'

  echo "[namespace-queue] parent delete must fail while child exists"
  set +e
  delete_output="$(kubectl delete namespacequeue parent -n "${NAMESPACE}" 2>&1)"
  delete_rc=$?
  set -e
  if [[ ${delete_rc} -eq 0 || "${delete_output}" != *"still has 1 child queue"* ]]; then
    echo "expected parent delete protection to trigger" >&2
    echo "${delete_output}" >&2
    return 1
  fi

  echo "[namespace-queue] cleanup leaf then parent"
  kubectl delete podgroup pg-open-child -n "${NAMESPACE}" --ignore-not-found=true
  kubectl delete podgroup pg-closed-child -n "${NAMESPACE}" --ignore-not-found=true
  kubectl delete namespacequeue child -n "${NAMESPACE}"
  kubectl delete namespacequeue parent -n "${NAMESPACE}"
  kubectl delete queue child --ignore-not-found=true
}

case "${MODE}" in
  unit)
    run_unit
    ;;
  live)
    run_live
    ;;
  *)
    echo "usage: $0 [unit|live]" >&2
    exit 1
    ;;
esac
