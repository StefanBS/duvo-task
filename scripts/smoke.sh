#!/usr/bin/env bash
# Smoke test: workloads Ready, every Consumer completes Jobs, pending list stays small.
set -euo pipefail

NS="${NS:-sandbox-orchestrator}"
TIMEOUT_S="${TIMEOUT_S:-30}"
MAX_PENDING="${MAX_PENDING:-10}"
k() { kubectl -n "$NS" "$@"; }

echo "==> Waiting for workloads to be Ready"
k rollout status statefulset/redis --timeout=60s
k rollout status deployment/producer --timeout=60s
k rollout status deployment/consumer --timeout=60s

mapfile -t consumers < <(k get pods -l app.kubernetes.io/name=consumer -o json \
  | jq -r '.items[] | select(.metadata.deletionTimestamp == null) | .metadata.name')
echo "==> Consumers: ${consumers[*]}"

echo "==> Waiting up to ${TIMEOUT_S}s for every Consumer to complete a Job"
deadline=$((SECONDS + TIMEOUT_S))
for pod in "${consumers[@]}"; do
  until k logs "$pod" | jq -e 'select(.event == "job.completed")' >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
      echo "FAIL: $pod completed no Jobs within ${TIMEOUT_S}s" >&2
      k logs "$pod" --tail=20 >&2
      exit 1
    fi
    sleep 2
  done
  echo "    $pod: $(k logs "$pod" | jq -s '[.[] | select(.event == "job.completed")] | length') Jobs completed"
done

pending=$(k exec redis-0 -- redis-cli XPENDING jobs consumers | head -1)
echo "==> Pending Jobs: $pending (max $MAX_PENDING)"
if ((pending > MAX_PENDING)); then
  echo "FAIL: too many pending Jobs" >&2
  exit 1
fi

echo "PASS"
