#!/usr/bin/env bash
# Smoke test: workloads Ready, every Consumer provisions Sandboxes, a Sandbox URL
# serves its own Job, no Jobs failed, pending list stays small.
set -euo pipefail

NS="${NS:-sandbox-orchestrator}"
TIMEOUT_S="${TIMEOUT_S:-120}"
MAX_PENDING="${MAX_PENDING:-10}"
k() { kubectl -n "$NS" "$@"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

echo "==> Waiting for workloads to be Ready"
k rollout status statefulset/redis --timeout=60s
k rollout status deployment/producer --timeout=60s
k wait --for=condition=Ready pod -l app.kubernetes.io/name=consumer --timeout=120s
echo "==> Consumer rollout: $(kubectl argo rollouts -n "$NS" status consumer --timeout 1s 2>&1 | tail -1)"

mapfile -t consumers < <(k get pods -l app.kubernetes.io/name=consumer -o json \
  | jq -r '.items[] | select(.metadata.deletionTimestamp == null) | .metadata.name')
echo "==> Consumers: ${consumers[*]}"

echo "==> Waiting up to ${TIMEOUT_S}s for every Consumer to make a Sandbox ready"
deadline=$((SECONDS + TIMEOUT_S))
for pod in "${consumers[@]}"; do
  until k logs "$pod" | jq -e 'select(.event == "sandbox.ready")' >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
      k logs "$pod" --tail=20 >&2
      fail "$pod made no Sandbox ready within ${TIMEOUT_S}s"
    fi
    sleep 2
  done
  echo "    $pod: $(k logs "$pod" | jq -cs '[.[] | select(.event == "sandbox.ready")] | {ready: length, p50Ms: (map(.durationMs) | sort | .[length/2|floor])}')"
done

failed=$(for pod in "${consumers[@]}"; do k logs "$pod"; done | jq -c 'select(.event == "job.failed") | {jobId, error}')
[[ -z "$failed" ]] || fail "Jobs failed:"$'\n'"$failed"

echo "==> Checking the newest Sandbox URL serves its own Pod"
ready=$(k logs "${consumers[0]}" | jq -c 'select(.event == "sandbox.ready")' | tail -1)
url=$(jq -r .url <<<"$ready"); name=$(jq -r .sandbox <<<"$ready")
hostname=$(k exec "${consumers[0]}" -- python -c \
  "import json,sys,urllib.request; print(json.load(urllib.request.urlopen(sys.argv[1], timeout=5))['hostname'])" "$url")
echo "    GET $url -> hostname=$hostname"
[[ "$hostname" == "$name" ]] || fail "expected hostname $name, got $hostname"

pending=$(k exec redis-0 -- redis-cli XPENDING jobs consumers | head -1)
echo "==> Pending Jobs: $pending (max $MAX_PENDING)"
((pending <= MAX_PENDING)) || fail "too many pending Jobs"

echo "PASS"
