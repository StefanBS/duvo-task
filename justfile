set shell := ["bash", "-euo", "pipefail", "-c"]

image_repo := "localhost/orchestrator"
bad_sandbox_image := "ghcr.io/stefanprodan/podinfo:does-not-exist"
cluster := "k3s-default"
ns := "sandbox-orchestrator"

default:
    @just --list

# Run unit tests
test:
    uv run pytest -q

# Build the image with a unique version tag (recorded in .build/version)
build:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p .build
    version="$(git rev-parse --short HEAD)$(git diff --quiet HEAD || echo -dirty)-$(date +%Y%m%d%H%M%S)"
    podman build -t "{{image_repo}}:build" .
    # Stamp the version in its own layer. (A --build-arg consumed by ENV is ignored by
    # Podman's layer cache, which silently reused the previous version's image.)
    printf 'FROM %s\nENV APP_VERSION=%s\n' "{{image_repo}}:build" "$version" | podman build -t "{{image_repo}}:$version" -f - .
    echo "$version" > .build/version

# Build and load the image into the k3d cluster
import: build
    #!/usr/bin/env bash
    set -euo pipefail
    rm -f .build/image.tar
    podman save --format docker-archive -o .build/image.tar "{{image_repo}}:$(cat .build/version)"
    # --mode direct: the default mode spawns a tools container that fails under Podman.
    k3d image import --mode direct -c {{cluster}} .build/image.tar

# Apply manifests using the last built version. Consumer changes go out as a canary.
apply:
    kubectl kustomize k8s | sed "s|{{image_repo}}:dev|{{image_repo}}:$(cat .build/version)|g" | kubectl apply -f -
    # One-off migration from step 3: the Consumer used to be a Deployment.
    kubectl -n {{ns}} delete deployment consumer --ignore-not-found
    kubectl -n {{ns}} rollout status statefulset/redis --timeout=120s
    kubectl -n {{ns}} rollout status deployment/producer deployment/alert-receiver --timeout=120s
    kubectl argo rollouts -n {{ns}} get rollout consumer

# Build, import and apply. Producer/alert-receiver roll immediately; the Consumer starts a canary.
deploy: import apply

# Tail logs for a component (producer | consumer | redis)
logs component="consumer":
    kubectl -n {{ns}} logs -l app.kubernetes.io/name={{component}} -f --prefix --max-log-requests 10

# Verify Jobs flow end to end
smoke:
    NS={{ns}} scripts/smoke.sh

# Run redis-cli against the in-cluster Redis, e.g. `just redis-cli XINFO GROUPS jobs`
redis-cli *args:
    kubectl -n {{ns}} exec redis-0 -- redis-cli {{args}}

# List Sandboxes and their state
sandboxes:
    kubectl -n sandboxes get pods,services -l app.kubernetes.io/name=sandbox -o wide

# Fetch a Sandbox's URL from inside the cluster (defaults to the newest), e.g. `just curl /status/500`
curl path="/" sandbox="":
    #!/usr/bin/env bash
    set -euo pipefail
    name="{{sandbox}}"
    if [[ -z "$name" ]]; then
      name=$(kubectl -n sandboxes get pods -l app.kubernetes.io/name=sandbox --field-selector=status.phase=Running \
        --sort-by=.metadata.creationTimestamp -o name | tail -1 | cut -d/ -f2)
    fi
    url="http://$name.sandboxes.svc.cluster.local:9898{{path}}"
    echo "GET $url" >&2
    kubectl -n {{ns}} exec deploy/producer -- python -c "import sys,urllib.request,urllib.error
    try: r=urllib.request.urlopen(sys.argv[1], timeout=5); print(r.status); print(r.read().decode())
    except urllib.error.HTTPError as e: print(e.code); print(e.read().decode())" "$url"

# Install Prometheus/Alertmanager/Grafana, Loki and Alloy, then our monitors, alerts and dashboard
obs-up:
    helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack --version 91.4.1 \
      -n monitoring --create-namespace -f observability/kube-prometheus-stack.yaml --wait --timeout 10m
    helm upgrade --install loki grafana/loki --version 7.3.0 -n monitoring -f observability/loki.yaml --wait --timeout 10m
    helm upgrade --install alloy grafana/alloy --version 1.12.1 -n monitoring -f observability/alloy.yaml --wait --timeout 5m
    just obs-apply

# Regenerate the dashboard and apply PodMonitors, PrometheusRules and the dashboard ConfigMap
obs-apply:
    uv run python observability/dashboards/generate.py > observability/dashboards/overview.json
    kubectl kustomize observability/k8s --load-restrictor LoadRestrictionsNone | kubectl apply -f -

# Grafana on http://localhost:3000 (admin/admin)
grafana:
    kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana 3000:80

# Prometheus on http://localhost:9090
prometheus:
    kubectl -n monitoring port-forward svc/kube-prometheus-stack-prometheus 9090:9090

# Alertmanager on http://localhost:9093
alertmanager:
    kubectl -n monitoring port-forward svc/kube-prometheus-stack-alertmanager 9093:9093

# Show alerts as received by the alert receiver
alerts:
    kubectl -n {{ns}} logs deploy/alert-receiver | jq -c 'select(.event | startswith("alert.")) | {ts, event, alertname, severity, summary}'

# --- Consumer A/B (canary) rollouts -------------------------------------------

# Watch the Consumer rollout (steps, weights, canary vs stable ReplicaSets)
rollout:
    kubectl argo rollouts -n {{ns}} get rollout consumer --watch

# Release the current code as a new Consumer version (canary at 25%, then paused)
canary: import
    kubectl argo rollouts -n {{ns}} set image consumer consumer={{image_repo}}:$(cat .build/version)
    kubectl argo rollouts -n {{ns}} get rollout consumer

# Release a known-bad Consumer config as a canary: its Sandboxes can't pull their image
canary-bad:
    kubectl -n {{ns}} patch rollout consumer --type=json \
      -p '[{"op":"add","path":"/spec/template/spec/containers/0/env","value":[{"name":"SANDBOX_IMAGE","value":"{{bad_sandbox_image}}"}]}]'
    kubectl argo rollouts -n {{ns}} get rollout consumer

# Remove the bad-config override (returns the Rollout to its stable spec)
canary-reset:
    kubectl -n {{ns}} patch rollout consumer --type=json -p '[{"op":"remove","path":"/spec/template/spec/containers/0/env"}]' || true

# Advance the canary to its next step
promote:
    kubectl argo rollouts -n {{ns}} promote consumer

# Skip remaining steps and roll the canary out to 100%
promote-full:
    kubectl argo rollouts -n {{ns}} promote consumer --full

# Abort the canary: scale it down and send all traffic back to stable
abort:
    kubectl argo rollouts -n {{ns}} abort consumer

# Argo Rollouts dashboard on http://localhost:3100
rollouts-ui:
    kubectl -n argo-rollouts port-forward svc/argo-rollouts-dashboard 3100:3100

# Install the Argo Rollouts controller + dashboard
rollouts-up:
    helm upgrade --install argo-rollouts argo/argo-rollouts --version 2.40.5 -n argo-rollouts --create-namespace \
      -f rollouts/argo-rollouts.yaml --wait --timeout 5m

# --- Chaos -------------------------------------------------------------------

# Chaos: stop all Consumers (expect JobBacklogGrowing, ConsumersDown, JobsNotCompleting)
chaos-consumers-down:
    kubectl -n {{ns}} scale rollout/consumer --replicas=0

# Chaos: make every Consumer's Sandboxes fail to pull their image (expect JobFailureRatioHigh)
chaos-bad-image: canary-bad promote-full

# Undo all chaos
chaos-reset: canary-reset
    kubectl -n {{ns}} scale rollout/consumer --replicas=4
    kubectl argo rollouts -n {{ns}} promote consumer --full || true

# Tear everything down (including Redis data and Sandboxes)
down:
    kubectl delete namespace {{ns}} sandboxes --ignore-not-found

# Remove the observability stack
obs-down:
    helm uninstall alloy loki kube-prometheus-stack -n monitoring --ignore-not-found
    kubectl delete namespace monitoring --ignore-not-found
