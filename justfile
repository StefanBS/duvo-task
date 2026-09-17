set shell := ["bash", "-euo", "pipefail", "-c"]

image_repo := "localhost/orchestrator"
bad_sandbox_image := "ghcr.io/stefanprodan/podinfo:does-not-exist"
cluster := "k3s-default"
ns := "sandbox-orchestrator"
# podman (default) or docker, e.g. `CONTAINER_CLI=docker just up`
container_cli := env("CONTAINER_CLI", "podman")

[private]
default:
    @just --list --unsorted

# --- Setup ---------------------------------------------------------------------

# Check prerequisites: required tools installed and a cluster reachable (not the deployment itself)
[group('setup')]
doctor:
    #!/usr/bin/env bash
    set -uo pipefail
    ok=true
    for tool in {{container_cli}} k3d kubectl helm uv jq git; do
      if command -v "$tool" >/dev/null; then echo "ok       $tool"; else echo "MISSING  $tool"; ok=false; fi
    done
    if kubectl argo rollouts version >/dev/null 2>&1; then echo "ok       kubectl argo rollouts plugin"
    else echo "MISSING  kubectl argo rollouts plugin (https://argoproj.github.io/argo-rollouts/installation/#kubectl-plugin-installation)"; ok=false; fi
    if kubectl version --request-timeout=5s >/dev/null 2>&1; then echo "ok       cluster reachable ($(kubectl config current-context))"
    else echo "MISSING  reachable cluster (try: just cluster-up)"; ok=false; fi
    $ok && echo "Prerequisites OK (this does not check the deployment; run: just up, then just smoke)"

# Step 1 of a full deployment: create the local k3d cluster (then run: just up)
[group('setup')]
cluster-up:
    #!/usr/bin/env bash
    set -euo pipefail
    args=()
    if [[ "{{container_cli}}" == podman && "$(podman info --format '{{{{.Host.Security.Rootless}}')" == true ]]; then
      # Rootless Podman (https://k3d.io/stable/usage/advanced/podman/): k3d must mount the user's
      # Podman socket instead of /var/run/docker.sock, and the kubelet runs in a user namespace.
      sock="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/podman/podman.sock"
      export DOCKER_HOST="${DOCKER_HOST:-unix://$sock}" DOCKER_SOCK="${DOCKER_SOCK:-$sock}"
      args+=(--k3s-arg "--kubelet-arg=feature-gates=KubeletInUserNamespace=true@server:*")
    fi
    k3d cluster create {{cluster}} --wait --timeout 180s "${args[@]}"

# Delete the local k3d cluster (removes everything)
[group('setup')]
cluster-down:
    k3d cluster delete {{cluster}}

# Step 2 of a full deployment (after cluster-up): monitoring stack → Argo Rollouts → app → our monitors/rules/dashboard
[group('setup')]
up: helm-repos obs-up rollouts-up deploy obs-apply
    @echo "All up. Next: just smoke"

# Remove everything `up` installed, keeping the cluster
[group('setup')]
down: undeploy rollouts-down obs-down

# Add the Helm chart repositories used by obs-up and rollouts-up
[group('setup')]
helm-repos:
    helm repo add prometheus-community https://prometheus-community.github.io/helm-charts --force-update
    helm repo add grafana https://grafana.github.io/helm-charts --force-update
    helm repo add argo https://argoproj.github.io/argo-helm --force-update
    helm repo update prometheus-community grafana argo

# Install Prometheus/Alertmanager/Grafana, Loki and Alloy (also provides the ServiceMonitor CRDs rollouts-up needs)
[group('setup')]
obs-up:
    helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack --version 91.4.1 \
      -n monitoring --create-namespace -f observability/kube-prometheus-stack.yaml --wait --timeout 10m
    helm upgrade --install loki grafana/loki --version 7.3.0 -n monitoring -f observability/loki.yaml --wait --timeout 10m
    helm upgrade --install alloy grafana/alloy --version 1.12.1 -n monitoring -f observability/alloy.yaml --wait --timeout 5m

# Regenerate the dashboard and apply PodMonitors, PrometheusRules and the dashboard ConfigMap (after deploy: they live in the app's namespace)
[group('setup')]
obs-apply:
    uv run python observability/dashboards/generate.py > observability/dashboards/overview.json
    kubectl kustomize observability/k8s --load-restrictor LoadRestrictionsNone | kubectl apply -f -

# Remove the observability stack
[group('setup')]
obs-down:
    helm uninstall alloy loki kube-prometheus-stack -n monitoring --ignore-not-found
    kubectl delete namespace monitoring --ignore-not-found

# Install the Argo Rollouts controller + dashboard (needs the ServiceMonitor CRD from obs-up)
[group('setup')]
rollouts-up:
    helm upgrade --install argo-rollouts argo/argo-rollouts --version 2.40.5 -n argo-rollouts --create-namespace \
      -f rollouts/argo-rollouts.yaml --wait --timeout 5m

# Remove the Argo Rollouts controller
[group('setup')]
rollouts-down:
    helm uninstall argo-rollouts -n argo-rollouts --ignore-not-found
    kubectl delete namespace argo-rollouts --ignore-not-found

# --- App -----------------------------------------------------------------------

# Run unit tests
[group('app')]
test:
    uv run pytest -q

# Build the image with a unique version tag (recorded in .build/version)
[group('app')]
build:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p .build
    version="$(git rev-parse --short HEAD)$(git diff --quiet HEAD || echo -dirty)-$(date +%Y%m%d%H%M%S)"
    {{container_cli}} build -t "{{image_repo}}:build" .
    # Stamp the version in its own layer. (A --build-arg consumed by ENV is ignored by
    # Podman's layer cache, which silently reused the previous version's image.)
    printf 'FROM %s\nENV APP_VERSION=%s\n' "{{image_repo}}:build" "$version" | {{container_cli}} build -t "{{image_repo}}:$version" -f - .
    echo "$version" > .build/version

# Build and load the image into the k3d cluster
[group('app')]
import: build
    #!/usr/bin/env bash
    set -euo pipefail
    rm -f .build/image.tar
    image="{{image_repo}}:$(cat .build/version)"
    if [[ "{{container_cli}}" == podman ]]; then
      podman save --format docker-archive -o .build/image.tar "$image"
    else
      {{container_cli}} save -o .build/image.tar "$image"
    fi
    # --mode direct: the default mode spawns a tools container that fails under Podman.
    k3d image import --mode direct -c {{cluster}} .build/image.tar

# Apply manifests using the last built version. Consumer changes go out as a canary.
[group('app')]
apply:
    kubectl kustomize k8s | sed "s|{{image_repo}}:dev|{{image_repo}}:$(cat .build/version)|g" | kubectl apply -f -
    # One-off migration from step 3: the Consumer used to be a Deployment.
    kubectl -n {{ns}} delete deployment consumer --ignore-not-found
    kubectl -n {{ns}} rollout status statefulset/redis --timeout=120s
    kubectl -n {{ns}} rollout status deployment/producer deployment/alert-receiver --timeout=120s
    kubectl argo rollouts -n {{ns}} get rollout consumer

# Build, import and apply. Producer/alert-receiver roll immediately; the Consumer starts a canary.
[group('app')]
deploy: import apply

# Delete the app namespaces (including Redis data and Sandboxes)
[group('app')]
undeploy:
    kubectl delete namespace {{ns}} sandboxes --ignore-not-found

# Verify Jobs flow end to end
[group('app')]
smoke:
    NS={{ns}} scripts/smoke.sh

# Tail logs for a component (producer | consumer | redis)
[group('app')]
logs component="consumer":
    kubectl -n {{ns}} logs -l app.kubernetes.io/name={{component}} -f --prefix --max-log-requests 10

# Run redis-cli against the in-cluster Redis, e.g. `just redis-cli XINFO GROUPS jobs`
[group('app')]
redis-cli *args:
    kubectl -n {{ns}} exec redis-0 -- redis-cli {{args}}

# List Sandboxes and their state
[group('app')]
sandboxes:
    kubectl -n sandboxes get pods,services -l app.kubernetes.io/name=sandbox -o wide

# Fetch a Sandbox's URL from inside the cluster (defaults to the newest), e.g. `just curl /status/500`
[group('app')]
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

# --- Observability -------------------------------------------------------------

# Grafana on http://localhost:3000 (admin/admin)
[group('observability')]
grafana:
    kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana 3000:80

# Prometheus on http://localhost:9090
[group('observability')]
prometheus:
    kubectl -n monitoring port-forward svc/kube-prometheus-stack-prometheus 9090:9090

# Alertmanager on http://localhost:9093
[group('observability')]
alertmanager:
    kubectl -n monitoring port-forward svc/kube-prometheus-stack-alertmanager 9093:9093

# Show alerts as received by the alert receiver
[group('observability')]
alerts:
    kubectl -n {{ns}} logs deploy/alert-receiver | jq -c 'select(.event | startswith("alert.")) | {ts, event, alertname, severity, summary}'

# --- Consumer A/B (canary) releases ------------------------------------------------

# Release the current code as a new Consumer version (automated, metric-gated cutover)
[group('canary')]
canary: import
    kubectl argo rollouts -n {{ns}} set image consumer consumer={{image_repo}}:$(cat .build/version)
    kubectl argo rollouts -n {{ns}} get rollout consumer

# Release a known-bad Consumer config as a canary: its Sandboxes can't pull their image
[group('canary')]
canary-bad:
    kubectl -n {{ns}} patch rollout consumer --type=json \
      -p '[{"op":"add","path":"/spec/template/spec/containers/0/env","value":[{"name":"SANDBOX_IMAGE","value":"{{bad_sandbox_image}}"}]}]'
    kubectl argo rollouts -n {{ns}} get rollout consumer

# Remove the bad-config override (returns the Rollout to its stable spec)
[group('canary')]
canary-reset:
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ -n "$(kubectl -n {{ns}} get rollout consumer -o jsonpath='{.spec.template.spec.containers[0].env}')" ]]; then
      kubectl -n {{ns}} patch rollout consumer --type=json -p '[{"op":"remove","path":"/spec/template/spec/containers/0/env"}]'
    else
      echo "No bad-config override to remove"
    fi

# Watch the Consumer rollout (steps, weights, canary vs stable ReplicaSets)
[group('canary')]
canary-watch:
    kubectl argo rollouts -n {{ns}} get rollout consumer --watch

# Manual override: advance the canary to its next step (e.g. after an inconclusive analysis)
[group('canary')]
canary-promote:
    kubectl argo rollouts -n {{ns}} promote consumer

# Manual override: skip remaining steps and analysis, roll the canary out to 100%
[group('canary')]
canary-promote-full:
    kubectl argo rollouts -n {{ns}} promote consumer --full

# Manual override: abort the canary, scale it down and send all traffic back to stable
[group('canary')]
canary-abort:
    kubectl argo rollouts -n {{ns}} abort consumer

# Argo Rollouts dashboard on http://localhost:3100
[group('canary')]
rollouts-ui:
    kubectl -n argo-rollouts port-forward svc/argo-rollouts-dashboard 3100:3100

# --- Chaos ---------------------------------------------------------------------

# Stop all Consumers (expect ConsumersDown, JobBacklogGrowing, JobsNotCompleting)
[group('chaos')]
chaos-consumers-down:
    kubectl -n {{ns}} scale rollout/consumer --replicas=0

# Make every Consumer's Sandboxes fail to pull their image (expect JobFailureRatioHigh)
[group('chaos')]
chaos-bad-image: canary-bad canary-promote-full

# Undo all chaos
[group('chaos')]
chaos-reset: canary-reset
    kubectl -n {{ns}} scale rollout/consumer --replicas=4
    kubectl argo rollouts -n {{ns}} promote consumer --full || true
