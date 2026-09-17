set shell := ["bash", "-euo", "pipefail", "-c"]

image := "localhost/orchestrator:dev"
cluster := "k3s-default"
ns := "sandbox-orchestrator"

default:
    @just --list

# Run unit tests
test:
    uv run pytest -q

# Build the container image
build:
    podman build -t {{image}} .

# Build and load the image into the k3d cluster
import: build
    mkdir -p .build
    rm -f .build/image.tar
    podman save --format docker-archive -o .build/image.tar {{image}}
    # --mode direct: the default mode spawns a tools container that fails under Podman.
    k3d image import --mode direct -c {{cluster}} .build/image.tar

# Deploy everything to k3d (rebuilds and restarts workloads)
deploy: import
    kubectl apply -k k8s
    kubectl -n {{ns}} rollout status statefulset/redis --timeout=120s
    kubectl -n {{ns}} rollout restart deployment/producer deployment/consumer
    kubectl -n {{ns}} rollout status deployment/producer --timeout=120s
    kubectl -n {{ns}} rollout status deployment/consumer --timeout=120s

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
    kubectl -n {{ns}} exec deploy/consumer -- python -c "import sys,urllib.request,urllib.error
    try: r=urllib.request.urlopen(sys.argv[1], timeout=5); print(r.status); print(r.read().decode())
    except urllib.error.HTTPError as e: print(e.code); print(e.read().decode())" "$url"

# Tear everything down (including Redis data and Sandboxes)
down:
    kubectl delete namespace {{ns}} sandboxes --ignore-not-found
