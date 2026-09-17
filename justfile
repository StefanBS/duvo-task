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

# Tear everything down (including Redis data)
down:
    kubectl delete namespace {{ns}} --ignore-not-found
