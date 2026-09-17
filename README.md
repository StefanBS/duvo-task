# Sandbox Orchestrator

A lightweight sandbox orchestration platform, built step by step. Domain language lives in [CONTEXT.md](CONTEXT.md); hard-to-reverse decisions in [docs/adr](docs/adr).

## Quick start

Requirements: a running k3d cluster (`k3s-default`), `podman`, `kubectl`, `uv`, `just`, `jq`.

```bash
just test     # unit tests
just deploy   # build image, import into k3d, apply manifests, restart workloads
just smoke    # end-to-end check
just logs     # tail Consumer logs (just logs producer)
just sandboxes            # list Sandbox pods/services
just curl /status/500     # hit the newest Sandbox from inside the cluster (mock errors)
just redis-cli XINFO GROUPS jobs
just down     # delete the namespace (including Redis data)
```

## Layout

```
src/orchestrator/
  job.py        Job model, Job Types, validation
  sandbox.py    Sandbox manifests + create/adopt, wait-ready, delete, reap
  producer.py   places synthetic Jobs on the Job Queue at JOBS_PER_SECOND
  consumer.py   reads, provisions a Sandbox per Job, acks; reclaims abandoned Jobs
  logging.py    JSON-lines structured logging
  config.py     env-var configuration
k8s/            Kustomize manifests (namespaces sandbox-orchestrator, sandboxes)
scripts/smoke.sh
```

## Step 1 — The basics: a Job Queue

```
Producer ──XADD──▶ Redis Stream "jobs" ──XREADGROUP──▶ Consumer ×2
                   (group "consumers")  ◀───XACK────── (after work completes)
                                        ◀─XAUTOCLAIM── (Jobs idle > 30s)
```

- **Queue:** Redis Streams with a consumer group ([ADR 0001](docs/adr/0001-redis-streams-job-queue.md)). Each stream entry holds a single `job` field containing `{"jobId": "...", "type": "http"}`.
- **Producer:** a Deployment that sends 1 Job/s by default. It creates a ULID `jobId` and picks a Job Type from `http`, `browser` and `shell`.
- **Consumers:** 2 replicas. Each takes one Job at a time and fakes the work with a type-dependent sleep. It logs `job.started`, then `job.completed` with `durationMs`.
- **Delivery is at-least-once:**
  - A Consumer acks a Job only after the work is done.
  - Every 5s, each Consumer runs `XAUTOCLAIM` to take over Jobs that have been pending for more than 30s. This covers Consumers that crashed, including restarted pods that come back with a new name.
- **Graceful shutdown:** on SIGTERM a Consumer stops reading new Jobs, finishes and acks the current one, then exits.
- **Invalid Jobs:** malformed JSON, a missing `jobId` or an unknown type are logged as `job.invalid` and acked, so they don't block the queue.
- **Redis:** a StatefulSet with a PVC and AOF persistence (`appendfsync everysec`).
- **Logs:** one JSON object per line. Every line has `event`, `service` and `instance`. Job events also have `jobId`, `type` and `entryId`.

### Verified

- `just smoke`: the workloads are Ready, both Consumers complete Jobs, and pending Jobs stay at or below 10.
- **Crash recovery:** a Consumer container was killed with SIGKILL (`crictl stop -t 0`) mid-Job. Its entry stayed pending, then after about 30s it was logged as `job.reclaimed`, re-run and acked.

### Shortcuts & tradeoffs

| Shortcut | Consequence | Would do instead |
|---|---|---|
| Single Redis, no replication | Redis is a single point of failure. AOF `everysec` can lose about 1s of Jobs. | Sentinel, or a managed Redis |
| `MAXLEN ~ 1000` trimming | If the backlog grows past about 1000, trimming can delete Jobs that were never processed | Trim by the oldest acked ID, and apply backpressure at the Producer |
| Work is simulated with `sleep` | No real sandbox yet | Later steps |
| No retry limit or dead-letter stream | A Job that crashes its Consumer is reclaimed forever. Invalid Jobs are only logged. | Check the delivery count and move the Job to a `jobs:dead` stream |
| No deduplication | A reclaimed Job can run twice | An idempotency key or a completion record per `jobId` |
| Producer drops a Job when Redis is unavailable | Jobs are lost while Redis is down (logged as `job.enqueue_failed`) | Buffer and retry, or fail loudly to the caller |
| Dead consumer names are never removed from the group | `XINFO CONSUMERS` fills up with old pod names | Periodic `XGROUP DELCONSUMER` for idle consumers with nothing pending |
| No liveness/readiness probes on Producer or Consumer | A hung process doesn't get restarted | Heartbeat-based probes (observability step) |
| Fake work durations are hard-coded per Job Type | Changing them needs a new image | Environment variables |
| Unit tests only, no integration test against a real Redis | Redis interaction is covered only by the smoke test | testcontainers |
| Image loaded with `k3d image import --mode direct` | The default import mode fails under Podman | A local registry (`k3d registry create`) |

## Step 2 — A Sandbox per Job

```
Consumer ──create Pod+Service──▶ ns "sandboxes"
   │                             sandbox-<jobid>  (podinfo, TTL 120s)
   ├──poll until Pod Ready + GET / returns hostname == sandbox name
   ├──log sandbox.ready {url}  ──▶ XACK
   └──every 30s: delete terminated Sandboxes (Services follow via ownerReferences)
```

- **What runs:** each Job gets a **Sandbox**: a bare Pod running [podinfo](https://github.com/stefanprodan/podinfo), plus a Service ([ADR 0002](docs/adr/0002-sandbox-as-bare-pod.md)). podinfo can fake failures: `/status/{code}`, `/delay/{s}`, `/panic`, and `POST /readyz/disable`. It also has health checks and `/metrics`.
- **Sandbox URL:** `http://sandbox-<jobid>.sandboxes.svc.cluster.local:9898`, logged in `sandbox.ready` together with `durationMs`. The URL only works inside the cluster.
- **When a Job completes:**
  - The Consumer waits for the Pod to be Ready.
  - It sends `GET /` to the URL and expects the `hostname` in the response to match the sandbox name, which proves the URL reaches *this* sandbox.
  - Then it acks the Job.
- **Startup timeout:** 20s, kept below the 30s claim timeout. On timeout or error, the Consumer deletes the sandbox, logs `job.failed` and acks the Job.
- **Redelivery:** a sandbox's name comes from its Job, so a Job delivered again reuses the existing sandbox (`sandbox.created` with `adopted: true`).
- **Lifetime:** `activeDeadlineSeconds: 120` sets the Sandbox TTL. Every 30s the Consumers delete stopped sandbox Pods, and each Pod's Service is deleted with it.
- **Isolation:**
  - Sandboxes run in their own namespace, as non-root with a read-only root filesystem, no capabilities, no service account token and a 64Mi memory limit.
  - A ResourceQuota allows at most 50 pods, which protects the single node's 110-pod limit.
  - The Consumer's ServiceAccount may only manage Pods and Services in `sandboxes`.
- **Capacity:** the Producer rate dropped to 0.2 Jobs/s. At about 1.5s per sandbox, 2 Consumers keep up easily, and about 25 sandboxes are alive at once (rate × TTL).

### Verified

- `just smoke`: both Consumers make sandboxes Ready (p50 about 1.5s), no Jobs fail, a sandbox URL returns its own hostname, and nothing is left pending.
- `just curl /status/503` returns a 503 from a sandbox.
- Sending a Job again with the same `jobId` logged `adopted: true`, and no second sandbox was created.
- After 2.5 minutes: 37 sandboxes had been created and about 25 were alive, with no `Failed` Pods and Services matching Pods.

### Shortcuts & tradeoffs

| Shortcut | Consequence | Would do instead |
|---|---|---|
| Every Job Type gets the same podinfo sandbox | `type` is only a label and env var | Pick the image or template per Job Type |
| Sandbox URL only works inside the cluster | No external access; use `just curl` or `kubectl port-forward` | Ingress or Gateway with a host per sandbox, plus auth |
| No NetworkPolicy | Sandboxes can reach each other and the control plane (including Redis) | Deny-all by default in `sandboxes`, allow only what's needed |
| A failed sandbox fails the Job, with no retry | A temporary API or image-pull problem loses the Job | Limited retries with backoff, then a dead-letter stream |
| Sandbox provisioning is a blocking poll, one Job at a time per Consumer | Throughput ≈ replicas ÷ provisioning time. Rate lowered to 0.2/s. | Watch-based waits and several Jobs at once per Consumer |
| A fixed TTL is the only teardown | Sandboxes can't end early or be extended, and no sandbox state is stored | Explicit lifecycle (states, teardown API, reaper) |
| Every Consumer runs the cleanup | Duplicate delete calls (harmless, since they do nothing twice) | A single controller, or leader election |
| Sandboxes use a public image pulled at runtime | The first start depends on ghcr.io being reachable | Mirror the image, or pre-pull it on the nodes |
| A dependency (the `kubernetes` client) increased image size | Bigger image, slower import | A lighter client (e.g. lightkube) |
