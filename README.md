# Sandbox Orchestrator

A lightweight sandbox orchestration platform, built step by step. Domain language lives in [CONTEXT.md](CONTEXT.md); hard-to-reverse decisions in [docs/adr](docs/adr).

## Quick start

Requirements: a running k3d cluster (`k3s-default`), `podman`, `kubectl`, `uv`, `just`, `jq`.

```bash
just test     # unit tests
just deploy   # build image, import into k3d, apply manifests, restart workloads
just smoke    # end-to-end check
just logs     # tail Consumer logs (just logs producer)
just redis-cli XINFO GROUPS jobs
just down     # delete the namespace (including Redis data)
```

## Layout

```
src/orchestrator/
  job.py        Job model, Job Types, validation
  producer.py   places synthetic Jobs on the Job Queue at JOBS_PER_SECOND
  consumer.py   reads, performs (simulated), acks; reclaims abandoned Jobs
  logging.py    JSON-lines structured logging
  config.py     env-var configuration
k8s/            Kustomize manifests (namespace sandbox-orchestrator)
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
