# Sandbox Orchestrator

A lightweight sandbox orchestration platform, built step by step. Domain language lives in [CONTEXT.md](CONTEXT.md); hard-to-reverse decisions in [docs/adr](docs/adr).

## Quick start

Requirements: a running k3d cluster (`k3s-default`), `podman`, `kubectl`, `helm`, `uv`, `just`, `jq`.

```bash
just test     # unit tests
just deploy   # build image, import into k3d, apply manifests, restart workloads
just obs-up   # observability stack (after deploy: our PodMonitors/rules live in its namespace)
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
  metrics.py    Prometheus metrics (served on :9000/metrics)
  alert_receiver.py  Alertmanager webhook target that logs alerts
  logging.py    JSON-lines structured logging
  config.py     env-var configuration
k8s/            Kustomize manifests (namespaces sandbox-orchestrator, sandboxes)
observability/  Helm values (kube-prometheus-stack, Loki, Alloy), PodMonitors,
                alert rules, dashboard generator
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

## Step 3 — Logs, metrics, dashboard, alerting

```
producer :9000/metrics ─┐
consumer :9000/metrics ─┤
redis_exporter :9121 ───┼──▶ Prometheus ──rules──▶ Alertmanager ──webhook──▶ alert-receiver ─┐
kube-state-metrics ─────┘        │                                                          │ logs
                                 ▼                                                          ▼
                              Grafana ◀──────────────────────── Loki ◀── Alloy (pod logs: sandbox-orchestrator, sandboxes)
```

```bash
just obs-up          # helm install kube-prometheus-stack, loki, alloy + our monitors/rules/dashboard
just grafana         # http://localhost:3000 (admin/admin) → "Sandbox Orchestrator — Overview"
just prometheus      # http://localhost:9090
just alertmanager    # http://localhost:9093
just alerts          # alerts as delivered to the alert receiver
just chaos-consumers-down | chaos-bad-image | chaos-reset
```

**Stack** (`observability/`, namespace `monitoring`):
- **kube-prometheus-stack:** Prometheus, Alertmanager, Grafana and kube-state-metrics. It's trimmed: no node-exporter, no control-plane scraping, no default rules or dashboards.
- **Loki:** single binary, filesystem storage.
- **Alloy:** tails Pod logs through the Kubernetes API.
- **Storage:** Prometheus and Loki keep 24h of data on `local-path` PVCs.

**What is measured**

| Area | Source | Signals |
|---|---|---|
| Producer | app metrics | `orchestrator_jobs_enqueued_total{type}`, `orchestrator_job_enqueue_failures_total`, `up` |
| Job Queue | `redis_exporter` sidecar (`--check-streams=jobs`) | `redis_stream_group_lag` (undelivered), `redis_stream_group_messages_pending` (unacked), stream length, consumers, `redis_up` |
| Jobs / Consumers | app metrics | `orchestrator_jobs_processed_total{type,outcome}`, `orchestrator_sandbox_provision_duration_seconds{type,outcome}`, `jobs_in_progress`, `jobs_reclaimed_total`, `redis_errors_total` |
| Sandbox lifecycle | kube-state-metrics + app metrics | Pods by phase, container waiting reasons (e.g. `ErrImagePull`), quota used vs hard, `sandboxes_reaped_total`, `sandbox_reap_failures_total` |
| Logs | Alloy → Loki | Every JSON log line. Labels: `namespace`, `app`, `pod`, `container`, `level`. `jobId`/`event`/`url` stay in the body and are extracted with `\| json` |

The queue is measured by the exporter, not by the Consumers, so "all Consumers dead" shows up as a growing backlog rather than as missing data. Metric labels never include `jobId` or sandbox names; per-Job questions go to Loki:

```logql
{namespace="sandbox-orchestrator"} |= "01M2QP5H9CYQ2RNSRSC3YXQ9VA" | json
sum by (event) (count_over_time({app="consumer"} | json [5m]))
```

**Dashboard.** *Sandbox Orchestrator — Overview* is generated from `observability/dashboards/generate.py` and loaded by Grafana's dashboard sidecar. Rows:
1. **Health at a glance:** Jobs completed per minute, failure ratio, backlog, provisioning p95, quota used, firing alerts.
2. **Producer**
3. **Job Queue**
4. **Jobs & Consumers**
5. **Sandbox lifecycle**
6. **Logs:** errors, failed Jobs and alerts, plus a "trace a jobId" panel driven by a dashboard variable.

**Alerts** (`observability/k8s/alerts.yaml`). They're symptom-based, built on recording rules, and each has a short `for:` so it can be demoed. Alertmanager sends them to a webhook `alert-receiver`, which logs each one as JSON, so alerts show up in Loki and on the dashboard.

| Alert | Fires when | Severity |
|---|---|---|
| `JobBacklogGrowing` | backlog (lag + pending) > 10 for 2m | warning |
| `JobsNotCompleting` | Jobs are being enqueued but the completion rate has been 0 for 3m | critical |
| `JobFailureRatioHigh` | > 20% of Jobs failed (5m window), for 2m | critical |
| `SandboxProvisioningSlow` | p95 time to ready > 10s for 5m (the startup timeout is 20s) | warning |
| `SandboxQuotaNearlyExhausted` | sandbox pods > 80% of quota for 5m | warning |
| `ProducerDown` / `ConsumersDown` / `RedisDown` | no healthy target for 1m | critical |

### Verified

- **Scrape targets:** all 4 are `up` (producer, 2 consumers, redis-exporter). All 8 alert rules and 5 recording rules load with `health=ok`.
- **Dashboard:** it loads in Grafana, and every Prometheus panel query returns data through Grafana's datasource proxy. The one exception is the "waiting containers" panel, which is empty because no container is waiting.
- **Loki:** it receives logs from `producer`, `consumer`, `redis`, `sandbox` and `alert-receiver`. `sum by (event) (count_over_time({app="consumer"} | json [10m]))` shows the full lifecycle: `job.started`, `sandbox.created`, `sandbox.ready`, `job.completed` and `sandbox.reaped`.
- **Baseline:** failure ratio 0, backlog 0, provisioning p95 about 3s, about 25 of 50 quota pods in use.
- **Chaos scenario 1** (`just chaos-consumers-down` at 13:06:27 UTC). Every alert went through Alertmanager to `alert-receiver` and was logged:

  | Alert | Received at | After chaos started |
  |---|---|---|
  | `ConsumersDown` | 13:08:23 | 1m56s |
  | `JobBacklogGrowing` | 13:09:53 | 3m26s |
  | `JobsNotCompleting` | 13:11:38 | 5m11s |

  Each delay is the rule's `for:`, plus scrape and evaluation intervals, Alertmanager's `group_wait`, and (for the backlog alert) about 50s for the backlog to pass 10 at 0.2 Jobs/s.
  Recovery: after `just chaos-reset` (13:11:40), `ConsumersDown` resolved at 13:12:23 and `JobBacklogGrowing` at 13:13:53.
- **Recovery surge caused failures (found by `JobFailureRatioHigh`):**
  - When the Consumers came back they drained the backlog of about 70 Jobs as fast as they could. That burst of sandboxes hit the **50-pod sandbox ResourceQuota**.
  - **38 Jobs failed** with `403 exceeded quota` between 13:12 and 13:13. `JobFailureRatioHigh` went pending at 13:12:58 (ratio about 48%) and was received at 13:15:08.
  - The alert did its job. The root cause is that the Consumer treats "quota full" as a permanent failure instead of backing off, so a Consumer outage turns into a wave of failed Jobs on recovery.
  - Fix, not done yet: leave the Job unacked (or requeue it with backoff) on a quota 403.
- **Scenario 2** (`just chaos-bad-image` at 13:14:03): Jobs failed as expected (`not ready after 20.0s: phase=Pending waiting=ImagePullBackOff`). Its effect on the alert overlapped with the recovery surge above, so this scenario has **not** been shown to trigger an alert on its own.
- **Not yet seen firing:** `SandboxProvisioningSlow`, `SandboxQuotaNearlyExhausted`, `ProducerDown`, `RedisDown`.

### Shortcuts & tradeoffs

| Shortcut | Consequence | Would do instead |
|---|---|---|
| Alerts go to a webhook that only logs them | Nobody gets paged | PagerDuty or Slack receivers, routed by severity, with runbook links in annotations |
| Short `for:` durations and fixed thresholds | Could be noisy in production | Tune on real traffic; SLO burn-rate alerts (e.g. on the Job success ratio) |
| Everything runs single-replica (Prometheus, Loki, Alertmanager, Grafana) | Monitoring goes down with the node | HA pairs, remote storage, or a managed backend (Grafana Cloud, Mimir) |
| The monitoring stack runs inside the cluster it watches | A cluster outage also blinds us | An external uptime or "dead man's switch" check (e.g. Watchdog → healthchecks.io) |
| Grafana uses `admin/admin` and is reachable only by port-forward | Not something to share | SSO and an Ingress |
| No logs collected from `monitoring` / `kube-system` | The monitoring stack can't be debugged from Loki | Collect them, with a shorter retention |
| No kubelet or cAdvisor metrics | No CPU or memory usage per sandbox | Turn on the kubelet ServiceMonitor |
| No tracing | Latency inside a Job isn't broken down beyond its log timestamps | OpenTelemetry spans around create / wait-ready |
| Our PodMonitors and rules live in `sandbox-orchestrator`, but are applied by `just obs-up` | `obs-up` fails if the app hasn't been deployed first | One chart or Kustomize overlay that owns the ordering |
