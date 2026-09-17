# Sandbox Orchestrator

A lightweight sandbox orchestration platform, built step by step. Domain language lives in [CONTEXT.md](CONTEXT.md); hard-to-reverse decisions in [docs/adr](docs/adr).

## Quick start

Requirements: a running k3d cluster (`k3s-default`), `podman`, `kubectl`, the [`kubectl argo rollouts`](https://argoproj.github.io/argo-rollouts/installation/#kubectl-plugin-installation) plugin, `helm`, `uv`, `just`, `jq`.

```bash
just test     # unit tests
just rollouts-up  # Argo Rollouts controller + dashboard (the Consumer is a Rollout)
just deploy   # build a versioned image, import into k3d, apply manifests (Consumer changes go out as a canary)
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
rollouts/       Argo Rollouts Helm values
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

## Step 4 — A/B deployments of the Consumer

```
                         consumer group "consumers" on stream "jobs"
Producer ──XADD──▶ jobs ─┬──▶ stable  consumer ×3   (track=stable, version A)
                         └──▶ canary  consumer ×1   (track=canary, version B)   ← 25% step
```

```bash
just canary        # build + import the current code as a new version → canary at 25%, paused
just rollout       # watch steps, weights and ReplicaSets
just promote       # 25% → 50% (paused) → 100%
just promote-full  # skip the remaining steps
just abort         # scale the canary down; stable takes all Jobs again
just canary-bad    # release a known-bad config as a canary (its sandboxes can't pull their image)
just canary-reset  # drop that override; the Rollout goes back to its stable spec
just rollouts-ui   # Argo Rollouts dashboard on http://localhost:3100
```

**How a fraction of the queue reaches the new version.** The Consumer is an Argo Rollouts `Rollout` with a canary strategy and **no traffic router** ([ADR 0003](docs/adr/0003-canary-split-by-consumer-replicas.md)):
- Stable and canary Consumers compete for Jobs in the *same* consumer group, so the canary's share of Jobs is roughly its share of replicas.
- With 4 replicas the steps are `setWeight: 25` → pause → `setWeight: 50` → pause → 100%. Each step is a Pod count (1 of 4, then 2 of 4), with `maxSurge: 1` and `maxUnavailable: 0`, so capacity never drops during a release.
- Every pause is a **manual gate**: look at the dashboard, then `just promote` or `just abort`. Automatic analysis is planned for step 5.

**Telling canary from stable.**
- **Track label:** `canaryMetadata` / `stableMetadata` put a `track: canary|stable` label on the Pods, and Argo updates it when a canary is promoted. The Consumer PodMonitor copies it, plus `rollouts-pod-template-hash` as `revision`, onto every Consumer metric.
- **Version:** each image is stamped with a unique `APP_VERSION` (`<git sha>[-dirty]-<timestamp>`). It's exported as `orchestrator_build_info{version, role}` and added to every log line as `version`.
- **Dashboard:** a new row, *Consumer rollout: canary vs stable*, shows Consumers by track and version, the **actual share of Jobs processed by track**, failure ratio by track and provisioning p95 by track. A stat panel shows the Rollout phase.
- **Alert:** `RolloutAborted` fires when `rollout_info{phase=~"Abort|Degraded|Error"}` lasts 15s, so an aborted release reaches the alert receiver. The Argo Rollouts controller is scraped through its ServiceMonitor.

**Rules for canary and stable running together.** Both versions share one Job Queue and one `sandboxes` namespace, so:
- Job formats must work in both directions: the old version must read Jobs from the new one, and the new version must read Jobs from the old one.
- A canary must not change sandbox naming, labels or cleanup in ways that break stable's sandboxes. Today every Consumer's cleanup deletes *any* stopped sandbox.
- Judge a canary on **ratios**, not counts. A crashing or slow canary pulls *fewer* Jobs, so its share shrinks exactly when it's bad.

### Verified

- **Migration:** `just deploy` created the Rollout, which came up Healthy with 4 stable Pods on its first revision (the first revision skips canary steps), then deleted the old Deployment. `just smoke` passes on all 4 Consumers.
- **Canary pause:** `just canary` produced exactly **1 canary Pod plus 3 stable Pods** (`track` labels set by the Rollout), paused at `SetWeight 25 / ActualWeight 25`.
- **Metrics labels:** Prometheus scrapes all 4 Consumers with `track=stable|canary` and `revision=<pod-template-hash>`. `rollout_info{name="consumer",phase="Paused"}` is exported by the controller.
- **Bug found and fixed:** the first canary image reported the *stable* version in `orchestrator_build_info`. Podman's layer cache ignored the changed `--build-arg APP_VERSION` used by `ENV`, so both tags were the same image. `just build` now stamps the version in a separate `FROM …:build / ENV APP_VERSION=<literal>` layer, and a local build confirms the correct version.
- **Faster demo timings** (so a full canary can be observed in about 2 minutes):
  - `JOBS_PER_SECOND` 0.2 → **1**
  - `SANDBOX_TTL_S` 120 → **20**, so about 20 sandboxes are alive at once, well under the 50-pod quota
  - `SANDBOX_STARTUP_TIMEOUT_S` 20 → **10**
  - `SANDBOX_REAP_INTERVAL_S` 30 → **10**
  - `RolloutAborted` `for:` 1m → 15s
  - 4 Consumers at about 2.6s per Job handle roughly 1.5 Jobs/s.
- **Canary share and abort run** (13:27–13:32 UTC). Jobs are counted per track from Consumer logs over 30s windows:

  | Step | Pods | Jobs finished (30s) | Canary share |
  |---|---|---|---|
  | 25% | 1 canary / 3 stable | canary 7 completed, stable 23 | **23%** |
  | 50% | 2 canary / 2 stable | canary 14 completed, stable 17 | **45%** |
  | 100% (`promote-full`) | 4 stable (new version) | — | — |
  | Bad canary, 25% | 1 canary / 3 stable | canary **2 failed**, stable 27 | **7%** |

  - The good canary's share followed its replica share. The bad canary got only 7% of Jobs: each Job ties it up for the full 10s startup timeout, so it pulls fewer. That confirms canaries must be judged on ratios, not counts (ADR 0003).
  - `just abort` brought the Rollout back to stable, and `just canary-reset` returned it to Healthy with 4 stable Pods.
- **`RolloutAborted` did not fire:** the rule assumed `phase="Degraded"` and `namespace="sandbox-orchestrator"`. The controller actually exports `phase="Abort"`, and because its metrics are scraped from the controller Pod, the Rollout's namespace is in `exported_namespace`. The rule is fixed (`phase=~"Abort|Degraded|Error"`, `exported_namespace=…`) and applied, but it has **not yet been seen firing**.

### Shortcuts & tradeoffs

| Shortcut | Consequence | Would do instead |
|---|---|---|
| Traffic share = share of replicas | 25% steps only; the share is approximate and drops when the canary is slow or crashing | Producer-side split into a canary stream, with an exact percentage from Rollouts (ADR 0003) |
| Manual promote/abort gates | Needs a person watching the dashboard | `AnalysisTemplate` on canary failure ratio and latency plus global backlog (step 5) |
| Canary and stable share the sandbox cleanup | A buggy canary could delete stable's sandboxes | Label sandboxes with the creating `track`/`revision` and reap only your own |
| `canary-bad` patches the live Rollout | `just apply` won't remove that override; `just canary-reset` must | Bad configs as a proper overlay or release, not a live patch |
| `just deploy` gives each build a new version | Every deploy starts a canary, even when the Consumer code didn't change | Tag by content hash so unchanged code isn't released again |
| Argo Rollouts controller runs as 1 replica; dashboard can make changes | If the controller is down, rollouts don't progress; anyone with port-forward access can promote/abort | HA controller with leader election; a read-only dashboard behind SSO |
| Converting the Consumer deletes the old Deployment right after applying the Rollout | Both run together for a moment (no drop in capacity), then the old Pods stop gracefully | Rollout `workloadRef` for a zero-surprise migration |

## Step 5 — Metric-based automated cutover

```
setWeight 25 ──▶ AnalysisRun ──pass──▶ setWeight 50 ──▶ AnalysisRun ──pass──▶ 100% (canary becomes stable)
                     │                                       │
                     ├─fail─────────▶ abort: canary scaled down, stable takes all Jobs, RolloutAborted
                     └─inconclusive─▶ pause for a human (RolloutPaused after 2m)
```

The manual pauses from step 4 are replaced by **analysis steps** (`k8s/analysis.yaml`, [ADR 0004](docs/adr/0004-cutover-gated-on-relative-failure-ratio.md)):
- Each step runs the `consumer-canary-health` AnalysisTemplate against Prometheus: an initial delay of 30s, then 3 measurements 20s apart, over `[1m]` windows.
- Metrics are selected by **revision** (pod-template hash, passed in as `canary-hash` / `stable-hash`), not by `track`, because `track` gets relabelled on promotion and Prometheus sees that late.
- The analysis reuses step 3's metrics through recording rules (`orchestrator:jobs_processed_by_revision:increase1m`, `…jobs_failed_by_revision…`, `…job_failure_ratio_by_revision:1m`). The dashboard's *Automated cutover* row plots the same series, so a person watching sees the numbers the controller acts on.

| Metric | Pass | Fail | Otherwise |
|---|---|---|---|
| `canary-sample-size`: canary Jobs processed (1m) | ≥ 8 | never | inconclusive: not enough signal yet |
| `canary-failure-ratio-vs-stable`: canary ratio − stable ratio, counted only once the canary has ≥ 3 failures | ≤ 5pp | > 5pp (one failed measurement aborts) | — |
| `canary-provisioning-p95` (Jobs that reached ready) | ≤ 7.5s | > 9.5s | inconclusive (7.5–9.5s or no data) |

Why these choices:
- **Relative to stable:** platform-wide failures (like step 3's quota 403s) don't roll back a healthy canary.
- **Minimum 3 failures:** a bad canary pulls few Jobs (7% at a 25% step in step 4), so it can still **fail fast** without having enough samples to *pass*.
- **Inconclusive pauses instead of aborting:** a paused canary is still limited to its current small share.

Human overrides still work: `just promote-full`, `just abort`. `just canary` now releases and cuts over on its own.

### Verified

- **Bad canary, automatic abort** (`just canary-bad` at 13:36:49 UTC):
  - At the 25% step, `canary-failure-ratio-vs-stable` measured `0 → 0 → 1.0`. By the third measurement the canary had ≥ 3 failed Jobs (100%) against stable's 0%.
  - The Rollout **aborted itself at 13:38:00, 71s after release**, with *"Metric canary-failure-ratio-vs-stable assessed Failed due to failed (1) > failureLimit (0)"*.
  - `canary-sample-size` stayed inconclusive (0 → 1.8 → 3.1 Jobs/min): the bad canary was under-sampled, as expected, and the minimum-failures rule is what caught it.
  - `RolloutAborted` reached the alert receiver at 13:38:47. That confirms step 4's rule fix.
- **Good canary, automatic cutover** (released 13:38:56 UTC):
  - 25% → analysis **Successful** (sample `Inconclusive 0 → Inconclusive 7.8 → Successful 15.8`, failure delta 0, p95 about 4.9s).
  - 50% → analysis **Successful** (sample 23–33, failure delta 0, p95 about 4.8–4.95s).
  - 100% Healthy **138s after release**, with no human action. All 4 Consumers run the new revision.
- **Bugs found and fixed:**
  - My first test script read `Healthy` before the controller had seen the new revision, and claimed a cutover that never happened. It now waits for `Progressing` before timing.
  - The first p95 measurement **errored** (`reflect: slice index out of range`), because `histogram_quantile` returns no series before the canary's first sample. The query now ends in `or vector(NaN)`, which counts as no data (inconclusive).
- **Caveat:** in the aborted run, `canary-provisioning-p95` shows `Successful` although all of its measurements were `Inconclusive`. Argo stopped the run once another metric failed, before p95 exceeded its inconclusive limit, so that label says nothing about latency.

- **p95 gate retuned from evidence:**
  - The first good cutover passed with p95 at 4.8–4.95s, just under the original ≤ 5s threshold.
  - Over 5 minutes at 1 Job/s, 92% of sandboxes were ready within 5s and 96% within 7.5s, so the real p95 was **5–7.5s**, and normal load would often have paused healthy releases as inconclusive.
  - The thresholds are now **≤ 7.5s pass, > 9.5s fail**. Both sit on histogram bucket edges, and a sandbox can't take longer than the 10s startup timeout to become ready.
- **Re-checked with the new gate** (released 13:43:57 UTC):
  - 25% analysis **Successful**: sample 6.3 → 11.6 → 21.2, failure delta 0, p95 **7.0s → 6.4s** → 4.7s.
  - 50% analysis **Successful**: sample 25.8–30.6, failure delta 0, p95 3.5–4.5s.
  - **Healthy 148s after release**, with no human action.
  - Those first two p95 measurements (7.0s, 6.4s) would have been inconclusive under the old ≤ 5s gate, and this healthy release would have paused.

### Shortcuts & tradeoffs

| Shortcut | Consequence | Would do instead |
|---|---|---|
| Short windows (1m) and 3 measurements per step | Fast demo, but few samples; noisy at low traffic | Longer windows, or sequential tests sized to traffic |
| Fixed thresholds (5pp, 8 Jobs, p95 seconds) | Must be retuned when traffic or latency changes | Compare canary latency to stable too; SLO-derived thresholds |
| p95 comes from histogram buckets (3s, 5s, 7.5s) | Values in a bucket are interpolated; thresholds near bucket edges are coarse | Finer buckets around the target, or native histograms |
| A regression that hits canary *and* stable (e.g. canary overloads the shared Redis) isn't caught by the relative check | Could promote a canary that harms everyone | Add global guardrails to the analysis (backlog growth, `JobsNotCompleting`) |
| The p95 slow-canary scenario wasn't tested | That path is only reviewed, not exercised | A fault-injection flag in the Consumer (e.g. a provisioning delay) |
| Analysis Prometheus address is hard-coded | Tied to this monitoring install | Pass it in via an argument / ClusterAnalysisTemplate |

## Personal notes

- **Timebox:** I finished and committed Step 3 at around the 1-hour mark (commit `8447689`). If you want to be strict about the timebox, you can evaluate up to that point. I kept recording and finished Steps 4 and 5 anyway; the full video is 01:44:00. That time includes all the testing between steps, which took a while.
- **Testing:** I didn't have time to test everything manually. I did check the dashboards and the canary deployments in Kubernetes myself, and everything seemed to work. The rest was checked through the scripted runs recorded in the "Verified" sections above.
- **Python typing:** I would have added a typing library to the Python application, with static type checking.
- **CI/CD:** there's no CI/CD pipeline. I would have added:
  - pipelines to build and push the container image
  - vulnerability reports that run periodically
  - unit tests and pre-commit checks as gates
- **Preparation:** I should have had the monitoring stack ready beforehand, not just the k3d cluster, and I didn't.
