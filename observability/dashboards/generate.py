"""Generate the Sandbox Orchestrator overview dashboard (dashboard-as-code).

Run: uv run python observability/dashboards/generate.py > observability/dashboards/overview.json
"""

import json

PROM = {"type": "prometheus", "uid": "prometheus"}
LOKI = {"type": "loki", "uid": "loki"}

panels: list[dict] = []
_id = 0
_y = 0


def _next_id() -> int:
    global _id
    _id += 1
    return _id


def row(title: str) -> None:
    global _y
    panels.append({"type": "row", "title": title, "id": _next_id(), "collapsed": False,
                   "gridPos": {"h": 1, "w": 24, "x": 0, "y": _y}})
    _y += 1


def targets(*exprs: tuple[str, str]) -> list[dict]:
    return [{"datasource": PROM, "expr": e, "legendFormat": legend, "refId": chr(65 + i)}
            for i, (e, legend) in enumerate(exprs)]


def stat(title, expr, x, w, unit="short", thresholds=None, desc=""):
    steps = [{"color": "green", "value": None}] + [{"color": c, "value": v} for v, c in (thresholds or [])]
    return {"type": "stat", "title": title, "description": desc, "datasource": PROM,
            "targets": targets((expr, "")),
            "fieldConfig": {"defaults": {"unit": unit, "thresholds": {"mode": "absolute", "steps": steps},
                                         "color": {"mode": "thresholds"}}, "overrides": []},
            "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background", "graphMode": "area"},
            "gridPos": {"h": 4, "w": w, "x": x, "y": _y}}


def ts(title, exprs, x, w, unit="short", h=7, stack=False, desc=""):
    return {"type": "timeseries", "title": title, "description": desc, "datasource": PROM,
            "targets": targets(*exprs),
            "fieldConfig": {"defaults": {"unit": unit, "custom": {
                "fillOpacity": 20 if stack else 5, "stacking": {"mode": "normal" if stack else "none"}}},
                "overrides": []},
            "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
            "gridPos": {"h": h, "w": w, "x": x, "y": _y}}


def logs(title, expr, x, w, h=9):
    return {"type": "logs", "title": title, "datasource": LOKI,
            "targets": [{"datasource": LOKI, "expr": expr, "refId": "A"}],
            "options": {"showTime": True, "wrapLogMessage": True, "sortOrder": "Descending", "prettifyLogMessage": False},
            "gridPos": {"h": h, "w": w, "x": x, "y": _y}}


def add(*ps, h):
    global _y
    for p in ps:
        p["id"] = _next_id()
        panels.append(p)
    _y += h


row("Health at a glance")
add(
    stat("Jobs completed / min", "orchestrator:jobs_completed:rate2m * 60", 0, 4),
    stat("Job failure ratio (5m)", "orchestrator:job_failure_ratio:rate5m", 4, 4, "percentunit",
         [(0.05, "orange"), (0.2, "red")]),
    stat("Job backlog", "orchestrator:job_backlog", 8, 4, thresholds=[(5, "orange"), (10, "red")],
         desc="Undelivered (lag) + delivered-but-unacked (pending) Jobs"),
    stat("Provisioning p95", "orchestrator:sandbox_provision_seconds:p95_5m", 12, 4, "s",
         [(5, "orange"), (10, "red")]),
    stat("Sandbox quota used", 'kube_resourcequota{namespace="sandboxes",resource="pods",type="used"} '
         '/ ignoring(type) kube_resourcequota{namespace="sandboxes",resource="pods",type="hard"}',
         16, 4, "percentunit", [(0.6, "orange"), (0.8, "red")]),
    stat("Firing alerts", 'count(ALERTS{alertstate="firing"}) or vector(0)', 20, 2,
         thresholds=[(1, "red")]),
    {**stat("Consumer rollout", 'max by (phase) (rollout_info{name="consumer"})', 22, 2),
     "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "textMode": "name", "colorMode": "none",
                 "graphMode": "none"}},
    h=4,
)

row("Producer")
add(
    ts("Jobs enqueued / s by type", [("sum by (type) (rate(orchestrator_jobs_enqueued_total[2m]))", "{{type}}")],
       0, 12, "reqps", stack=True),
    ts("Enqueue failures / s", [("sum(rate(orchestrator_job_enqueue_failures_total[2m])) or vector(0)", "failures"),
                                ('count(up{job="sandbox-orchestrator/producer"} == 1) or vector(0)', "producers up")],
       12, 12),
    h=7,
)

row("Job Queue")
add(
    ts("Backlog", [('sum(redis_stream_group_lag{stream="jobs"})', "lag (undelivered)"),
                   ('sum(redis_stream_group_messages_pending{stream="jobs"})', "pending (unacked)")], 0, 8),
    ts("Stream length", [('sum(redis_stream_length{stream="jobs"})', "entries")], 8, 8,
       desc="Capped at ~1000 by MAXLEN trimming"),
    ts("Redis", [("max(redis_up)", "up"), ("sum(redis_connected_clients)", "clients"),
                 ('sum(redis_stream_group_consumers{stream="jobs"})', "registered consumers")], 16, 8),
    h=7,
)

row("Jobs & Consumers")
add(
    ts("Jobs processed / s by outcome",
       [("sum by (outcome) (rate(orchestrator_jobs_processed_total[2m]))", "{{outcome}}")], 0, 8, "reqps", stack=True),
    ts("Sandbox provisioning latency", [
        (f'histogram_quantile({q}, sum by (le) (rate(orchestrator_sandbox_provision_duration_seconds_bucket{{outcome="ready"}}[5m])))', f"p{int(q * 100)}")
        for q in (0.5, 0.95, 0.99)], 8, 8, "s"),
    ts("Consumers", [
        ("sum(orchestrator_jobs_in_progress)", "in progress"),
        ('count(up{job="sandbox-orchestrator/consumer"} == 1) or vector(0)', "consumers up"),
        ("sum(increase(orchestrator_jobs_reclaimed_total[5m]))", "reclaimed (5m)"),
        ("sum(increase(orchestrator_redis_errors_total[5m]))", "redis errors (5m)")], 16, 8),
    h=7,
)

row("Consumer rollout: canary vs stable")
add(
    ts("Consumers by track / version",
       [('count by (track, version) (orchestrator_build_info{role="consumer"})', "{{track}} {{version}}")],
       0, 6, stack=True, desc="Pod labels set by the Rollout; canary share of Jobs follows canary share of replicas"),
    ts("Share of Jobs processed by track",
       [("sum by (track) (rate(orchestrator_jobs_processed_total[5m])) / scalar(sum(rate(orchestrator_jobs_processed_total[5m])))",
         "{{track}}")], 6, 6, "percentunit", stack=True),
    ts("Job failure ratio by track",
       [('sum by (track) (rate(orchestrator_jobs_processed_total{outcome=~"failed|invalid"}[5m])) '
         "/ sum by (track) (rate(orchestrator_jobs_processed_total[5m]))", "{{track}}")], 12, 6, "percentunit"),
    ts("Provisioning p95 by track",
       [('histogram_quantile(0.95, sum by (track, le) (rate(orchestrator_sandbox_provision_duration_seconds_bucket{outcome="ready"}[5m])))',
         "{{track}}")], 18, 6, "s"),
    h=7,
)

row("Automated cutover: canary analysis inputs & results")
add(
    ts("Analysis input: Jobs processed per revision (1m)",
       [("orchestrator:jobs_processed_by_revision:increase1m", "{{revision}}")], 0, 6,
       desc="canary-sample-size passes at >= 8 for the canary revision"),
    ts("Analysis input: failure ratio per revision (1m)",
       [("orchestrator:job_failure_ratio_by_revision:1m", "{{revision}}")], 6, 6, "percentunit",
       desc="canary-failure-ratio-vs-stable fails when canary - stable > 5pp with >= 3 canary failures"),
    ts("Analysis metric results",
       [('max by (metric, phase) (analysis_run_metric_phase{exported_namespace="sandbox-orchestrator"} == 1)',
         "{{metric}}: {{phase}}")], 12, 6),
    ts("Rollout phase",
       [('max by (phase) (rollout_info{name="consumer"} == 1)', "{{phase}}")], 18, 6, stack=True),
    h=7,
)

row("Sandbox lifecycle")
add(
    ts("Sandbox pods by phase",
       [('sum by (phase) (kube_pod_status_phase{namespace="sandboxes"} == 1)', "{{phase}}")], 0, 8, stack=True),
    ts("Sandboxes created vs reaped / min", [
        ("sum(rate(orchestrator_sandbox_provision_duration_seconds_count[5m])) * 60", "created"),
        ("sum(rate(orchestrator_sandboxes_reaped_total[5m])) * 60", "reaped"),
        ("sum(increase(orchestrator_sandbox_reap_failures_total[5m]))", "reap failures (5m)")], 8, 8),
    ts("Sandbox containers waiting (by reason)",
       [('sum by (reason) (kube_pod_container_status_waiting_reason{namespace="sandboxes"})', "{{reason}}")],
       16, 8, desc="ImagePullBackOff / ErrImagePull / CrashLoopBackOff show up here"),
    h=7,
)

row("Logs")
add(
    logs("Errors, failed Jobs & alerts",
         '{namespace="sandbox-orchestrator", level=~"error|warning"} | json | line_format '
         '"{{.event}} {{.alertname}} {{.jobId}} {{.error}}{{.summary}}"', 0, 12),
    logs("Trace a Job (set jobId above)",
         '{namespace="sandbox-orchestrator"} |= "$jobId" | json | line_format "{{.instance}} {{.event}} {{.url}}{{.error}}"',
         12, 12),
    h=9,
)

dashboard = {
    "uid": "sandbox-orchestrator",
    "title": "Sandbox Orchestrator — Overview",
    "tags": ["sandbox-orchestrator"],
    "timezone": "browser",
    "schemaVersion": 39,
    "refresh": "30s",
    "time": {"from": "now-1h", "to": "now"},
    "templating": {"list": [{"type": "textbox", "name": "jobId", "label": "jobId", "query": ""}]},
    "panels": panels,
}
print(json.dumps(dashboard, indent=2))
