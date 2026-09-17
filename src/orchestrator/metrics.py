"""Prometheus metrics. Labels stay low-cardinality: never jobId or Sandbox name."""

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from orchestrator import config
from orchestrator.job import JOB_TYPES

JOBS_ENQUEUED = Counter("orchestrator_jobs_enqueued_total", "Jobs placed on the Job Queue", ["type"])
ENQUEUE_FAILURES = Counter("orchestrator_job_enqueue_failures_total", "Jobs the Producer failed to enqueue")

JOBS_PROCESSED = Counter(
    "orchestrator_jobs_processed_total",
    "Jobs taken off the Job Queue and acked, by outcome",
    ["type", "outcome"],  # outcome: completed | failed | invalid
)
JOBS_IN_PROGRESS = Gauge("orchestrator_jobs_in_progress", "Jobs currently being processed by this Consumer")
JOBS_RECLAIMED = Counter("orchestrator_jobs_reclaimed_total", "Abandoned Jobs reclaimed from another Consumer")
SANDBOX_PROVISION_SECONDS = Histogram(
    "orchestrator_sandbox_provision_duration_seconds",
    "Time from Sandbox creation to ready (or failure)",
    ["type", "outcome"],  # outcome: ready | failed
    buckets=(0.5, 1, 1.5, 2, 3, 5, 7.5, 10, 15, 20, 30),
)
SANDBOXES_REAPED = Counter("orchestrator_sandboxes_reaped_total", "Terminated Sandboxes deleted")
SANDBOX_REAP_FAILURES = Counter("orchestrator_sandbox_reap_failures_total", "Failed reap sweeps")
REDIS_ERRORS = Counter("orchestrator_redis_errors_total", "Redis errors in the Consumer loop")


def serve(role: str) -> None:
    """Expose /metrics, pre-creating this role's labelled series at zero so rate()/ratio
    queries and alerts see them before the first event."""
    for job_type in JOB_TYPES:
        if role == "producer":
            JOBS_ENQUEUED.labels(job_type)
        elif role == "consumer":
            for outcome in ("completed", "failed"):
                JOBS_PROCESSED.labels(job_type, outcome)
            for outcome in ("ready", "failed"):
                SANDBOX_PROVISION_SECONDS.labels(job_type, outcome)
    if role == "consumer":
        JOBS_PROCESSED.labels("unknown", "invalid")
    start_http_server(config.METRICS_PORT)
