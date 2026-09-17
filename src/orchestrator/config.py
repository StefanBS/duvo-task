"""Runtime configuration, read from environment variables."""

import os
import socket

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
STREAM = os.environ.get("JOB_STREAM", "jobs")
GROUP = os.environ.get("CONSUMER_GROUP", "consumers")
# Approximate cap on stream length. Trimming can drop unprocessed Jobs if the
# backlog exceeds it (documented shortcut).
STREAM_MAXLEN = int(os.environ.get("STREAM_MAXLEN", "1000"))

JOBS_PER_SECOND = float(os.environ.get("JOBS_PER_SECOND", "0.2"))

CONSUMER_NAME = os.environ.get("CONSUMER_NAME") or socket.gethostname()
READ_BLOCK_MS = int(os.environ.get("READ_BLOCK_MS", "2000"))
# A pending Job idle longer than this is assumed abandoned and gets reclaimed.
CLAIM_IDLE_MS = int(os.environ.get("CLAIM_IDLE_MS", "30000"))
CLAIM_INTERVAL_S = float(os.environ.get("CLAIM_INTERVAL_S", "5"))

SANDBOX_NAMESPACE = os.environ.get("SANDBOX_NAMESPACE", "sandboxes")
SANDBOX_IMAGE = os.environ.get("SANDBOX_IMAGE", "ghcr.io/stefanprodan/podinfo:6.9.2")
SANDBOX_PORT = int(os.environ.get("SANDBOX_PORT", "9898"))
# Kubernetes stops the Sandbox after this long (activeDeadlineSeconds).
SANDBOX_TTL_S = int(os.environ.get("SANDBOX_TTL_S", "120"))
# Must stay below CLAIM_IDLE_MS, or a Job still provisioning gets reclaimed.
SANDBOX_STARTUP_TIMEOUT_S = float(os.environ.get("SANDBOX_STARTUP_TIMEOUT_S", "20"))
SANDBOX_REAP_INTERVAL_S = float(os.environ.get("SANDBOX_REAP_INTERVAL_S", "30"))

METRICS_PORT = int(os.environ.get("METRICS_PORT", "9000"))
ALERT_RECEIVER_LISTEN_PORT = int(os.environ.get("ALERT_RECEIVER_LISTEN_PORT", "8080"))
