"""Runtime configuration, read from environment variables."""

import os
import socket

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
STREAM = os.environ.get("JOB_STREAM", "jobs")
GROUP = os.environ.get("CONSUMER_GROUP", "consumers")
# Approximate cap on stream length. Trimming can drop unprocessed Jobs if the
# backlog exceeds it (documented shortcut).
STREAM_MAXLEN = int(os.environ.get("STREAM_MAXLEN", "1000"))

JOBS_PER_SECOND = float(os.environ.get("JOBS_PER_SECOND", "1"))

CONSUMER_NAME = os.environ.get("CONSUMER_NAME") or socket.gethostname()
READ_BLOCK_MS = int(os.environ.get("READ_BLOCK_MS", "2000"))
# A pending Job idle longer than this is assumed abandoned and gets reclaimed.
CLAIM_IDLE_MS = int(os.environ.get("CLAIM_IDLE_MS", "30000"))
CLAIM_INTERVAL_S = float(os.environ.get("CLAIM_INTERVAL_S", "5"))
