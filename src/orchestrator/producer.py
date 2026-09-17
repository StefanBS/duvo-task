"""Producer: places synthetic Jobs onto the Job Queue at a fixed rate."""

import random
import socket

import redis

from orchestrator import config
from orchestrator.job import JOB_TYPES, Job
from orchestrator.lifecycle import stop_event
from orchestrator.logging import setup


def main() -> None:
    log = setup("producer", socket.gethostname())
    stop = stop_event()
    r = redis.Redis.from_url(config.REDIS_URL, decode_responses=True)
    interval = 1 / config.JOBS_PER_SECOND
    log.info("producer.started", extra={"stream": config.STREAM, "jobsPerSecond": config.JOBS_PER_SECOND})

    while not stop.is_set():
        job = Job.new(random.choice(list(JOB_TYPES)))
        fields = {"jobId": job.job_id, "type": job.type}
        try:
            entry_id = r.xadd(
                config.STREAM,
                {"job": job.to_json()},
                maxlen=config.STREAM_MAXLEN,
                approximate=True,
            )
            log.info("job.enqueued", extra={**fields, "entryId": entry_id})
        except redis.RedisError:
            # Shortcut: the Job is dropped, not retried or buffered.
            log.exception("job.enqueue_failed", extra=fields)
        stop.wait(interval)

    log.info("producer.stopped")


if __name__ == "__main__":
    main()
