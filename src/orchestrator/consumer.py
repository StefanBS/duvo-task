"""Consumer: takes Jobs off the Job Queue, performs them, then acknowledges.

Delivery is at-least-once: a Job is acked only after its work completes. Jobs
left pending by a Consumer that died are reclaimed via XAUTOCLAIM.
"""

import logging
import random
import time

import redis

from orchestrator import config
from orchestrator.job import JOB_TYPES, InvalidJob, Job
from orchestrator.lifecycle import stop_event
from orchestrator.logging import setup

log = logging.getLogger("consumer")


def ensure_group(r: redis.Redis) -> None:
    try:
        r.xgroup_create(config.STREAM, config.GROUP, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def process(r: redis.Redis, entry_id: str, fields: dict | None) -> None:
    try:
        job = Job.from_json((fields or {}).get("job"))
    except InvalidJob as e:
        # Ack so a poison message doesn't block the queue. Shortcut: no dead-letter stream.
        log.error("job.invalid", extra={"entryId": entry_id, "raw": fields, "error": str(e)})
        r.xack(config.STREAM, config.GROUP, entry_id)
        return

    ctx = {"jobId": job.job_id, "type": job.type, "entryId": entry_id}
    log.info("job.started", extra=ctx)
    started = time.monotonic()
    low, high = JOB_TYPES[job.type]
    time.sleep(random.uniform(low, high) / 1000)  # Shortcut: simulated work.
    r.xack(config.STREAM, config.GROUP, entry_id)
    log.info("job.completed", extra={**ctx, "durationMs": round((time.monotonic() - started) * 1000)})


def reclaim(r: redis.Redis) -> list[tuple[str, dict | None]]:
    _next, entries, *_ = r.xautoclaim(
        config.STREAM,
        config.GROUP,
        config.CONSUMER_NAME,
        min_idle_time=config.CLAIM_IDLE_MS,
        start_id="0-0",
        count=10,
    )
    for entry_id, _fields in entries:
        log.warning("job.reclaimed", extra={"entryId": entry_id})
    return entries


def main() -> None:
    global log
    log = setup("consumer", config.CONSUMER_NAME)
    stop = stop_event()
    r = redis.Redis.from_url(config.REDIS_URL, decode_responses=True)
    log.info("consumer.started", extra={"stream": config.STREAM, "group": config.GROUP})

    last_claim = 0.0
    group_ready = False
    while not stop.is_set():
        try:
            if not group_ready:
                ensure_group(r)
                group_ready = True
            entries: list[tuple[str, dict | None]] = []
            if time.monotonic() - last_claim >= config.CLAIM_INTERVAL_S:
                entries = reclaim(r)
                last_claim = time.monotonic()
            if not entries:
                resp = r.xreadgroup(
                    config.GROUP,
                    config.CONSUMER_NAME,
                    {config.STREAM: ">"},
                    count=1,
                    block=config.READ_BLOCK_MS,
                )
                entries = [e for _stream, stream_entries in resp or [] for e in stream_entries]
            for entry_id, fields in entries:
                if stop.is_set():
                    break  # Unprocessed entries stay pending and will be reclaimed.
                process(r, entry_id, fields)
        except redis.RedisError as e:
            if "NOGROUP" in str(e):
                group_ready = False  # Redis lost the group (e.g. data loss); recreate it.
            log.exception("redis.error")
            stop.wait(1)

    log.info("consumer.stopped")


if __name__ == "__main__":
    main()
