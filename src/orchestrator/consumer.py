"""Consumer: takes Jobs off the Job Queue, provisions a Sandbox for each, then acknowledges.

Delivery is at-least-once: a Job is acked only after its work completes. Jobs
left pending by a Consumer that died are reclaimed via XAUTOCLAIM.
"""

import logging
import time

import redis

from orchestrator import config
from orchestrator.job import InvalidJob, Job
from orchestrator.lifecycle import stop_event
from orchestrator.logging import setup
from orchestrator.sandbox import SandboxManager

log = logging.getLogger("consumer")


def ensure_group(r: redis.Redis) -> None:
    try:
        r.xgroup_create(config.STREAM, config.GROUP, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def process(r: redis.Redis, sandboxes: SandboxManager, entry_id: str, fields: dict | None) -> None:
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
    sandbox_name = None
    try:
        sandbox, adopted = sandboxes.create(job)
        sandbox_name = sandbox.name
        ctx["sandbox"] = sandbox.name
        log.info("sandbox.created", extra={**ctx, "adopted": adopted})
        sandboxes.wait_ready(sandbox, config.SANDBOX_STARTUP_TIMEOUT_S)
    except Exception as e:
        # Shortcut: no retries. Tear down what we created and fail the Job visibly.
        log.exception("job.failed", extra={**ctx, "error": str(e), "durationMs": _ms_since(started)})
        if sandbox_name:
            try:
                sandboxes.delete(sandbox_name)
            except Exception:
                log.exception("sandbox.delete_failed", extra=ctx)
        r.xack(config.STREAM, config.GROUP, entry_id)
        return

    log.info("sandbox.ready", extra={**ctx, "url": sandbox.url, "durationMs": _ms_since(started)})
    r.xack(config.STREAM, config.GROUP, entry_id)
    log.info("job.completed", extra={**ctx, "durationMs": _ms_since(started)})


def _ms_since(started: float) -> int:
    return round((time.monotonic() - started) * 1000)


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
    sandboxes = SandboxManager()
    log.info("consumer.started", extra={"stream": config.STREAM, "group": config.GROUP})

    last_claim = last_reap = 0.0
    group_ready = False
    while not stop.is_set():
        try:
            if not group_ready:
                ensure_group(r)
                group_ready = True
            entries: list[tuple[str, dict | None]] = []
            if time.monotonic() - last_reap >= config.SANDBOX_REAP_INTERVAL_S:
                last_reap = time.monotonic()
                try:
                    sandboxes.reap_terminated()
                except Exception:
                    log.exception("sandbox.reap_failed")
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
                process(r, sandboxes, entry_id, fields)
        except redis.RedisError as e:
            if "NOGROUP" in str(e):
                group_ready = False  # Redis lost the group (e.g. data loss); recreate it.
            log.exception("redis.error")
            stop.wait(1)

    log.info("consumer.stopped")


if __name__ == "__main__":
    main()
