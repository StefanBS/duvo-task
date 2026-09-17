import json
import logging

from orchestrator.logging import JsonFormatter


def test_extras_become_top_level_fields():
    record = logging.LogRecord("consumer", logging.INFO, __file__, 1, "job.completed", None, None)
    record.jobId = "abc123"
    record.durationMs = 42
    out = json.loads(JsonFormatter({"service": "consumer"}).format(record))
    assert out["event"] == "job.completed"
    assert out["jobId"] == "abc123"
    assert out["durationMs"] == 42
    assert out["service"] == "consumer"
    assert out["level"] == "INFO"
