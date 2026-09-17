import json

import pytest

from orchestrator.job import JOB_TYPES, InvalidJob, Job


def test_round_trip():
    job = Job.new("http")
    assert Job.from_json(job.to_json()) == job
    assert json.loads(job.to_json()) == {"jobId": job.job_id, "type": "http"}


def test_new_generates_unique_ids():
    assert len({Job.new("http").job_id for _ in range(100)}) == 100


@pytest.mark.parametrize(
    "raw",
    [
        None,
        "not json",
        "[]",
        '{"type": "http"}',
        '{"jobId": "", "type": "http"}',
        '{"jobId": 123, "type": "http"}',
        '{"jobId": "abc123"}',
        '{"jobId": "abc123", "type": "ftp"}',
    ],
)
def test_invalid_jobs_rejected(raw):
    with pytest.raises(InvalidJob):
        Job.from_json(raw)
