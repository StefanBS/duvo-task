from orchestrator import config
from orchestrator.consumer import process
from orchestrator.job import Job


class FakeRedis:
    def __init__(self):
        self.acked = []

    def xack(self, stream, group, entry_id):
        self.acked.append((stream, group, entry_id))


def test_valid_job_is_acked_after_work(monkeypatch):
    monkeypatch.setattr("orchestrator.consumer.time.sleep", lambda _: None)
    r = FakeRedis()
    process(r, "1-0", {"job": Job.new("http").to_json()})
    assert r.acked == [(config.STREAM, config.GROUP, "1-0")]


def test_invalid_job_is_acked_so_it_does_not_block_the_queue():
    r = FakeRedis()
    process(r, "2-0", {"job": "garbage"})
    process(r, "3-0", None)  # entry trimmed from the stream while pending
    assert [a[2] for a in r.acked] == ["2-0", "3-0"]
