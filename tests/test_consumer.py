from orchestrator import config
from orchestrator.consumer import process
from orchestrator.job import Job
from orchestrator.sandbox import Sandbox, SandboxError


class FakeRedis:
    def __init__(self):
        self.acked = []

    def xack(self, stream, group, entry_id):
        self.acked.append((stream, group, entry_id))


class FakeSandboxes:
    def __init__(self, fail_with=None):
        self.fail_with = fail_with
        self.created, self.deleted = [], []

    def create(self, job):
        sandbox = Sandbox(name=f"sandbox-{job.job_id.lower()}", url="http://example")
        self.created.append(sandbox.name)
        return sandbox, False

    def wait_ready(self, sandbox, timeout_s):
        if self.fail_with:
            raise self.fail_with

    def delete(self, name):
        self.deleted.append(name)


def test_job_is_acked_once_sandbox_is_ready():
    r, sandboxes = FakeRedis(), FakeSandboxes()
    process(r, sandboxes, "1-0", {"job": Job.new("http").to_json()})
    assert r.acked == [(config.STREAM, config.GROUP, "1-0")]
    assert len(sandboxes.created) == 1 and sandboxes.deleted == []


def test_failed_sandbox_is_deleted_and_job_acked():
    r, sandboxes = FakeRedis(), FakeSandboxes(fail_with=SandboxError("not ready"))
    process(r, sandboxes, "1-0", {"job": Job.new("http").to_json()})
    assert [a[2] for a in r.acked] == ["1-0"]
    assert sandboxes.deleted == sandboxes.created


def test_invalid_job_is_acked_without_creating_a_sandbox():
    r, sandboxes = FakeRedis(), FakeSandboxes()
    process(r, sandboxes, "2-0", {"job": "garbage"})
    process(r, sandboxes, "3-0", None)  # entry trimmed from the stream while pending
    assert [a[2] for a in r.acked] == ["2-0", "3-0"]
    assert sandboxes.created == []
