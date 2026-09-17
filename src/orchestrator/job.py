"""The Job: a unit of work requested of the platform."""

import json
from dataclasses import dataclass

from ulid import ULID

# Shortcut: every Job Type currently gets the same HTTP Sandbox.
JOB_TYPES: tuple[str, ...] = ("http", "browser", "shell")


class InvalidJob(ValueError):
    pass


@dataclass(frozen=True)
class Job:
    job_id: str
    type: str

    @classmethod
    def new(cls, job_type: str) -> "Job":
        return cls(job_id=str(ULID()), type=job_type)

    def to_json(self) -> str:
        return json.dumps({"jobId": self.job_id, "type": self.type})

    @classmethod
    def from_json(cls, raw: str | None) -> "Job":
        if raw is None:
            raise InvalidJob("missing 'job' field")
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            raise InvalidJob(f"malformed JSON: {e}") from e
        if not isinstance(data, dict):
            raise InvalidJob("job must be a JSON object")
        job_id, job_type = data.get("jobId"), data.get("type")
        if not isinstance(job_id, str) or not job_id:
            raise InvalidJob("missing or invalid 'jobId'")
        if job_type not in JOB_TYPES:
            raise InvalidJob(f"unknown job type: {job_type!r}")
        return cls(job_id=job_id, type=job_type)
