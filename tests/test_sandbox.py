import re
from types import SimpleNamespace

import pytest
from kubernetes.client.exceptions import ApiException

from orchestrator import config
from orchestrator.job import Job
from orchestrator.sandbox import SandboxManager, pod_manifest, sandbox_name, service_manifest

DNS_1123_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")


def test_sandbox_name_is_deterministic_and_dns_safe():
    job = Job.new("http")
    assert sandbox_name(job) == sandbox_name(Job(job.job_id, "shell"))
    assert DNS_1123_LABEL.match(sandbox_name(job))


def test_pod_is_isolated_and_time_limited():
    spec = pod_manifest(Job.new("http"))["spec"]
    container = spec["containers"][0]
    assert spec["activeDeadlineSeconds"] == config.SANDBOX_TTL_S
    assert spec["restartPolicy"] == "Never"
    assert spec["automountServiceAccountToken"] is False
    assert spec["securityContext"]["runAsNonRoot"] is True
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    assert container["resources"]["limits"]["memory"]


def test_service_selects_only_its_pod_and_is_owned_by_it():
    job = Job.new("http")
    svc = service_manifest(job, pod_uid="uid-1")
    pod_labels = pod_manifest(job)["metadata"]["labels"]
    assert svc["spec"]["selector"].items() <= pod_labels.items()
    assert svc["metadata"]["ownerReferences"][0]["uid"] == "uid-1"


class FakeApi:
    def __init__(self, pod_exists=False):
        self.pod_exists = pod_exists
        self.services = []

    def create_namespaced_pod(self, ns, body):
        if self.pod_exists:
            raise ApiException(status=409)
        return SimpleNamespace(metadata=SimpleNamespace(uid="new-uid"))

    def read_namespaced_pod(self, name, ns):
        return SimpleNamespace(metadata=SimpleNamespace(uid="existing-uid"))

    def create_namespaced_service(self, ns, body):
        self.services.append(body)


@pytest.mark.parametrize("pod_exists, adopted, uid", [(False, False, "new-uid"), (True, True, "existing-uid")])
def test_create_adopts_existing_sandbox_on_redelivery(pod_exists, adopted, uid):
    api = FakeApi(pod_exists=pod_exists)
    sandbox, was_adopted = SandboxManager(api).create(Job.new("http"))
    assert was_adopted is adopted
    assert api.services[0]["metadata"]["ownerReferences"][0]["uid"] == uid
    assert sandbox.url.startswith(f"http://{sandbox.name}.{config.SANDBOX_NAMESPACE}.svc")
