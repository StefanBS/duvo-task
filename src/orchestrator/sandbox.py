"""Sandbox: the isolated environment a Job runs in (a Pod + Service in k8s)."""

import json
import time
import urllib.request
from dataclasses import dataclass

from kubernetes import client, config as kube_config
from kubernetes.client.exceptions import ApiException

from orchestrator import config
from orchestrator.job import Job


class SandboxError(Exception):
    pass


@dataclass(frozen=True)
class Sandbox:
    name: str
    url: str


def sandbox_name(job: Job) -> str:
    # Deterministic, so a redelivered Job adopts its existing Sandbox.
    # ULIDs are 26 [0-9A-Z] chars: lowercased they are valid DNS-1123 labels.
    return f"sandbox-{job.job_id.lower()}"


def sandbox_url(name: str) -> str:
    return f"http://{name}.{config.SANDBOX_NAMESPACE}.svc.cluster.local:{config.SANDBOX_PORT}"


def labels(job: Job) -> dict[str, str]:
    return {
        "app.kubernetes.io/name": "sandbox",
        "app.kubernetes.io/managed-by": "orchestrator",
        "orchestrator/job-id": job.job_id.lower(),
        "orchestrator/job-type": job.type,
    }


def pod_manifest(job: Job) -> dict:
    name = sandbox_name(job)
    probe = lambda path: {  # noqa: E731
        "httpGet": {"path": path, "port": "http"},
        "periodSeconds": 2,
    }
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "labels": labels(job)},
        "spec": {
            "restartPolicy": "Never",
            "activeDeadlineSeconds": config.SANDBOX_TTL_S,
            "automountServiceAccountToken": False,
            "enableServiceLinks": False,
            "securityContext": {
                "runAsNonRoot": True,
                "runAsUser": 100,
                "runAsGroup": 101,
                "seccompProfile": {"type": "RuntimeDefault"},
            },
            "containers": [
                {
                    "name": "http",
                    "image": config.SANDBOX_IMAGE,
                    "command": ["./podinfo", f"--port={config.SANDBOX_PORT}", f"--ui-message=job {job.job_id}"],
                    "env": [{"name": "JOB_ID", "value": job.job_id}, {"name": "JOB_TYPE", "value": job.type}],
                    "ports": [{"name": "http", "containerPort": config.SANDBOX_PORT}],
                    "readinessProbe": probe("/readyz"),
                    "livenessProbe": probe("/healthz"),
                    "resources": {
                        "requests": {"cpu": "10m", "memory": "16Mi"},
                        "limits": {"memory": "64Mi"},
                    },
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "readOnlyRootFilesystem": True,
                        "capabilities": {"drop": ["ALL"]},
                    },
                    "volumeMounts": [{"name": "data", "mountPath": "/data"}],
                }
            ],
            "volumes": [{"name": "data", "emptyDir": {"sizeLimit": "10Mi"}}],
        },
    }


def service_manifest(job: Job, pod_uid: str) -> dict:
    name = sandbox_name(job)
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": name,
            "labels": labels(job),
            # Deleting the Pod garbage-collects the Service.
            "ownerReferences": [{"apiVersion": "v1", "kind": "Pod", "name": name, "uid": pod_uid}],
        },
        "spec": {
            "selector": {"orchestrator/job-id": job.job_id.lower()},
            "ports": [{"name": "http", "port": config.SANDBOX_PORT, "targetPort": "http"}],
        },
    }


class SandboxManager:
    def __init__(self, api: client.CoreV1Api | None = None):
        if api is None:
            try:
                kube_config.load_incluster_config()
            except kube_config.ConfigException:
                kube_config.load_kube_config()
            api = client.CoreV1Api()
        self.api = api
        self.ns = config.SANDBOX_NAMESPACE

    def create(self, job: Job) -> tuple[Sandbox, bool]:
        """Create the Sandbox, or adopt it if it already exists. Returns (sandbox, adopted)."""
        name = sandbox_name(job)
        adopted = False
        try:
            pod = self.api.create_namespaced_pod(self.ns, pod_manifest(job))
        except ApiException as e:
            if e.status != 409:
                raise
            pod = self.api.read_namespaced_pod(name, self.ns)
            adopted = True
        try:
            self.api.create_namespaced_service(self.ns, service_manifest(job, pod.metadata.uid))
        except ApiException as e:
            if e.status != 409:
                raise
        return Sandbox(name=name, url=sandbox_url(name)), adopted

    def wait_ready(self, sandbox: Sandbox, timeout_s: float) -> None:
        """Block until the Pod is Ready and its URL serves this Sandbox, or raise SandboxError."""
        deadline = time.monotonic() + timeout_s
        last_state = "unknown"
        while time.monotonic() < deadline:
            pod = self.api.read_namespaced_pod(sandbox.name, self.ns)
            phase = pod.status.phase
            if phase in ("Failed", "Succeeded"):
                raise SandboxError(f"pod terminated: phase={phase} reason={pod.status.reason}")
            last_state = _describe(pod)
            if _is_ready(pod) and _serves(sandbox):
                return
            time.sleep(0.5)
        raise SandboxError(f"not ready after {timeout_s}s: {last_state}")

    def reap_terminated(self) -> list[str]:
        """Delete Sandboxes whose Pod has stopped (e.g. TTL expired); their Services follow via ownerReferences."""
        reaped = []
        for phase in ("Failed", "Succeeded"):
            pods = self.api.list_namespaced_pod(
                self.ns,
                label_selector="app.kubernetes.io/name=sandbox",
                field_selector=f"status.phase={phase}",
            )
            for pod in pods.items:
                self.delete(pod.metadata.name)
                reaped.append(pod.metadata.name)
        return reaped

    def delete(self, name: str) -> None:
        try:
            self.api.delete_namespaced_pod(name, self.ns, grace_period_seconds=0)
        except ApiException as e:
            if e.status != 404:
                raise


def _is_ready(pod) -> bool:
    return any(c.type == "Ready" and c.status == "True" for c in pod.status.conditions or [])


def _describe(pod) -> str:
    for cs in pod.status.container_statuses or []:
        if cs.state and cs.state.waiting:
            return f"phase={pod.status.phase} waiting={cs.state.waiting.reason}"
    return f"phase={pod.status.phase} ready={_is_ready(pod)}"


def _serves(sandbox: Sandbox) -> bool:
    """The URL answers and the response comes from this Sandbox's Pod."""
    try:
        with urllib.request.urlopen(sandbox.url + "/", timeout=2) as resp:
            return resp.status == 200 and json.load(resp).get("hostname") == sandbox.name
    except (OSError, ValueError):
        return False
