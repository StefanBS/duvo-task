# Sandbox Orchestration

A lightweight platform that accepts work on behalf of AI agents and runs it inside isolated, on-demand sandboxes, with tooling to reason about and act on the platform's health.

## Language

**Job**:
A unit of work requested of the platform, identified by a `jobId` and carrying a Job Type.
_Avoid_: Task, message, request

**Job Type**:
The kind of workload a Job represents (e.g. `http`); it will later determine how the Job's Sandbox is provisioned.
_Avoid_: Kind, category, runtime

**Sandbox**:
The isolated environment a Job runs in. Distinct from the Job: one Job may, over retries, be attempted in more than one Sandbox.
_Avoid_: Container, pod, VM

**Sandbox TTL**:
The maximum lifetime of a Sandbox, after which the platform stops it regardless of what is running inside.
_Avoid_: Timeout, expiry, deadline

**Sandbox URL**:
The address at which a ready Sandbox's HTTP server can be reached from inside the platform.
_Avoid_: Endpoint, address, link

**Job Queue**:
The durable, ordered backlog of Jobs waiting to be picked up.
_Avoid_: Stream, topic, bus

**Job Backlog**:
Jobs placed on the Job Queue that have not yet been completed: those not yet taken by any Consumer plus those taken but not yet acknowledged.
_Avoid_: Queue depth, lag

**Producer**:
Anything that places Jobs onto the Job Queue.
_Avoid_: Publisher, enqueuer

**Consumer**:
A worker that takes Jobs off the Job Queue, performs them, and acknowledges them once done.
_Avoid_: Worker, handler, subscriber

**Consumer Release**:
A new version of the Consumer being introduced alongside the current one, receiving a fraction of the Job Queue before it replaces it.
_Avoid_: Deployment, rollout, upgrade

**Canary**:
The Consumers running the version under test during a Consumer Release.
_Avoid_: B, new, candidate

**Cutover**:
The automated completion of a Consumer Release: the Canary replaces Stable once metrics show it is healthy, or is withdrawn if they show it is not.
_Avoid_: Promotion, go-live, switch

**Stable**:
The Consumers running the last fully released version; they take every Job the Canary does not.
_Avoid_: A, old, baseline, production
