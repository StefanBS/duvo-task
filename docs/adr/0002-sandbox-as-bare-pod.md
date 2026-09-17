# A Sandbox is a bare Pod + Service, named after its Job

Each Sandbox is a bare Pod (`restartPolicy: Never`, `activeDeadlineSeconds` = Sandbox TTL) plus a ClusterIP Service owned by that Pod, in a dedicated `sandboxes` namespace. The Pod name is derived from the `jobId` (`sandbox-<jobid lowercased>`), so an at-least-once redelivery adopts the existing Sandbox instead of creating a duplicate.

## Considered Options

- **Deployment / k8s Job per Sandbox** — adds controllers that silently restart or reschedule, which hides Sandbox failures and blurs "one Job, one Sandbox".
- **Pod IP as the URL, no Service** — one fewer object, but less readable URLs and nothing stable to point at by name.
- **Random Sandbox names** — simpler, but every redelivered Job would leak a second Sandbox.

## Consequences

- Nothing restarts a Sandbox; a crashed Sandbox stays dead (by design, for now).
- Pods stopped by the TTL linger as `Failed`, so the Consumer periodically deletes terminated Sandboxes; Services follow via ownerReferences.
- The Consumer needs RBAC to create/delete Pods and Services in `sandboxes`.
