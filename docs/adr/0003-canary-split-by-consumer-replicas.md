# Canary Consumers take a share of Jobs by replica count, not by routing

Consumer Releases use an Argo Rollouts canary with no traffic router: Canary and Stable Consumers compete in the same Redis consumer group, so the Canary's share of Jobs is (approximately) its share of Consumer replicas. We chose this because Consumers *pull* work — there is no request path for a router to split — and it needs no application or Producer changes.

## Considered Options

- **Producer-side split** (X% of Jobs to a `jobs:canary` stream read only by Canary Consumers) — exact, replica-independent fraction, but couples the Producer to release state and adds a second stream to operate.
- **Canary self-selects by `jobId` hash** — Redis Streams have no clean way to hand a claimed entry back, so non-matching Jobs would be delayed or duplicated.
- **Experiment / blue-green** — blue-green is all-or-nothing; Experiments don't progress into a release.

## Consequences

- Granularity is 1 / replicas (4 Consumers → 25% steps).
- The share is only approximate and *self-correcting in the wrong direction*: a slow or crashing Canary pulls fewer Jobs, so it is under-sampled exactly when it is bad. Judge the Canary on ratios (failure ratio, latency), not absolute counts.
- Canary and Stable share one Job Queue and one `sandboxes` namespace, so Job formats must stay compatible in both directions and a Canary can affect Stable's Sandboxes (e.g. via reaping).
