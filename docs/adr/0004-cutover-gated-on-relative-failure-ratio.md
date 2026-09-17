# Cutover is gated on canary-vs-stable failure ratio, with a minimum-failures rule

Automated Cutover advances a Consumer Release only when an Argo Rollouts analysis over Prometheus passes three checks for the Canary revision: enough Jobs processed to judge (≥ 8 per minute, else *inconclusive* → pause), failure ratio no more than 5 percentage points above Stable's, and Sandbox provisioning p95 ≤ 7.5s (> 9.5s fails; thresholds on histogram bucket edges, since measured p95 at 1 Job/s is 5–7.5s). A single failed measurement aborts the release.

## Why relative, and why a minimum-failures rule

- **Relative to Stable:** platform-wide failures (e.g. the sandbox quota 403s seen in step 3) hit every version equally; an absolute threshold would blame and roll back a healthy Canary.
- **Minimum 3 failures instead of a minimum sample to fail:** Canary share is set by replica count and *shrinks* when the Canary is slow or failing (ADR 0003; a bad canary got 7% of Jobs in step 4). Requiring a large sample before failing would make bad Canaries inconclusive; requiring ≥ 3 failures lets them fail fast while one unlucky failure can't.
- **Inconclusive pauses rather than aborts:** not knowing isn't evidence of harm, and a paused Canary is still capped at its current small share. `RolloutPaused` alerts if that lasts > 2m.

## Consequences

- Metrics are selected by revision (pod-template hash) passed as analysis args, not by `track`, because `track` is relabelled on promotion and Prometheus sees that late.
- Windows are 1m with 3 measurements per step for the demo; real traffic would warrant longer windows and more measurements.
- A regression that affects Canary and Stable equally (e.g. shared Redis overload caused by the Canary) is not caught by the relative check.
