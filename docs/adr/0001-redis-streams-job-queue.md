# Redis Streams as the Job Queue

We use Redis Streams with a consumer group as the Job Queue, delivering Jobs at-least-once (acknowledge only after the work is done). It is the smallest option that still gives explicit acks, a pending-entries list for redelivering Jobs from crashed Consumers, and cheap queue-depth/lag signals for later observability work.

## Considered Options

- **Postgres with `FOR UPDATE SKIP LOCKED`** — queryable Job state, but we would hand-roll claim/visibility-timeout logic.
- **RabbitMQ / NATS JetStream** — native redelivery and dead-lettering, but more to operate and explain than this system needs.
- **In-process queue** — no durability or cross-process delivery; can't demonstrate any reliability property.

## Consequences

- Consumers must tolerate processing the same Job more than once.
- A single, non-replicated Redis is a single point of failure (accepted shortcut).
