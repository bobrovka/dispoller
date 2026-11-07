# dispoller
An agnostic, high-performance Go background task runner utilizing PostgreSQL polling and SKIP LOCKED as a concurrent message broker.

## Documentation

- [docs/design.md](docs/design.md) — full technical design: schema (DDL), SQL operations
  (`FOR UPDATE SKIP LOCKED` claim, fencing, backoff), Go API, concurrency model,
  testing plan, trade-offs.
- [docs/spec-notes.md](docs/spec-notes.md) — analysis of the original spec: contradictions,
  deviations (with rationale), open questions, and a risk register.

