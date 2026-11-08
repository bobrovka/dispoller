# dispoller
An agnostic, high-performance Go background task runner utilizing PostgreSQL polling and SKIP LOCKED as a concurrent message broker.

## Documentation

- [docs/design.md](docs/design.md) — full technical design: schema (DDL), SQL operations
  (`FOR UPDATE SKIP LOCKED` claim, fencing, backoff), Go API, concurrency model,
  testing plan, trade-offs.
- [docs/spec-notes.md](docs/spec-notes.md) — design decision log (with considered
  alternatives) and a risk register.

## Usage

See the runnable example: [example/email](example/email) (Transactional Outbox:
a job enqueued in the same transaction as business data, then processed by the
engine). The schema (`schema/schema.sql`) is applied by the client's own
migrator — the library is not a migration tool.

## Development

```sh
# unit tests (no database needed)
go test -race ./...

# integration tests (docker compose brings up postgres:18)
docker compose up -d postgres
DISPOLLER_TEST_DSN=postgres://dispoller:dispoller@localhost:5433/dispoller_test \
  go test -race -tags integration -p 1 ./...
```

