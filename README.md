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

## Runner (optional)

Typical worker-process plumbing on top of the engine — a periodic purge loop
for completed jobs, an ops HTTP server (`/healthz`, `/readyz`, pprof), a
fail-fast schema check and a carefully ordered startup/shutdown — lives in the
additive subpackage [`runner`](runner). The core stays independent: building
your own loop directly on `Engine`/`Purge` remains an equivalent path.

```go
pool, err := runner.OpenPool(databaseURL, 2) // MaxConns ≥ concurrency + 2
if err != nil { ... }
defer pool.Close()

r := runner.New(pool,
    runner.WithInterval(time.Second),
    runner.WithConcurrency(2),
    runner.WithRegister(func(e *dispoller.Engine) error {
        return e.RegisterHandler("email.send", &SendEmailHandler{},
            dispoller.WithExecutionTimeout(30*time.Second))
    }),
    runner.WithPurge(24*time.Hour, 168*time.Hour, 10000), // optional
    runner.WithHTTPAddr("localhost:8011"),                // optional; keep private
    runner.WithSchemaCheck(),                              // optional fail-fast
)
err = r.Run(ctx) // blocks until ctx is cancelled, then drains in-flight jobs
```

## Development

```sh
# unit tests (no database needed)
go test -race ./...

# integration tests (docker compose brings up postgres:18)
docker compose up -d postgres
DISPOLLER_TEST_DSN=postgres://dispoller:dispoller@localhost:5433/dispoller_test \
  go test -race -tags integration -p 1 ./...
```

