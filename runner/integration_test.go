//go:build integration

// Интеграционные тесты runner против живой БД (как §9.2 ядра): билд-тег
// integration, DSN из DISPOLLER_TEST_DSN (иначе skip), схема применяется из
// embed перед прогоном. Запуск: docker compose up -d postgres && \
//
//	DISPOLLER_TEST_DSN=postgres://dispoller:dispoller@localhost:5433/dispoller_test \
//	  go test -race -tags integration -p 1 ./...
package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bobrovka/dispoller"
	"github.com/bobrovka/dispoller/schema"
)

var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	dsn := os.Getenv("DISPOLLER_TEST_DSN")
	if dsn == "" {
		fmt.Println("DISPOLLER_TEST_DSN is not set, skipping integration tests")
		os.Exit(0)
	}
	ctx := context.Background()

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "parse DSN:", err)
		os.Exit(1)
	}
	cfg.MaxConns = 10
	testPool, err = pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect:", err)
		os.Exit(1)
	}
	if err := applySchema(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "apply schema:", err)
		os.Exit(1)
	}
	code := m.Run()
	testPool.Close()
	os.Exit(code)
}

// applySchema применяет schema.sql по инструкции за раз; повторный прогон
// против живой БД терпит уже существующие объекты (42P07/42710/42701).
// Хелперы duplicated из integration_test.go ядра: пакеты тестов независимы.
func applySchema(ctx context.Context) error {
	for _, stmt := range splitStatements(schema.DDL) {
		if _, err := testPool.Exec(ctx, stmt); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) &&
				(pgErr.Code == "42P07" || pgErr.Code == "42710" || pgErr.Code == "42701") {
				continue // duplicate_table / duplicate_object / duplicate_column
			}
			return fmt.Errorf("%w (statement %q)", err, firstLine(stmt))
		}
	}
	return nil
}

// splitStatements режет скрипт на инструкции: комментарии вырезаются построчно.
func splitStatements(sqlText string) []string {
	var stmts []string
	var cur []string
	for _, line := range strings.Split(sqlText, "\n") {
		if idx := strings.Index(line, "--"); idx >= 0 {
			line = line[:idx]
		}
		cur = append(cur, line)
		if strings.Contains(line, ";") {
			stmt := strings.TrimSpace(strings.Join(cur, "\n"))
			stmt = strings.TrimSuffix(stmt, ";")
			if stmt != "" {
				stmts = append(stmts, stmt)
			}
			cur = nil
		}
	}
	if rest := strings.TrimSpace(strings.Join(cur, "")); rest != "" {
		stmts = append(stmts, rest)
	}
	return stmts
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i > 0 {
		return s[:i]
	}
	return s
}

// --- хелперы --------------------------------------------------------------

func waitInteg(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", desc)
}

// startRunner запускает Run в горутине; остановка — отмена контекста с
// проверкой, что Run вернулся (graceful shutdown). Возвращаемый stop
// идемпотентен (sync.Once): тест может вызвать его явно и продолжить проверки
// после остановки — Cleanup повторного ожидания не устроит.
func startRunner(t *testing.T, r *Runner) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("run: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Error("Run не вернулся за 10s после отмены контекста")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

// registerNoop — регистрация единственного noop-обработчика.
func registerNoop(taskType string) func(*dispoller.Engine) error {
	return func(e *dispoller.Engine) error {
		return e.RegisterHandler(taskType, dispoller.HandlerFunc(func(context.Context, []byte) error {
			return nil
		}))
	}
}

// jobStatus возвращает статус задачи ("" — строки нет).
func jobStatus(t *testing.T, id int64) string {
	t.Helper()
	var status string
	err := testPool.QueryRow(context.Background(),
		`SELECT status FROM background.jobs WHERE id = $1`, id).Scan(&status)
	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return ""
		}
		t.Fatalf("fetch job %d: %v", id, err)
	}
	return status
}

func mustEnqueue(t *testing.T, taskType string) int64 {
	t.Helper()
	id, err := dispoller.Enqueue(context.Background(), testPool, taskType, []byte(`{}`))
	if err != nil {
		t.Fatalf("enqueue %q: %v", taskType, err)
	}
	return id
}

// --- тесты ----------------------------------------------------------------

// RT-1: полный конвейер — WithSchemaCheck, регистрация колбеком, задача
// доводится до completed, отмена ctx возвращает Run с nil.
func TestRT1_RunnerPipeline(t *testing.T) {
	r := New(testPool,
		WithLogger(slog.Default()),
		WithInterval(20*time.Millisecond),
		WithConcurrency(2),
		WithRegister(registerNoop("rt1")),
		WithSchemaCheck(),
	)
	startRunner(t, r)

	id := mustEnqueue(t, "rt1")
	waitInteg(t, 10*time.Second, "completed", func() bool { return jobStatus(t, id) == "completed" })
}

// RT-2: WithPurge — completed-задача удаляется по тику очистки
// (retention 0 — удалять сразу; сам Purge — функциональность ядра, здесь
// проверяется только цикл runner). Выполнение фиксируется счётчиком, а не
// статусом в БД: purge с retention 0 может удалить completed-строку между
// опросами — ждать «completed» по базе это гонка со cleanup'ом теста.
func TestRT2_RunnerPurgesCompleted(t *testing.T) {
	var executed atomic.Int64
	r := New(testPool,
		WithInterval(20*time.Millisecond),
		WithConcurrency(1),
		WithRegister(func(e *dispoller.Engine) error {
			return e.RegisterHandler("rt2", dispoller.HandlerFunc(func(context.Context, []byte) error {
				executed.Add(1)
				return nil
			}))
		}),
		WithPurge(100*time.Millisecond, 0, 100),
	)
	startRunner(t, r)

	id := mustEnqueue(t, "rt2")
	waitInteg(t, 10*time.Second, "executed", func() bool { return executed.Load() == 1 })
	waitInteg(t, 15*time.Second, "purged", func() bool { return jobStatus(t, id) == "" })
	if n := executed.Load(); n != 1 {
		t.Errorf("обработчик запущен %d раз, хотим 1", n)
	}
}

// RT-3: WithSchemaCheck на БД без схемы очереди — понятная ошибка на старте.
func TestRT3_RunnerSchemaCheckMissingTable(t *testing.T) {
	ctx := context.Background()

	// Одноразовая база без схемы: DSN тестовой с заменённым именем БД.
	u, err := url.Parse(os.Getenv("DISPOLLER_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	dbName := "runner_no_schema_test"
	exec := func(q string) {
		if _, err := testPool.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec(`DROP DATABASE IF EXISTS ` + dbName)
	exec(`CREATE DATABASE ` + dbName)
	defer exec(`DROP DATABASE ` + dbName)

	u.Path = "/" + dbName
	bare, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer bare.Close() // Close не возвращает значение — errcheck не касается

	r := New(bare,
		WithInterval(time.Second), WithConcurrency(1), WithRegister(registerNoop("rt3")),
		WithSchemaCheck(),
	)
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "background.jobs") {
			t.Fatalf("хотим понятную ошибку про отсутствие background.jobs, получили: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run не упал на отсутствующей схеме за 10s")
	}
}

// RT-4: WithHTTPAddr — служебный HTTP поднимается вместе с движком и отвечает
// (healthz — liveness, readyz — ping живой БД); после остановки (Run вернулся,
// in-flight задачи дренированы) слушатель закрыт — соединения отбиваются.
// Регрессия «Shutdown забыт» ловится последней проверкой.
func TestRT4_RunnerServesHTTP(t *testing.T) {
	// Свободный порт: занять :0, запомнить адрес, освободить. Окно гонки
	// между Close и Listen ничтожно.
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen :0: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	r := New(testPool,
		WithInterval(20*time.Millisecond), WithConcurrency(1), WithRegister(registerNoop("rt4")),
		WithHTTPAddr(addr),
	)
	stop := startRunner(t, r)

	client := &http.Client{Timeout: 2 * time.Second}
	for _, path := range []string{"/healthz", "/readyz"} {
		waitInteg(t, 10*time.Second, "GET "+path, func() bool {
			resp, err := client.Get("http://" + addr + path)
			if err != nil {
				return false
			}
			defer func() { _ = resp.Body.Close() }()
			return resp.StatusCode == http.StatusOK
		})
	}

	// Слушатель закрывается вместе с остановкой: healthcheck не должен
	// отвечать «ok» от уже остановленного воркера.
	stop()
	waitInteg(t, 5*time.Second, "listener closed after stop", func() bool {
		_, err := client.Get("http://" + addr + "/healthz")
		return err != nil
	})
}
