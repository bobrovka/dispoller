//go:build integration

// Интеграционные тесты (design.md §9.2): билд-тег integration, DSN из
// DISPOLLER_TEST_DSN (иначе skip), схема применяется из embed перед прогоном.
// Запуск: docker compose up -d postgres && \
//
//	DISPOLLER_TEST_DSN=postgres://dispoller:dispoller@localhost:5433/dispoller_test \
//	  go test -race -tags integration -p 1 ./...
package dispoller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

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

	// MaxConns с запасом: тесты поднимают по 2 движка × concurrency 4 плюс
	// финализации (design.md §8: MaxConns ≥ concurrency + 2).
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "parse DSN:", err)
		os.Exit(1)
	}
	cfg.MaxConns = 20
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

// applySchema применяет schema.sql по инструкции за раз; повторный прогон против
// живой БД терпит уже существующие объекты (42P07/42710/42701).
func applySchema(ctx context.Context) error {
	for _, stmt := range splitStatements(schema.DDL) {
		if _, err := testPool.Exec(ctx, stmt); err != nil {
			var pgErr *pgconn.PgError
			switch {
			case errors.As(err, &pgErr) &&
				(pgErr.Code == "42P07" || pgErr.Code == "42710" || pgErr.Code == "42701"):
				continue // duplicate_table / duplicate_object / duplicate_column
			default:
				return fmt.Errorf("%w (statement %q)", err, firstLine(stmt))
			}
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
	if rest := strings.TrimSpace(strings.Join(cur, "\n")); rest != "" {
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

// --- хелперы -------------------------------------------------------------

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

type jobRow struct {
	status      string
	attempts    int
	reclaims    int
	maxAttempts int
	maxReclaims int
	errMsg      *string
	leaseNull   bool
	deadlineSet bool
	nextAttempt time.Time
}

func fetchJob(t *testing.T, id int64) jobRow {
	t.Helper()
	var (
		r        jobRow
		lease    *string
		deadline *time.Time
		errMsg   *string
		nextAtte time.Time
		maxAtt   int
		maxRecl  int
	)
	err := testPool.QueryRow(context.Background(),
		`SELECT status, attempts, reclaims, max_attempts, max_reclaims,
		        error_message, lease_token, must_be_finished_at, next_attempt_at
		 FROM background.jobs WHERE id = $1`, id).
		Scan(&r.status, &r.attempts, &r.reclaims, &maxAtt, &maxRecl,
			&errMsg, &lease, &deadline, &nextAtte)
	if err != nil {
		t.Fatalf("fetch job %d: %v", id, err)
	}
	r.maxAttempts, r.maxReclaims, r.errMsg, r.nextAttempt = maxAtt, maxRecl, errMsg, nextAtte
	r.leaseNull = lease == nil
	r.deadlineSet = deadline != nil
	return r
}

func mustEnqueue(t *testing.T, taskType string, payload []byte, opts ...EnqueueOption) int64 {
	t.Helper()
	id, err := Enqueue(context.Background(), testPool, taskType, payload, opts...)
	if err != nil {
		t.Fatalf("enqueue %q: %v", taskType, err)
	}
	return id
}

func execSQL(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := testPool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("exec %q: %v", firstLine(sql), err)
	}
}

// execCounter — потокобезопасный счётчик запусков обработчика по payload-id.
type execCounter struct {
	mu     sync.Mutex
	counts map[int64]int
}

func newExecCounter() *execCounter { return &execCounter{counts: map[int64]int{}} }

func (c *execCounter) incr(id int64) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[id]++
	return c.counts[id]
}

func (c *execCounter) get(id int64) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[id]
}

func (c *execCounter) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, v := range c.counts {
		n += v
	}
	return n
}

// idPayload кодирует идентификатор задачи в payload (для счётчика запусков).
func idPayload(id int64) []byte { return []byte(fmt.Sprintf(`{"id":%d}`, id)) }

// payloadID — декодирует id из payload. Возвращает ошибку (а не t.Fatalf):
// хелпер зовётся из горутины воркера, где Fatal недопустим.
func payloadID(payload []byte) (int64, error) {
	var p struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return 0, fmt.Errorf("test: unmarshal payload %s: %w", payload, err)
	}
	return p.ID, nil
}

// startEngines поднимает движки и возвращает функцию graceful-остановки,
// проверяющую, что Start вернул nil.
func startEngines(t *testing.T, interval time.Duration, concurrency int, count int, register func(*Engine)) func() {
	t.Helper()
	engines := make([]*Engine, count)
	cancels := make([]context.CancelFunc, count)
	dones := make([]chan error, count)
	for i := range count {
		e := New(testPool, WithBackoff(100*time.Millisecond, time.Second))
		register(e)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- e.Start(ctx, interval, concurrency) }()
		engines[i], cancels[i], dones[i] = e, cancel, done
	}
	return func() {
		for _, cancel := range cancels {
			cancel()
		}
		for _, done := range dones {
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Start returned %v, want nil", err)
				}
			case <-time.After(10 * time.Second):
				t.Error("Start did not return after cancel")
			}
		}
	}
}

// insertStuck вставляет processing-строку «умершего» воркера (T-6, T-15..T-17):
// lease уже истёк (или NULL), поля счётчиков и payload — по аргументам.
// Payload кладётся в тот же INSERT: движки уже запущены, отдельный UPDATE
// создавал бы окно, где claim забирает строку с дефолтным '{}'.
func insertStuck(t *testing.T, taskType string, attempts, reclaims int, deadline string, payload []byte) int64 {
	t.Helper()
	var id int64
	err := testPool.QueryRow(context.Background(),
		`INSERT INTO background.jobs (task_type, status, payload, attempts, reclaims, must_be_finished_at)
		 VALUES ($1, 'processing', $4::jsonb, $2, $3, `+deadline+`) RETURNING id`,
		taskType, attempts, reclaims, payload).Scan(&id)
	if err != nil {
		t.Fatalf("insert stuck row: %v", err)
	}
	return id
}

// --- T-1 … T-17 (design.md §9.2) ------------------------------------------

// T-1: Enqueue → Start — задача completed, payload доставлен.
func TestT1_EnqueueStartCompletes(t *testing.T) {
	var mu sync.Mutex
	var got []byte
	stop := startEngines(t, 20*time.Millisecond, 2, 1, func(e *Engine) {
		if err := e.RegisterHandler("t1", HandlerFunc(func(_ context.Context, p []byte) error {
			mu.Lock()
			defer mu.Unlock()
			got = p
			return nil
		})); err != nil {
			t.Fatal(err)
		}
	})
	defer stop()

	id := mustEnqueue(t, "t1", []byte(`{"k":"v"}`))
	waitInteg(t, 10*time.Second, "completed", func() bool { return fetchJob(t, id).status == statusCompleted })

	mu.Lock()
	defer mu.Unlock()
	if string(got) != `{"k": "v"}` && string(got) != `{"k":"v"}` {
		t.Errorf("payload delivered = %q", got)
	}
}

// T-2: ретраи — падающий обработчик: 2 неудачи → успех; статус/attempts корректны.
func TestT2_RetriesWithBackoff(t *testing.T) {
	counter := newExecCounter()
	stop := startEngines(t, 20*time.Millisecond, 2, 1, func(e *Engine) {
		if err := e.RegisterHandler("t2", HandlerFunc(func(_ context.Context, p []byte) error {
			id, err := payloadID(p)
			if err != nil {
				return err
			}
			if counter.incr(id) < 3 {
				return errors.New("temporary")
			}
			return nil
		})); err != nil {
			t.Fatal(err)
		}
	})
	defer stop()

	id := mustEnqueue(t, "t2", idPayload(42))
	waitInteg(t, 15*time.Second, "completed after 2 failures", func() bool {
		return fetchJob(t, id).status == statusCompleted
	})
	row := fetchJob(t, id)
	if row.attempts != 3 {
		t.Errorf("attempts = %d, want 3", row.attempts)
	}
	if n := counter.get(42); n != 3 {
		t.Errorf("executions = %d, want 3", n)
	}
}

// T-3: исчерпание — max_attempts достигнут → failed, error_message записан.
func TestT3_AttemptsExhausted(t *testing.T) {
	stop := startEngines(t, 20*time.Millisecond, 2, 1, func(e *Engine) {
		if err := e.RegisterHandler("t3", HandlerFunc(func(context.Context, []byte) error {
			return errors.New("always fails")
		})); err != nil {
			t.Fatal(err)
		}
	})
	defer stop()

	id := mustEnqueue(t, "t3", []byte(`{}`), WithMaxAttempts(2))
	waitInteg(t, 15*time.Second, "failed", func() bool { return fetchJob(t, id).status == statusFailed })
	row := fetchJob(t, id)
	if row.attempts != 2 {
		t.Errorf("attempts = %d, want 2", row.attempts)
	}
	if row.errMsg == nil || *row.errMsg != "always fails" {
		t.Errorf("error_message = %v, want %q", row.errMsg, "always fails")
	}
	if row.leaseNull != true {
		t.Error("lease_token must be NULL after terminal fail")
	}
}

// T-4: Permanent — failed с первой попытки, ретраев нет.
func TestT4_PermanentError(t *testing.T) {
	counter := newExecCounter()
	stop := startEngines(t, 20*time.Millisecond, 2, 1, func(e *Engine) {
		if err := e.RegisterHandler("t4", HandlerFunc(func(_ context.Context, p []byte) error {
			id, err := payloadID(p)
			if err != nil {
				return err
			}
			counter.incr(id)
			return Permanent(errors.New("bad payload"))
		})); err != nil {
			t.Fatal(err)
		}
	})
	defer stop()

	id := mustEnqueue(t, "t4", idPayload(7))
	waitInteg(t, 10*time.Second, "failed", func() bool { return fetchJob(t, id).status == statusFailed })
	row := fetchJob(t, id)
	if row.attempts != 1 {
		t.Errorf("attempts = %d, want 1 (no retries for Permanent)", row.attempts)
	}
	if n := counter.get(7); n != 1 {
		t.Errorf("executions = %d, want 1", n)
	}
}

// T-5: конкурентный захват — 2 движка × concurrency 4, 200 задач; дублей нет.
func TestT5_ConcurrentClaim(t *testing.T) {
	const jobsN = 200
	counter := newExecCounter()
	stop := startEngines(t, 10*time.Millisecond, 4, 2, func(e *Engine) {
		if err := e.RegisterHandler("t5", HandlerFunc(func(_ context.Context, p []byte) error {
			id, err := payloadID(p)
			if err != nil {
				return err
			}
			counter.incr(id)
			return nil
		})); err != nil {
			t.Fatal(err)
		}
	})
	defer stop()

	ids := make([]int64, 0, jobsN)
	for i := range int64(jobsN) {
		ids = append(ids, mustEnqueue(t, "t5", idPayload(i)))
	}

	waitInteg(t, 30*time.Second, "all completed", func() bool {
		var n int
		err := testPool.QueryRow(context.Background(),
			`SELECT count(*) FROM background.jobs WHERE status = 'completed' AND id = ANY($1)`, ids).Scan(&n)
		return err == nil && n == jobsN
	})

	dupes := 0
	for i := range int64(jobsN) {
		if n := counter.get(i); n != 1 {
			dupes++
			t.Errorf("job payload-id %d executed %d times", i, n)
		}
	}
	if dupes > 0 {
		t.Fatalf("%d jobs were executed not exactly once", dupes)
	}
}

// T-6: дедлайн — processing-строка с истёкшим lease перехвачена и выполнена.
func TestT6_ReclaimStuckJob(t *testing.T) {
	counter := newExecCounter()
	stop := startEngines(t, 20*time.Millisecond, 2, 1, func(e *Engine) {
		if err := e.RegisterHandler("t6", HandlerFunc(func(_ context.Context, p []byte) error {
			id, err := payloadID(p)
			if err != nil {
				return err
			}
			counter.incr(id)
			return nil
		})); err != nil {
			t.Fatal(err)
		}
	})
	defer stop()

	id := insertStuck(t, "t6", 1, 0, "now() - interval '1 minute'", idPayload(6))

	waitInteg(t, 10*time.Second, "stuck job completed", func() bool {
		return fetchJob(t, id).status == statusCompleted
	})
	row := fetchJob(t, id)
	if row.reclaims != 1 {
		t.Errorf("reclaims = %d, want 1", row.reclaims)
	}
	if row.attempts != 2 {
		t.Errorf("attempts = %d, want 2 (1 в строке + 1 на перехвате)", row.attempts)
	}
	if n := counter.get(6); n != 1 {
		t.Errorf("executions = %d, want 1", n)
	}
}

// T-7: fencing — обработчик дольше lease: после перехвата его финализация по
// чужому lease_token затрагивает 0 строк, состояние не портится (SN-4).
//
// Первая попытка переживает lease и завершается ОШИБКОЙ: без fencing-предиката
// в Fail опоздавший воркер перевернул бы completed → pending, и тест увидел бы
// лишние attempts/executions. Успешный late-Complete такой детекции не даёт
// (он писал бы те же значения). Детерминированный прямой тест предиката —
// TestFencingDirectSQL.
func TestT7_FencingLostUpdate(t *testing.T) {
	counter := newExecCounter()
	var firstStart atomic.Int64 // unix nano старта первой попытки
	stop := startEngines(t, 20*time.Millisecond, 4, 1, func(e *Engine) {
		if err := e.RegisterHandler("t7", HandlerFunc(func(ctx context.Context, p []byte) error {
			id, err := payloadID(p)
			if err != nil {
				return err
			}
			// Первая попытка «дорабатывает» дольше lease в 150ms, игнорируя ctx,
			// — как медленный SMTP, — и всё равно падает; перехват (вторая)
			// завершает мгновенно и успешно.
			if counter.incr(id) == 1 {
				firstStart.Store(time.Now().UnixNano())
				time.Sleep(500 * time.Millisecond)
				return errors.New("late failure")
			}
			return nil
		}), WithExecutionTimeout(150*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
	})
	defer stop()

	id := mustEnqueue(t, "t7", idPayload(9))
	waitInteg(t, 15*time.Second, "completed by second claim", func() bool {
		return fetchJob(t, id).status == statusCompleted
	})
	// Ждём момента, к которому поздний Fail первого воркера (старт + 500ms)
	// гарантированно приземлился — включая гипотетический повторный claim,
	// который устроил бы сломанный fencing (backoff ≤ 120ms + тик 20ms).
	if rest := time.Until(time.Unix(0, firstStart.Load()).Add(900 * time.Millisecond)); rest > 0 {
		time.Sleep(rest)
	}
	row := fetchJob(t, id)
	if row.status != statusCompleted {
		t.Errorf("status = %q, want completed (late Fail must not flip it)", row.status)
	}
	if row.attempts != 2 {
		t.Errorf("attempts = %d, want 2 (первый claim + перехват)", row.attempts)
	}
	if row.reclaims != 1 {
		t.Errorf("reclaims = %d, want 1", row.reclaims)
	}
	if n := counter.get(9); n != 2 {
		t.Errorf("executions = %d, want 2 (at-least-once дубль виден)", n)
	}
}

// TestFencingDirectSQL — детерминированная проверка fencing-предиката
// completeSQL/failSQL/releaseSQL (design.md §5.2–§5.4, SN-4): финализация по
// чужому lease_token затрагивает 0 строк (errFencingLost) и не меняет состояние.
func TestFencingDirectSQL(t *testing.T) {
	ctx := context.Background()
	st := &pgStore{pool: testPool}
	wrong, err := newLeaseToken()
	if err != nil {
		t.Fatal(err)
	}
	claim := func(taskType string, id int64) *claimedJob {
		t.Helper()
		// Захватываем конкретную строку: тип одноразовый, других кандидатов нет.
		job, err := st.Claim(ctx, []string{taskType}, []float64{60})
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if job == nil || job.id != id {
			t.Fatalf("claim returned %+v, want job %d", job, id)
		}
		return job
	}

	for _, tc := range []struct {
		name       string
		right      func(job *claimedJob) error
		wrong      func(id int64, token [16]byte) error
		afterWrong func(t *testing.T, id int64)
		afterRight func(t *testing.T, id int64)
	}{
		{
			name:  "complete",
			right: func(job *claimedJob) error { return st.Complete(ctx, job.id, job.token) },
			wrong: func(id int64, token [16]byte) error { return st.Complete(ctx, id, token) },
			afterWrong: func(t *testing.T, id int64) {
				if row := fetchJob(t, id); row.status != statusProcessing || row.leaseNull {
					t.Fatalf("wrong-token Complete changed state: %+v", row)
				}
			},
			afterRight: func(t *testing.T, id int64) {
				if row := fetchJob(t, id); row.status != statusCompleted || !row.leaseNull {
					t.Fatalf("right-token Complete did not finalize: %+v", row)
				}
			},
		},
		{
			name:  "fail",
			right: func(job *claimedJob) error { return st.Fail(ctx, job.id, job.token, "boom", 1, false) },
			wrong: func(id int64, token [16]byte) error { return st.Fail(ctx, id, token, "boom", 1, false) },
			afterWrong: func(t *testing.T, id int64) {
				if row := fetchJob(t, id); row.status != statusProcessing || row.errMsg != nil {
					t.Fatalf("wrong-token Fail changed state: %+v", row)
				}
			},
			afterRight: func(t *testing.T, id int64) {
				row := fetchJob(t, id)
				if row.status != statusPending || row.errMsg == nil || *row.errMsg != "boom" {
					t.Fatalf("right-token Fail did not apply: %+v", row)
				}
			},
		},
		{
			name:  "release",
			right: func(job *claimedJob) error { return st.Release(ctx, job.id, job.token) },
			wrong: func(id int64, token [16]byte) error { return st.Release(ctx, id, token) },
			afterWrong: func(t *testing.T, id int64) {
				if row := fetchJob(t, id); row.status != statusProcessing {
					t.Fatalf("wrong-token Release changed state: %+v", row)
				}
			},
			afterRight: func(t *testing.T, id int64) {
				if row := fetchJob(t, id); row.status != statusPending || row.attempts != 0 {
					t.Fatalf("right-token Release did not apply: %+v", row)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			taskType := uniqType(t, time.Now().UnixNano())
			id := mustEnqueue(t, taskType, []byte(`{}`))
			job := claim(taskType, id)

			if err := tc.wrong(id, wrong); !errors.Is(err, errFencingLost) {
				t.Fatalf("wrong token: got %v, want errFencingLost", err)
			}
			tc.afterWrong(t, id)

			if err := tc.right(job); err != nil {
				t.Fatalf("right token: %v", err)
			}
			tc.afterRight(t, id)
		})
	}
}

// uniqType — одноразовый task_type для изолированного claim конкретной строки.
func uniqType(t *testing.T, seed int64) string {
	t.Helper()
	return fmt.Sprintf("fence.%d.%d", seed, time.Now().UnixNano())
}

// T-8: таймаут попытки — обработчик-«спальня» дольше execution_timeout: ctx
// отменяется, попытка провалена, ретрай.
func TestT8_AttemptTimeout(t *testing.T) {
	counter := newExecCounter()
	stop := startEngines(t, 20*time.Millisecond, 2, 1, func(e *Engine) {
		if err := e.RegisterHandler("t8", HandlerFunc(func(ctx context.Context, p []byte) error {
			id, err := payloadID(p)
			if err != nil {
				return err
			}
			if counter.incr(id) == 1 {
				<-ctx.Done() // первая попытка «зависает» до таймаута
				return ctx.Err()
			}
			return nil
		}), WithExecutionTimeout(150*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
	})
	defer stop()

	id := mustEnqueue(t, "t8", idPayload(3))
	waitInteg(t, 15*time.Second, "completed on retry", func() bool {
		return fetchJob(t, id).status == statusCompleted
	})
	row := fetchJob(t, id)
	if row.attempts != 2 {
		t.Errorf("attempts = %d, want 2 (таймаут + ретрай)", row.attempts)
	}
	if n := counter.get(3); n != 2 {
		t.Errorf("executions = %d, want 2", n)
	}
}

// T-9: graceful shutdown — снятая задача возвращается в pending без сжигания
// попытки; Release выполняется после отмены ctx движка; reclaims не меняется;
// Start возвращает nil.
func TestT9_GracefulShutdown(t *testing.T) {
	started := make(chan struct{})
	e := New(testPool, WithBackoff(100*time.Millisecond, time.Second))
	if err := e.RegisterHandler("t9", HandlerFunc(func(ctx context.Context, _ []byte) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Start(ctx, 20*time.Millisecond, 2) }()

	id := mustEnqueue(t, "t9", []byte(`{}`))
	<-started
	waitInteg(t, 5*time.Second, "processing", func() bool { return fetchJob(t, id).status == statusProcessing })

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after cancel")
	}

	row := fetchJob(t, id)
	if row.status != statusPending {
		t.Errorf("status = %q, want pending (released)", row.status)
	}
	if row.attempts != 0 {
		t.Errorf("attempts = %d, want 0 (release не сжигает попытку)", row.attempts)
	}
	if row.reclaims != 0 {
		t.Errorf("reclaims = %d, want 0 (shutdown не жжёт бюджет)", row.reclaims)
	}
	if row.deadlineSet {
		t.Error("must_be_finished_at must be NULL after release")
	}

	// Перезапуск доводит задачу до конца.
	stop := startEngines(t, 20*time.Millisecond, 2, 1, func(e *Engine) {
		if err := e.RegisterHandler("t9", HandlerFunc(func(context.Context, []byte) error { return nil })); err != nil {
			t.Fatal(err)
		}
	})
	defer stop()
	waitInteg(t, 10*time.Second, "completed after restart", func() bool {
		return fetchJob(t, id).status == statusCompleted
	})
	if row := fetchJob(t, id); row.attempts != 1 {
		t.Errorf("attempts after restart = %d, want 1", row.attempts)
	}
}

// T-10: EnqueueTx — rollback транзакции → задачи нет; commit → есть.
func TestT10_EnqueueTx(t *testing.T) {
	ctx := context.Background()

	tx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := EnqueueTx(ctx, tx, "t10", []byte(`{"case":"rollback"}`)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := testPool.QueryRow(ctx,
		`SELECT count(*) FROM background.jobs WHERE task_type = 't10' AND payload->>'case' = 'rollback'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("rollback: rows = %d, want 0", n)
	}

	tx, err = testPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := EnqueueTx(ctx, tx, "t10", []byte(`{"case":"commit"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if id == 0 {
		t.Error("EnqueueTx returned id 0 after commit")
	}
	if err := testPool.QueryRow(ctx,
		`SELECT count(*) FROM background.jobs WHERE id = $1`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("commit: rows = %d, want 1", n)
	}
}

// T-11: незарегистрированный тип — задача не захватывается, остаётся pending.
func TestT11_UnregisteredType(t *testing.T) {
	stop := startEngines(t, 20*time.Millisecond, 2, 1, func(e *Engine) {
		if err := e.RegisterHandler("t11.registered", HandlerFunc(func(context.Context, []byte) error {
			return nil
		})); err != nil {
			t.Fatal(err)
		}
	})
	defer stop()

	id := mustEnqueue(t, "t11.unregistered", []byte(`{}`))
	time.Sleep(500 * time.Millisecond)
	row := fetchJob(t, id)
	if row.status != statusPending {
		t.Errorf("status = %q, want pending (SN-9: не выжигаем попытки)", row.status)
	}
	if row.attempts != 0 {
		t.Errorf("attempts = %d, want 0", row.attempts)
	}
}

// T-12: паника — статус failed/retry, процесс жив.
func TestT12_HandlerPanic(t *testing.T) {
	counter := newExecCounter()
	stop := startEngines(t, 20*time.Millisecond, 2, 1, func(e *Engine) {
		if err := e.RegisterHandler("t12", HandlerFunc(func(_ context.Context, p []byte) error {
			id, err := payloadID(p)
			if err != nil {
				return err
			}
			counter.incr(id)
			panic("boom")
		})); err != nil {
			t.Fatal(err)
		}
	})
	defer stop()

	id := mustEnqueue(t, "t12", idPayload(5), WithMaxAttempts(2))
	waitInteg(t, 15*time.Second, "failed after panics", func() bool {
		return fetchJob(t, id).status == statusFailed
	})
	row := fetchJob(t, id)
	if row.errMsg == nil || !strings.HasPrefix(*row.errMsg, "handler panic:") {
		t.Errorf("error_message = %v, want prefix %q", row.errMsg, "handler panic:")
	}
	if n := counter.get(5); n != 2 {
		t.Errorf("executions = %d, want 2", n)
	}
	// Процесс жив — проверяет сам факт, что тест дошёл сюда без падения.
}

// T-13: Purge — удаляет только completed старше порога и в рамках limit;
// pending/processing/failed не тронуты.
func TestT13_Purge(t *testing.T) {
	// БД переиспользуется между прогонами: чистим чужие старые completed, чтобы
	// точные счётчики ниже не зависели от возраста строк прошлых прогонов.
	execSQL(t, `DELETE FROM background.jobs
	            WHERE status = 'completed' AND updated_at < now() - interval '1 hour'`)

	// 3 старых completed, 1 старый failed, 1 старый pending (незарегистрированный
	// тип — никто не тронет), 1 свежий completed.
	var oldCompleted []int64
	for range 3 {
		oldCompleted = append(oldCompleted, mustEnqueue(t, "t13.done", []byte(`{}`)))
	}
	freshCompleted := mustEnqueue(t, "t13.done", []byte(`{}`))
	oldFailed := mustEnqueue(t, "t13.dead", []byte(`{}`))
	oldPending := mustEnqueue(t, "t13.wait", []byte(`{}`))
	execSQL(t, `UPDATE background.jobs
	            SET status = CASE task_type WHEN 't13.dead' THEN 'failed' ELSE 'completed' END,
	                updated_at = now() - interval '2 hours'
	            WHERE id = ANY($1) OR id = $2`,
		oldCompleted, oldFailed)
	execSQL(t, `UPDATE background.jobs SET updated_at = now() - interval '2 hours' WHERE id = $1`, oldPending)

	// limit: сначала удаляем не больше 2.
	n, err := Purge(context.Background(), testPool, time.Hour, 2)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("first purge deleted %d, want 2 (limit)", n)
	}
	// Осталась 1 старая completed.
	n, err = Purge(context.Background(), testPool, time.Hour, 100)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("second purge deleted %d, want 1", n)
	}
	// Больше нечего.
	if n, err = Purge(context.Background(), testPool, time.Hour, 100); err != nil || n != 0 {
		t.Fatalf("third purge = (%d, %v), want (0, nil)", n, err)
	}

	var remains int
	for _, id := range append(append([]int64(nil), oldCompleted...), freshCompleted) {
		if err := testPool.QueryRow(context.Background(),
			`SELECT count(*) FROM background.jobs WHERE id = $1`, id).Scan(&remains); err != nil {
			t.Fatal(err)
		}
		want := 0
		if id == freshCompleted {
			want = 1 // свежий completed не удаляется
		}
		if remains != want {
			t.Errorf("completed job %d: rows = %d, want %d", id, remains, want)
		}
	}
	for _, id := range []int64{oldFailed, oldPending} {
		if err := testPool.QueryRow(context.Background(),
			`SELECT count(*) FROM background.jobs WHERE id = $1`, id).Scan(&remains); err != nil {
			t.Fatal(err)
		}
		if remains != 1 {
			t.Errorf("non-completed job %d purged (rows = %d)", id, remains)
		}
	}
}

// T-14: WithRunAt — отложенная задача не захватывается до наступления времени,
// затем выполняется.
func TestT14_RunAt(t *testing.T) {
	counter := newExecCounter()
	stop := startEngines(t, 20*time.Millisecond, 2, 1, func(e *Engine) {
		if err := e.RegisterHandler("t14", HandlerFunc(func(_ context.Context, p []byte) error {
			id, err := payloadID(p)
			if err != nil {
				return err
			}
			counter.incr(id)
			return nil
		})); err != nil {
			t.Fatal(err)
		}
	})
	defer stop()

	runAt := time.Now().Add(700 * time.Millisecond)
	id := mustEnqueue(t, "t14", idPayload(11), WithRunAt(runAt))

	// До наступления времени задача не захватывается.
	time.Sleep(300 * time.Millisecond)
	row := fetchJob(t, id)
	if row.status != statusPending || row.attempts != 0 {
		t.Fatalf("before runAt: status=%q attempts=%d, want pending/0", row.status, row.attempts)
	}
	if n := counter.get(11); n != 0 {
		t.Fatalf("executed %d times before runAt", n)
	}

	waitInteg(t, 10*time.Second, "completed after runAt", func() bool {
		return fetchJob(t, id).status == statusCompleted
	})
}

// T-15: NULL-дедлайн (SN-5) — processing-строка с must_be_finished_at IS NULL
// перехватывается и выполняется.
func TestT15_NullDeadlineReclaim(t *testing.T) {
	stop := startEngines(t, 20*time.Millisecond, 2, 1, func(e *Engine) {
		if err := e.RegisterHandler("t15", HandlerFunc(func(context.Context, []byte) error { return nil })); err != nil {
			t.Fatal(err)
		}
	})
	defer stop()

	id := insertStuck(t, "t15", 1, 0, "NULL", []byte(`{}`))

	waitInteg(t, 10*time.Second, "null-deadline row completed", func() bool {
		return fetchJob(t, id).status == statusCompleted
	})
	if row := fetchJob(t, id); row.reclaims != 1 {
		t.Errorf("reclaims = %d, want 1", row.reclaims)
	}
}

// T-16: перехват сверх лимита (SN-16) — зависшая задача с attempts = max_attempts
// и неполным бюджетом перехватывается и запускается повторно; reclaims растёт.
func TestT16_ReclaimBeyondMaxAttempts(t *testing.T) {
	counter := newExecCounter()
	stop := startEngines(t, 20*time.Millisecond, 2, 1, func(e *Engine) {
		if err := e.RegisterHandler("t16", HandlerFunc(func(_ context.Context, p []byte) error {
			id, err := payloadID(p)
			if err != nil {
				return err
			}
			counter.incr(id)
			return nil
		})); err != nil {
			t.Fatal(err)
		}
	})
	defer stop()

	// Воркер умер на последней попытке: attempts = max_attempts = 3, lease истёк.
	id := insertStuck(t, "t16", 3, 0, "now() - interval '1 minute'", idPayload(16))

	waitInteg(t, 10*time.Second, "completed despite attempts = max", func() bool {
		return fetchJob(t, id).status == statusCompleted
	})
	row := fetchJob(t, id)
	if row.attempts != 4 {
		t.Errorf("attempts = %d, want 4 (единичный запуск сверх max_attempts, SN-16)", row.attempts)
	}
	if row.reclaims != 1 {
		t.Errorf("reclaims = %d, want 1", row.reclaims)
	}
	if n := counter.get(16); n != 1 {
		t.Errorf("executions = %d, want 1", n)
	}
}

// T-17: бюджет перехватов (SN-18) — dead-letter без запуска обработчика,
// итоговый reclaims = max_reclaims + 1; Requeue возвращает задачу в строй.
func TestT17_ReclaimBudgetDeadLetter(t *testing.T) {
	counter := newExecCounter()
	stop := startEngines(t, 20*time.Millisecond, 2, 1, func(e *Engine) {
		if err := e.RegisterHandler("t17", HandlerFunc(func(_ context.Context, p []byte) error {
			id, err := payloadID(p)
			if err != nil {
				return err
			}
			counter.incr(id)
			return nil
		})); err != nil {
			t.Fatal(err)
		}
	})
	defer stop()

	// Бюджет исчерпан: reclaims = max_reclaims = 10 (дефолт колонки), lease истёк.
	id := insertStuck(t, "t17", 2, 10, "now() - interval '1 minute'", idPayload(17))

	waitInteg(t, 10*time.Second, "dead-lettered", func() bool {
		return fetchJob(t, id).status == statusFailed
	})
	row := fetchJob(t, id)
	if row.errMsg == nil || *row.errMsg != ReclaimBudgetExhaustedMessage {
		t.Errorf("error_message = %v, want %q", row.errMsg, ReclaimBudgetExhaustedMessage)
	}
	if !row.leaseNull {
		t.Error("lease_token must be NULL after dead-letter")
	}
	if row.attempts != 2 {
		t.Errorf("attempts = %d, want 2 (запуска не было)", row.attempts)
	}
	if row.reclaims != 11 {
		t.Errorf("reclaims = %d, want max_reclaims + 1 = 11", row.reclaims)
	}
	if n := counter.get(17); n != 0 {
		t.Errorf("обработчик запущен %d раз, want 0 (dead-letter без запуска)", n)
	}

	// Requeue возвращает failed-задачу в pending с нулевыми счётчиками.
	if err := Requeue(context.Background(), testPool, id); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	row = fetchJob(t, id)
	if row.status != statusPending || row.attempts != 0 || row.reclaims != 0 {
		t.Fatalf("after requeue: status=%q attempts=%d reclaims=%d, want pending/0/0",
			row.status, row.attempts, row.reclaims)
	}

	// Задача выполняется с чистого листа.
	waitInteg(t, 10*time.Second, "completed after requeue", func() bool {
		return fetchJob(t, id).status == statusCompleted
	})
	if row := fetchJob(t, id); row.attempts != 1 || row.reclaims != 0 {
		t.Errorf("after replay: attempts=%d reclaims=%d, want 1/0", row.attempts, row.reclaims)
	}
	if n := counter.get(17); n != 1 {
		t.Errorf("executions after requeue = %d, want 1", n)
	}

	// Requeue не-failed строки — ошибка (completed выше).
	if err := Requeue(context.Background(), testPool, id); err == nil {
		t.Error("requeue completed job: want error")
	}
	// Requeue несуществующей задачи — ошибка.
	if err := Requeue(context.Background(), testPool, -1); err == nil {
		t.Error("requeue missing job: want error")
	}
}

// Сверка мониторинговых запросов design.md §10 против живой схемы.
func TestMonitoringQueries(t *testing.T) {
	ctx := context.Background()
	for _, q := range []string{
		`SELECT count(*) FROM background.jobs WHERE status='pending' AND next_attempt_at <= now()`,
		`SELECT count(*) FROM background.jobs WHERE status='processing' AND (must_be_finished_at < now() OR must_be_finished_at IS NULL)`,
		`SELECT count(*) FROM background.jobs WHERE status='failed' AND updated_at > now() - interval '1 hour'`,
		`SELECT id, task_type, reclaims FROM background.jobs WHERE status='failed' AND error_message='reclaim budget exhausted'`,
		`SELECT DISTINCT task_type FROM background.jobs WHERE status IN ('pending','processing')`,
	} {
		if _, err := testPool.Exec(ctx, q); err != nil {
			t.Errorf("monitoring query failed: %v\n%s", err, firstLine(q))
		}
	}
}

// Дополнительная проверка SKIP LOCKED на двух конкурентных сессиях (design.md §5):
// конкурентная сессия забирает следующую задачу, не блокируясь на занятой строке.
func TestSkipLockedTwoSessions(t *testing.T) {
	ctx := context.Background()
	for range 2 {
		mustEnqueue(t, "skiplock.probe", []byte(`{}`))
	}

	c1, err := testPool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Release()
	c2, err := testPool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Release()

	// Сессия 1 держит первую задачу в незакрытой транзакции.
	tx1, err := c1.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx1.Rollback(ctx)
	id1, err := claimProbe(ctx, tx1)
	if err != nil {
		t.Fatalf("claim in tx1: %v", err)
	}

	// Сессия 2 должна забрать вторую, не блокируясь на занятой строке,
	// — с таймаутом на всякий случай.
	sctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	id2, err := claimProbe(sctx, c2)
	if err != nil {
		t.Fatalf("claim in session 2 (must not block on locked row): %v", err)
	}
	if id1 == id2 {
		t.Fatalf("both sessions claimed id %d — SKIP LOCKED broken", id1)
	}
}

// claimProbe выполняет claim-запрос по probe-типу и возвращает id задачи.
func claimProbe(ctx context.Context, q querier) (int64, error) {
	token, err := newLeaseToken()
	if err != nil {
		return 0, err
	}
	var (
		id                    int64
		taskType              string
		payload               []byte
		attempts, maxAttempts int
		reclaims, maxReclaims int
		lease                 pgtype.UUID
		deadline              pgtype.Timestamptz
		status                string
	)
	err = q.QueryRow(ctx, claimSQL,
		[]string{"skiplock.probe"}, []float64{30},
		pgtype.UUID{Bytes: token, Valid: true}).
		Scan(&id, &taskType, &payload, &attempts, &maxAttempts,
			&reclaims, &maxReclaims, &lease, &deadline, &status)
	return id, err
}
