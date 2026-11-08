package dispoller

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeStore — in-memory подмена store для unit-тестов движка (design.md §9.1).
type fakeStore struct {
	mu         sync.Mutex
	queue      []*claimedJob // FIFO задач, выдаваемых Claim
	claimCalls int
	complete   []int64
	fails      []failCall
	releases   []int64
	enqueues   []string

	// errs — ошибки, выдаваемые финализациями по очереди (пусто → успех).
	completeErrs []error
	failErrs     []error
	releaseErrs  []error
}

type failCall struct {
	id      int64
	msg     string
	backoff float64
	force   bool
}

func (f *fakeStore) Claim(_ context.Context, _ []string, _ []float64) (*claimedJob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claimCalls++
	if len(f.queue) == 0 {
		return nil, nil // очередь пуста
	}
	job := f.queue[0]
	f.queue = f.queue[1:]
	return job, nil
}

func (f *fakeStore) popErr(errs *[]error) error {
	if len(*errs) == 0 {
		return nil
	}
	err := (*errs)[0]
	*errs = (*errs)[1:]
	return err
}

func (f *fakeStore) Complete(_ context.Context, id int64, _ [16]byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.complete = append(f.complete, id)
	return f.popErr(&f.completeErrs)
}

func (f *fakeStore) Fail(_ context.Context, id int64, _ [16]byte, msg string, backoff float64, force bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fails = append(f.fails, failCall{id: id, msg: msg, backoff: backoff, force: force})
	return f.popErr(&f.failErrs)
}

func (f *fakeStore) Release(_ context.Context, id int64, _ [16]byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releases = append(f.releases, id)
	return f.popErr(&f.releaseErrs)
}

func (f *fakeStore) Enqueue(_ context.Context, taskType string, _ []byte, _, _ int, _ *time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.enqueues = append(f.enqueues, taskType)
	return int64(len(f.enqueues)), nil
}

func (f *fakeStore) claims() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.claimCalls
}

func (f *fakeStore) completed() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.complete...)
}

func (f *fakeStore) failed() []failCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]failCall(nil), f.fails...)
}

func (f *fakeStore) released() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.releases...)
}

// newTestEngine — движок с fake store и ускоренными ретраями финализации.
func newTestEngine(t *testing.T) (*Engine, *fakeStore) {
	t.Helper()
	e := New(nil)
	fs := &fakeStore{}
	e.store = fs
	e.finalizeDelays = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	t.Cleanup(func() {})
	return e, fs
}

// register — регистрирует обработчик и готовит снапшот (как Start).
func register(t *testing.T, e *Engine, taskType string, h JobHandler) {
	t.Helper()
	if err := e.RegisterHandler(taskType, h); err != nil {
		t.Fatalf("register %q: %v", taskType, err)
	}
	e.snapshot = make(map[string]*registeredHandler, len(e.handlers))
	for k, v := range e.handlers {
		e.snapshot[k] = v
	}
}

func testJob(id int64, taskType string) *claimedJob {
	return &claimedJob{id: id, taskType: taskType, payload: []byte(`{}`), attempts: 1, maxAttempts: 3, timeout: time.Second}
}

func waitCond(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", desc)
}

func TestRegisterHandlerValidation(t *testing.T) {
	e, _ := newTestEngine(t)
	h := HandlerFunc(func(context.Context, []byte) error { return nil })

	if err := e.RegisterHandler("", h); err == nil {
		t.Error("empty task type: want error")
	}
	if err := e.RegisterHandler(strings.Repeat("x", 256), h); err == nil {
		t.Error("task type longer than 255 chars: want error")
	}
	if err := e.RegisterHandler("ok", nil); err == nil {
		t.Error("nil handler: want error")
	}
	if err := e.RegisterHandler("ok", h, WithExecutionTimeout(0)); err == nil {
		t.Error("zero timeout: want error")
	}
	if err := e.RegisterHandler("ok", h, WithExecutionTimeout(-time.Second)); err == nil {
		t.Error("negative timeout: want error")
	}
	if err := e.RegisterHandler("ok", h); err != nil {
		t.Fatalf("valid registration: %v", err)
	}
	if err := e.RegisterHandler("ok", h); err == nil {
		t.Error("duplicate task type: want error")
	}
}

func TestRegisterHandlerAfterStart(t *testing.T) {
	e, _ := newTestEngine(t)
	register(t, e, "t", HandlerFunc(func(context.Context, []byte) error { return nil }))

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = e.Start(ctx, time.Millisecond, 1) }()
	waitCond(t, time.Second, "engine start", func() bool { return e.started.Load() })

	err := e.RegisterHandler("other", HandlerFunc(func(context.Context, []byte) error { return nil }))
	if err == nil {
		t.Error("RegisterHandler after Start: want error")
	}
	cancel()
}

func TestStartValidation(t *testing.T) {
	t.Run("interval", func(t *testing.T) {
		e, _ := newTestEngine(t)
		register(t, e, "t", HandlerFunc(func(context.Context, []byte) error { return nil }))
		if err := e.Start(context.Background(), 0, 1); err == nil {
			t.Error("interval 0: want error")
		}
		if err := e.Start(context.Background(), -time.Second, 1); err == nil {
			t.Error("negative interval: want error")
		}
	})
	t.Run("concurrency", func(t *testing.T) {
		e, _ := newTestEngine(t)
		register(t, e, "t", HandlerFunc(func(context.Context, []byte) error { return nil }))
		if err := e.Start(context.Background(), time.Second, 0); err == nil {
			t.Error("concurrency 0: want error")
		}
	})
	t.Run("no handlers", func(t *testing.T) {
		e, _ := newTestEngine(t)
		if err := e.Start(context.Background(), time.Second, 1); err == nil {
			t.Error("no handlers: want error")
		}
	})
	t.Run("double start", func(t *testing.T) {
		e, _ := newTestEngine(t)
		register(t, e, "t", HandlerFunc(func(context.Context, []byte) error { return nil }))
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- e.Start(ctx, time.Millisecond, 1) }()
		waitCond(t, time.Second, "engine start", func() bool { return e.started.Load() })

		if err := e.Start(context.Background(), time.Second, 1); err == nil {
			t.Error("second Start while running: want error")
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("first Start returned %v, want nil", err)
		}
		if err := e.Start(context.Background(), time.Second, 1); err == nil {
			t.Error("second Start after stop: want error")
		}
	})
}

// TestRunOutcomes покрывает исходы run (design.md §7.3, §9.1):
// nil → Complete; ошибка с попытками → Fail(pending); Permanent → Fail(force);
// паника → Fail; отмена ctx движка → Release.
func TestRunOutcomes(t *testing.T) {
	t.Run("success completes", func(t *testing.T) {
		e, fs := newTestEngine(t)
		register(t, e, "t", HandlerFunc(func(context.Context, []byte) error { return nil }))
		e.run(context.Background(), testJob(1, "t"))
		if got := fs.completed(); len(got) != 1 || got[0] != 1 {
			t.Errorf("Complete calls = %v, want [1]", got)
		}
		if len(fs.failed()) != 0 || len(fs.released()) != 0 {
			t.Errorf("unexpected Fail/Release calls: %v %v", fs.failed(), fs.released())
		}
	})
	t.Run("error fails with backoff", func(t *testing.T) {
		e, fs := newTestEngine(t)
		e.backoffBase, e.backoffMax = 30*time.Second, 15*time.Minute
		register(t, e, "t", HandlerFunc(func(context.Context, []byte) error { return errors.New("boom") }))
		e.run(context.Background(), testJob(1, "t"))
		fails := fs.failed()
		if len(fails) != 1 {
			t.Fatalf("Fail calls = %d, want 1", len(fails))
		}
		if fails[0].force {
			t.Error("force = true, want false")
		}
		if fails[0].msg != "boom" {
			t.Errorf("msg = %q, want %q", fails[0].msg, "boom")
		}
		// attempts=1 → base*2^0*jitter = 24s..36s
		lo, hi := 0.8*30, 1.2*30
		if fails[0].backoff < lo || fails[0].backoff > hi {
			t.Errorf("backoff = %v, want in [%v, %v]", fails[0].backoff, lo, hi)
		}
	})
	t.Run("permanent error forces fail", func(t *testing.T) {
		e, fs := newTestEngine(t)
		register(t, e, "t", HandlerFunc(func(context.Context, []byte) error {
			return Permanent(errors.New("bad payload"))
		}))
		e.run(context.Background(), testJob(1, "t"))
		fails := fs.failed()
		if len(fails) != 1 || !fails[0].force {
			t.Fatalf("Fail calls = %+v, want one forced", fails)
		}
	})
	t.Run("handler panic becomes fail", func(t *testing.T) {
		e, fs := newTestEngine(t)
		register(t, e, "t", HandlerFunc(func(context.Context, []byte) error { panic("boom") }))
		e.run(context.Background(), testJob(1, "t"))
		fails := fs.failed()
		if len(fails) != 1 {
			t.Fatalf("Fail calls = %d, want 1", len(fails))
		}
		if !strings.HasPrefix(fails[0].msg, "handler panic:") {
			t.Errorf("msg = %q, want prefix %q", fails[0].msg, "handler panic:")
		}
	})
	t.Run("engine shutdown releases", func(t *testing.T) {
		e, fs := newTestEngine(t)
		register(t, e, "t", HandlerFunc(func(ctx context.Context, _ []byte) error {
			<-ctx.Done() // уважаем отмену
			return ctx.Err()
		}))
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // движок уже останавливается
		e.run(ctx, testJob(1, "t"))
		if got := fs.released(); len(got) != 1 || got[0] != 1 {
			t.Errorf("Release calls = %v, want [1]", got)
		}
		if len(fs.failed()) != 0 {
			t.Errorf("unexpected Fail calls: %v", fs.failed())
		}
	})
	t.Run("unregistered type fails", func(t *testing.T) {
		e, fs := newTestEngine(t)
		register(t, e, "t", HandlerFunc(func(context.Context, []byte) error { return nil }))
		e.run(context.Background(), testJob(1, "unknown"))
		if len(fs.failed()) != 1 {
			t.Errorf("Fail calls = %d, want 1", len(fs.failed()))
		}
	})
}

func TestBackoffFor(t *testing.T) {
	e, _ := newTestEngine(t)
	e.backoffBase, e.backoffMax = 30*time.Second, 15*time.Minute

	cases := []struct {
		attempts     int
		loSec, hiSec float64
	}{
		{1, 0.8 * 30, 1.2 * 30},
		{2, 0.8 * 60, 1.2 * 60},
		{5, 0.8 * 480, 1.2 * 480},
		// exp = 960s: минимальный джиттер (0.8) ниже потолка 900s, максимум — кламп.
		{6, 0.8 * 960, 900},
	}
	for _, tc := range cases {
		for range 50 {
			d := e.backoffFor(tc.attempts)
			sec := d.Seconds()
			if sec < tc.loSec || sec > tc.hiSec {
				t.Fatalf("backoffFor(%d) = %vs, want in [%v, %v]", tc.attempts, sec, tc.loSec, tc.hiSec)
			}
		}
	}
	// Большой номер попытки: экспонента выше cap → жёсткий потолок max.
	for range 50 {
		if d := e.backoffFor(30); d != e.backoffMax {
			t.Fatalf("backoffFor(30) = %v, want cap %v", d, e.backoffMax)
		}
	}
	// attempts < 1 — защита от деления/отрицательных, трактуем как 1.
	if d := e.backoffFor(0); d < 24*time.Second || d > 36*time.Second {
		t.Fatalf("backoffFor(0) = %v, want corridor of attempt 1", d)
	}
}

func TestFinalizeRetries(t *testing.T) {
	e, fs := newTestEngine(t)
	register(t, e, "t", HandlerFunc(func(context.Context, []byte) error { return nil }))
	// Две ошибки соединения, успех на третьей попытке (§7.3).
	fs.completeErrs = []error{errors.New("conn reset"), errors.New("conn reset")}

	e.run(context.Background(), testJob(1, "t"))

	if got := len(fs.completed()); got != 3 {
		t.Fatalf("Complete attempts = %d, want 3 (initial + 2 retries)", got)
	}
	fs.mu.Lock()
	left := len(fs.completeErrs)
	fs.mu.Unlock()
	if left != 0 {
		t.Fatalf("unconsumed error queue: %d", left)
	}
}

func TestFinalizeFencingLostIsNotRetried(t *testing.T) {
	e, fs := newTestEngine(t)
	register(t, e, "t", HandlerFunc(func(context.Context, []byte) error { return nil }))
	fs.completeErrs = []error{errFencingLost, errFencingLost}

	e.run(context.Background(), testJob(1, "t"))

	// Ровно один вызов; вторая заготовленная ошибка не израсходована — ретрая нет.
	if got := len(fs.completed()); got != 1 {
		t.Fatalf("Complete attempts = %d, want 1 (fencing lost is terminal)", got)
	}
	fs.mu.Lock()
	left := len(fs.completeErrs)
	fs.mu.Unlock()
	if left != 1 {
		t.Fatalf("fencing lost must not be retried, unconsumed errors = %d", left)
	}
}

func TestFinalizeAfterRootCancel(t *testing.T) {
	// Финализация обязана выполниться с ctx, отвязанным от корневого (SN-17):
	// корневой уже отменён, а Complete всё равно вызван.
	e, fs := newTestEngine(t)
	register(t, e, "t", HandlerFunc(func(ctx context.Context, _ []byte) error {
		<-ctx.Done()
		return nil // успех, увиденный уже после отмены корня
	}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	e.run(ctx, testJob(1, "t"))

	if got := fs.completed(); len(got) != 1 {
		t.Fatalf("Complete calls = %v, want [1] even with cancelled root ctx", got)
	}
}

func TestDispatcherHappyPath(t *testing.T) {
	e, fs := newTestEngine(t)
	var mu sync.Mutex
	var payloads [][]byte
	register(t, e, "t", HandlerFunc(func(_ context.Context, p []byte) error {
		mu.Lock()
		defer mu.Unlock()
		payloads = append(payloads, p)
		return nil
	}))

	job := testJob(1, "t")
	job.payload = []byte(`{"route":"me"}`)
	fs.queue = []*claimedJob{job}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- e.Start(ctx, time.Millisecond, 2) }()

	waitCond(t, 2*time.Second, "job executed", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(payloads) == 1
	})
	// Слот семафора возвращён: после завершения задачи семафор пуст.
	waitCond(t, 2*time.Second, "semaphore drained", func() bool { return len(e.sem) == 0 })

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned %v, want nil", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if string(payloads[0]) != `{"route":"me"}` {
		t.Errorf("payload = %q, want original", payloads[0])
	}
	if got := fs.completed(); len(got) != 1 || got[0] != 1 {
		t.Errorf("Complete calls = %v, want [1]", got)
	}
}

func TestDispatcherDeadLetter(t *testing.T) {
	// Dead-letter-исход claim (§5.1): задача не диспетчеризуется в обработчик,
	// слот освобождается, dispatcher немедленно повторяет claim в том же тике.
	e, fs := newTestEngine(t)
	var executed atomic.Int64
	register(t, e, "t", HandlerFunc(func(context.Context, []byte) error {
		executed.Add(1)
		return nil
	}))

	dl := testJob(1, "t")
	dl.deadLettered = true
	dl.reclaims = 11
	fs.queue = []*claimedJob{dl, testJob(2, "t")}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- e.Start(ctx, time.Millisecond, 1) }()

	// Вторая (нормальная) задача выполнена — значит dead-letter был пропущен.
	waitCond(t, 2*time.Second, "second job executed", func() bool { return executed.Load() == 1 })
	// Оба claim'а уложились быстро: немедленный повтор после dead-letter.
	if n := fs.claims(); n < 2 {
		t.Errorf("claim calls = %d, want >= 2 (immediate re-claim)", n)
	}
	// Слоты возвращены.
	waitCond(t, 2*time.Second, "semaphore drained", func() bool { return len(e.sem) == 0 })

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned %v, want nil", err)
	}
	if got := fs.completed(); len(got) != 1 || got[0] != 2 {
		t.Errorf("Complete calls = %v, want [2] (only the live job)", got)
	}
}

func TestDispatcherReleasesOnShutdown(t *testing.T) {
	// Задача, снятая при остановке, возвращается через Release без сжигания
	// попытки (§5.4, T-9 unit-часть).
	e, fs := newTestEngine(t)
	started := make(chan struct{})
	register(t, e, "t", HandlerFunc(func(ctx context.Context, _ []byte) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}))
	fs.queue = []*claimedJob{testJob(1, "t")}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Start(ctx, time.Millisecond, 1) }()
	<-started
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned %v, want nil", err)
	}
	if got := fs.released(); len(got) != 1 || got[0] != 1 {
		t.Errorf("Release calls = %v, want [1]", got)
	}
	if len(fs.failed()) != 0 {
		t.Errorf("unexpected Fail calls: %v", fs.failed())
	}
}

func TestShutdownClosesJobsCh(t *testing.T) {
	e, fs := newTestEngine(t)
	register(t, e, "t", HandlerFunc(func(context.Context, []byte) error { return nil }))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Start(ctx, time.Millisecond, 3) }()
	waitCond(t, time.Second, "engine start", func() bool { return e.started.Load() })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned %v, want nil", err)
	}
	select {
	case _, ok := <-e.jobsCh:
		if ok {
			t.Fatal("jobsCh is not closed after shutdown")
		}
	default:
		t.Fatal("jobsCh receive should not block after close")
	}
	if n := fs.claims(); n == 0 {
		t.Skip("no claim happened before shutdown — ticker interval not reached")
	}
}

func TestConcurrentWorkersRace(t *testing.T) {
	// N воркеров × fake store с мьютексом под -race (design.md §9.1).
	const jobsN = 200
	e, fs := newTestEngine(t)
	var executed atomic.Int64
	register(t, e, "t", HandlerFunc(func(context.Context, []byte) error {
		executed.Add(1)
		return nil
	}))
	for i := range int64(jobsN) {
		fs.queue = append(fs.queue, testJob(i+1, "t"))
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Start(ctx, time.Millisecond, 8) }()

	waitCond(t, 5*time.Second, "all jobs executed", func() bool { return executed.Load() == jobsN })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned %v, want nil", err)
	}
	if got := len(fs.completed()); got != jobsN {
		t.Errorf("Complete calls = %d, want %d", got, jobsN)
	}
}

func TestEnqueueValidation(t *testing.T) {
	ctx := context.Background()
	if _, err := Enqueue(ctx, nil, "", []byte("{}")); err == nil {
		t.Error("empty task type: want error")
	}
	if _, err := Enqueue(ctx, nil, strings.Repeat("x", 256), []byte("{}")); err == nil {
		t.Error("task type longer than 255 chars: want error")
	}
	if _, err := Enqueue(ctx, nil, "t", []byte("{}"), WithMaxAttempts(0)); err == nil {
		t.Error("WithMaxAttempts(0): want error")
	}
	if _, err := Enqueue(ctx, nil, "t", []byte("{}"), WithMaxReclaims(-1)); err == nil {
		t.Error("WithMaxReclaims(-1): want error")
	}
}

func TestPermanentWrapper(t *testing.T) {
	if Permanent(nil) != nil {
		t.Error("Permanent(nil) must be nil")
	}
	base := errors.New("boom")
	wrapped := Permanent(base)
	if !IsPermanent(wrapped) {
		t.Error("IsPermanent(Permanent(err)) = false")
	}
	if !errors.Is(wrapped, base) {
		t.Error("Permanent must unwrap to original error")
	}
	if IsPermanent(base) {
		t.Error("IsPermanent(plain error) = true")
	}
	if IsPermanent(nil) {
		t.Error("IsPermanent(nil) = true")
	}
	// Обёртка сохраняется через fmt.Errorf %w.
	if !IsPermanent(fmtWrap(wrapped)) {
		t.Error("IsPermanent must survive %w wrapping")
	}
}

func fmtWrap(err error) error { return errors.Join(errors.New("ctx: "), err) }

func TestTruncate(t *testing.T) {
	if got := truncate("short", 4096); got != "short" {
		t.Errorf("truncate(short) = %q", got)
	}
	long := strings.Repeat("a", 5000)
	if got := truncate(long, 4096); len(got) != 4096 {
		t.Errorf("truncate length = %d, want 4096", len(got))
	}
	// Многобайтовые руны не рвутся посимвольно.
	multibyte := strings.Repeat("ю", 3000) // 2 байта на руну
	got := truncate(multibyte, 4096)
	if len(got)%2 != 0 {
		t.Errorf("truncate cut a multibyte rune: len=%d", len(got))
	}
}

func TestClaimSQLMatchesDeadLetterContract(t *testing.T) {
	// Строка 'reclaim budget exhausted' — контракт мониторинга (§6.4, §10):
	// SQL и экспортированная константа обязаны совпадать.
	if !strings.Contains(claimSQL, ReclaimBudgetExhaustedMessage) {
		t.Fatal("claimSQL does not contain ReclaimBudgetExhaustedMessage")
	}
}
