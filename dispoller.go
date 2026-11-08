// Package dispoller — агностичная Go-библиотека фоновой обработки задач на
// PostgreSQL: постановка задач INSERT'ом (в т.ч. внутри транзакции клиента —
// Transactional Outbox), исполнение пулом воркеров через polling и
// UPDATE ... FOR UPDATE SKIP LOCKED, ретраи с backoff, lease-дедлайн попытки
// и fencing-токены против потери обновлений.
//
// Гарантия доставки — at-least-once в пределах бюджетов max_attempts и
// max_reclaims: обработчик обязан быть идемпотентным (design.md §2).
//
// Полный технический дизайн — docs/design.md, журнал решений — docs/spec-notes.md.
package dispoller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Engine — движок очереди: dispatcher, пул воркеров и семафор параллелизма
// (design.md §3, §7). Обычно один процесс держит один Engine; несколько Engine
// на одной таблице (в процессах или внутри одного) безопасны: координация —
// FOR UPDATE SKIP LOCKED.
type Engine struct {
	store store

	log            *slog.Logger
	defaultTimeout time.Duration
	backoffBase    time.Duration
	backoffMax     time.Duration

	// handlers пишутся до Start (контракт §6.2), snapshot — замороженная копия
	// для dispatcher'а и воркеров (§7.2, R-3): пишется один раз в Start до
	// старта горутин, дальше только читается.
	handlers map[string]*registeredHandler
	snapshot map[string]*registeredHandler

	started atomic.Bool

	interval time.Duration
	jobsCh   chan *claimedJob
	sem      chan struct{}

	// finalizeDelays — паузы ретраев финализации (§7.3); поле — для тестов.
	finalizeDelays []time.Duration
}

type registeredHandler struct {
	handler JobHandler
	timeout time.Duration
}

// Option настраивает движок.
type Option func(*config)

type config struct {
	log            *slog.Logger
	defaultTimeout time.Duration
	backoffBase    time.Duration
	backoffMax     time.Duration
}

// WithLogger задаёт логгер движка. Дефолт: slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(c *config) {
		if l != nil {
			c.log = l
		}
	}
}

// WithDefaultExecutionTimeout задаёт дефолтный дедлайн одной попытки
// (переопределяется на task_type через WithExecutionTimeout). Дефолт: 60s.
func WithDefaultExecutionTimeout(d time.Duration) Option {
	return func(c *config) { c.defaultTimeout = d }
}

// WithBackoff задаёт параметры экспоненциального backoff между ретраями:
// base * 2^(attempts-1) с джиттером, жёсткий потолок max (§7.5).
// Дефолт: 30s / 15m.
func WithBackoff(base, max time.Duration) Option {
	return func(c *config) {
		c.backoffBase = base
		c.backoffMax = max
	}
}

// New создаёт движок. pool != nil.
func New(pool *pgxpool.Pool, opts ...Option) *Engine {
	cfg := config{
		log:            slog.Default(),
		defaultTimeout: 60 * time.Second,
		backoffBase:    30 * time.Second,
		backoffMax:     15 * time.Minute,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Engine{
		store:          &pgStore{pool: pool},
		log:            cfg.log,
		defaultTimeout: cfg.defaultTimeout,
		backoffBase:    cfg.backoffBase,
		backoffMax:     cfg.backoffMax,
		handlers:       make(map[string]*registeredHandler),
		finalizeDelays: []time.Duration{100 * time.Millisecond, time.Second, 5 * time.Second},
	}
}

// RegisterHandler привязывает обработчик к task_type (design.md §6.2).
// Возвращает ошибку при пустом task_type, nil-обработчике, d <= 0 или повторной
// регистрации того же task_type. Не конкурентно-безопасен: вызывается до Start
// (после Start — ошибка).
func (e *Engine) RegisterHandler(taskType string, h JobHandler, opts ...HandlerOption) error {
	if e.started.Load() {
		return errors.New("dispoller: handlers must be registered before Start")
	}
	if err := validateTaskType(taskType); err != nil {
		return err
	}
	if h == nil {
		return errors.New("dispoller: handler must not be nil")
	}
	if _, dup := e.handlers[taskType]; dup {
		return fmt.Errorf("dispoller: handler for task type %q is already registered", taskType)
	}
	cfg := handlerConfig{timeout: e.defaultTimeout}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.timeout <= 0 {
		return fmt.Errorf("dispoller: execution timeout for task type %q must be > 0", taskType)
	}
	e.handlers[taskType] = &registeredHandler{handler: h, timeout: cfg.timeout}
	return nil
}

// Start блокируется до отмены ctx, затем дожидается завершения in-flight задач
// и возвращает управление. Второй вызов — ошибка.
// interval > 0 — период опроса; concurrency >= 1 — макс. число одновременно
// выполняемых задач этим процессом.
// Возвращает ошибку при некорректных аргументах (interval <= 0, concurrency < 1,
// ни один обработчик не зарегистрирован) или повторном запуске; после
// graceful остановки возвращает nil.
func (e *Engine) Start(ctx context.Context, interval time.Duration, concurrency int) error {
	if interval <= 0 {
		return errors.New("dispoller: interval must be > 0")
	}
	if concurrency < 1 {
		return errors.New("dispoller: concurrency must be >= 1")
	}
	if len(e.handlers) == 0 {
		return errors.New("dispoller: no handlers registered")
	}
	if e.defaultTimeout <= 0 {
		return errors.New("dispoller: default execution timeout must be > 0")
	}
	if e.backoffBase <= 0 || e.backoffMax < e.backoffBase {
		return errors.New("dispoller: backoff config is invalid: need base > 0 and max >= base")
	}
	if !e.started.CompareAndSwap(false, true) {
		return errors.New("dispoller: engine already started")
	}

	// Заморозка карты обработчиков копией для dispatcher'а и воркеров (§7.2, R-3).
	// Типы отсортированы — стабильный текст claim-запроса лучше кэшируется.
	e.snapshot = make(map[string]*registeredHandler, len(e.handlers))
	taskTypes := make([]string, 0, len(e.handlers))
	timeoutSecs := make([]float64, 0, len(e.handlers))
	for _, taskType := range slices.Sorted(maps.Keys(e.handlers)) {
		h := e.handlers[taskType]
		e.snapshot[taskType] = h
		taskTypes = append(taskTypes, taskType)
		timeoutSecs = append(timeoutSecs, h.timeout.Seconds())
	}

	e.interval = interval
	e.jobsCh = make(chan *claimedJob) // небуферизованный (§3)
	e.sem = make(chan struct{}, concurrency)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		e.dispatchLoop(ctx, taskTypes, timeoutSecs)
	}()
	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			e.worker(ctx)
		}()
	}

	// Блокируется до отмены ctx (горутины выходят по ctx.Done) и дожидается
	// in-flight задач (§7.4).
	wg.Wait()
	return nil
}

// dispatchLoop — единственная горутина-диспетчер (§7.2): по тику берёт слот
// семафора, делает один claim-запрос, отправляет задачу в канал. Единственный
// отправитель jobsCh — закрывает его на выходе (§7.4).
func (e *Engine) dispatchLoop(ctx context.Context, taskTypes []string, timeoutSecs []float64) {
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()
	defer close(e.jobsCh)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		// Внутренний цикл: один тик может дать несколько claim-запросов, т.к.
		// dead-letter-исход (бюджет перехватов исчерпан, §5.1) не приносит
		// задачу — повторяем немедленно, чтобы пачка stuck-строк после шторма
		// не голодала свежие pending по одному dead-letter'у на тик.
		for {
			// Слот берём ДО claim: не захватываем задачу, пока нет воркера,
			// который гарантированно её возьмёт (иначе lease прогорает впустую).
			select {
			case e.sem <- struct{}{}:
			case <-ctx.Done():
				return
			}

			job, err := e.store.Claim(ctx, taskTypes, timeoutSecs)
			if err != nil {
				<-e.sem
				if !errors.Is(err, pgx.ErrNoRows) {
					e.log.Error("claim", "err", err) // БД недоступна — ждём следующий тик
				}
				break // ждём следующий тик
			}
			if job == nil { // очередь пуста
				<-e.sem
				break
			}
			if job.deadLettered { // перехват сверх бюджета → failed в claim (§5.1)
				<-e.sem
				e.log.Warn("job dead-lettered: reclaim budget exhausted",
					"id", job.id, "task_type", job.taskType, "reclaims", job.reclaims)
				continue // немедленный повторный claim в этом же тике
			}

			select {
			case e.jobsCh <- job: // воркер заберёт; слот освободит воркер по завершении
			case <-ctx.Done():
				<-e.sem                    // слот больше не нужен: получателя не будет
				e.releaseQuietly(ctx, job) // вернуть в pending, не сжигая попытку
				return
			}
			break // задача ушла воркеру — ждём следующий тик
		}
	}
}

// worker — горутина пула: читает jobsCh и исполняет задачи (§7.3).
func (e *Engine) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-e.jobsCh:
			if !ok {
				return // канал закрыт при остановке
			}
			e.runWithSlot(ctx, job)
		}
	}
}

// runWithSlot гарантирует освобождение слота семафора даже при панике внутри
// библиотеки (паники обработчика перехватываются раньше, в execute, SN-7).
func (e *Engine) runWithSlot(ctx context.Context, job *claimedJob) {
	defer func() { <-e.sem }()
	e.run(ctx, job)
}

// run исполняет одну попытку и финализирует её (§7.3).
func (e *Engine) run(ctx context.Context, job *claimedJob) {
	// Локальный таймаут = lease из БД. Двойная защита:
	// БД-lease перехватывает упавшие процессы, локальный ctx — зависшие горутины.
	hctx, cancel := context.WithTimeout(ctx, job.timeout)
	defer cancel()

	err := e.execute(hctx, job)

	// Финализация — с ctx, отвязанным от корневого: при graceful shutdown корневой
	// ctx уже отменён, а Complete/Release/Fail выполнить всё равно нужно (SN-17).
	fctx, fcancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer fcancel()

	switch {
	case err == nil:
		e.finalize(fctx, job, func() error { return e.store.Complete(fctx, job.id, job.token) })

	case isShutdown(ctx, err): // движок останавливается, попытку не сжигаем
		e.finalize(fctx, job, func() error { return e.store.Release(fctx, job.id, job.token) })

	default:
		backoff := e.backoffFor(job.attempts) // base * 2^(attempts-1), cap, jitter
		permanent := IsPermanent(err)
		msg := truncate(err.Error(), 4096)
		e.finalize(fctx, job, func() error {
			return e.store.Fail(fctx, job.id, job.token, msg, backoff.Seconds(), permanent)
		})
	}
}

// execute запускает обработчик задачи; паника в чужом коде превращается в
// обычную ошибку и уходит в путь Fail — процесс воркера не падает (SN-7).
func (e *Engine) execute(ctx context.Context, job *claimedJob) (err error) {
	h, ok := e.snapshot[job.taskType]
	if !ok {
		// claim фильтрует по зарегистрированным типам (SN-9) — сюда попасть
		// нельзя; защита на случай рассинхронизации снапшота.
		return fmt.Errorf("no handler registered for task_type %q", job.taskType)
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return h.handler.Execute(ctx, job.payload)
}

// isShutdown отличает отмену корневого ctx движка от ошибок обработчика (§7.3):
// ошибка пришла от отмены корневого контекста, а не из внутренних механизмов
// обработчика. Эвристика неточная, но консервативно безопасная: Release лишь
// вернёт задачу в очередь, а не сожжёт попытку.
func isShutdown(ctx context.Context, err error) bool {
	return errors.Is(err, context.Canceled) && ctx.Err() != nil
}

// finalize повторяет запрос финализации до 3 раз с паузами (100ms/1s/5s), т.к.
// соединение может мигрировать. Потеря fencing-токена (0 строк) ретраям не
// подлежит — это нормальный исход (лог warning). Если не удалось — лог error
// и отказ: задача самовосстановится, когда истечёт её lease, и её перехватит
// другой воркер (двойное выполнение возможно — заложенный at-least-once, R-1).
func (e *Engine) finalize(ctx context.Context, job *claimedJob, op func() error) {
	var err error
	for attempt := 0; ; attempt++ {
		err = op()
		switch {
		case err == nil:
			return
		case errors.Is(err, errFencingLost):
			e.log.Warn("fencing token lost, finalization discarded",
				"id", job.id, "task_type", job.taskType)
			return
		}
		if attempt >= len(e.finalizeDelays) {
			break
		}
		select {
		case <-time.After(e.finalizeDelays[attempt]):
		case <-ctx.Done():
			e.log.Error("finalize context done", "err", ctx.Err(), "id", job.id)
			return
		}
	}
	e.log.Error("finalize failed; job will be reclaimed after lease expiry",
		"err", err, "id", job.id, "task_type", job.taskType)
}

// releaseQuietly возвращает задачу, снятую с отправки воркеру при остановке
// движка, в pending без сжигания попытки (§5.4). Контекст отвязан от корневого
// (SN-17): в момент вызова корневой уже отменён.
func (e *Engine) releaseQuietly(ctx context.Context, job *claimedJob) {
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if err := e.store.Release(fctx, job.id, job.token); err != nil && !errors.Is(err, errFencingLost) {
		e.log.Error("release", "err", err, "id", job.id)
	}
}

// backoffFor считает паузу ретрая (§7.5):
//
//	delay(attempts) = min(base * 2^(attempts-1) * jitter(0.8..1.2), max)
//
// jitter применяется до cap'а, max — жёсткий потолок. attempts — номер только
// что проваленной попытки (после claim). Счёт в float64: экспонента не
// переполняется даже у задач, разогнанных перехватами далеко за max_attempts.
func (e *Engine) backoffFor(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	exp := float64(e.backoffBase) * math.Pow(2, float64(attempts-1))
	maxF := float64(e.backoffMax)
	if exp*0.8 >= maxF { // даже минимальный джиттер упирается в потолок
		return e.backoffMax
	}
	jitter := 0.8 + 0.4*rand.Float64() // 0.8..1.2
	delay := time.Duration(exp * jitter)
	if delay > e.backoffMax {
		delay = e.backoffMax
	}
	return delay
}

// truncate обрезает s до n байт, не разрывая UTF-8-последовательность:
// PostgreSQL отклоняет невалидный UTF-8 в text.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := s[:n]
	for len(cut) > 0 && !utf8.ValidString(cut) {
		cut = cut[:len(cut)-1]
	}
	return cut
}
