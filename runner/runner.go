// Package runner — опциональная обвязка ядра dispoller для типового
// процесса-воркера: движок очереди, периодическая очистка completed-задач
// и служебный HTTP (/healthz, /readyz, pprof) с выверенным порядком запуска
// и остановки.
//
// Слой аддитивный: runner построен только на публичном API пакета dispoller
// (Engine, Enqueue, Purge), ядро о нём не знает. Проект может взять Runner
// целиком, а может собрать свою обвязку напрямую на Engine — как example/email
// — результат идентичен.
package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/bobrovka/dispoller"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Runner оркестрирует типовой процесс-воркер поверх ядра dispoller: создаёт
// Engine, регистрирует обработчики проекта, поднимает служебный HTTP и цикл
// очистки и блокируется в Run до отмены контекста. Пул соединений принадлежит
// вызывающему: OpenPool помогает его собрать, Close — на вызывающем.
type Runner struct {
	pool *pgxpool.Pool
	log  *slog.Logger

	interval    time.Duration
	concurrency int
	register    func(*dispoller.Engine) error
	engineOpts  []dispoller.Option

	purgeEnabled   bool
	purgeInterval  time.Duration
	purgeRetention time.Duration
	purgeLimit     int64

	httpAddr    string
	schemaCheck bool
}

// Option настраивает Runner.
type Option func(*Runner)

// WithLogger задаёт логгер. Дефолт: slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(r *Runner) {
		if l != nil {
			r.log = l
		}
	}
}

// WithInterval задаёт период опроса очереди (передаётся в Engine.Start).
// Обязателен, > 0; валидируется Engine.Start.
func WithInterval(d time.Duration) Option {
	return func(r *Runner) { r.interval = d }
}

// WithConcurrency задаёт максимальное число одновременно исполняемых задач
// этим процессом. Обязателен, >= 1; валидируется Engine.Start.
func WithConcurrency(n int) Option {
	return func(r *Runner) { r.concurrency = n }
}

// WithRegister подключает обработчики проекта: колбек вызывается один раз до
// старта движка, внутри — Engine.RegisterHandler по каждому task_type.
// Обязателен.
func WithRegister(f func(*dispoller.Engine) error) Option {
	return func(r *Runner) { r.register = f }
}

// WithEngineOptions прокидывает опции движка в dispoller.New (WithBackoff,
// WithDefaultExecutionTimeout и т.п.). Дефолтов достаточно большинству
// проектов.
func WithEngineOptions(opts ...dispoller.Option) Option {
	return func(r *Runner) { r.engineOpts = append(r.engineOpts, opts...) }
}

// WithPurge включает периодическую очистку completed-задач (dispoller.Purge):
// раз в interval удаляются строки старше retention, не более limit за раз.
// Без опции очистки нет; failed не удаляются (диагностика).
func WithPurge(interval, retention time.Duration, limit int64) Option {
	return func(r *Runner) {
		r.purgeEnabled = true
		r.purgeInterval = interval
		r.purgeRetention = retention
		r.purgeLimit = limit
	}
}

// WithHTTPAddr включает служебный HTTP (healthz/readyz/pprof) на addr
// (например "localhost:8011"). Без опции (или при addr == "") HTTP нет.
// Адрес наружу не публиковать: heap-дамп pprof содержит содержимое памяти,
// включая секреты.
func WithHTTPAddr(addr string) Option {
	return func(r *Runner) { r.httpAddr = addr }
}

// WithSchemaCheck требует наличия таблицы background.jobs на старте: без
// очереди воркер бессмысленен — ошибка конфигурации видна сразу, а не первыми
// claim-ошибками движка. Библиотека мигратором не является, схему применяет
// проект.
func WithSchemaCheck() Option {
	return func(r *Runner) { r.schemaCheck = true }
}

// New собирает Runner. Валидация значений — в Run: ошибки конфигурации
// возвращаются из него синхронно, до запуска горутин.
func New(pool *pgxpool.Pool, opts ...Option) *Runner {
	r := &Runner{pool: pool, log: slog.Default()}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// OpenPool открывает пул соединений к PostgreSQL для воркера: MaxConns не
// меньше concurrency + 2 (claim диспетчера и финализации попыток — §8), с
// проверкой доступности БД (10s). Закрывает пул вызывающий.
func OpenPool(databaseURL string, concurrency int) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("runner: parse DATABASE_URL: %w", err)
	}
	if max := int32(concurrency + 2); poolCfg.MaxConns < max {
		poolCfg.MaxConns = max
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return nil, fmt.Errorf("runner: open postgres pool: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("runner: ping postgres: %w", err)
	}
	return pool, nil
}

// Run запускает движок с обвязкой и блокируется до отмены ctx, затем
// дожидается завершения in-flight задач (см. dispoller.Engine.Start), глушит
// служебный HTTP и цикл очистки.
//
// Порядок подобран под эксплуатацию: слушатель HTTP биндится синхронно
// (ошибка бинда — ошибка конфигурации, она должна быть видна сразу, а не из
// горутины), healthcheck отвечает до самого конца дрена in-flight задач;
// цикл очистки живёт на производном от ctx контексте — синхронная ошибка
// Engine.Start (невалидный interval/concurrency) не оставляет его висеть.
func (r *Runner) Run(ctx context.Context) error {
	if r.pool == nil {
		return errors.New("runner: pool must not be nil")
	}
	if r.register == nil {
		return errors.New("runner: WithRegister is required")
	}
	if r.purgeEnabled {
		if r.purgeInterval <= 0 {
			return fmt.Errorf("runner: purge interval must be > 0, got %s", r.purgeInterval)
		}
		if r.purgeRetention < 0 {
			return fmt.Errorf("runner: purge retention must be >= 0, got %s", r.purgeRetention)
		}
		if r.purgeLimit < 1 {
			return fmt.Errorf("runner: purge limit must be >= 1, got %d", r.purgeLimit)
		}
	}
	if r.schemaCheck {
		// ctx отвязан от отмены: проверка — часть старта, и отменённый к моменту
		// вызова ctx процесса не должен превращать её в ложную ошибку про схему.
		if err := CheckQueueSchema(context.WithoutCancel(ctx), r.pool); err != nil {
			return err
		}
	}

	engine := dispoller.New(r.pool, append([]dispoller.Option{dispoller.WithLogger(r.log)}, r.engineOpts...)...)
	if err := r.register(engine); err != nil {
		return fmt.Errorf("runner: register handlers: %w", err)
	}

	// Служебный HTTP (/healthz, /readyz, pprof): слушатель создаём
	// синхронно — ошибка бинда это ошибка конфигурации, она должна быть
	// видна сразу, а не из горутины. WriteTimeout не ставим сознательно:
	// профили собираются дольше секунды (например ?seconds=30).
	var httpSrv *http.Server
	if r.httpAddr != "" {
		ln, err := net.Listen("tcp", r.httpAddr)
		if err != nil {
			return fmt.Errorf("runner: listen %s: %w", r.httpAddr, err)
		}
		httpSrv = &http.Server{Handler: newHTTPMux(r.pool.Ping, r.log), ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				r.log.Error("ops http server", "err", err)
			}
		}()
		r.log.Info("ops http: healthz/readyz/pprof", "addr", ln.Addr().String())
	}

	// Цикл очистки — на производном контексте: Engine.Start может вернуть
	// синхронную ошибку конфигурации (interval/concurrency) задолго до
	// отмены ctx, и purge.Wait() не должен висеть после этого вечно.
	purgeCtx, cancelPurge := context.WithCancel(ctx)
	var purge sync.WaitGroup
	if r.purgeEnabled {
		purge.Add(1)
		go func() {
			defer purge.Done()
			r.purgeLoop(purgeCtx)
		}()
	}

	err := engine.Start(ctx, r.interval, r.concurrency)
	cancelPurge()       // иначе при синхронной ошибке Start Run зависнет в purge.Wait()
	if httpSrv != nil { // движок дрейнил in-flight задачи — health отвечал до конца
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if serr := httpSrv.Shutdown(shutdownCtx); serr != nil {
			r.log.Error("ops http shutdown", "err", serr)
		}
		cancel()
	}
	purge.Wait()
	return err
}

// CheckQueueSchema проверяет существование таблицы background.jobs (бюджет
// 10s). Без таблицы очереди воркер и постановка задач не работают; миграции —
// ответственность проекта. Публичный хелпер — для fail-fast вне Run (например,
// CLI постановки задач до старта воркера); Run с WithSchemaCheck зовёт его сам.
func CheckQueueSchema(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return errors.New("runner: pool must not be nil")
	}
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var exists bool
	if err := pool.QueryRow(checkCtx, `SELECT EXISTS (
		SELECT 1 FROM information_schema.tables
		WHERE table_schema = 'background' AND table_name = 'jobs')`).Scan(&exists); err != nil {
		return fmt.Errorf("runner: check background.jobs: %w", err)
	}
	if !exists {
		return errors.New("runner: background.jobs table is missing: apply the queue schema before starting (the library is not a migrator, see schema/schema.sql)")
	}
	return nil
}

// purgeLoop раз в purgeInterval удаляет completed-задачи старше
// purgeRetention, не более purgeLimit строк за раз (dispoller.Purge).
func (r *Runner) purgeLoop(ctx context.Context) {
	ticker := time.NewTicker(r.purgeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := dispoller.Purge(ctx, r.pool, r.purgeRetention, r.purgeLimit)
			if err != nil {
				if ctx.Err() == nil { // отмена ctx при остановке — не ошибка очистки
					r.log.Error("purge", "err", err) // БД недоступна — ждём следующий тик
				}
				continue
			}
			if n > 0 {
				r.log.Info("purge: deleted completed jobs", "count", n)
			}
		}
	}
}
