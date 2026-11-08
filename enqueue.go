package dispoller

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Дефолты Go-API, зеркалят DEFAULT колонок background.jobs (design.md §4, §8).
const (
	defaultMaxAttempts = 3
	defaultMaxReclaims = 10
)

// ReclaimBudgetExhaustedMessage — значение error_message, которое claim
// проставляет задаче, переведённой в failed по исчерпанию бюджета перехватов
// (dead-letter, design.md §5.1, SN-18). Строка — контракт мониторинга: алерт
// dead-letter'а (design.md §10) матчится по status='failed' AND error_message =
// этой константе, а не по счётчику reclaims (после dead-letter он равен
// max_reclaims + 1). Изменение значения — ломающее изменение для алертов
// клиентов.
const ReclaimBudgetExhaustedMessage = "reclaim budget exhausted"

// EnqueueOption настраивает постановку задачи.
type EnqueueOption func(*enqueueConfig)

type enqueueConfig struct {
	maxAttempts int
	maxReclaims int
	runAt       *time.Time
}

// WithMaxAttempts задаёт бюджет штатных запусков обработчика. Дефолт: 3
// (DEFAULT колонки).
func WithMaxAttempts(n int) EnqueueOption {
	return func(c *enqueueConfig) { c.maxAttempts = n }
}

// WithMaxReclaims задаёт бюджет перехватов зависших задач (SN-18).
// Дефолт: 10 (DEFAULT колонки).
func WithMaxReclaims(n int) EnqueueOption {
	return func(c *enqueueConfig) { c.maxReclaims = n }
}

// WithRunAt задаёт отложенный запуск (не раньше t). Дефолт: now().
func WithRunAt(t time.Time) EnqueueOption {
	return func(c *enqueueConfig) { c.runAt = &t }
}

func newEnqueueConfig(opts []EnqueueOption) (*enqueueConfig, error) {
	cfg := &enqueueConfig{
		maxAttempts: defaultMaxAttempts,
		maxReclaims: defaultMaxReclaims,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.maxAttempts < 1 {
		return nil, fmt.Errorf("dispoller: WithMaxAttempts: must be >= 1, got %d", cfg.maxAttempts)
	}
	if cfg.maxReclaims < 1 {
		return nil, fmt.Errorf("dispoller: WithMaxReclaims: must be >= 1, got %d", cfg.maxReclaims)
	}
	return cfg, nil
}

// validateTaskType проверяет task_type до похода в БД: длина 1..255 символов —
// как CONSTRAINT jobs_task_type_chk (design.md §4).
func validateTaskType(taskType string) error {
	if taskType == "" {
		return errors.New("dispoller: task type must not be empty")
	}
	if utf8.RuneCountInString(taskType) > 255 {
		return fmt.Errorf("dispoller: task type must be at most 255 characters, got %d", utf8.RuneCountInString(taskType))
	}
	return nil
}

// querier — минимум, нужный enqueueRow: подходит и pgxpool.Pool, и pgx.Tx.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Enqueue вставляет задачу отдельной транзакцией пула.
func Enqueue(ctx context.Context, pool *pgxpool.Pool, taskType string, payload []byte, opts ...EnqueueOption) (int64, error) {
	if pool == nil {
		return 0, errors.New("dispoller: pool must not be nil")
	}
	cfg, err := newEnqueueConfig(opts)
	if err != nil {
		return 0, err
	}
	return enqueueRow(ctx, pool, taskType, payload, cfg.maxAttempts, cfg.maxReclaims, cfg.runAt)
}

// EnqueueTx вставляет задачу в транзакцию клиента (Transactional Outbox,
// design.md §2, SN-2): откат бизнес-транзакции откатывает и задачу (и наоборот —
// задача не видна воркерам до коммита). Коммит/rollback — ответственность
// вызывающего.
func EnqueueTx(ctx context.Context, tx pgx.Tx, taskType string, payload []byte, opts ...EnqueueOption) (int64, error) {
	if tx == nil {
		return 0, errors.New("dispoller: tx must not be nil")
	}
	cfg, err := newEnqueueConfig(opts)
	if err != nil {
		return 0, err
	}
	return enqueueRow(ctx, tx, taskType, payload, cfg.maxAttempts, cfg.maxReclaims, cfg.runAt)
}

func enqueueRow(ctx context.Context, q querier, taskType string, payload []byte, maxAttempts, maxReclaims int, runAt *time.Time) (int64, error) {
	if err := validateTaskType(taskType); err != nil {
		return 0, err
	}
	if len(payload) == 0 {
		payload = []byte("{}") // NULL/пустой payload нарушил бы NOT NULL колонки
	}
	var id int64
	err := q.QueryRow(ctx, enqueueSQL, taskType, payload, maxAttempts, maxReclaims, runAt).Scan(&id)
	if err != nil {
		return 0, wrapCheckError(fmt.Errorf("dispoller: enqueue: %w", err))
	}
	return id, nil
}

// wrapCheckError добавляет имя CHECK-ограничения к ошибке постановки (§6.4):
// гонка с параллельной записью могла сломать инварианты до валидации.
func wrapCheckError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23514" { // check_violation
		name := pgErr.ConstraintName
		if name == "" {
			name = "unnamed"
		}
		return fmt.Errorf("%w (constraint %s)", err, name)
	}
	return err
}

// Purge удаляет completed-задачи старше olderThan, батчами не более limit строк
// (SQL — §5.6), и возвращает число удалённых строк. Чистятся только completed;
// failed не трогается (диагностика). Не подключена к движку — вызывайте по
// расписанию (§10).
func Purge(ctx context.Context, pool *pgxpool.Pool, olderThan time.Duration, limit int64) (int64, error) {
	if pool == nil {
		return 0, errors.New("dispoller: pool must not be nil")
	}
	if olderThan < 0 {
		return 0, fmt.Errorf("dispoller: olderThan must be >= 0, got %s", olderThan)
	}
	if limit < 1 {
		return 0, fmt.Errorf("dispoller: limit must be >= 1, got %d", limit)
	}
	tag, err := pool.Exec(ctx, purgeSQL, olderThan.Seconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("dispoller: purge: %w", err)
	}
	return tag.RowsAffected(), nil
}

// Requeue возвращает failed-задачу в очередь с чистого листа: pending,
// attempts=0, reclaims=0 (SQL — §5.7). Назначение — replay после массового
// dead-letter'а, например когда инфра-шторм выжег бюджеты перехватов (SN-18).
// Возвращает ошибку, если задача не найдена или не в failed.
func Requeue(ctx context.Context, pool *pgxpool.Pool, id int64) error {
	if pool == nil {
		return errors.New("dispoller: pool must not be nil")
	}
	tag, err := pool.Exec(ctx, requeueSQL, id)
	if err != nil {
		return fmt.Errorf("dispoller: requeue: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("dispoller: requeue: job %d not found or not in failed status", id)
	}
	return nil
}
