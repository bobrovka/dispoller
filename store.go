package dispoller

import (
	"context"
	crand "crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// errFencingLost — финализация затронула 0 строк: lease_token уже принадлежит
// другому воркеру (задача перехвачена), результат молча отбрасывается
// (design.md §5.2). Это нормальный исход, а не ошибка соединения: ретраям
// не подлежит.
var errFencingLost = errors.New("dispoller: fencing token lost")

// Статусы задач (design.md §4).
const (
	statusPending    = "pending"
	statusProcessing = "processing"
	statusCompleted  = "completed"
	statusFailed     = "failed"
)

// claimedJob — задача, захваченная claim-запросом (design.md §5.1). lease_token
// генерирует pgStore (UUID v4 из crypto/rand) в момент захвата; timeout
// вычисляется там же как must_be_finished_at − now() — часы приложения, только
// для локального таймаута обработчика (§2 «Часы»), с клампом снизу (не меньше 1s).
type claimedJob struct {
	id          int64
	taskType    string
	payload     []byte
	attempts    int
	maxAttempts int
	reclaims    int
	maxReclaims int
	token       [16]byte
	timeout     time.Duration
	// deadLettered → задача ушла в failed по бюджету перехватов прямо в claim
	// (§5.1, SN-18): token нулевой, обработчик не запускать, слот освободить
	// и повторить claim.
	deadLettered bool
}

// store — тонкий слой над SQL (design.md §3): все операции §5. Реализация pgStore
// работает на pgxpool.Pool; в unit-тестах подменяется in-memory фейком (§9).
type store interface {
	// Claim атомарно захватывает одну задачу (§5.1). nil, nil — очередь пуста.
	Claim(ctx context.Context, taskTypes []string, timeoutSecs []float64) (*claimedJob, error)
	// Complete финализирует задачу успешно (§5.2); errFencingLost — 0 строк.
	Complete(ctx context.Context, id int64, token [16]byte) error
	// Fail финализирует попытку ошибкой с backoff (§5.3); errFencingLost — 0 строк.
	Fail(ctx context.Context, id int64, token [16]byte, msg string, backoffSecs float64, force bool) error
	// Release возвращает задачу в очередь без сжигания попытки (§5.4).
	Release(ctx context.Context, id int64, token [16]byte) error
	// Enqueue вставляет задачу (§5.5).
	Enqueue(ctx context.Context, taskType string, payload []byte, maxAttempts, maxReclaims int, runAt *time.Time) (int64, error)
}

// pgStore — реализация store на pgxpool.Pool.
type pgStore struct {
	pool *pgxpool.Pool
}

var _ store = (*pgStore)(nil)

// newLeaseToken генерирует UUID v4 из crypto/rand — fencing-токен владельца
// задачи (design.md §2, §7.3).
func newLeaseToken() ([16]byte, error) {
	var b [16]byte
	if _, err := crand.Read(b[:]); err != nil {
		return b, fmt.Errorf("dispoller: read crypto/rand: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant RFC 4122
	return b, nil
}

// Claim захватывает одну задачу (§5.1). Токен генерируется до запроса и
// предъявляется в UPDATE; пустой результат — (nil, nil), «очередь пуста».
func (s *pgStore) Claim(ctx context.Context, taskTypes []string, timeoutSecs []float64) (*claimedJob, error) {
	token, err := newLeaseToken()
	if err != nil {
		return nil, err
	}
	row := s.pool.QueryRow(ctx, claimSQL, taskTypes, timeoutSecs, pgtype.UUID{Bytes: token, Valid: true})
	var (
		job      claimedJob
		lease    pgtype.UUID
		deadline pgtype.Timestamptz
		status   string
	)
	err = row.Scan(&job.id, &job.taskType, &job.payload, &job.attempts, &job.maxAttempts,
		&job.reclaims, &job.maxReclaims, &lease, &deadline, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("dispoller: claim: %w", err)
	}
	job.token = token
	if lease.Valid {
		// Локальный таймаут = lease из БД; кламп снизу — не меньше 1s (§7.3).
		if d := time.Until(deadline.Time); d > time.Second {
			job.timeout = d
		} else {
			job.timeout = time.Second
		}
	} else {
		// Dead-letter прямо в claim (§5.1, SN-18): lease_token IS NULL + failed.
		job.deadLettered = status == statusFailed
	}
	return &job, nil
}

// Complete помечает задачу completed (§5.2).
func (s *pgStore) Complete(ctx context.Context, id int64, token [16]byte) error {
	tag, err := s.pool.Exec(ctx, completeSQL, id, pgtype.UUID{Bytes: token, Valid: true})
	if err != nil {
		return fmt.Errorf("dispoller: complete: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return errFencingLost
	}
	return nil
}

// Fail записывает ошибку попытки и планирует ретрай (§5.3).
func (s *pgStore) Fail(ctx context.Context, id int64, token [16]byte, msg string, backoffSecs float64, force bool) error {
	tag, err := s.pool.Exec(ctx, failSQL, id, pgtype.UUID{Bytes: token, Valid: true}, msg, backoffSecs, force)
	if err != nil {
		return fmt.Errorf("dispoller: fail: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return errFencingLost
	}
	return nil
}

// Release возвращает задачу в pending без сжигания попытки (§5.4).
func (s *pgStore) Release(ctx context.Context, id int64, token [16]byte) error {
	tag, err := s.pool.Exec(ctx, releaseSQL, id, pgtype.UUID{Bytes: token, Valid: true})
	if err != nil {
		return fmt.Errorf("dispoller: release: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return errFencingLost
	}
	return nil
}

// Enqueue вставляет задачу (§5.5).
func (s *pgStore) Enqueue(ctx context.Context, taskType string, payload []byte, maxAttempts, maxReclaims int, runAt *time.Time) (int64, error) {
	return enqueueRow(ctx, s.pool, taskType, payload, maxAttempts, maxReclaims, runAt)
}
