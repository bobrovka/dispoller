package dispoller

import (
	"context"
	"errors"
	"time"
)

// JobHandler реализуется в клиентском приложении.
// Обработчик ОБЯЗАН быть идемпотентным (гарантия доставки — at-least-once,
// см. дизайн §2): в аварийных сценариях задача может быть выполнена повторно.
//
// ctx отменяется по истечении execution_timeout task_type и при остановке движка —
// обработчик должен уважать ctx и не игнорировать его отмену.
type JobHandler interface {
	Execute(ctx context.Context, payload []byte) error
}

// HandlerFunc — адаптер для функций (аналог http.HandlerFunc).
type HandlerFunc func(ctx context.Context, payload []byte) error

// Execute реализует JobHandler.
func (f HandlerFunc) Execute(ctx context.Context, payload []byte) error { return f(ctx, payload) }

// HandlerOption настраивает обработчик при регистрации.
type HandlerOption func(*handlerConfig)

type handlerConfig struct {
	timeout time.Duration
}

// WithExecutionTimeout задаёт дедлайн одной попытки для task_type.
// Должен быть > 0. Дефолт — WithDefaultExecutionTimeout движка.
func WithExecutionTimeout(d time.Duration) HandlerOption {
	return func(c *handlerConfig) { c.timeout = d }
}

// permanentError — маркер перманентной ошибки: задача переводится сразу в failed,
// без ретраев (SN-8).
type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent оборачивает ошибку: задача переводится сразу в failed, без ретраев
// (невалидный payload, «пользователь не найден» и т.п. — ретраи бессмысленны,
// SN-8). err == nil возвращает nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent сообщает, помечена ли ошибка как перманентная.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}
