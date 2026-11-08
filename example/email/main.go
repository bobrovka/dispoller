// Пример клиентского приложения dispoller (design.md §6.5): постановка задачи
// в одной транзакции с бизнес-данными (Transactional Outbox) и запуск движка.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/bobrovka/dispoller"
	"github.com/jackc/pgx/v5/pgxpool"
)

type SendEmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

// emailClient — заглушка внешнего почтового клиента приложения (SMTP/API).
type emailClient struct{}

func (c *emailClient) Send(ctx context.Context, to, subject, body string) error {
	return nil // реальная отправка
}

// SendEmailHandler — пример бизнес-обработчика.
type SendEmailHandler struct{ cli *emailClient }

func (h *SendEmailHandler) Execute(ctx context.Context, payload []byte) error {
	var p SendEmailPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return dispoller.Permanent(err) // битый payload ретраить бессмысленно
	}
	return h.cli.Send(ctx, p.To, p.Subject, p.Body)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		slog.Error("pool", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	engine := dispoller.New(pool,
		dispoller.WithLogger(slog.Default()),
		dispoller.WithDefaultExecutionTimeout(time.Minute),
	)

	if err := engine.RegisterHandler("email.send", &SendEmailHandler{cli: &emailClient{}},
		dispoller.WithExecutionTimeout(30*time.Second)); err != nil {
		slog.Error("register", "err", err)
		os.Exit(1)
	}

	// Постановка задачи в одной транзакции с бизнес-данными:
	err = pool.AcquireFunc(ctx, func(c *pgxpool.Conn) error {
		tx, err := c.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()

		payload, err := json.Marshal(SendEmailPayload{To: "user@example.com", Subject: "hi"})
		if err != nil {
			return err
		}
		if _, err := dispoller.EnqueueTx(ctx, tx, "email.send", payload); err != nil {
			return err
		}
		// ...здесь же другие бизнес-записи в той же tx...
		return tx.Commit(ctx)
	})
	if err != nil {
		slog.Error("enqueue", "err", err)
		os.Exit(1)
	}

	// Блокируется до сигнала, затем дожидается in-flight задач.
	if err := engine.Start(ctx, time.Second, 8); err != nil {
		slog.Error("engine", "err", err)
		os.Exit(1)
	}
}
