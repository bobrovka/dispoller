package runner

// Тесты без БД: валидация конфигурации Run (ошибки — синхронно, до запуска
// горутин), регрессия «Run не висит при синхронной ошибке Engine.Start» и
// обработчики служебного HTTP с фейковым ping. Конвейер против живой БД —
// integration_test.go.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bobrovka/dispoller"
)

// lazyPool — пул, который не трогает сеть до первого запроса: все кейсы ниже
// завершаются раньше любого обращения к БД.
func lazyPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://nobody@127.0.0.1:1/nodb?sslmode=disable")
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// noopRegister — валидный колбек регистрации для тестов ошибок после него.
func noopRegister(*dispoller.Engine) error { return nil }

// runSync прогоняет Run с таймаутом: зависание (возвращение ошибки из горутины
// вместо синхронного выхода) проваливает тест, а не вешает прогон.
func runSync(t *testing.T, r *Runner, ctx context.Context) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("Run не вернулся за 10s (ожидались синхронные ошибки без горутин)")
		return nil
	}
}

// TestRunValidatesConfig: ошибки конфигурации возвращаются из Run синхронно.
func TestRunValidatesConfig(t *testing.T) {
	cases := []struct {
		name string
		r    *Runner
		want string
	}{
		{
			name: "nil pool",
			r: New(nil,
				WithInterval(time.Second), WithConcurrency(1), WithRegister(noopRegister)),
			want: "pool must not be nil",
		},
		{
			name: "no register",
			r: New(lazyPool(t),
				WithInterval(time.Second), WithConcurrency(1)),
			want: "WithRegister is required",
		},
		{
			name: "purge interval",
			r: New(lazyPool(t),
				WithInterval(time.Second), WithConcurrency(1), WithRegister(noopRegister),
				WithPurge(0, time.Hour, 100)),
			want: "purge interval",
		},
		{
			name: "purge retention",
			r: New(lazyPool(t),
				WithInterval(time.Second), WithConcurrency(1), WithRegister(noopRegister),
				WithPurge(time.Hour, -time.Second, 100)),
			want: "purge retention",
		},
		{
			name: "purge limit",
			r: New(lazyPool(t),
				WithInterval(time.Second), WithConcurrency(1), WithRegister(noopRegister),
				WithPurge(time.Hour, time.Hour, 0)),
			want: "purge limit",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := runSync(t, tc.r, context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("хотим ошибку с %q, получили: %v", tc.want, err)
			}
		})
	}
}

// TestRunReturnsStartError — регрессия: Engine.Start возвращает ошибку
// конфигурации (interval <= 0) синхронно; Run обязан размазать запущенный
// purge-цикл и вернуть эту ошибку, а не зависнуть в purge.Wait() до отмены
// контекста (которой при ошибке может не быть).
func TestRunReturnsStartError(t *testing.T) {
	r := New(lazyPool(t),
		WithInterval(0), // невалидный конфиг: Start падает сразу
		WithConcurrency(1),
		WithRegister(noopRegister),
		WithPurge(time.Hour, time.Hour, 100), // purge запущен — выйти нужно через отмену его ctx
	)

	err := runSync(t, r, context.Background())
	if err == nil || !strings.Contains(err.Error(), "interval") {
		t.Fatalf("хотим ошибку валидации interval из engine.Start, получили: %v", err)
	}
}

// TestRunListenError: бинд на занятый адрес — ошибка конфигурации, Run обязан
// упасть сразу, а не молча работать без healthcheck.
func TestRunListenError(t *testing.T) {
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen :0: %v", err)
	}
	defer ln.Close()

	r := New(lazyPool(t),
		WithInterval(time.Second), WithConcurrency(1), WithRegister(noopRegister),
		WithHTTPAddr(ln.Addr().String()))

	err = runSync(t, r, context.Background())
	if err == nil || !strings.Contains(err.Error(), "listen") {
		t.Fatalf("хотим ошибку listen на занятом адресе, получили: %v", err)
	}
}

// TestRunSchemaCheckUnreachable: WithSchemaCheck при недоступной БД — ошибка
// с именем проверки, а не стектрейсом движка.
func TestRunSchemaCheckUnreachable(t *testing.T) {
	r := New(lazyPool(t),
		WithInterval(time.Second), WithConcurrency(1), WithRegister(noopRegister),
		WithSchemaCheck())

	err := runSync(t, r, context.Background())
	if err == nil || !strings.Contains(err.Error(), "check background.jobs") {
		t.Fatalf("хотим ошибку check background.jobs, получили: %v", err)
	}
}

// TestRunRegisterError: ошибка колбека регистрации — до старта HTTP и движка.
func TestRunRegisterError(t *testing.T) {
	boom := errors.New("boom")
	r := New(lazyPool(t),
		WithInterval(time.Second), WithConcurrency(1),
		WithRegister(func(*dispoller.Engine) error { return boom }))

	err := runSync(t, r, context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("хотим ошибку регистрации, получили: %v", err)
	}
}

// --- служебный HTTP (фейковый ping, без БД) --------------------------------

func httpGet(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(body)
}

func TestHTTPMux(t *testing.T) {
	muxOK := newHTTPMux(func(context.Context) error { return nil }, slog.Default())
	srv := httptest.NewServer(muxOK)
	defer srv.Close()

	t.Run("healthz", func(t *testing.T) {
		if code, body := httpGet(t, srv.URL+"/healthz"); code != http.StatusOK || body != "ok" {
			t.Errorf("healthz: status=%d body=%q, хотим 200 ok", code, body)
		}
	})
	t.Run("readyz ok", func(t *testing.T) {
		if code, body := httpGet(t, srv.URL+"/readyz"); code != http.StatusOK || body != "ok" {
			t.Errorf("readyz: status=%d body=%q, хотим 200 ok", code, body)
		}
	})
	t.Run("unknown path", func(t *testing.T) {
		if code, _ := httpGet(t, srv.URL+"/nope"); code != http.StatusNotFound {
			t.Errorf("GET /nope: status=%d, хотим 404", code)
		}
	})
	t.Run("pprof index", func(t *testing.T) {
		// Index отдаёт html-список профилей; сами профили — забота stdlib.
		if code, _ := httpGet(t, srv.URL+"/debug/pprof/"); code != http.StatusOK {
			t.Errorf("GET /debug/pprof/: status=%d, хотим 200", code)
		}
	})

	t.Run("readyz 503 на лежащей БД", func(t *testing.T) {
		muxDown := newHTTPMux(func(context.Context) error { return errors.New("conn refused") }, slog.Default())
		srvDown := httptest.NewServer(muxDown)
		defer srvDown.Close()
		if code, _ := httpGet(t, srvDown.URL+"/readyz"); code != http.StatusServiceUnavailable {
			t.Errorf("readyz с упавшим ping: status=%d, хотим 503", code)
		}
	})
}
