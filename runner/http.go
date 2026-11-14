// Служебный HTTP-сервер воркера: liveness (/healthz), readiness с пингом БД
// (/readyz) и pprof для диагностики в рантайме.
package runner

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"time"
)

// readyzPingTimeout — бюджет пинга БД в /readyz: заметно меньше типичных
// таймаутов healthcheck'ов оркестратора (например 5s у Docker), чтобы ответ
// успевал прийти.
const readyzPingTimeout = 2 * time.Second

// newHTTPMux собирает служебные обработчики; ping — проверка БД для /readyz
// (обычно pool.Ping; отдельный параметр — для тестов без БД).
// /healthz — чистый liveness (процесс жив, без БД): рестартовать воркер из-за
// лежащей БД бессмысленно, БД проверяет только /readyz. pprof регистрируется
// на этом mux явно, а не на http.DefaultServeMux, и слушатель обязан быть на
// localhost или закрытом контуре: heap-дамп содержит содержимое памяти,
// включая секреты.
func newHTTPMux(ping func(context.Context) error, log *slog.Logger) *http.ServeMux {
	m := http.NewServeMux()
	m.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	m.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readyzPingTimeout)
		defer cancel()
		if err := ping(ctx); err != nil {
			log.Error("readyz: ping postgres", "err", err)
			http.Error(w, "postgres: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok"))
	})
	m.HandleFunc("GET /debug/pprof/", pprof.Index)
	m.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	m.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	m.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	m.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	return m
}
