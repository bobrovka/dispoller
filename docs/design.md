# dispoller — технический дизайн

Библиотека фоновой обработки задач для Go на базе PostgreSQL (polling + `FOR UPDATE SKIP LOCKED`).

Статус: проектный документ (до реализации). Анализ противоречий ТЗ и отклонений от него —
в [spec-notes.md](spec-notes.md); ссылки вида «SN-7» ведут к пунктам того документа.

---

## Содержание

1. [Цель и рамки](#1-цель-и-рамки)
2. [Терминология и гарантии доставки](#2-терминология-и-гарантии-доставки)
3. [Архитектура](#3-архитектура)
4. [Модель данных (DDL)](#4-модель-данных-ddl)
5. [SQL-операции (DML)](#5-sql-операции-dml)
6. [Go API: интерфейсы и типы](#6-go-api-интерфейсы-и-типы)
7. [Движок: конкурентность и жизненный цикл](#7-движок-конкурентность-и-жизненный-цикл)
8. [Конфигурация и значения по умолчанию](#8-конфигурация-и-значения-по-умолчанию)
9. [План тестирования](#9-план-тестирования)
10. [Эксплуатация](#10-эксплуатация)
11. [Сводные компромиссы](#11-сводные-компромиссы)
12. [Roadmap (v2 и далее)](#12-roadmap-v2-и-далее)
13. [Приложение A: DDL строго по ТЗ](#13-приложение-a-ddl-строго-по-тз)
14. [Приложение B: сравнение с существующими решениями](#14-приложение-b-сравнение-с-существующими-решениями)

---

## 1. Цель и рамки

**Цель** — изолированная, переиспользуемая Go-библиотека (один модуль, без бизнес-логики),
реализующая очередь фоновых задач поверх PostgreSQL:

- постановка задачи — `INSERT` в таблицу `background.jobs`, в т.ч. **внутри транзакции
  клиента** (суть Transactional Outbox, см. §2);
- исполнение — in-process пул воркеров, поллящих таблицу через
  `UPDATE ... FOR UPDATE SKIP LOCKED`;
- надёжность — счётчик попыток, дедлайн выполнения (lease) с автоматическим
  перехватом «зависших» задач, защита от потери обновлений (fencing).

**В рамках:**

| Да | Нет |
|---|---|
| Одна таблица в одной PostgreSQL-БД | Шардирование, несколько БД, брокеры (Kafka/RabbitMQ) |
| At-least-once доставка | Exactly-once доставка (см. §2) |
| Ретраи с backoff, дедлайн попытки | Оркестрация, DAG, cron-расписания (v2: см. §12) |
| Горизонтальное масштабирование воркеров (несколько процессов на одной таблице) | Приоритеты очередей, изоляция task_type по пропускной способности (v2) |
| SQL-скрипт схемы + API для миграций клиента | Мигратор (миграции применяет клиент) |

**Стек:** Go (минимум 1.23; в `go.mod` фиксируется актуальная стабильная), `pgx/v5`
(`pgxpool.Pool`), стандартный `log/slog`. Внешних зависимостей, кроме `pgx`, нет.

---

## 2. Терминология и гарантии доставки

**Transactional Outbox.** Классический паттерн: бизнес-данные и «сообщение о работе»
записываются в одной транзакции БД, а отдельный процесс (relay) забирает сообщения и
исполняет их. В `dispoller` роль relay выполняют воркеры самой библиотеки, а таблица
`background.jobs` — и есть outbox. Из этого следует ключевое API-требование, отсутствующее
в ТЗ: библиотека обязана уметь вставлять задачу **через `pgx.Tx` клиента**
(`EnqueueTx`), иначе «transactional» в названии паттерна теряет смысл (SN-2).

**Гарантия — at-least-once.** Задача будет выполнена *как минимум* один раз; в аварийных
сценариях (воркер завис и его lease истёк, а задача на самом деле ещё дорабатывала) —
возможно **повторное** выполнение. Поэтому контракт библиотеки для клиентского кода:

> **Обработчик обязан быть идемпотентным.** Библиотека даёт at-least-once;
> exactly-once на стороне БД без бизнес-ключа идемпотентности не достижим.

Это главная цена выбранной архитектуры, и она неустранима в рамках ТЗ (фиксируется как
риск R-1, SN-15).

**Lease (аренда задачи).** При захвате воркер проставляет `must_be_finished_at = now() + execution_timeout`.
Если к этому моменту задача не финализирована — она считается потерянной (воркер упал или
завис) и может быть перехвачена другим воркером.

**Fencing token.** Токен (`lease_token UUID`), который воркер получает при захвате и
предъявляет при финализации. `UPDATE ... WHERE lease_token = $2` гарантирует, что
«опоздавший» старый воркер не перезапишет состояние задачи, уже перехваченной другим
воркером (защита от lost update; подробное обоснование — SN-4).

**Упорядочивание.** Строгого FIFO нет: `ORDER BY id` в claim-запросе даёт лишь
приблизительный порядок «старые раньше новых», который дополнительно искажается
`SKIP LOCKED` и ретраями. Если порядок важен — его обеспечивает клиент внутри одного
payload'а.

**Часы.** Все решения о времени (готовность к ретраю, истечение lease) принимает **БД**
через `now()` в SQL. Часы приложения используются только для локального таймаута
контекста обработчика и логов. Это устраняет проблемы рассинхронизации NTP между
инстансами воркеров.

---

## 3. Архитектура

Компоненты библиотеки:

```
┌─────────────────────────────── Engine ────────────────────────────────┐
│                                                                      │
│  handlers: map[task_type] → handler + execution_timeout              │
│                                                                      │
│  ┌─────────────┐   claim (1 job)   ┌──────────────────────────────┐  │
│  │ Dispatcher  │──────────────────▶│ jobsCh chan *claimedJob      │  │
│  │ (1 горутина,│                   │ (небуферизованный)           │  │
│  │  тикер      │   sem chan{}(N)   ├──────────────────────────────┤  │
│  │  interval)  │◀─────────────────▶│ Worker ×N                    │  │
│  └─────────────┘  «есть слот»      │ (горутины из пула)           │  │
│                                    └──────────┬───────────────────┘  │
│                                               │ Execute + финализация│
└───────────────────────────────────────────────┼──────────────────────┘
                                                ▼
                                         PostgreSQL
                                    background.jobs
```

- **Dispatcher** — одна горутина. По тику (или сразу после старта) берёт свободный слот
  семафора, делает **один** claim-запрос, отправляет задачу в канал. Если задач нет —
  отпускает слот и ждёт следующего тика.
- **Семафор `sem` (канал ёмкости `concurrency`)** — ограничитель параллелизма. Слот
  берётся **до** claim: dispatcher никогда не захватывает задачу, если нет гарантии, что
  её возьмёт свободный воркер (иначе lease будет «гореть», пока задача ждёт в канале).
- **Пул из N воркеров** — фиксированные горутины, читающие небуферизованный канал.
  Инвариант: свободный слот семафора ⟺ есть воркер, не занятый задачей.
- **store** — тонкий слой над SQL (интерфейс из 5 методов). Реализация `pgStore` на
  `pgxpool.Pool`; в unit-тестах подменяется in-memory фейком (§9).

Один процесс может иметь один `Engine`. Несколько процессов (горизонтальное
масштабирование) безопасно работают с одной таблицей: `FOR UPDATE SKIP LOCKED` —
это и есть координация.

Поток данных (happy path):

```mermaid
sequenceDiagram
    participant C as Клиент (EnqueueTx)
    participant D as Dispatcher
    participant DB as PostgreSQL
    participant W as Worker
    C->>DB: INSERT background.jobs (в tx клиента)
    D->>D: sem ← слот
    D->>DB: UPDATE ... FOR UPDATE SKIP LOCKED (claim)
    DB-->>D: id, task_type, payload, lease_token, deadline
    D->>W: jobsCh ← job
    W->>W: ctx=WithTimeout и Execute(ctx, payload)
    alt Execute → nil
        W->>DB: Complete(id, lease_token)
    else Execute → err
        W->>DB: Fail(id, lease_token, backoff)
    end
    W->>D: sem → слот
```

---

## 4. Модель данных (DDL)

Полный скрипт будет жить в `schema/schema.sql` (для тестов embed'ится в бинарник
через `go:embed`); ниже — его содержимое. Клиент применяет его своим мигратором —
библиотека мигратором не является.

```sql
CREATE SCHEMA IF NOT EXISTS background;

CREATE TABLE IF NOT EXISTS background.jobs
(
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_type           TEXT        NOT NULL,
    payload             JSONB       NOT NULL DEFAULT '{}'::jsonb,
    status              TEXT        NOT NULL DEFAULT 'pending',
    -- Дедлайн текущей попытки (lease). NULL допустим только вне 'processing';
    -- предикат claim'а обрабатывает NULL defensively (см. SN-5).
    must_be_finished_at TIMESTAMPTZ,
    -- Когда задача снова доступна для захвата (ретрай-пауза / отложенный запуск).
    next_attempt_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts            INTEGER     NOT NULL DEFAULT 0,
    max_attempts        INTEGER     NOT NULL DEFAULT 3,
    -- Fencing-токен текущего владельца (NULL = задачи никто не держит).
    lease_token         UUID,
    error_message       TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT jobs_status_chk       CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    CONSTRAINT jobs_attempts_chk     CHECK (attempts >= 0),
    CONSTRAINT jobs_max_attempts_chk CHECK (max_attempts >= 1),
    CONSTRAINT jobs_task_type_chk    CHECK (length(task_type) BETWEEN 1 AND 255)
);
```

Отклонения от колонок, перечисленных в ТЗ, — две добавленные: `next_attempt_at`
(ретрай-пауза; без неё ретраи выжигают попытки за секунды, SN-3) и `lease_token`
(fencing; SN-4). Вариант схемы строго по ТЗ — в Приложении A.

### Индексы

```sql
-- Ветка A claim-запроса: pending, готовые по next_attempt_at.
-- Частичный: завершённые задачи в индекс не попадают, он остаётся маленьким.
CREATE INDEX jobs_pending_idx
    ON background.jobs (next_attempt_at, id)
    WHERE status = 'pending';

-- Ветка B claim-запроса: processing с истёкшим lease (кандидаты на перехват).
-- Таких строк мало (≤ общего числа воркеров), индекс крошечный.
CREATE INDEX jobs_stuck_idx
    ON background.jobs (must_be_finished_at)
    WHERE status = 'processing';

-- Для мониторинга и очистки (§10).
CREATE INDEX jobs_finished_idx
    ON background.jobs (updated_at)
    WHERE status IN ('completed', 'failed');
```

Оба поллинг-индекса покрывают свою ветку предиката `OR` — планировщик выполнит
`BitmapOr` по двум bitmap-сканам. Индекса по «просто status» из ТЗ достаточно лишь
для мониторинга; поллингу нужны именно частичные индексы под полный предикат.

### Обоснование выбора типов

| Решение | Альтернатива | Почему так |
|---|---|---|
| `BIGINT IDENTITY` | `UUID` | 8 байт против 16, упорядочен (≈FIFO по `id`, компактнее лок-hot index). UUID нужен только при записи с нескольких несинхронизированных узлов — не наш случай, пишем в одну БД. SN-11 |
| `status TEXT + CHECK` | `enum` | Добавление статуса (например, `cancelled` в v2) — это `ALTER`, а не `CREATE TYPE` + перекладка колонки; миграции у клиента, эволюция должна быть дешёвой. SN-12 |
| `JSONB` payload | `JSONB` + json-schema колонка | Библиотека агностична к содержимому, валидация — обязанность обработчика |
| `TIMESTAMPTZ` | `TIMESTAMP` | Только tz-типы; все сравнения — в UTC на стороне БД |
| без триггера на `updated_at` | триггер | `updated_at` пишется явно в каждом DML библиотеки; триггер — скрытая стоимость и зависимость от объекта, которым владеет клиент |
| без таблицы истории | append-only лог попыток | v2 (SN-10): сейчас последняя ошибка в `error_message` и `attempts`-счётчик дают минимум, достаточный для очереди |

---

## 5. SQL-операции (DML)

Все запросы — статические константы с параметрами (никакой конкатенации SQL).
Соединение из пула удерживается только на время самого запроса; на время исполнения
обработчика соединение не держим.

> **Проверено.** Все запросы этого раздела и DDL §4 прогнаны на живом PostgreSQL 18
> (контейнер `postgres:18`): захват свежих/отложенных/зависших/NULL-дедлайнных задач,
> fencing (повторный Complete → 0 строк), переходы pending↔processing→completed/failed,
> release без сжигания попытки, `EXPLAIN` показал `BitmapOr` по `jobs_pending_idx` +
> `jobs_stuck_idx`, а двухсессионный тест подтвердил `SKIP LOCKED`: конкурентная
> сессия забрала следующую задачу за ~90 мс, не блокируясь на занятой строке.

### 5.1. Захват задачи (claim) — ключевая операция

Параметры: `$1 text[]` — зарегистрированные task_type, `$2 float8[]` — параллельный
массив таймаутов (сек) для каждого типа, `$3 uuid` — свежесгенерированный lease_token.

```sql
WITH cfg(task_type, timeout_seconds) AS (
    SELECT * FROM unnest($1::text[], $2::float8[])
),
candidate AS (
    SELECT c.id
    FROM background.jobs AS c
    JOIN cfg ON cfg.task_type = c.task_type
    WHERE (c.status = 'pending' AND c.next_attempt_at <= now())
       OR (c.status = 'processing'
           AND (c.must_be_finished_at IS NULL OR now() > c.must_be_finished_at))
    ORDER BY c.id
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE background.jobs AS j
SET status              = 'processing',
    attempts            = j.attempts + 1,
    lease_token         = $3::uuid,
    must_be_finished_at = now() + make_interval(secs => cfg.timeout_seconds),
    updated_at          = now()
FROM cfg, candidate
WHERE j.id = candidate.id
  AND j.task_type = cfg.task_type
RETURNING j.id, j.task_type, j.payload, j.attempts, j.max_attempts,
          j.lease_token, j.must_be_finished_at;
```

Разбор построчно:

- **`FOR UPDATE SKIP LOCKED`** — ядро конкурентности. Строки, уже заблокированные
  другим dispatcher'ом (в параллельной транзакции), молча пропускаются; конкуренты не
  ждут друг друга и не захватывают одну задачу дважды. Классический паттерн очередей
  на PG (так работают River, gue, Oban).
- **Атомарность одним запросом.** `UPDATE ... WHERE id = (подзапрос с блокировкой) ... RETURNING`
  — атомарный захват за один round-trip, без явного `BEGIN/COMMIT`. Альтернатива
  `BEGIN; SELECT ... FOR UPDATE SKIP LOCKED; UPDATE ...; COMMIT` корректна, но дороже.
- **`attempts = j.attempts + 1` при захвате.** Попытка инкрементируется один раз — в
  момент захвата. ТЗ в п.5 требует инкрементировать ещё и при ошибке — это двойной
  подсчёт (SN-1); принято решение: инкремент только на claim, сравнение с
  `max_attempts` — на финализации. Семантика: **`max_attempts` = число запусков
  обработчика**, при `max_attempts = 3` задача выполняется максимум 3 раза.
- **Ветка `OR`** реализует оба требования ТЗ: свежие `pending` и «застрявшие»
  `processing` с истёкшим lease. `must_be_finished_at IS NULL` для `processing` —
  defensive-ветка на случай ручных вставок/рестормов (SN-5).
- **`JOIN cfg`** фильтрует кандидатов по зарегистрированным типам и задаёт
  **таймаут на каждый task_type** (требование ТЗ) без второго round-trip. Задачи
  незарегистрированного типа не захватываются вовсе — остаются `pending` и видимы для
  мониторинга, а не выжигают попытки на ошибке «handler not found» (SN-9).
- **`now()` из БД** и в сравнениях, и в арифметике — единый источник времени.
- **`ORDER BY id LIMIT 1`** — приблизительный FIFO. Известная особенность связки
  `ORDER BY + LIMIT + SKIP LOCKED`: если первая по порядку строка заблокирована, она
  пропускается и может вернуть пустой результат, хотя свободные строки позже по порядку
  есть. Для очереди это безвредно (следующий тик заберёт), но задокументировано (R-9).

Ноль строк в ответе — «очередь пуста».

### 5.2. Успешное завершение (Complete)

```sql
UPDATE background.jobs
SET status = 'completed',
    error_message      = NULL,
    must_be_finished_at = NULL,
    lease_token        = NULL,
    updated_at         = now()
WHERE id = $1
  AND lease_token = $2;
```

`WHERE lease_token = $2` — fencing: если обновление затронуло **0 строк**, значит lease
уже принадлежит другому воркеру (истёк и задача перехвачена), и старый воркер **молча
отбрасывает** свой результат (лог warning). Именно поэтому «опоздавшее» повторное
выполнение при at-least-once не портит состояние задачи.

### 5.3. Ошибка (Fail)

Параметры: `$3` — сообщение, `$4` — backoff в секундах (считает движок, §7),
`$5 bool` — принудительный `failed` (перманентная ошибка, SN-8).

```sql
UPDATE background.jobs
SET status = CASE
               WHEN $5 OR attempts >= max_attempts THEN 'failed'
               ELSE 'pending'
             END,
    error_message      = $3,
    must_be_finished_at = NULL,
    lease_token        = NULL,
    next_attempt_at    = now() + make_interval(secs => $4),
    updated_at         = now()
WHERE id = $1
  AND lease_token = $2
RETURNING status;
```

`attempts` уже увеличен на claim, поэтому здесь только сравнение: после 3-го
(последнего) запуска `attempts = 3 >= max_attempts = 3` → `failed`. Backoff пишется и в
случае `failed` (не используется, но не мешает — упрощает один запрос на оба исхода).

### 5.4. Возврат при остановке (Release)

Вызывается при graceful shutdown, когда задача снята с выполнения из-за отмены
контекста движка, а не из-за ошибки обработчика. Попытка **не сжигается**:

```sql
UPDATE background.jobs
SET status = 'pending',
    attempts            = attempts - 1,
    must_be_finished_at = NULL,
    lease_token         = NULL,
    next_attempt_at     = now(),
    updated_at          = now()
WHERE id = $1
  AND lease_token = $2;
```

### 5.5. Постановка задачи (Enqueue / EnqueueTx)

```sql
INSERT INTO background.jobs (task_type, payload, max_attempts, next_attempt_at)
VALUES ($1, $2::jsonb, $3, COALESCE($4, now()))
RETURNING id;
```

`EnqueueTx` выполняет тот же запрос на переданном `pgx.Tx` — это и есть точка
Transactional Outbox: откат бизнес-транзакции откатывает и задачу (и наоборот — задача
не видна воркерам до коммита).

### 5.6. Очистка (Purge, helper)

```sql
DELETE FROM background.jobs
WHERE id IN (
    SELECT id FROM background.jobs
    WHERE status = 'completed'
      AND updated_at < now() - make_interval(secs => $1::float8)
    ORDER BY id
    LIMIT $2
);
```

Не подключена к движку; вызывается клиентом по расписанию (обычный cron/задача).
Пакетность (`LIMIT`) — чтобы не создавать длинные транзакции и не мешать autovacuum.
`failed` по умолчанию не чистится (диагностика); параметр статусов — на усмотрение
вызова. Подробности и риски роста таблицы — §10, R-6.

---

## 6. Go API: интерфейсы и типы

Пакет `dispoller` в корне модуля `github.com/bobrovka/dispoller`.
Полный список файлов — §6.5.

### 6.1. Обработчик

```go
// JobHandler реализуется в клиентском приложении.
// Обработчик ОБЯЗАН быть идемпотентным (гарантия доставки — at-least-once, см. дизайн §2).
// ctx отменяется по истечении execution_timeout task_type и при остановке движка —
// обработчик должен уважать ctx и не игнорировать его отмену.
type JobHandler interface {
    Execute(ctx context.Context, payload []byte) error
}

// HandlerFunc — адаптер для функций (аналог http.HandlerFunc).
type HandlerFunc func(ctx context.Context, payload []byte) error

func (f HandlerFunc) Execute(ctx context.Context, payload []byte) error { return f(ctx, payload) }
```

Рекомендация клиентам: перманентные ошибки (невалидный payload, «пользователь не
найден») оборачивать в `Permanent`, чтобы не тратить попытки на бессмысленные ретраи
(SN-8):

```go
// Permanent оборачивает ошибку: задача переводится сразу в failed, без ретраев.
func Permanent(err error) error

// IsPermanent сообщает, помечена ли ошибка как перманентная.
func IsPermanent(err error) bool
```

### 6.2. Регистрация обработчиков

```go
// HandlerOption настраивает обработчик при регистрации.
type HandlerOption func(*handlerConfig)

// WithExecutionTimeout задаёт дедлайн одной попытки для task_type
// (требование ТЗ: таймаут настраивается на каждый тип).
// Должен быть > 0. Дефолт — WithDefaultExecutionTimeout движка.
func WithExecutionTimeout(d time.Duration) HandlerOption

// RegisterHandler привязывает обработчик к task_type.
// Возвращает ошибку при пустом task_type, nil-обработчике, d <= 0
// или повторной регистрации того же task_type.
// Не конкурентно-безопасен: вызывается до Start.
func (e *Engine) RegisterHandler(taskType string, h JobHandler, opts ...HandlerOption) error
```

Отклонение от сигнатуры ТЗ `RegisterHandler(taskType, handler)` — добавлены вариадик-опции
(куда класть per-type таймаут) и возвращаемая ошибка (дубликаты типов — частая ошибка,
молчаливое затирание недопустимо). SN-6.

### 6.3. Движок

```go
// New создаёт движок. pool != nil.
func New(pool *pgxpool.Pool, opts ...Option) *Engine

type Option func(*config)

func WithLogger(l *slog.Logger) Option                  // дефолт: slog.Default()
func WithDefaultExecutionTimeout(d time.Duration) Option // дефолт: 60s
func WithBackoff(base, max time.Duration) Option         // дефолт: 30s, 15m

// Start блокируется до отмены ctx, затем дожидается завершения in-flight задач
// и возвращает управление. Второй вызов — ошибка.
// interval > 0 — период опроса; concurrency >= 1 — макс. число одновременно
// выполняемых задач этим процессом.
// Возвращает ошибку при некорректных аргументах/повторном запуске; после
// graceful остановки возвращает nil.
func (e *Engine) Start(ctx context.Context, interval time.Duration, concurrency int) error
```

Отклонение от ТЗ (`Start` без возвращаемого значения) — ошибки конфигурации должны
всплывать при запуске, а не теряться. SN-6.

### 6.4. Постановка задач

```go
type EnqueueOption func(*enqueueConfig)

func WithMaxAttempts(n int) EnqueueOption // дефолт: колонка (3)
func WithRunAt(t time.Time) EnqueueOption // отложенный запуск; дефолт: now()

// Enqueue вставляет задачу отдельной транзакцией пула.
func Enqueue(ctx context.Context, pool *pgxpool.Pool, taskType string, payload []byte, opts ...EnqueueOption) (int64, error)

// EnqueueTx вставляет задачу в транзакцию клиента (Transactional Outbox).
// Коммит/rollback — ответственность вызывающего.
func EnqueueTx(ctx context.Context, tx pgx.Tx, taskType string, payload []byte, opts ...EnqueueOption) (int64, error)
```

### 6.5. Пример клиентского приложения

```go
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

// SendEmailHandler — пример бизнес-обработчика.
type SendEmailHandler struct{ cli *email.Client }

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

    if err := engine.RegisterHandler("email.send", &SendEmailHandler{cli: email.New()},
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
        defer tx.Rollback(ctx)

        payload, _ := json.Marshal(SendEmailPayload{To: "user@example.com", Subject: "hi"})
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
```

### 6.6. Структура репозитория

```
.
├── dispoller.go        // Engine, Option, Start
├── handler.go          // JobHandler, HandlerFunc, HandlerOption, Permanent
├── enqueue.go          // Enqueue, EnqueueTx, EnqueueOption
├── store.go            // интерфейс store + pgStore (все SQL)
├── queries.go          // тексты SQL-запросов (константы)
├── engine_test.go      // unit-тесты (fake store)
├── integration_test.go // +build integration; требует DATABASE_URL
├── schema/
│   ├── schema.sql      // DDL из §4
│   └── schema.go       // go:embed для тестов
├── example/email/      // пример из §6.5 (компилируется в CI)
└── docs/
```

---

## 7. Движок: конкурентность и жизненный цикл

### 7.1. Состояния задачи

```mermaid
stateDiagram-v2
    [*] --> pending: Enqueue/EnqueueTx
    pending --> processing: claim (attempts+1, lease_token, deadline)
    processing --> completed: Execute=nil
    processing --> pending: Execute=err и attempts<max (backoff)
    processing --> failed: Execute=err и attempts>=max или Permanent
    processing --> pending: release при shutdown (attempts-1)
    pending --> processing: повторный claim
    completed --> [*]
    failed --> [*]
```

Переход `processing → pending` по истечении lease не меняет статус в БД: строка так и
остаётся `processing` с просроченным `must_be_finished_at`, а «переход» происходит
логически — её забирает ветка B claim-запроса.

### 7.2. Dispatcher

```go
// Схема псевдокода; реальный код — с полной обработкой ошибок.
func (e *Engine) dispatchLoop(ctx context.Context) {
    ticker := time.NewTicker(e.interval)
    for {
        select {
        case <-ctx.Done():
            return // graceful: см. 7.4
        case <-ticker.C:
        }

        // Слот берём ДО claim: не захватываем задачу, пока нет воркера,
        // который гарантированно её возьмёт (иначе lease прогорает впустую).
        select {
        case e.sem <- struct{}{}:
        case <-ctx.Done():
            return
        }

        job, err := e.store.Claim(ctx, e.types(), e.timeouts())
        if err != nil {
            <-e.sem
            if !errors.Is(err, pgx.ErrNoRows) {
                e.log.Error("claim", "err", err) // БД недоступна — ждём следующий тик
            }
            continue
        }
        if job == nil { // очередь пуста
            <-e.sem
            continue
        }

        select {
        case e.jobsCh <- job: // воркер заберёт; слот освободит воркер по завершении
        case <-ctx.Done():
            e.releaseQuietly(job) // вернуть в pending, не сжигая попытку
            return
        }
    }
}
```

Ключевые решения:

- **Один claim-запрос на тик на процесс** — нагрузка поллинга не зависит от числа
  воркеров и равна `1/interval` запросов/сек на инстанс (дёшево: два маленьких частичных
  индекса).
- **Сначала слот — потом захват.** Альтернатива (захват, потом блокирующая отправка в
  канал) горит lease'ом, пока задача ждёт воркера. Отвергнута.
- **Альтернатива «каждый воркер поллит сам»** (N конкурирующих запросов на тик) даёт
  меньшую задержку при пуле воркеров, но нагружает БД и дублирует логику backpressure.
  Отвергнута; eager-пробуждение dispatcher'а после завершения задачи — в roadmap (§12, SN-14).
- `pgx.ErrNoRows` («пусто») не логируется как ошибка — это нормальный исход поллинга.

### 7.3. Воркер

```go
func (e *Engine) worker(ctx context.Context) {
    for {
        select {
        case <-ctx.Done():
            return
        case job, ok := <-e.jobsCh:
            if !ok {
                return // канал закрыт при остановке
            }
            e.run(ctx, job)
            <-e.sem // освободить слот
        }
    }
}

func (e *Engine) run(ctx context.Context, job *claimedJob) {
    // Локальный таймаут = lease из БД. Двойная защита:
    // БД-lease перехватывает упавшие процессы, локальный ctx — зависшие горутины.
    hctx, cancel := context.WithTimeout(ctx, job.timeout)
    defer cancel()

    err := e.execute(hctx, job) // см. паники ниже

    switch {
    case err == nil:
        e.finalize(ctx, func() error { return e.store.Complete(ctx, job.id, job.token) })

    case isShutdown(ctx, err): // движок останавливается, попытку не сжигаем
        e.finalize(ctx, func() error { return e.store.Release(ctx, job.id, job.token) })

    default:
        backoff := e.backoffFor(job.attempts) // base * 2^(attempts-1), cap, jitter
        permanent := dispoller.IsPermanent(err)
        msg := truncate(err.Error(), 4096)
        e.finalize(ctx, func() error {
            return e.store.Fail(ctx, job.id, job.token, msg, backoff, permanent)
        })
    }
}
```

- **Паники обработчика** перехватываются в `execute` через `recover` и превращаются в
  обычную ошибку `"handler panic: ..."` → путь Fail. Паника в чужом коде не должна
  ронять процесс воркера (SN-7).
- **`isShutdown`**: `errors.Is(err, context.Canceled) && ctx.Err() != nil` — ошибка
  пришла от отмены корневого контекста движка, а не из внутренних механизмов
  обработчика. Эвристика неточная (обработчик может вернуть `context.Canceled` по своим
  причинам) — но в обоих случаях консервативно безопаснее пере-выполнить задачу, чем
  сжечь попытку: Release лишь вернёт её в очередь.
- **`finalize`** повторяет запрос финализации до 3 раз с паузами (100ms/1s/5s), т.к.
  соединение может мигрировать. Если не удалось — лог error и отказ: задача
  самовосстановится, когда истечёт её lease, и её перехватит другой воркер (двойное
  выполнение возможно — это заложенный at-least-once, R-1).
- Важно: финализация выполняется с контекстом **без** таймаута попытки (даже если
  lease истёк, корректная финализация по токену безопасна).

### 7.4. Graceful shutdown

1. Отмена `ctx` (сигнал ОС у клиента) → dispatcher выходит из цикла.
2. Dispatcher закрывает `jobsCh`.
3. Воркеры дорабатывают текущую задачу и выходят (задачи, снятые по отмене
   контекста, возвращаются через Release без сжигания попытки).
4. `Start` ждёт все воркеры (`WaitGroup`) и возвращает `nil`.

Если процесс умер жёстко (SIGKILL, OOM) — ничего возвращать некому; задача висит в
`processing` до истечения `must_be_finished_at`, затем её перехватывает любой живой
воркер (ветка B claim). Пауза восстановления ≈ execution_timeout — плата за
отказоустойчивость без внешнего координатора.

### 7.5. Расчёт backoff

```
delay(attempts) = min(base * 2^(attempts-1), max) * jitter(0.8..1.2)
```

`attempts` — номер только что проваленной попытки (после claim). Пример при дефолтах
(base=30s, max=15m, max_attempts=3): попытка 1 → ~30s, попытка 2 → ~60s, попытка 3 →
`failed`. Jitter размазывает ретраи пачки упавших задач. Значение передаётся в
`Fail($4)` и пишется в `next_attempt_at` — БД единолично решает готовность.

---

## 8. Конфигурация и значения по умолчанию

| Параметр | Где | Дефолт | Комментарий |
|---|---|---|---|
| `interval` | `Start` | — (пример: 1s) | Компромисс задержка/нагрузка: задержка старта задачи ≤ interval; нагрузка = 1 запрос/interval/инстанс |
| `concurrency` | `Start` | — (пример: 2×CPU) | Ограничение на процесс; глобальный предел = сумма по инстансам |
| `execution_timeout` | `WithExecutionTimeout` | 60s (engine) | lease одной попытки; после истечения задача может быть перехвачена |
| `backoff base/max` | `WithBackoff` | 30s / 15m | ×2 с джиттером (§7.5) |
| `max_attempts` | колонка / `WithMaxAttempts` | 3 | Всего запусков обработчика |
| пул соединений | клиент (`pgxpool`) | pgx-дефолты | `MaxConns ≥ concurrency + 2` (dispatcher + финализации); соединение не удерживается на время Execute |

---

## 9. План тестирования

### 9.1. Unit-тесты (`engine_test.go`, fake store)

Движок зависит от интерфейса `store` (`Claim/Complete/Fail/Release/Enqueue`), что даёт
проверку логики без БД:

- регистрация: дубликат типа, пустой тип, nil-обработчик, битый таймаут;
- маршрутизация payload в нужный обработчик;
- исходы `run`: nil → Complete; ошибка с попытками → Fail(pending); ошибка без попыток
  → Fail(failed); `Permanent` → Fail(failed, force); паника обработчика → Fail;
  отмена ctx движка → Release;
- расчёт backoff (границы, cap, jitter-коридор);
- shutdown: `Start` возвращает nil, воркеры завершаются, канал закрывается;
- конкурентность под `-race`: N воркеров × fake store с мьютексом.

### 9.2. Интеграционные тесты (`integration_test.go`)

Билд-тег `integration`; DSN из `DISPOLLER_TEST_DSN` (иначе `t.Skip`); схема применяется
из embed перед прогоном. Для локального запуска — `docker-compose.yml` c `postgres:18`
(в песочнице образ уже есть).

| # | Сценарий | Проверка |
|---|---|---|
| T-1 | Enqueue → Start | задача completed, payload доставлен |
| T-2 | ретраи | падающий обработчик: 2 неудачи → успех; статус/`attempts`/`next_attempt_at` корректны |
| T-3 | исчерпание | `max_attempts` достигнут → `failed`, `error_message` записан |
| T-4 | `Permanent` | `failed` с первой попытки, ретраев нет |
| T-5 | конкурентный захват | 2 движка × concurrency 4, 200 задач, счётчик с мьютексом: каждая задача выполнена ровно столько раз, сколько claim'ов; дублей нет |
| T-6 | дедлайн | вставка `processing`-строки с `must_be_finished_at` в прошлом → перехвачена и выполнена (симуляция упавшего воркера) |
| T-7 | fencing | длинный обработчик дольше lease: после перехвата его Complete/Fail затрагивает 0 строк, состояние не портится |
| T-8 | таймаут попытки | обработчик-«спальня» > execution_timeout: ctx отменяется, попытка провалена, ретрай |
| T-9 | graceful shutdown | in-flight задача дорабатывает; снятая возвращается в pending без сжигания попытки |
| T-10 | `EnqueueTx` | rollback транзакции → задачи нет; commit → есть |
| T-11 | незарегистрированный тип | задача не захватывается, остаётся pending |
| T-12 | паника | статус failed/рetry, процесс жив |

### 9.3. Ограничения среды

Тесты интеграционные гоняются с ограничением параллелизма: `go test -p 1` и без
`t.Parallel()` в интеграционных кейсах (среда разработки — контейнер с 4 CPU; см.
правила репозитория). `-race` включён по умолчанию в CI-команде:

```
go test -race ./...
DISPOLLER_TEST_DSN=... go test -race -tags integration -p 1 ./...
```

---

## 10. Эксплуатация

- **Нагрузка поллинга** — 1 запрос на инстанс каждый `interval`, по двум крошечным
  частичным индексам. 10 инстансов × interval 1s ≈ 10 QPS — несущественно для PG.
- **Рост таблицы.** `completed`/`failed` строки копятся: нужен периодический `Purge`
  (§5.6) — например, суточная задача с `LIMIT 10000`. `failed` стоит хранить дольше
  (диагностика). Планировать autovacuum: очередь — churn-нагрузка (R-6).
- **Мониторинг** (запросы для дашборда клиента):
  - глубина очереди: `SELECT count(*) FROM background.jobs WHERE status='pending' AND next_attempt_at <= now();`
  - застрявшие: `... WHERE status='processing' AND must_be_finished_at < now();`
  - ошибки за час: `... WHERE status='failed' AND updated_at > now() - interval '1 hour';`
  - незарегистрированные типы: `SELECT DISTINCT task_type FROM background.jobs WHERE status='pending' AND task_type NOT IN (...);`
- **Безопасность.** Роли БД: enqueue-коду достаточно `INSERT`; воркерам —
  `SELECT/UPDATE` на `background.jobs`. `DELETE` нужен только джобу очистки. Payload —
  открытые данные: секреты в payload не класть (R-13).
- **Размер payload.** JSONB >2KB уходит в TOAST, распаковка на каждый claim —
  передавать ссылки (ID документов), а не содержимое (R-12).
- **Совместимость схемы.** Клиент владеет миграциями; при рассинхронизации версии схемы
  и библиотеки (например, нет колонки `lease_token`) первый же claim упадёт с понятной
  ошибкой — логируем и повторяем. Проверка схемы при старте (`SELECT` метаданных) —
  опционально в v2.

---

## 11. Сводные компромиссы

| # | Компромисс | Альтернатива | Почему принято |
|---|---|---|---|
| 1 | At-least-once (возможны дубли) | Exactly-once | Не достижимо без бизнес-идемпотентности; честно фиксируем в контракте |
| 2 | Polling с интервалом | LISTEN/NOTIFY | Проще, нет persistent-соединений; задержка ≤ interval компенсируется малым интервалом (миллисекундные тики слишком дороги, секундные — ок). NOTIFY-будильник — v2 |
| 3 | Одна таблица, один глобальный пул воркеров | Отдельные очереди/приоритеты на task_type | Требование ТЗ — простота; изоляция типов по слотам — v2 (R-8: долгий тип может занять всех воркеров) |
| 4 | Один claim-запрос = одна задача | Пакетный `LIMIT k` | Требование ТЗ; пакет — очевидная оптимизация v2 без смены схемы |
| 5 | Схема чуть шире ТЗ (+2 колонки) | Строго ТЗ | `next_attempt_at` и `lease_token` закрывают классы инцидентов (SN-3, SN-4); вариант строго по ТЗ — Приложение A с явной таблицей потерь гарантий |
| 6 | БД-часы как истина | Часы приложения | NTP-перекос между инстансами не влияет на корректность |
| 7 | Восстановление упавшего воркера ≈ за execution_timeout | Мгновенное (heartbeat) | Внешний координатор не нужен; heartbeat-продление lease — v2 |
| 8 | Последняя ошибка только в `error_message` | Полный лог попыток | Минимализм; аудит-таблица попыток — v2 |

---

## 12. Roadmap (v2 и далее)

Идеи, осознанно не вошедшие в v1 (каждая — отдельное обсуждение, порядок по ценности):

1. **Eager-поллинг**: сигнал dispatcher'у сразу после освобождения слота воркером —
   уменьшение латентности без уменьшения interval (SN-14).
2. **Пакетный claim** (`LIMIT k`) — меньше round-trip при глубокой очереди.
3. **Продление lease (heartbeat)** для честно-долгих задач.
4. **Изоляция task_type**: per-type concurrency limits.
5. **Таблица `background.job_attempts`** — аудит попыток (замена компромисса 8).
6. **Dedupe-ключ** (`unique` на колонке) для защиты от дублей постановки.
7. **Cron-расписания** (`scheduled_at` + периодичность).
8. **Метрики** (интерфейс-счётчики claim/complete/fail/reclaim).
9. **Отмена задачи** (`cancelled` статус) и `LISTEN/NOTIFY`-будильник.

---

## 13. Приложение A: DDL строго по ТЗ

Если по каким-то причинам нельзя добавить колонки (SN-3/SN-4 отклонены), схема по ТЗ:

```sql
CREATE TABLE background.jobs
(
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_type           TEXT        NOT NULL,
    payload             JSONB       NOT NULL DEFAULT '{}'::jsonb,
    status              TEXT        NOT NULL DEFAULT 'pending',
    must_be_finished_at TIMESTAMPTZ,
    attempts            INTEGER     NOT NULL DEFAULT 0,
    max_attempts        INTEGER     NOT NULL DEFAULT 3,
    error_message       TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT jobs_status_chk CHECK (status IN ('pending', 'processing', 'completed', 'failed'))
);

CREATE INDEX jobs_poll_idx ON background.jobs (status, must_be_finished_at);
```

Claim без `next_attempt_at` (ретрай сразу) и fencing по `attempts`
(`WHERE id=$1 AND attempts=$2` — работает во всех сценариях, кроме
release-then-reclaim с обработчиком, игнорирующим ctx, SN-4):

```sql
UPDATE background.jobs AS j
SET status = 'processing', attempts = j.attempts + 1,
    must_be_finished_at = now() + make_interval(secs => $3::float8), updated_at = now()
WHERE j.id = (
    SELECT c.id FROM background.jobs AS c
    WHERE c.task_type = ANY($1::text[])
      AND (c.status = 'pending' OR (c.status = 'processing' AND now() > c.must_be_finished_at))
    ORDER BY c.id LIMIT 1 FOR UPDATE SKIP LOCKED)
RETURNING j.id, j.task_type, j.payload, j.attempts, j.max_attempts;
```

| Гарантия | Рекомендуемая схема | Строго ТЗ |
|---|---|---|
| Backoff ретраев | да (`next_attempt_at`) | нет — мгновенный ретрай, попытки выжигаются за секунды |
| Fencing при release+reclaim | да (`lease_token`) | дыра при обработчиках, игнорирующих ctx |
| Отложенный запуск (`WithRunAt`) | да | нет |

## 14. Приложение B: сравнение с существующими решениями

Библиотека пишется по ТЗ, но выбор «очередь на PG» стоит делать с открытыми глазами:

| Решение | Язык | Что даёт сверх нашего v1 | Чего нет у нас |
|---|---|---|---|
| [River](https://riverqueue.com) | Go | UI, пакетный claim, периодические задачи, middleware | — |
| [gue](https://github.com/vgarvardt/gue) | Go | минимум зависимостей, похожая схематика | — |
| Oban | Elixir | приоритеты, unique-jobs, plugin-экосистема | — |
| graphile-worker | JS | batch, cron | — |
| Sidekiq/RabbitMQ | Ruby/брокер | мгновенная доставка, приоритеты | требует инфраструктуры помимо PG |

Вывод: если задача «фоновые задания в одном PG без новой инфраструктуры» — паттерн
проверен и оправдан; `dispoller` имеет право на жизнь как учебно-продуктовая
минималистичная реализация. Если появятся потребности в приоритетах/cron/UI —
сначала сравнить с River (R-14).
