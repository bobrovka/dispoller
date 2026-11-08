package dispoller

// Тексты SQL-запросов (design.md §5) — статические константы с параметрами,
// никакой конкатенации SQL.

// claimSQL захватывает одну задачу (design.md §5.1).
//
// Параметры: $1 text[] — зарегистрированные task_type; $2 float8[] — параллельный
// массив таймаутов (сек) для каждого типа; $3 uuid — свежесгенерированный lease_token.
//
// Ветка A: pending с наступившим next_attempt_at. Ветка B: processing с истёкшим
// lease (или NULL-дедлайном — defensive, SN-5). Перехват ветки B инкрементирует
// reclaims; при исчерпанном бюджете строка вместо запуска переводится в failed
// (dead-letter, SN-18) — в RETURNING это отличается парой
// (lease_token IS NULL, status = 'failed').
const claimSQL = `
WITH cfg(task_type, timeout_seconds) AS (
    SELECT * FROM unnest($1::text[], $2::float8[])
),
candidate AS (
    SELECT c.id,
           (c.status = 'processing') AS is_reclaim,
           (c.status = 'processing' AND c.reclaims >= c.max_reclaims) AS dead_letter
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
SET status              = CASE WHEN candidate.dead_letter THEN 'failed' ELSE 'processing' END,
    attempts            = CASE WHEN candidate.dead_letter THEN j.attempts ELSE j.attempts + 1 END,
    reclaims            = CASE WHEN candidate.is_reclaim THEN j.reclaims + 1 ELSE j.reclaims END,
    lease_token         = CASE WHEN candidate.dead_letter THEN NULL ELSE $3::uuid END,
    must_be_finished_at = CASE WHEN candidate.dead_letter THEN NULL
                               ELSE now() + make_interval(secs => cfg.timeout_seconds) END,
    error_message       = CASE WHEN candidate.dead_letter THEN 'reclaim budget exhausted'
                               ELSE j.error_message END,
    updated_at          = now()
FROM cfg, candidate
WHERE j.id = candidate.id
  AND j.task_type = cfg.task_type
RETURNING j.id, j.task_type, j.payload, j.attempts, j.max_attempts,
          j.reclaims, j.max_reclaims, j.lease_token, j.must_be_finished_at, j.status`

// completeSQL — успешное завершение (design.md §5.2).
// WHERE lease_token = $2 — fencing: 0 затронутых строк означают, что задача уже
// перехвачена другим воркером, и результат молча отбрасывается.
const completeSQL = `
UPDATE background.jobs
SET status              = 'completed',
    error_message       = NULL,
    must_be_finished_at = NULL,
    lease_token         = NULL,
    updated_at          = now()
WHERE id = $1
  AND lease_token = $2`

// failSQL — неуспешное завершение (design.md §5.3).
// Параметры: $3 — сообщение, $4 — backoff (сек), $5 — принудительный failed
// (перманентная ошибка, SN-8). attempts уже увеличен на claim — здесь только
// сравнение с max_attempts.
const failSQL = `
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
RETURNING status`

// releaseSQL — возврат задачи при остановке движка (design.md §5.4).
// Попытка не сжигается: attempts - 1 компенсирует инкремент на claim.
const releaseSQL = `
UPDATE background.jobs
SET status             = 'pending',
    attempts           = attempts - 1,
    must_be_finished_at = NULL,
    lease_token        = NULL,
    next_attempt_at    = now(),
    updated_at         = now()
WHERE id = $1
  AND lease_token = $2`

// enqueueSQL — постановка задачи (design.md §5.5).
const enqueueSQL = `
INSERT INTO background.jobs (task_type, payload, max_attempts, max_reclaims, next_attempt_at)
VALUES ($1, $2::jsonb, $3, $4, COALESCE($5, now()))
RETURNING id`

// purgeSQL — очистка completed-задач (design.md §5.6). Пакетность (LIMIT) —
// чтобы не создавать длинные транзакции и не мешать autovacuum.
const purgeSQL = `
DELETE FROM background.jobs
WHERE id IN (
    SELECT id FROM background.jobs
    WHERE status = 'completed'
      AND updated_at < now() - make_interval(secs => $1::float8)
    LIMIT $2
)`

// requeueSQL — возврат failed-задачи в очередь с чистого листа (design.md §5.7).
const requeueSQL = `
UPDATE background.jobs
SET status              = 'pending',
    attempts            = 0,
    reclaims            = 0,
    error_message       = NULL,
    must_be_finished_at = NULL,
    lease_token         = NULL,
    next_attempt_at     = now(),
    updated_at          = now()
WHERE id = $1
  AND status = 'failed'
RETURNING id`
