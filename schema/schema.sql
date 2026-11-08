-- Схема очереди dispoller (design.md §4).
-- Применяется клиентом своим мигратором: библиотека мигратором не является.

CREATE SCHEMA IF NOT EXISTS background;

CREATE TABLE IF NOT EXISTS background.jobs
(
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_type           TEXT        NOT NULL,
    payload             JSONB       NOT NULL DEFAULT '{}'::jsonb,
    status              TEXT        NOT NULL DEFAULT 'pending',
    -- Дедлайн текущей попытки (lease). Инвариант протокола: NULL бывает только
    -- вне 'processing' (claim всегда ставит дедлайн, финализация стирает его).
    -- Намеренно НЕ CHECK-ограничение: строки внешних писателей (pg_restore,
    -- ручные вставки, сторонний сервис) не отклоняются, а перехватываются
    -- defensive-веткой claim'а (см. SN-5).
    must_be_finished_at TIMESTAMPTZ,
    -- Когда задача снова доступна для захвата (ретрай-пауза / отложенный запуск).
    next_attempt_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts            INTEGER     NOT NULL DEFAULT 0,
    max_attempts        INTEGER     NOT NULL DEFAULT 3,
    -- Перехваты веткой B claim'а (восстановления зависших processing-строк).
    -- Бюджет ограничивает серийный перезапуск после аварий воркера (SN-18);
    -- штатный цикл claim→Fail и Release счётчик не меняют.
    reclaims            INTEGER     NOT NULL DEFAULT 0,
    max_reclaims        INTEGER     NOT NULL DEFAULT 10,
    -- Fencing-токен текущего владельца (NULL = задачи никто не держит).
    lease_token         UUID,
    error_message       TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT jobs_status_chk       CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    CONSTRAINT jobs_attempts_chk     CHECK (attempts >= 0),
    CONSTRAINT jobs_max_attempts_chk CHECK (max_attempts >= 1),
    CONSTRAINT jobs_reclaims_chk     CHECK (reclaims >= 0),
    CONSTRAINT jobs_max_reclaims_chk CHECK (max_reclaims >= 1),
    CONSTRAINT jobs_task_type_chk    CHECK (length(task_type) BETWEEN 1 AND 255)
);

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
