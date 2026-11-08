// Package schema содержит DDL очереди dispoller (design.md §4).
// Скрипт применяется клиентским мигратором; здесь он встраивается в тестовый
// бинарник через go:embed.
package schema

import _ "embed"

// DDL — полный скрипт схемы background.jobs (таблица + частичные индексы).
//
//go:embed schema.sql
var DDL string
