#!/usr/bin/env bash

# Акт 3: сколько стоит само подключение.
# pgbench -C переподключается на КАЖДУЮ транзакцию - ровно так ведёт себя
# короткоживущий воркер / serverless-функция / скрипт аналитика.

set -u
export PGPASSWORD=${PGPASSWORD:-app}
PGUSER=${PGUSER:-app}; DB=${DB:-shop}
PG_HOST=${PG_HOST:-postgres};   PG_PORT=${PG_PORT:-5432}
PGB_HOST=${PGB_HOST:-pgbouncer}; PGB_PORT=${PGB_PORT:-6432}
CLIENTS=${CLIENTS:-10}; TIME=${TIME:-15}

echo "Готовим данные (pgbench -i, масштаб 5)..."
pgbench -h "$PG_HOST" -p "$PG_PORT" -U "$PGUSER" -i -s 5 -q "$DB" 2>&1 | tail -1

run() {  # $1 = подпись, $2 = хост, $3 = порт
  echo
  echo "--- $1 ---"
  pgbench -h "$2" -p "$3" -U "$PGUSER" -d "$DB" \
          -c "$CLIENTS" -j 2 -T "$TIME" -C -S -n 2>&1 \
    | grep -E 'tps|latency average|failed'
}

run "НАПРЯМУЮ в PostgreSQL ($PG_HOST:$PG_PORT)"   "$PG_HOST"  "$PG_PORT"
run "ЧЕРЕЗ PgBouncer ($PGB_HOST:$PGB_PORT)"       "$PGB_HOST" "$PGB_PORT"
echo
echo "Разница в tps - это цена fork() нового бэкенда + аутентификации на каждый коннект."
