#!/usr/bin/env bash
# Акт 4: чем мы платим за пул. transaction pooling ломает состояние сессии.
#
# Важно: в спокойной обстановке PgBouncer обычно возвращает клиенту тот же самый
# серверный коннект, и всё "как будто работает". Под нагрузкой - перестаёт.
# Чтобы показать это детерминированно, мы руками просим PgBouncer пересобрать
# серверные коннекты (RECONNECT) - ровно то, что в проде случается само:
# server_lifetime, failover, рестарт базы, вытеснение из пула.

set -u
export PGPASSWORD=${PGPASSWORD:-app}
PGUSER=${PGUSER:-app}; DB=${DB:-shop}
PG_HOST=${PG_HOST:-postgres};    PG_PORT=${PG_PORT:-5432}
PGB_HOST=${PGB_HOST:-pgbouncer}; PGB_PORT=${PGB_PORT:-6432}
ADMIN_USER=${ADMIN_USER:-postgres}; ADMIN_PASS=${ADMIN_PASS:-postgres}

echo "=== 1. Напрямую в PostgreSQL: одна сессия - один бэкенд навсегда ==="
psql -h "$PG_HOST" -p "$PG_PORT" -U "$PGUSER" -d "$DB" -Atq <<'EOSQL'
select 'backend pid: ' || pg_backend_pid();
drop table if exists t_demo;
create temp table t_demo(x int);
insert into t_demo values (1);
set statement_timeout = '7s';
select 'backend pid: ' || pg_backend_pid();
select 'строк во временной таблице: ' || count(*) from t_demo;
select 'statement_timeout: ' || current_setting('statement_timeout');
EOSQL

echo
echo "=== 2. Через PgBouncer (pool_mode=transaction), пул пересобрался между транзакциями ==="
psql -h "$PGB_HOST" -p "$PGB_PORT" -U "$PGUSER" -d "$DB" -Atq <<EOSQL
select 'backend pid: ' || pg_backend_pid();
drop table if exists t_demo;
create temp table t_demo(x int);
insert into t_demo values (1);
set statement_timeout = '7s';
select 'строк во временной таблице: ' || count(*) from t_demo;
\! PGPASSWORD=$ADMIN_PASS psql -h $PGB_HOST -p $PGB_PORT -U $ADMIN_USER -d pgbouncer -Atqc 'RECONNECT $DB;' >/dev/null 2>&1
select 'backend pid: ' || pg_backend_pid();
select 'строк во временной таблице: ' || count(*) from t_demo;
select 'statement_timeout: ' || current_setting('statement_timeout');
EOSQL

echo
cat <<'TXT'
=== Что мы увидели ===
  * pid бэкенда сменился прямо посреди "одной" сессии;
  * временная таблица исчезла (relation ... does not exist);
  * statement_timeout сбросился в значение по умолчанию.

Что ломается в transaction pooling:
  TEMP TABLE, SET/RESET (кроме SET LOCAL внутри транзакции),
  advisory locks на уровне сессии, LISTEN/NOTIFY, WITH HOLD курсоры.

Что с этим делать:
  1. писать приложение так, чтобы вся сессионная магия жила внутри транзакции;
  2. вынести "сессионные" воркеры в отдельную базу с pool_mode = session;
  3. не тащить pgbouncer туда, где он не нужен (одна долгоживущая связка воркеров).
TXT
