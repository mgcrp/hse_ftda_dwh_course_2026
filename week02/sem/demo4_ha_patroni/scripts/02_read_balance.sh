#!/usr/bin/env bash
# Читающая нагрузка идёт на порт 5001 HAProxy и раскидывается по репликам.
set -u
export PGPASSWORD=${PGPASSWORD:-postgres}
HOST=${HOST:-pgbouncer}; PORT=${PORT:-6432}; DB=${DB:-app_ro}; U=${PGUSER:-postgres}
N=${N:-50}

echo "50 читающих запросов через ro-точку входа:"
for i in $(seq 1 "$N"); do
  psql -h "$HOST" -p "$PORT" -U "$U" -d "$DB" -Atqc \
    "select coalesce(inet_server_addr()::text,'?') || '  in_recovery=' || pg_is_in_recovery();" 2>&1
done | sort | uniq -c | sed 's/^/  /'
echo
echo "in_recovery=t означает, что запрос обслужила реплика, а не лидер."
