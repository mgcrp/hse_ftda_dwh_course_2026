#!/usr/bin/env bash
# Непрерывная запись в кластер через PgBouncer -> HAProxy -> текущий лидер.
# Печатает, КАКОЙ узел обслужил запись. Во время failover видно всё:
# несколько секунд ошибок, потом запись едет на новый узел.
set -u
export PGPASSWORD=${PGPASSWORD:-postgres}
HOST=${HOST:-pgbouncer}; PORT=${PORT:-6432}; DB=${DB:-app}; U=${PGUSER:-postgres}

psql -h "$HOST" -p "$PORT" -U "$U" -d "$DB" -Atqc \
  "create table if not exists ha_demo(id bigserial primary key, node text, ts timestamptz default now());" \
  >/dev/null 2>&1

echo "  время    | результат | узел, который обслужил запись"
echo "-----------+-----------+------------------------------"
while true; do
  out=$(psql -h "$HOST" -p "$PORT" -U "$U" -d "$DB" -Atqc \
        "insert into ha_demo(node) values (coalesce(inet_server_addr()::text,'?')) returning node;" 2>&1)
  if [ $? -eq 0 ]; then
    printf '  %s |    OK     | %s\n' "$(date +%H:%M:%S)" "$out"
  else
    printf '  %s |  ОШИБКА   | %s\n' "$(date +%H:%M:%S)" "$(echo "$out" | head -1 | cut -c1-70)"
  fi
  sleep 0.5
done
