#!/usr/bin/env bash

# Акт 1-2: наплыв клиентов.
#   /scripts/01_flood.sh direct   - все ломятся прямо в postgres
#   /scripts/01_flood.sh pooled   - все ходят через pgbouncer
#
# Переменные: CLIENTS (сколько клиентов), HOLD (сколько секунд держим транзакцию)

set -u
MODE=${1:-direct}
CLIENTS=${CLIENTS:-50}
HOLD=${HOLD:-3}
export PGPASSWORD=${PGPASSWORD:-postgres}
PGUSER=${PGUSER:-postgres}; DB=${DB:-shop}

case "$MODE" in
  direct) HOST=${PG_HOST:-postgres};    PORT=${PG_PORT:-5432};  LABEL="НАПРЯМУЮ в PostgreSQL" ;;
  pooled) HOST=${PGB_HOST:-pgbouncer};  PORT=${PGB_PORT:-6432}; LABEL="ЧЕРЕЗ PgBouncer" ;;
  *) echo "usage: $0 direct|pooled"; exit 2 ;;
esac

echo "=================================================================="
echo " $LABEL   ($HOST:$PORT)"
echo " $CLIENTS параллельных клиентов, каждый держит транзакцию ${HOLD}s"
echo "=================================================================="

tmp=$(mktemp -d); start=$(date +%s)
for i in $(seq 1 "$CLIENTS"); do
  (
    if psql -h "$HOST" -p "$PORT" -U "$PGUSER" -d "$DB" -v ON_ERROR_STOP=1 -Atq \
         -c "begin; select pg_sleep($HOLD); select count(*) from pg_stat_activity; commit;" \
         >/dev/null 2>"$tmp/err.$i"
    then echo ok > "$tmp/res.$i"
    else echo fail > "$tmp/res.$i"
    fi
  ) &
done
wait
elapsed=$(( $(date +%s) - start ))

ok=$(grep -lx ok   "$tmp"/res.* 2>/dev/null | wc -l)
fail=$(grep -lx fail "$tmp"/res.* 2>/dev/null | wc -l)

echo
printf ' ПОДКЛЮЧИЛИСЬ: %s / %s\n' "$ok" "$CLIENTS"
printf ' ОТВАЛИЛИСЬ:   %s / %s\n' "$fail" "$CLIENTS"
printf ' ВРЕМЯ:        %ss\n' "$elapsed"
if [ "$fail" -gt 0 ]; then
  echo
  echo ' Типичные ошибки:'
  cat "$tmp"/err.* 2>/dev/null | grep -io 'FATAL:.*' | sort | uniq -c | sort -rn | head -3 | sed 's/^/   /'
fi
rm -rf "$tmp"
