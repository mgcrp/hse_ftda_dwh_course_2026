#!/usr/bin/env bash

# Акт 5: PgBouncer как "пауза" для базы.
# Пока клиенты продолжают слать запросы, мы замораживаем пул, перезапускаем
# PostgreSQL и размораживаем. Клиенты не увидят ни одной ошибки - только паузу.

set -u
export PGPASSWORD=${PGPASSWORD:-app}
PGUSER=${PGUSER:-app}; DB=${DB:-shop}
PGB_HOST=${PGB_HOST:-pgbouncer}; PGB_PORT=${PGB_PORT:-6432}
ADMIN_USER=${ADMIN_USER:-postgres}; ADMIN_PASS=${ADMIN_PASS:-postgres}
RESTART_CMD=${RESTART_CMD:-}
PAUSE_WINDOW=${PAUSE_WINDOW:-25}

admin() { PGPASSWORD=$ADMIN_PASS psql -h "$PGB_HOST" -p "$PGB_PORT" -U "$ADMIN_USER" -d pgbouncer -Atqc "$1"; }

echo "Запускаем непрерывную нагрузку через PgBouncer (45 секунд)..."
errors=0; oks=0
( 
  end=$(( $(date +%s) + 45 ))
  while [ "$(date +%s)" -lt "$end" ]; do
    if psql -h "$PGB_HOST" -p "$PGB_PORT" -U "$PGUSER" -d "$DB" -Atqc 'select 1;' >/dev/null 2>&1
    then echo ok; else echo ERR; fi
    sleep 0.2
  done
) > /tmp/load.out &
loadpid=$!

sleep 5
echo ">>> PAUSE $DB   (PgBouncer дожидается конца текущих транзакций и держит клиентов)"
admin "PAUSE $DB;"

if [ -n "$RESTART_CMD" ]; then
  echo ">>> перезапускаем PostgreSQL: $RESTART_CMD"
  eval "$RESTART_CMD"
else
  echo
  echo "  ############################################################"
  echo "  #  СЕЙЧАС в СОСЕДНЕМ окне выполни:                         #"
  echo "  #      docker compose restart postgres                     #"
  echo "  ############################################################"
  for s in $(seq "$PAUSE_WINDOW" -1 1); do printf "\r  ждём %2ds... " "$s"; sleep 1; done
  echo
fi

echo ">>> RESUME $DB"
admin "RESUME $DB;"
wait $loadpid

oks=$(grep -c '^ok$'  /tmp/load.out || true)
errors=$(grep -c '^ERR$' /tmp/load.out || true)
echo
echo "Успешных запросов: $oks"
echo "Ошибок у клиентов: $errors"
echo
echo "Это же используют для переключения мастера и для минорных апгрейдов:"
echo "  PAUSE -> переключили -> RESUME, приложение ничего не заметило."
rm -f /tmp/load.out
