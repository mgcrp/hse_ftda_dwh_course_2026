#!/usr/bin/env bash

# Живой дашборд для проектора.
# Слева - что происходит в PgBouncer, справа - что реально творится в PostgreSQL.

set -u
PGB_HOST=${PGB_HOST:-pgbouncer}; PGB_PORT=${PGB_PORT:-6432}
PG_HOST=${PG_HOST:-postgres};    PG_PORT=${PG_PORT:-5432}
PGUSER=${PGUSER:-postgres};      DB=${DB:-shop}
export PGPASSWORD

hdr() {
  echo
  echo "           |      PgBouncer (пул)        |   PostgreSQL (реальность)"
  echo "  время    | clients  waiting  servers   |  backends   max_conn"
  echo "-----------+-----------------------------+-----------------------"
}

pools() {
  psql -h "$PGB_HOST" -p "$PGB_PORT" -U "$PGUSER" -d pgbouncer -A -F'|' -c 'SHOW POOLS;' 2>/dev/null \
  | awk -F'|' -v db="$DB" '
      NR==1 { for (i=1;i<=NF;i++) col[$i]=i; next }
      $1==db { printf "%s %s %s\n", $col["cl_active"], $col["cl_waiting"], $col["sv_active"]; found=1; exit }
      END { if (!found) print "- - -" }'
}

backends() {
  psql -h "$PG_HOST" -p "$PG_PORT" -U "$PGUSER" -d "$DB" -tAc \
    "select count(*) from pg_stat_activity where datname = '$DB';" 2>/dev/null || echo "?"
}

maxconn() {
  psql -h "$PG_HOST" -p "$PG_PORT" -U "$PGUSER" -d "$DB" -tAc 'show max_connections;' 2>/dev/null || echo "?"
}

LIMIT=$(maxconn)
i=0
hdr
while true; do
  read -r cl_act cl_wait sv_act <<<"$(pools)"
  b=$(backends)
  printf '  %-8s | %7s  %7s  %7s   | %9s  %9s\n' "$(date +%H:%M:%S)" "$cl_act" "$cl_wait" "$sv_act" "$b" "$LIMIT"
  i=$((i+1)); [ $((i % 20)) -eq 0 ] && { LIMIT=$(maxconn); hdr; }
  sleep 1
done
