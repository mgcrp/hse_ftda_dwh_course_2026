# Демо 1. Физическая репликация руками

Цель — **увидеть каждый шаг**, необходимый для репликации. Поэтому реплика поднимается
последней командой, а до этого мы делаем всё вручную.

## 0. Мастер

```bash
docker compose up -d postgres_master
docker compose logs -f postgres_master
# ждём "database system is ready"
```

## 1. Пользователь для репликации и слот

```bash
docker compose exec postgres_master psql -U postgres
```

```sql
CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'replicator';
SELECT * FROM pg_create_physical_replication_slot('replication_slot_1');
SELECT slot_name, slot_type, active, wal_status FROM pg_replication_slots;
```

## 2. Немного данных, чтобы было что реплицировать

```sql
CREATE TABLE orders(id serial primary key, amount numeric, created_at timestamptz default now());
INSERT INTO orders(amount) SELECT random()*100 FROM generate_series(1, 1000);
\q
```

## 3. Базовый бэкап прямо в том реплики

Одноразовый контейнер, которому подмонтирован том реплики. Он делает
`pg_basebackup` и умирает — реплики как сервиса ещё не существует.

```bash
docker compose run --rm --user postgres \
  -e PGPASSWORD=replicator \
  postgres_replica \
  pg_basebackup -h postgres_master -p 5432 -U replicator \
                -D /var/lib/postgresql/18/docker \
                -S replication_slot_1 -X stream -Fp -R -P -v
```

Ключи:

| ключ  | что делает |
|-------|------------|
| `-X stream` | параллельно тянет WAL, накопившийся за время бэкапа |
| `-S`  | использует наш слот, чтобы мастер не выкинул нужный WAL |
| `-Fp` | plain-формат: каталог, готовый к запуску, а не tar |
| `-R`  | сам пишет `primary_conninfo` в `postgresql.auto.conf` и создаёт файл-маркер `standby.signal` |
| `-P -v` | прогресс и подробные логи |

Убедимся:

```bash
docker compose run --rm --user postgres postgres_replica \
  sh -c 'ls -l /var/lib/postgresql/18/docker/standby.signal; \
         grep -E "primary_conninfo|primary_slot_name" /var/lib/postgresql/18/docker/postgresql.auto.conf'
```

## 4. Поднимаем реплику

```bash
docker compose up -d postgres_replica
docker compose logs -f postgres_replica
# ждём "entering standby mode" / "streaming"
```

## 5. Проверяем

```bash
# на мастере: кто ко мне подключён как реплика и насколько отстаёт
docker compose exec postgres_master psql -U postgres -x -c "
  SELECT application_name, state, sync_state,
         sent_lsn, write_lsn, flush_lsn, replay_lsn,
         write_lag, flush_lag, replay_lag
  FROM pg_stat_replication;"
```

```bash
# на реплике: подтверждение, что мы standby и тянем WAL
docker compose exec postgres_replica psql -U postgres -c "SELECT pg_is_in_recovery();"
docker compose exec postgres_replica psql -U postgres -x -c "SELECT * FROM pg_stat_wal_receiver;"
```

Живой тест — вставляем на мастере, читаем на реплике:

```bash
docker compose exec postgres_master  psql -U postgres -c "INSERT INTO orders(amount) VALUES (999);"
docker compose exec postgres_replica psql -U postgres -c "SELECT count(*) FROM orders;"
```

И убеждаемся, что реплика — только для чтения:

```bash
docker compose exec postgres_replica psql -U postgres -c "INSERT INTO orders(amount) VALUES (1);"
-- ERROR:  cannot execute INSERT in a read-only transaction
```

## 6. Отставание реплики и конфликты восстановления

### Три лага, а не один

Отставание реплики — не одно число. На **мастере** есть `pg_stat_replication`,
и в нём четыре LSN-а, которые показывают четыре стадии жизни записи:

```bash
docker compose exec postgres_master psql -U postgres -x -c "
  SELECT application_name, state, sync_state,
         sent_lsn, write_lsn, flush_lsn, replay_lsn,
         write_lag, flush_lag, replay_lag
  FROM pg_stat_replication;"
```

| LSN | что уже произошло | парный лаг | соответствует `synchronous_commit` |
|---|---|---|---|
| `sent_lsn` | мастер отправил | — | — |
| `write_lsn` | реплика записала в ОС | `write_lag` | `remote_write` |
| `flush_lsn` | реплика сделала fsync | `flush_lag` | `on` |
| `replay_lsn` | реплика **применила**, видно в запросах | `replay_lag` | `remote_apply` |

Три `*_lag` — это **время**, а не байты: сколько прошло от локального сброса
WAL на мастере до подтверждения от реплики. Шкала здесь ровно та же, что у
`synchronous_commit` в Демо 4 — только там мы её *требуем*, а тут *измеряем*.

Разрыв между `flush_lsn` и `replay_lsn` — самая коварная ситуация: **данные на
реплике уже лежат, но их не видно**, потому что накат WAL однопоточный
(процесс `startup`) и его может тормозить долгий читающий запрос, конфликт
восстановления или просто диск.

### Ловушка, на которой горит половина мониторингов

Напрашивающийся запрос на реплике:

```sql
SELECT now() - pg_last_xact_replay_timestamp() AS replication_delay;
```

выглядит разумно и **врёт на простаивающем мастере**. Если в базу давно никто
не писал, последняя применённая транзакция старая, и «лаг» растёт линейно,
хотя реплика догнала мастер полностью. Проверено:

```
все четыре LSN одинаковые, write_lag/flush_lag/replay_lag = NULL
   ...при этом now() - pg_last_xact_replay_timestamp() = 00:02:58
   через 4 секунды                                     = 00:03:02
   ещё через 4                                         = 00:03:06
```

Правильный запрос на реплике сначала спрашивает «а есть ли вообще что
применять», и только потом считает время:

```sql
SELECT CASE
         WHEN pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn()
           THEN interval '0'
         ELSE now() - pg_last_xact_replay_timestamp()
       END AS replication_delay;
```

Что мониторить на самом деле:

* **отсутствие строки** в `pg_stat_replication` на мастере — самая тяжёлая
  авария, и именно её пропускают алерты, которые смотрят только на величину лага;
* **байты**: `pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)` — не зависит
  от того, идёт ли запись;
* **`replay_lag`** — время, но с поправкой на ловушку выше.

На **реплике** зеркальные источники — `pg_stat_wal_receiver`,
`pg_last_wal_receive_lsn()`, `pg_last_wal_replay_lsn()`.

### Почему накат вообще отстаёт — и главный компромисс

#### Подготовка: таблица, на которой эффект видно

На **мастере**:

```sql
DROP TABLE IF EXISTS public.conflict_demo;

CREATE TABLE public.conflict_demo (
    id         bigserial PRIMARY KEY,
    amount     numeric NOT NULL,
    status     text NOT NULL DEFAULT 'new',
    updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO public.conflict_demo (amount)
SELECT (random() * 1000)::numeric(10,2)
FROM generate_series(1, 200000);

-- автовакуум выключаем, чтобы VACUUM запускали МЫ и в нужный момент
ALTER TABLE public.conflict_demo SET (autovacuum_enabled = off);

VACUUM ANALYZE public.conflict_demo;
SELECT pg_size_pretty(pg_total_relation_size('public.conflict_demo'));  -- ~16 MB
```

200 тысяч строк — чтобы `UPDATE` успел создать заметное количество мёртвых
версий строк и чтобы раздувание было видно глазами, но всё ещё выполнялось за секунды.

Чтобы не ждать 30 секунд дефолтного таймаута, укоротим его на **реплике**:

```sql
ALTER SYSTEM SET max_standby_streaming_delay = '5s';
SELECT pg_reload_conf();
```

### `hot_standby_feedback = off` — реплика получает по рукам

На **реплике**:

```sql
ALTER SYSTEM SET hot_standby_feedback = off;
SELECT pg_reload_conf();
SHOW hot_standby_feedback;   -- off
```

Окно 1, **реплика** — длинный читающий запрос. Важно взять
`REPEATABLE READ`: снимок должен жить всю транзакцию, иначе конфликта не будет.

```sql
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) FROM public.conflict_demo;   -- снимок зафиксирован
SELECT pg_sleep(60);                         -- аналитик ушёл за кофе
SELECT count(*) FROM public.conflict_demo;   -- сюда он уже не доберётся
COMMIT;
```

Окно 2, **мастер** — массовый `UPDATE`, который делает старые версии строк
мусором, и `VACUUM`, который этот мусор вычищает:

```sql
UPDATE public.conflict_demo SET amount = amount + 1, updated_at = now();
UPDATE public.conflict_demo SET amount = amount - 1, updated_at = now();

VACUUM (VERBOSE) public.conflict_demo;
```

`VACUUM` отрапортует, что мусор реально удалён:

```
tuples: 200000 removed, 200000 remain, 0 are dead but not yet removable
index scan needed: 2942 pages ... 399944 dead item identifiers removed
```

Через несколько секунд реплика доигрывает эти WAL-записи, обнаруживает, что
удаляет версии строк, нужные открытому снимку, и убивает запрос:

```
ERROR:  canceling statement due to conflict with recovery
DETAIL:  User query might have needed to see row versions that must be removed.
```

**Мастер здоров, аналитик потерял час работы.**

### `hot_standby_feedback = on` — по рукам получает мастер

На **реплике** возвращаем как было:

```sql
ALTER SYSTEM SET hot_standby_feedback = on;
SELECT pg_reload_conf();
```

Повторяем ровно те же два окна. Теперь запрос на реплике **доживает до конца**,
а `VACUUM` на мастере разводит руками:

```
tuples: 0 removed, 800000 remain, 600000 are dead but not yet removable
```

Смотрим на мастере, кто виноват — реплика прислала свой `xmin`,
и мастер обязан беречь эти версии строк:

```sql
SELECT application_name, state, backend_xmin
FROM pg_stat_replication;
```

```
 application_name |   state   | backend_xmin
------------------+-----------+--------------
 walreceiver      | streaming |          797
```

И главное — размер таблицы на мастере:

```sql
SELECT pg_size_pretty(pg_total_relation_size('public.conflict_demo'));
```

```
16 MB   ->   59 MB
```

**Аналитик доволен, мастер раздулся вчетверо.** Три `UPDATE` под открытым
запросом на реплике — и почти 60 МБ вместо 16. На таблице в сотни гигабайт
и с отчётом, который идёт всю ночь, это заканчивается забитым диском.

### Что должно остаться в головах

| | `hot_standby_feedback = off` | `hot_standby_feedback = on` |
|---|---|---|
| длинные запросы на реплике | отменяются | выполняются |
| мастер | чистый | раздувается |
| кто страдает | аналитика | прод |

Универсально правильного ответа нет — есть три рабочих компромисса:

* поднять `max_standby_streaming_delay` (реплика отстаёт сильнее, но запросы живут);
* держать **отдельную** реплику под аналитику с `feedback = on`, а HA-реплику — с `off`;
* включить `feedback = on` вместе с жёстким `statement_timeout` на реплике,
  чтобы забытый запрос не держал `xmin` вечно.

Вернуть таблицу в исходный вид после демо:

```sql
VACUUM FULL public.conflict_demo;
```

## 7. Failover руками (мостик к Демо 4)

```bash
docker compose stop postgres_master
docker compose exec postgres_replica psql -U postgres -c "SELECT pg_promote();"
docker compose exec postgres_replica psql -U postgres -c "SELECT pg_is_in_recovery();"  -- f
```

Реплика стала мастером. Но: приложение об этом не знает, старый мастер при
возврате устроит split-brain, а сделал это всё человек руками ночью.
Именно эти три проблемы решает Patroni в Демо 4.

## Уборка

```bash
docker compose down -v
```
