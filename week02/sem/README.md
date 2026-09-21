# Семинар 2. Отказоустойчивость PostgreSQL

Главная мысль семинара:

> Отказоустойчивость — это не «поставить Patroni». Это последовательность
> вопросов «а что, если умрёт вот это?» и осознанная цена каждого ответа.

Поэтому демо идут не по списку технологий, а по нарастанию проблемы:
у нас есть одна база → у нас есть копия → копия поднимается сама →
нас заваливают подключениями → узел умер, а сервис жив.

**Перед семинаром** прогреваем кэши:

```bash
docker pull postgres:18
docker pull edoburu/pgbouncer:v1.25.2-p0
docker pull haproxy:3.2-alpine
docker pull quay.io/coreos/etcd:v3.5.21
(cd demo4_ha_patroni && docker compose build)
```

Один раз после клонирования или распаковки — проверить, что конфиги читаемы
не только владельцем:

```bash
chmod -R a+rX .
```

Файлы с правами `600` ломают демо неочевидно: `COPY` в Dockerfile сохраняет
режим файла из контекста сборки, а bind-mount отдаёт его в контейнер как есть.
Процессы внутри контейнеров работают не от root (`postgres`, `haproxy`),
поэтому получают `Permission denied` на собственный конфиг. После `git clone`
права нормализуются в `644`/`755` сами — команда нужна тем, кто получил
материалы архивом.

## Демо-кейсы

| # | демо | о чём |
|---|------|-------|
| 0 | [`demo0_logical_replication`](demo0_logical_replication) | логическая репликация: publication/subscription |
| 1 | [`demo1_manual_replication`](demo1_manual_replication)   | физическая репликация руками, шаг за шагом |
| 2 | [`demo2_automated_replication`](demo2_automated_replication) | то же самое одной командой |
| 3 | [`demo3_pgbouncer`](demo3_pgbouncer)                     | пул соединений: зачем и чем платим |
| 4 | [`demo4_ha_patroni`](demo4_ha_patroni)                   | Patroni + etcd + HAProxy: failover вживую |

---

## Демо 0. Логическая репликация

```bash
cd demo0_logical_replication && docker compose up -d
```

**На мастере** (`docker compose exec postgres_master psql -U postgres`):

```sql
CREATE TABLE public.test_table_1 (id int PRIMARY KEY, value text);

CREATE PUBLICATION pub_test_table_1 FOR TABLE public.test_table_1;
```

**На реплике** (`docker compose exec postgres_replica psql -U postgres`) —
таблицу надо создать заранее, логическая репликация не переносит DDL:

```sql
CREATE TABLE public.test_table_1 (id int PRIMARY KEY, value text);

CREATE SUBSCRIPTION sub_test_table_1
  CONNECTION 'host=postgres_master dbname=postgres user=postgres password=postgres'
  PUBLICATION pub_test_table_1;
```

**Проверяем:**

```sql
-- на мастере
INSERT INTO public.test_table_1 VALUES (1, 'lol'), (2, 'kek');
-- на реплике
SELECT * FROM public.test_table_1;
```

### Важно:

* `wal_level = logical` нужен **только публикующей** стороне, и он дороже:
  в WAL пишется больше. Для физической репликации он не нужен.
* Таблице нужен **primary key** (или `REPLICA IDENTITY FULL`), иначе
  `UPDATE`/`DELETE` не реплицируются — упадут с ошибкой.
* Логическая репликация **не переносит**: DDL, значения последовательностей,
  крупные объекты. Логическая реплика != готовая замена мастеру.
* Зато она умеет то, чего не может физическая: репликация **между разными
  мажорными версиями** PostgreSQL (основа апгрейда без даунтайма),
  выборочная репликация отдельных таблиц, репликация в другую схему.

Важно, что при конфликте ключей вставка упадет:

```sql
-- на реплике вставляем строку с тем же ключом
INSERT INTO public.test_table_1 VALUES (3, 'конфликт');
-- на мастере
INSERT INTO public.test_table_1 VALUES (3, 'мастер');
-- на реплике смотрим, как встала подписка:
SELECT * FROM pg_stat_subscription;
SELECT * FROM pg_subscription;
```

```bash
cd demo0_logical_replication && docker compose down -v
```

---

## Демо 1. Физическая репликация руками

Подробный пошаговый сценарий: [`demo1_manual_replication/README.md`](demo1_manual_replication/README.md).

Коротко о том, ради чего демо:

1. правим `postgresql.conf` (`wal_level`, `max_wal_senders`, `hot_standby`…)
   и `pg_hba.conf` (отдельная строка `replication`);
2. создаём роль `replicator` и **слот репликации**;
3. `pg_basebackup -R` снимает базовый бэкап прямо в том будущей реплики
   и сам пишет `primary_conninfo` + `standby.signal`;
4. поднимаем реплику, смотрим `pg_stat_replication` и `pg_stat_wal_receiver`;
5. руками делаем `pg_promote()` — и видим, почему руками так делать нельзя.

---

## Демо 2. То же самое автоматически

```bash
cd demo2_automated_replication && docker compose up -d
```

Одна команда. Смысл демо — показать, что вся последовательность из Демо 1
выражается штатными средствами compose.
Можно еще проще - у многих образов есть переменная окружения для настройки репликации, но это уже будет у вас в ДЗ.

---

## Демо 3. PgBouncer

Подробности: [`demo3_pgbouncer/README.md`](demo3_pgbouncer/README.md).

Пять актов: ломаем базу наплывом подключений → повторяем через пул →
меряем цену подключения через `pgbench -C` → показываем, что ломает
transaction pooling → перезапускаем базу под нагрузкой с нулём ошибок
через `PAUSE`/`RESUME`.

---

## Демо 4. Отказоустойчивый кластер

Подробности: [`demo4_ha_patroni/README.md`](demo4_ha_patroni/README.md).

`docker kill pg-patroni-1` наживую, живой лог записи,
возврат узла через `pg_rewind`, плановый `switchover`, ключи в etcd,
и разговор про цену синхронной репликации.

---

## Итоговая таблица для доски

| отказ | кто чинит | цена |
|-------|-----------|------|
| много подключений | PgBouncer | состояние сессии, очередь вместо ошибки |
| упал узел БД | Patroni + etcd | простой ≈ `ttl`, нужен кворум DCS |
| клиент не знает нового адреса | HAProxy / VIP | ещё одна точка отказа, её тоже дублируем |
| потеря последних транзакций | синхронная репликация | латентность каждой записи |
| `DROP TABLE` / логическая ошибка | **бэкапы**, не репликация | место, время восстановления |
| ЦОД целиком | реплика в другом ЦОД | сетевая задержка, стоимость |
