# Демо 2. Та же репликация — одной командой

```bash
docker compose up -d
```

Всё. Через ~20 секунд есть мастер и работающая реплика.

## Что поменялось по сравнению с Демо 1

В Демо 1 последовательность шагов держал в голове человек. Здесь её держит
`docker compose`:

1. `postgres_master` стартует; `/docker-entrypoint-initdb.d/01-replication.sql`
   при первой инициализации создаёт роль `replicator` и слот репликации;
2. `healthcheck` (`pg_isready`) переводит мастер в состояние `healthy`;
3. `pg_cli` ждёт `condition: service_healthy`, делает `pg_basebackup -R`
   в том реплики и завершается с кодом 0;
4. `postgres_replica` ждёт `condition: service_completed_successfully`
   и стартует уже на готовом каталоге.

Ни одного `sleep 10` — все ожидания выражены через состояния, а не через
«наверное, за десять секунд успеет».

`pg_cli` идемпотентен: при повторном `docker compose up` он видит
`PG_VERSION` в томе и ничего не делает.

## Проверка

```bash
docker compose ps
docker compose exec postgres_master  psql -U postgres -x -c "SELECT * FROM pg_stat_replication;"
docker compose exec postgres_replica psql -U postgres    -c "SELECT pg_is_in_recovery();"
docker compose exec postgres_replica psql -U postgres    -c "SELECT count(*) FROM orders;"
```

## Уборка

```bash
docker compose down -v
```

## Чего всё ещё не хватает

Реплика есть, но:

* если умрёт мастер — реплику надо промоутить руками;
* приложение продолжит ходить по старому адресу;
* вернувшийся старый мастер устроит split-brain.

→ Демо 4.
