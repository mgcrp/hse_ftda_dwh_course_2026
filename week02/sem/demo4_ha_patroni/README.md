# Демо 4. Отказоустойчивый кластер: Patroni + etcd + HAProxy + PgBouncer

Собираем всё вместе и отвечаем на три вопроса из презентации:

| хотим                                            | кто решает     |
|--------------------------------------------------|----------------|
| чтобы базу нельзя было завалить подключениями    | PgBouncer      |
| чтобы мастер переключался сам                    | Patroni + etcd |
| чтобы запросы всегда шли в живой узел            | HAProxy        |

## Топология

```
   приложение
       │
       ▼
   PgBouncer :6432          пул соединений, PAUSE/RESUME
       │
       ├── база "app"    ──► HAProxy :5005  (проверка GET /primary) ──► ЛИДЕР
       └── база "app_ro" ──► HAProxy :5001  (проверка GET /replica) ──► реплики
                                    │
              ┌─────────────────────┼─────────────────────┐
              ▼                     ▼                     ▼
         pg-patroni-1          pg-patroni-2          pg-patroni-3
          Patroni               Patroni               Patroni
              └─────────── выборы лидера в etcd ──────────┘
```

Ключевая идея: **HAProxy ничего не знает про PostgreSQL.**
Он просто дёргает HTTP-ручку Patroni на порту 8008.
Patroni отвечает `200` на `GET /primary` только на лидере и на `GET /replica`
только на здоровой реплике. Вся «магия» — в двух строчках конфига HAProxy.

## Запуск

```bash
docker compose up -d --build
```

Первый запуск — 2–3 минуты (собирается образ с Patroni). Дальше:

```bash
docker compose exec pg-patroni-1 patronictl list
```

```
+ Cluster: hse-cluster --------+---------+-----------+----+-----------+
| Member       | Host          | Role    | State     | TL | Lag in MB |
+--------------+---------------+---------+-----------+----+-----------+
| pg-patroni-1 | pg-patroni-1  | Leader  | running   |  1 |           |
| pg-patroni-2 | pg-patroni-2  | Replica | streaming |  1 |         0 |
| pg-patroni-3 | pg-patroni-3  | Replica | streaming |  1 |         0 |
+--------------+---------------+---------+-----------+----+-----------+
```

Панель HAProxy — открыть в браузере на проекторе: <http://localhost:7005>.
Там видно, какой сервер зелёный в `postgres_write`, а какие — в `postgres_read`.

## Сценарий

### Куда попадает запись и куда — чтение

Окно 1:

```bash
docker compose run --rm pg_cli /scripts/01_write_loop.sh
```

```
  время    | результат | узел, который обслужил запись
-----------+-----------+------------------------------
  12:40:01 |    OK     | 10.5.0.5
  12:40:02 |    OK     | 10.5.0.5
```

Окно 2:

```bash
docker compose run --rm pg_cli /scripts/02_read_balance.sh
```

Чтение раскидывается между `10.5.0.6` и `10.5.0.7` — по репликам, и у них
`in_recovery=t`.

### Убиваем лидера

Не останавливаем аккуратно, а именно убиваем — как будто узел сгорел:

```bash
docker kill pg-patroni-1
```

Что происходит на экране (окно 1 продолжает писать):

1. ~2–4 с: HAProxy помечает узел `DOWN` по health-check, клиенты получают ошибки;
2. ~10–30 с: истекает `ttl` лидера в etcd, Patroni на оставшихся узлах
   проводит выборы, победитель делает `pg_promote()`;
3. HAProxy видит `200` на `/primary` у нового узла и снова пускает трафик;
4. в окне 1 записи пошли на **другой IP**.

Время простоя = `ttl` (30 с) в худшем случае. Это настраивается: меньше `ttl` —
быстрее failover, но выше риск ложного срабатывания на сетевой моргании.
Показать компромисс вживую:

```bash
docker compose exec pg-patroni-2 patronictl edit-config -s ttl=15 --force
```

### Возвращаем упавший узел

```bash
docker start pg-patroni-1
docker compose exec pg-patroni-2 patronictl list
```

Старый лидер возвращается **репликой**, а не мастером. Если его WAL успел
разойтись с новой линией времени, Patroni вызовет `pg_rewind` (у нас
`use_pg_rewind: true`) и подтянет узел без полного пересоздания.
Это ровно та проблема split-brain, которую в Демо 1 мы решать не умели.

### Плановое переключение

Failover — авария. Switchover — плановая операция, например перед обновлением ядра:

```bash
docker compose exec pg-patroni-1 patronictl switchover --force
```

В окне 1 видно: простой измеряется секундами, а не десятками секунд,
потому что никто не ждёт истечения `ttl`.

### Что etcd об этом знает

```bash
docker compose exec etcd etcdctl get --prefix /service/hse-cluster --keys-only
docker compose exec etcd etcdctl get /service/hse-cluster/leader
```

Вся «правда» о кластере — несколько ключей в распределённом хранилище.
Кто владеет ключом `leader` с непросроченной арендой, тот и мастер.

**Отсюда — про безопасность.** У нас etcd поднят без аутентификации: любой,
кто дотянется до порта 2379, может переписать конфигурацию кластера, включая
`archive_command`, а это выполнение произвольной команды на сервере БД.
В проде обязательны TLS-сертификаты клиентов (`--client-cert-auth`,
`--trusted-ca-file`) и изоляция сети. Это не теоретическая придирка —
это самый прямой способ получить RCE на вашей базе.

### Синхронная репликация: сколько стоит «не потерять ни одной транзакции»

По умолчанию репликация асинхронная: коммит на лидере возвращается клиенту
раньше, чем данные доехали до реплики. Если лидер сгорит в эту миллисекунду —
транзакция потеряна, хотя клиенту сказали «ок».

```bash
docker compose exec pg-patroni-1 patronictl edit-config -s synchronous_mode=true --force
docker compose exec pg-patroni-1 patronictl list      # появится роль Sync Standby
```

Теперь `COMMIT` ждёт подтверждения от синхронной реплики. Данные не теряются,
но каждая запись платит сетевой round-trip, а падение синхронной реплики
может остановить запись вовсе (если не включён `synchronous_mode_strict = false`).

Это главный вопрос семинара, на который нет универсального ответа:
**что для вашего сервиса дороже — потерянная транзакция или лишние миллисекунды?**

## Полезные команды

```bash
docker compose exec pg-patroni-1 patronictl list
docker compose exec pg-patroni-1 patronictl topology
docker compose exec pg-patroni-1 patronictl show-config
docker compose exec pg-patroni-1 patronictl reinit hse-cluster pg-patroni-3
curl -s localhost:8008/patroni | python3 -m json.tool
```

## Уборка

```bash
docker compose down -v
```

## Что в этом стенде «учебного», а не продового

* **etcd в одном экземпляре.** Для кворума нужно 3 или 5 узлов: одиночный etcd
  — единая точка отказа, и при его падении кластер уходит в read-only.
* **etcd без аутентификации и TLS** (см. Акт 5).
* **watchdog выключен.** В Docker его нет; на железе он нужен как защита от
  «зависшего» лидера.
* **Пароли в открытом виде** в `docker-compose.yml` и `userlist.txt`.
* **HAProxy в одном экземпляре** — тоже единая точка отказа. В проде его
  дублируют и ставят перед ним keepalived/VRRP или облачный балансировщик.
* **Нет бэкапов.** Репликация — это не бэкап: `DROP TABLE` уедет на реплики
  за миллисекунды. Нужен pgBackRest / WAL-G и проверенное восстановление.
