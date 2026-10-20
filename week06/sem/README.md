## Семинар. Kafka и Debezium

Четыре демо:

1. **Kafka**: producer и consumer руками, смотрим, как сообщения раскладываются по партициям.
2. **Debezium**: подключаем Postgres к Kafka и смотрим, как insert, update и delete превращаются в события.
3. **Debezium + JDBC sink**: переливаем таблицы из одного Postgres в другой через Kafka.
4. **WAL из слота в SQL**: читаем изменения прямо из логического слота Postgres, без Debezium. Это то, что коннектор делает внутри.

Нужны Docker и Python 3. Все команды ниже запускаются из папки `week06/sem`.

### Подготовка

```bash
sh env/env_setup.sh
```

Скрипт создаёт виртуальное окружение в `env/.venv`, ставит зависимости и запускает Jupyter. Jupyter останется работать в этом терминале, так что для всего остального открывайте новые.

В новом терминале, где нужен Python (consumer и producer в демо 1), сначала активируйте окружение:

```bash
source env/.venv/bin/activate
```

Образы большие (один Debezium около 2 ГБ), поэтому до семинара лучше один раз прогнать `docker-compose pull` в папке каждого демо.

### Демо 1. Kafka

```bash
cd demo1_kafka
docker-compose up -d
```

Через 20 секунд всё готово: Kafka на `localhost:9092`, топик `service_backend_feed` на 4 партиции и веб-интерфейс Kafka UI на http://localhost:8080.

Дальше два терминала с активированным окружением:

```bash
python3 consumer.py   # первый терминал, оставляем работать
python3 producer.py   # второй терминал, каждый запуск шлёт одно сообщение
```

Consumer печатает партицию, offset и само сообщение, producer пишет, куда оно легло. То же самое видно в Kafka UI: Topics → service_backend_feed → Messages.

Если хочется, producer можно запускать из ноутбука `demo_kafka.ipynb`. Consumer при этом всё равно в терминале.

### Демо 2. Debezium

```bash
cd demo2_debezium
docker-compose up -d
```

Через полминуты можно открывать `demo_debezium.ipynb` и идти по нему сверху вниз.

### Демо 3. Debezium + JDBC sink

```bash
cd demo3_debezium_sink
docker-compose up -d
```

Тоже около полминуты. Открываем `debezium_sink.ipynb`: регистрируем коннекторы, смотрим, что данные доехали из master в DWH, потом меняем данные в master и смотрим на DWH. Отдельно видно, что удаления в DWH не переносятся.

Этот ноутбук можно запускать повторно на том же стенде: коннекторы регистрируются через PUT, поэтому ничего не ломается. Только в DWH к тому моменту останутся строки, которые вы удалили в master в прошлый раз.

### Демо 4. WAL из слота в SQL

```bash
cd demo4_wal_slot
docker-compose up -d
```

Здесь только Postgres, он поднимается за несколько секунд. Открываем `demo_wal_slot.ipynb`: создаём логический слот и читаем из него обычными `select`. Сначала текстом (плагин `test_decoding`), потом бинарным `pgoutput`, который использует Debezium. В конце небольшой парсер, который собирает из байтов те же `op`, `before` и `after`, что мы видели в демо 2.

### Куда заходить

| Что | Адрес | Демо |
| --- | --- | --- |
| Kafka | `localhost:9092` | 1, 2, 3 |
| Kafka UI | http://localhost:8080 | 1 |
| Debezium UI | http://localhost:8080 | 2, 3 |
| Kafka Connect (REST) | http://localhost:8083 | 2, 3 |
| Postgres (в демо 3 это master) | `localhost:5432`, логин и пароль `postgres` | 2, 3, 4 |
| Postgres DWH | `localhost:5434`, логин и пароль `postgres` | 3 |
