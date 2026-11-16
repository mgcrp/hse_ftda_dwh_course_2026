#!/usr/bin/env python3
"""Public checker for the HW02 warehouse contract and batch scenario."""

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid

import yaml


HERE = Path(__file__).resolve().parent
BUNDLED_CONTRACT = HERE / "hw02_contract.yaml"
DEFAULT_CONTRACT = (
    BUNDLED_CONTRACT if BUNDLED_CONTRACT.exists()
    else HERE.parent / "hw02/contract/hw02_contract.yaml"
)
SERVICES = ("crm", "catalog", "pos", "wms")
GROUPS = {
    "sales": ("v_sale_line",),
    "purchases": ("v_purchase_line",),
    "movements": ("v_movement",),
    "dimensions": (
        "v_sku", "v_customer", "v_pharmacy", "v_batch", "v_supplier",
        "v_distribution_center",
    ),
}
MANUAL = {
    "model.diagram": 1, "model.dds": 1, "model.mapping": 1,
    "engine.start": 1, "engine.initialize": 1,
    "incremental.scope": 1, "journal": 1,
    "bonus.iceberg": 2, "bonus.dbt": 1, "bonus.solo": 2,
}
TYPES = {
    "text": {"text", "varchar", "character varying", "char", "character", "string"},
    "int": {"smallint", "integer", "int", "bigint", "tinyint"},
    "numeric": {"numeric", "decimal"},
    "boolean": {"bool", "boolean"},
    "date": {"date"},
    "timestamptz": {"timestamp", "timestamp with time zone", "timestamp without time zone", "datetime"},
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def ident(value):
    require(isinstance(value, str) and re.fullmatch(r"[a-z_][a-z_0-9]*", value),
            f"Недопустимое SQL-имя {value!r}")
    return value


def canonical_json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def normalize(value, kind):
    if value is None:
        return None
    if kind == "text" and isinstance(value, str):
        return value
    if kind == "int" and type(value) is int:
        return value
    if kind == "boolean" and (type(value) is bool or type(value) is int and value in (0, 1)):
        return bool(value)
    if kind == "numeric" and isinstance(value, Decimal) and value.is_finite():
        return value
    if kind == "date" and type(value) is date:
        return value
    if kind == "timestamptz" and isinstance(value, datetime):
        return (value if value.tzinfo else value.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)
    raise ValueError(f"Ожидался {kind}, получен {type(value).__name__}")


class Database:
    def __init__(self, engine, conn, timeout, schema="contract", catalog=None):
        self.engine = engine
        self.schema = ident(schema)
        self.catalog = ident(catalog) if catalog else None
        self.conn = None
        self.config = conn
        self.timeout = timeout

    def connect(self):
        if self.conn is not None:
            return
        if self.engine in ("postgres", "greenplum"):
            import psycopg2

            self.conn = psycopg2.connect(
                os.environ[self.config["dsn_env"]], connect_timeout=self.timeout,
                options=f"-c statement_timeout={self.timeout * 1000} -c timezone=UTC",
            )
            self.conn.set_session(readonly=True, autocommit=True)
        else:
            settings = self.config
            password = os.environ[settings["password_env"]] if settings.get("password_env") else None
            if self.engine == "starrocks":
                import pymysql

                self.conn = pymysql.connect(
                    host=settings["host"], port=settings["port"], user=settings["user"],
                    password=password or "", connect_timeout=self.timeout,
                    read_timeout=self.timeout, write_timeout=self.timeout, autocommit=True,
                )
            else:
                import trino.dbapi
                from trino.auth import BasicAuthentication

                self.conn = trino.dbapi.connect(
                    host=settings["host"], port=settings["port"], user=settings["user"],
                    http_scheme=settings.get("http_scheme", "http"),
                    auth=BasicAuthentication(settings["user"], password) if password else None,
                    catalog=self.catalog, schema=self.schema, timezone="UTC",
                    request_timeout=self.timeout, max_attempts=1,
                    session_properties={"query_max_run_time": f"{self.timeout}s"},
                )

    def query(self, sql):
        self.connect()
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql)
            names = [col[0].lower() for col in cursor.description]
            return [dict(zip(names, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def relation(self, name):
        prefix = f"{self.catalog}." if self.catalog else ""
        return f"{prefix}{self.schema}.{ident(name)}"

    def read(self, name, columns):
        fields = ", ".join(map(ident, columns))
        return self.query(f"SELECT {fields} FROM {self.relation(name)}")

    def metadata(self, name):
        prefix = f"{self.catalog}." if self.catalog else ""
        where = f"table_schema = '{self.schema}' AND table_name = '{ident(name)}'"
        views = self.query(f"SELECT table_name FROM {prefix}information_schema.views WHERE {where}")
        require(views, f"{name}: представление отсутствует")
        rows = self.query(
            f"SELECT column_name, data_type FROM {prefix}information_schema.columns WHERE {where}"
        )
        return {row["column_name"].lower(): row["data_type"].lower() for row in rows}

    def close(self):
        if self.conn is not None:
            self.conn.close()


def check_types(db, name, columns):
    metadata = db.metadata(name)
    for field, spec in columns.items():
        actual = metadata.get(field, "")
        base = re.sub(r"\([^)]*\)", "", actual).strip()
        kind = spec["type"]
        compatible = base in TYPES[kind] or (
            db.engine == "starrocks" and kind == "boolean" and base == "tinyint"
        )
        require(compatible, f"{name}.{field}: ожидался {kind}, получен {actual or 'нет колонки'}")


def index_rows(rows, name, spec):
    result = {}
    for raw in rows:
        row = {}
        for field, column in spec["columns"].items():
            value = normalize(raw[field], column["type"])
            require(value is not None or column["nullable"], f"{name}.{field}: NULL")
            row[field] = value
        key = tuple(row[field] for field in spec["key"])
        require(key not in result, f"{name}: повтор ключа {key}")
        result[key] = row
    return result


def compare(expected, actual):
    missing = expected.keys() - actual.keys()
    extra = actual.keys() - expected.keys()
    changed = []
    count = 0
    for key in expected.keys() & actual.keys():
        if expected[key] != actual[key]:
            count += 1
            if len(changed) < 3:
                fields = [col for col in expected[key] if expected[key][col] != actual[key][col]]
                changed.append((key, fields))
    require(not (missing or extra or count),
            f"нет строк: {len(missing)} {list(missing)[:3]}; "
            f"лишних: {len(extra)} {list(extra)[:3]}; "
            f"расхождений: {count}; примеры ключей и полей: {changed}")
    return f"{len(expected)} строк совпали"


def validate_intervals(rows, fields):
    groups = defaultdict(list)
    for row in rows:
        groups[tuple(row[f] for f in fields)].append(row)
    for key, versions in groups.items():
        versions.sort(key=lambda row: row["valid_from"])
        for i, row in enumerate(versions):
            end = row["valid_to"]
            require(end is None or row["valid_from"] < end, f"{key}: пустой интервал")
            if i + 1 < len(versions):
                require(end is not None and end <= versions[i + 1]["valid_from"],
                        f"{key}: интервалы пересекаются")
    return groups


class Report:
    def __init__(self):
        self.checks = {}

    def add(self, name, points, status, detail):
        self.checks[name] = {
            "status": status, "points": points if status == "pass" else None
            if status in ("manual", "skipped", "blocked") else 0,
            "max": points, "detail": detail,
        }

    def run(self, name, points, check):
        try:
            detail = check()
        except Exception as exc:
            self.add(name, points, "fail", str(exc))
            return False
        self.add(name, points, "pass", detail)
        return True


class Checker:
    def __init__(self, contract, submission, root, timeout):
        self.views = contract["views"]
        self.submission = submission
        self.root = root
        self.timeout = timeout
        target = submission["target"]
        self.target = Database(target["engine"], target, timeout, contract["schema"], target.get("catalog"))
        self.sources = {
            service: Database("postgres", {"dsn_env": submission["sources"][service]}, timeout)
            for service in SERVICES
        }
        self.report = Report()

    def close(self):
        for db in [self.target, *self.sources.values()]:
            db.close()

    def target_rows(self, name, check_metadata=False):
        spec = self.views[name]
        if check_metadata:
            check_types(self.target, name, spec["columns"])
        return index_rows(self.target.read(name, spec["columns"]), name, spec)

    def source_rows(self, name):
        spec = self.views[name]
        service, view = spec["source"].split(".")
        db = self.sources[service]
        cols = list(spec["columns"])
        if name == "v_sale_line":
            header_cols = ("pharmacy_bk", "customer_bk", "receipt_dt", "doc_type",
                           "parent_receipt_bk", "currency")
            cols = [col for col in cols if col not in header_cols]
            headers = {
                row["receipt_bk"]: row
                for row in db.read("v_receipt", ("receipt_bk", *header_cols))
            }
        rows = db.read(view, cols)
        if name in ("v_sale_line", "v_movement"):
            visible = {
                row["customer_bk"] for row in
                self.sources["crm"].read("v_customer", ["customer_bk"])
            }
            for row in rows:
                if name == "v_sale_line":
                    row.update(headers[row["receipt_bk"]])
                    if row["customer_bk"] not in visible:
                        row["customer_bk"] = None
                elif row["dst_type"] == "customer" and row["dst_bk"] not in visible:
                    row["dst_bk"] = None
        return index_rows(rows, name, spec)

    def compare_view(self, name, minimum=False):
        actual = self.target_rows(name, check_metadata=minimum)
        expected = self.source_rows(name)
        if minimum:
            require(len(expected) >= self.views[name]["min_rows"],
                    f"{name}: в источнике {len(expected)} строк, минимум {self.views[name]['min_rows']}")
        return compare(expected, actual)

    def static_group(self, names):
        for name in names:
            self.compare_view(name, minimum=True)
        self.check_links(names)
        return ", ".join(names)

    def check_links(self, names):
        dimensions = {}

        def lookup(name):
            if name not in dimensions:
                dimensions[name] = self.target_rows(name)
            return dimensions[name]

        def link(name, value):
            if value is None:
                return None
            row = lookup(name).get((value,))
            require(row is not None, f"{name}: отсутствует ссылка {value}")
            return row

        for name in names:
            if name == "v_batch":
                for row in lookup(name).values():
                    link("v_sku", row["sku"])
            if name not in ("v_sale_line", "v_purchase_line", "v_movement"):
                continue
            rows = self.target_rows(name)
            if name == "v_sale_line":
                receipts = {row["receipt_bk"]: row["doc_type"] for row in rows.values()}
            for row in rows.values():
                link("v_sku", row["sku"])
                batch = link("v_batch", row["batch_bk"])
                require(batch is None or batch["sku"] == row["sku"],
                        f"Партия {row['batch_bk']} не соответствует SKU {row['sku']}")
                if name == "v_sale_line":
                    link("v_customer", row["customer_bk"])
                    link("v_pharmacy", row["pharmacy_bk"])
                    if row["doc_type"] == "refund":
                        require(receipts.get(row["parent_receipt_bk"]) == "sale",
                                f"Возврат {row['receipt_bk']}: отсутствует исходная продажа")
                if name == "v_purchase_line":
                    link("v_supplier", row["supplier_bk"])
                    link("v_distribution_center", row["dc_bk"])
                if name == "v_movement":
                    dimensions_by_type = {"dc": "v_distribution_center", "pharmacy": "v_pharmacy",
                                          "supplier": "v_supplier", "customer": "v_customer"}
                    for endpoint in ("src", "dst"):
                        kind = row[f"{endpoint}_type"]
                        key = row[f"{endpoint}_bk"]
                        if key is not None:
                            require(kind in dimensions_by_type, f"Неизвестный тип локации: {kind}")
                            link(dimensions_by_type[kind], key)

    def snapshot_services(self):
        name = "v_cdc_events"
        check_types(self.target, name, self.views[name]["columns"])
        rows = self.target.query(
            f"SELECT source_system, COUNT(*) AS n FROM {self.target.relation(name)} "
            "WHERE op = 'r' GROUP BY source_system"
        )
        return {row["source_system"]: row["n"] for row in rows}

    def static(self):
        for group, names in GROUPS.items():
            self.report.run(f"dds.{group}", 1, lambda: self.static_group(names))
        try:
            counts = self.snapshot_services()
        except Exception as exc:
            for service in SERVICES:
                self.report.add(f"cdc.{service}.snapshot", 0.5, "fail", str(exc))
        else:
            for service in SERVICES:
                count = counts.get(service, 0)
                self.report.add(f"cdc.{service}.snapshot", 0.5, "pass" if count else "fail",
                                f"{count} записей snapshot в raw")

    def command(self, name, *args, payload=None):
        argv = self.submission["commands"][name]
        completed = subprocess.run(
            [*argv, *args], cwd=self.root, input=payload, text=True, capture_output=True,
            timeout=self.timeout, check=False,
        )
        require(completed.returncode == 0,
                f"{name}: код {completed.returncode}; {completed.stderr.strip()[-800:]}")
        return json.loads(completed.stdout) if name == "fixture" else None

    def load(self, batch_id, replay=False):
        args = ["--batch-id", batch_id]
        if replay:
            args.append("--replay")
        self.command("load", *args)

    def run_phase(self, consumer, phase, request, batch_id):
        context = consumer.mark()
        payload = canonical_json({**request, "batch_id": batch_id})
        fixture = self.command("fixture", phase, payload=payload)
        events = consumer.validate_events(fixture["events"])
        if phase == "delta":
            require({event[0] for event in events} == set(SERVICES),
                    "delta: нужны события всех четырёх сервисов")
        elif phase == "history":
            require(any(event[0] == "catalog" and event[3] == "u" for event in events),
                    "history: ожидается UPDATE из каталога")
        elif phase == "privacy":
            require(any(event[0] == "crm" and event[3] == "u" for event in events),
                    "privacy: ожидается UPDATE из CRM")
        delivered = consumer.await_events(context, events, self.timeout)
        raw_seen = self.await_raw(delivered)
        if phase == "delta":
            for service in SERVICES:
                expected = sum(event[0] == service for event in events)
                kafka_count = sum(record[0] == service for record in delivered)
                raw_count = sum(record[0] == service for record in raw_seen)
                self.report.add(f"cdc.{service}.delta", 0.5,
                                "pass" if raw_count == expected else "fail",
                                f"ожидалось {expected}; в Kafka {kafka_count}; в raw {raw_count}")
        require(len(raw_seen) == len(events), f"{phase}: в raw дошли {len(raw_seen)} из {len(events)} событий")
        self.load(batch_id)
        return fixture, raw_seen

    def await_raw(self, records):
        pending = set(records)
        seen = set()
        deadline = time.monotonic() + self.timeout
        relation = self.target.relation("v_cdc_events")
        while pending and time.monotonic() < deadline:
            by_topic = defaultdict(set)
            for _, _, _, _, topic, partition, offset in pending:
                by_topic[topic].add((partition, offset))
            for topic, positions in by_topic.items():
                # Topics are validated against the manifest and the allowed Kafka alphabet.
                conditions = " OR ".join(
                    f"(partition_id = {partition} AND kafka_offset = {offset})"
                    for partition, offset in positions
                )
                rows = self.target.query(
                    "SELECT source_system, source_table, event_key, op, topic, partition_id, kafka_offset "
                    f"FROM {relation} WHERE topic = '{topic}' AND ({conditions})"
                )
                keys = [(row["partition_id"], row["kafka_offset"]) for row in rows]
                require(len(keys) == len(set(keys)), f"В v_cdc_events повтор offset в топике {topic}")
                for row in rows:
                    record = (
                        row["source_system"], row["source_table"], canonical_json(json.loads(row["event_key"])),
                        row["op"], row["topic"], row["partition_id"], row["kafka_offset"],
                    )
                    if record in pending:
                        pending.remove(record)
                        seen.add(record)
            if pending:
                time.sleep(0.5)
        return seen

    def check_delta(self, keys, before):
        scopes = (
            ("v_customer", "customer_bk"), ("v_sku", "sku"), ("v_sale_line", "receipt_bk"),
            ("v_purchase_line", "purchase_bk"), ("v_movement", "movement_bk"),
        )
        for name, field in scopes:
            value = keys[field]
            require(not any(row[field] == value for row in before[name].values()),
                    f"delta: ключ {field}={value} уже существовал")
            actual = self.target_rows(name)
            require(any(row[field] == value for row in actual.values()),
                    f"delta: {name} не содержит {field}={value}")
            self.compare_view(name)
        for name, column in (("v_sale_line", "receipt_bk"), ("v_purchase_line", "purchase_bk"),
                             ("v_movement", "movement_bk")):
            rows = [row for row in self.target_rows(name).values() if row[column] == keys[column]]
            require(any(row["sku"] == keys["sku"] and
                        (name != "v_sale_line" or row["customer_bk"] == keys["customer_bk"])
                        for row in rows), f"delta: новые записи {name} не связаны с тестовыми SKU/клиентом")
        self.check_links(tuple(name for name, _ in scopes))
        return "Новые ключи совпали с источниками, связи проверены"

    def check_history(self, keys, before, new_name):
        sku = keys["sku"]
        current = self.target_rows("v_sku")[(sku,)]
        require(current["sku_name"] == new_name, "Не применено новое название SKU")
        self.compare_view("v_sku")
        price = self.target_rows("v_price_history", check_metadata=True)
        self.compare_view("v_price_history")
        validate_intervals(price.values(), ("sku", "price_scope", "scope_bk"))
        old_price = {key: row for key, row in before["v_price_history"].items() if row["sku"] == sku}
        require(old_price, "У тестового SKU до изменения должна быть цена")
        for key, row in old_price.items():
            require(key in price and price[key]["price"] == row["price"],
                    "Предыдущая версия цены потеряна или переписана")
        require(any(key not in before["v_price_history"] and
                    any(old_key[:3] == key[:3] and price[old_key]["valid_to"] == row["valid_from"]
                        and old["price"] != row["price"] for old_key, old in old_price.items())
                    for key, row in price.items() if row["sku"] == sku),
                "Нет новой цены в той же области, начинающейся в конце старой версии")
        history = self.target_rows("v_sku_history", check_metadata=True)
        groups = validate_intervals(history.values(), ("sku",))
        for (current_sku,), row in self.target_rows("v_sku").items():
            versions = groups.get((current_sku,), [])
            opened = [version for version in versions if version["valid_to"] is None]
            require(len(opened) == 1 and all(opened[0][field] == row[field]
                    for field in ("sku_name", "category", "is_rx")),
                    f"{current_sku}: история не отражает текущее состояние SKU")
        previous = [key for key, row in before["v_sku_history"].items()
                    if row["sku"] == sku and row["valid_to"] is None]
        require(len(previous) == 1, "До изменения должна быть одна открытая версия SKU")
        key = previous[0]
        require(key in history and history[key]["valid_to"] is not None and
                all(history[key][field] == before["v_sku_history"][key][field]
                    for field in ("sku_name", "category", "is_rx")),
                "Предыдущая версия SKU не сохранена")
        compare(before["v_sale_line"], self.target_rows("v_sale_line"))
        return "Старые и новые версии доступны, снимок продажи прежний"

    def check_privacy(self, keys, before):
        customer = keys["customer_bk"]
        self.compare_view("v_customer")
        require((customer,) not in self.target_rows("v_customer"), "Клиент остался в аналитике")
        sales = before["v_sale_line"]
        affected = [row for row in sales.values() if row["customer_bk"] == customer]
        require(affected, "У тестового клиента до удаления должен быть чек")
        for row in affected:
            row["customer_bk"] = None
        compare(sales, self.target_rows("v_sale_line"))
        self.compare_view("v_sale_line")
        self.compare_view("v_movement")
        return "Клиент скрыт, позиции чеков и суммы сохранены"

    def batch_record(self, batch_id):
        row = self.target_rows("v_load_batches", check_metadata=True)[(batch_id,)]
        require(row["status"] == "success", "Порция не завершилась со статусом success")
        offsets = json.loads(row["offsets_json"])
        require(isinstance(offsets, list) and offsets, "Ожидался непустой массив диапазонов")
        ranges = {}
        for entry in offsets:
            topic = entry["topic"]
            partition = entry["partition"]
            start = entry["start_offset"]
            end = entry["end_offset"]
            require(all(type(value) is int for value in (partition, start, end))
                    and partition >= 0 and 0 <= start < end,
                    "Нужны целые значения partition и непустой диапазон [start_offset, end_offset)")
            key = (topic, partition)
            require(key not in ranges, f"Повтор диапазона {key}")
            ranges[key] = (start, end)
        return ranges

    def check_journal(self, batch_id, before_offsets, raw):
        ranges = self.batch_record(batch_id)
        require(ranges == before_offsets, "При повторе изменились границы порции")
        for _, _, _, _, topic, partition, offset in raw:
            bounds = ranges.get((topic, partition))
            require(bounds is not None and bounds[0] <= offset < bounds[1],
                    f"Событие {topic}/{partition}/{offset} вне границ порции")
        return "Границы порции покрывают тестовые события и не изменились при повторе"

    def scenario(self):
        def warehouse():
            return {name: self.target_rows(name) for name in self.views if name not in
                    ("v_cdc_events", "v_load_batches")}

        with KafkaProbe(self.submission["kafka"], self.timeout) as consumer:
            # Baseline source state is sufficient to test novelty even when DDS is incomplete.
            before = {name: self.source_rows(name) for name in
                      ("v_customer", "v_sku", "v_sale_line", "v_purchase_line", "v_movement")}
            run_id = uuid.uuid4().hex
            batch_id = f"{run_id}-delta"
            fixture, raw = self.run_phase(consumer, "delta", {}, batch_id)
            keys = fixture["keys"]
            required_keys = ("customer_bk", "sku", "receipt_bk", "purchase_bk", "movement_bk")
            require(all(isinstance(keys.get(field), str) and keys[field] for field in required_keys),
                    "delta: отсутствуют тестовые бизнес-ключи")
            self.report.run("incremental.delta", 1, lambda: self.check_delta(keys, before))

            try:
                saved = warehouse()
            except Exception as exc:
                self.report.add("incremental.replay", 1, "blocked", str(exc))
                saved = None
            try:
                offsets = self.batch_record(batch_id)
            except Exception as exc:
                self.report.add("incremental.batch_log", 1, "fail", str(exc))
                offsets = None

            def check_replay():
                for name, rows in warehouse().items():
                    compare(saved[name], rows)
                return "Данные и версии не изменились"

            try:
                self.load(batch_id, replay=True)
            except Exception as exc:
                self.report.add("incremental.replay", 1, "fail", str(exc))
                if offsets is not None:
                    self.report.add("incremental.batch_log", 1, "blocked",
                                    "Проверка зависит от успешного повтора порции")
            else:
                if saved is not None:
                    self.report.run("incremental.replay", 1, check_replay)
                if offsets is not None:
                    self.report.run("incremental.batch_log", 1,
                                    lambda: self.check_journal(batch_id, offsets, raw))

            new_name = f"Проверка ДЗ2 {run_id[:10]}"

            def history():
                previous = {name: self.target_rows(name) for name in
                            ("v_sku_history", "v_price_history", "v_sale_line")}
                self.run_phase(consumer, "history", {"keys": keys, "sku_name": new_name},
                               f"{run_id}-history")
                return self.check_history(keys, previous, new_name)

            def privacy():
                previous = {"v_sale_line": self.target_rows("v_sale_line")}
                self.run_phase(consumer, "privacy", {"keys": keys}, f"{run_id}-privacy")
                return self.check_privacy(keys, previous)

            self.report.run("history.versions", 1, history)
            self.report.run("history.privacy", 1, privacy)


class KafkaProbe:
    def __init__(self, config, timeout):
        from confluent_kafka import Consumer

        self.topics = config["topics"]
        self.timeout = timeout
        self.client = Consumer({
            "bootstrap.servers": config["bootstrap_servers"],
            "group.id": f"hw02-check-{uuid.uuid4().hex}",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "error",
        })

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.client.close()

    def mark(self):
        from confluent_kafka import TopicPartition

        metadata = self.client.list_topics(timeout=self.timeout)
        positions = []
        for topic in self.topics:
            require(topic in metadata.topics and not metadata.topics[topic].error,
                    f"Топик {topic} отсутствует в Kafka")
            for partition in metadata.topics[topic].partitions:
                _, end = self.client.get_watermark_offsets(TopicPartition(topic, partition),
                                                           timeout=self.timeout)
                positions.append(TopicPartition(topic, partition, end))
        return positions

    def validate_events(self, events):
        result = []
        for event in events:
            topic = event["topic"]
            route = self.topics[topic]
            require(event["key"] is not None and event["op"] in ("c", "u", "d"),
                    "Для события нужны ненулевой JSON-ключ и операция c/u/d")
            result.append((route["service"], route["table"], canonical_json(event["key"]), event["op"], topic))
        require(result, "check_fixture не вернул ни одного ожидаемого события")
        return result

    def await_events(self, positions, events, timeout):
        self.client.assign(positions)
        pending = Counter(events)
        records = []
        deadline = time.monotonic() + timeout
        try:
            while pending and time.monotonic() < deadline:
                message = self.client.poll(min(0.5, max(0, deadline - time.monotonic())))
                if message is None:
                    continue
                require(not message.error(), f"Kafka: {message.error()}")
                if message.key() is None or message.value() is None:
                    continue
                key = unwrap(json.loads(message.key()))
                payload = unwrap(json.loads(message.value()))
                route = self.topics[message.topic()]
                if not isinstance(payload, dict):
                    continue
                source = payload.get("source") or {}
                if f"{source.get('schema')}.{source.get('table')}" != route["table"]:
                    continue
                event = (route["service"], route["table"], canonical_json(key),
                         payload.get("op"), message.topic())
                if pending[event]:
                    records.append((*event, message.partition(), message.offset()))
                    pending[event] -= 1
                    if not pending[event]:
                        del pending[event]
        finally:
            self.client.unassign()
        return records


def unwrap(value):
    if isinstance(value, dict) and {"schema", "payload"} <= value.keys():
        return value["payload"]
    return value


def validate_manifest(submission, scenario=False):
    require(submission["version"] == 1, "Версия манифеста должна быть 1")
    require(set(submission["sources"]) == set(SERVICES), "Укажите подключения ко всем четырём источникам")
    target = submission["target"]
    require(target["engine"] in ("greenplum", "starrocks", "trino"), "Неизвестный движок")
    require(target["engine"] != "trino" or target.get("catalog"), "Укажите Iceberg-каталог")
    if not scenario:
        return
    topics = submission["kafka"]["topics"]
    require({route["service"] for route in topics.values()} == set(SERVICES),
            "kafka.topics должен покрывать четыре сервиса")
    for topic, route in topics.items():
        require(re.fullmatch(r"[A-Za-z0-9_.-]+", topic), f"Недопустимое имя топика {topic}")
        require(len(route["table"].split(".")) == 2, "Укажите исходную таблицу в формате schema.table")
    for command in ("fixture", "load"):
        argv = submission["commands"][command]
        require(isinstance(argv, list) and argv and all(isinstance(arg, str) for arg in argv),
                f"commands.{command}: ожидается список аргументов")


def self_test(contract, contract_path):
    require(contract["version"] == 1 and contract["homework"] == "hw02", "Неверный контракт")
    require(contract["schema"] == "contract_dwh", "Неверное имя схемы контракта")
    views = contract["views"]
    for name, spec in views.items():
        ident(name)
        require(spec["key"] and len(set(spec["key"])) == len(spec["key"]), f"{name}: неверный key")
        for field, column in spec["columns"].items():
            ident(field)
            require(column["type"] in TYPES and type(column["nullable"]) is bool,
                    f"{name}.{field}: неверная спецификация")
        for field in spec["key"]:
            require(not spec["columns"][field]["nullable"], f"{name}.{field}: ключ допускает NULL")
        if "source" in spec:
            service, view = spec["source"].split(".")
            require(service in SERVICES and spec["min_rows"] > 0, f"{name}: неверный источник")
            ident(view)
    example = contract_path.parent / "hw02_submission.example.yaml"
    if not example.exists():
        example = contract_path.parent.parent / "student_kit/hw02_submission.example.yaml"
    validate_manifest(yaml.safe_load(example.read_text(encoding="utf-8")), scenario=True)
    require(sum(points for _, points in automatic_checks()) +
            sum(points for name, points in MANUAL.items() if not name.startswith("bonus.")) == 20,
            "Обязательная часть должна давать 20 баллов")
    print(f"OK: {len(views)} представлений, пример манифеста, 20 + 5 баллов")


def automatic_checks():
    yield from ((f"dds.{group}", 1) for group in GROUPS)
    for service in SERVICES:
        yield f"cdc.{service}.snapshot", 0.5
        yield f"cdc.{service}.delta", 0.5
    for name in ("history.versions", "history.privacy", "incremental.delta",
                 "incremental.replay", "incremental.batch_log"):
        yield name, 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    parser.add_argument("--submission", type=Path)
    parser.add_argument("--scenario", action="store_true", help="Разрешить тестовые записи в учебный стенд")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--timeout", type=int, default=90, help="Секунды на отдельную команду, запрос или ожидание")
    parser.add_argument("--json", type=Path, help="Путь для JSON-отчёта")
    args = parser.parse_args()
    require(args.timeout > 0, "--timeout должен быть положительным")
    contract = yaml.safe_load(args.contract.read_text(encoding="utf-8"))
    if args.self_test:
        self_test(contract, args.contract)
        return 0
    if not args.submission:
        parser.error("укажите --submission или --self-test")
    submission = yaml.safe_load(args.submission.read_text(encoding="utf-8"))
    validate_manifest(submission)
    checker = Checker(contract, submission, args.submission.resolve().parent, args.timeout)
    blocked = ""
    try:
        checker.static()
        if args.scenario:
            try:
                validate_manifest(submission, scenario=True)
                checker.scenario()
            except Exception as exc:
                blocked = str(exc)
                print(f"Сценарий прерван: {blocked}", file=sys.stderr)
    finally:
        checker.close()

    report = checker.report
    for name, points in automatic_checks():
        if name not in report.checks:
            report.add(name, points, "blocked" if args.scenario else "skipped",
                       blocked or ("Не выполнено в сценарии" if args.scenario else "Нужен --scenario"))
    for name, points in MANUAL.items():
        report.add(name, points, "manual", "Проверяет преподаватель")
    for name, check in report.checks.items():
        points = "—" if check["points"] is None else f"{check['points']:g}"
        print(f"{check['status']:7} {name:28} {points}/{check['max']:g}  {check['detail']}")
    automatic = sum(check["points"] or 0 for check in report.checks.values())
    print(f"Подтверждено автоматически: {automatic:g}/13. Ручная часть: до 7, бонусы: до 5.")
    if args.json:
        args.json.write_text(json.dumps({
            "checks": report.checks, "automatic_points": automatic, "automatic_max": 13,
            "manual_max": 7, "bonus_max": 5, "full_credit": 20,
        }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return int(any(check["status"] in ("fail", "blocked") for check in report.checks.values()))


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, KeyError, TypeError, yaml.YAMLError) as exc:
        sys.exit(f"Ошибка: {exc}")
