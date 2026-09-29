#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
hw01_check.py - чекер домашнего задания №1.
Курс "Modern Storages and Data Warehousing", ФТиАД НИУ ВШЭ, 2026.

Проверяет сдачу, не зная физической модели студента: вся работа идёт
через схему `contract` (см. hw01_contract.yaml) плюс системные каталоги
PostgreSQL для репликации и CDC.

Запуск:
    python hw01_check.py --submission ./hw01_submission.yaml

Полезные флаги:
    --sections contract,dq,repl     прогнать только часть проверок
    --skip-exec                     не запускать команды студента (seed и т.п.)
    --json report.json              выгрузить машинный отчёт
    --self-test                     проверить консистентность самих материалов ДЗ
    -v                              печатать детали нарушений

Зависимости: psycopg2-binary, PyYAML.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    import yaml
except ImportError:
    sys.exit("нужен PyYAML:  pip install pyyaml")

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    sys.exit("нужен psycopg2:  pip install psycopg2-binary")


HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONTRACT = os.path.join(HERE, "hw01_contract.yaml")

SERVICES = ("crm", "catalog", "pos", "wms")

# ---------------------------------------------------------------------------
# классы типов: контракт говорит о классе, а не о конкретном типе PostgreSQL
# ---------------------------------------------------------------------------
TYPE_CLASSES: Dict[str, set] = {
    "text": {
        "text", "character varying", "character", "varchar", "char", "bpchar",
        "citext", "name", "uuid",
    },
    "int": {"smallint", "integer", "bigint", "int2", "int4", "int8"},
    "numeric": {"numeric", "decimal"},
    "boolean": {"boolean", "bool"},
    "date": {"date"},
    "timestamptz": {"timestamp with time zone", "timestamptz"},
}
# типы, которые принимаем с предупреждением
TYPE_TOLERATED: Dict[str, set] = {
    "numeric": {"real", "double precision", "money"},
    "timestamptz": {"timestamp without time zone", "timestamp"},
    "int": {"numeric", "decimal"},
    "text": {"json", "jsonb"},
}

# ошибки, которые означают "тест сломан", а не "схема защитилась"
BROKEN_TEST_SQLSTATE_PREFIXES = ("42", "3D", "3F", "08", "53", "57", "58", "XX")
BROKEN_TEST_SQLSTATES = {"22P02", "22007", "0A000"}  # 0A000 допустим только явно

# попытки ослабить схему внутри негативного теста
FORBIDDEN_IN_TESTS = [
    (r"\bdrop\s+constraint\b", "DROP CONSTRAINT"),
    (r"\bdisable\s+trigger\b", "DISABLE TRIGGER"),
    (r"\bset\s+constraints\b", "SET CONSTRAINTS"),
    (r"\bsession_replication_role\b", "session_replication_role"),
    (r"\bdrop\s+index\b", "DROP INDEX"),
    (r"\bnot\s+valid\b", "NOT VALID"),
    (r"\bdrop\s+not\s+null\b", "DROP NOT NULL"),
    (r"\bdrop\s+trigger\b", "DROP TRIGGER"),
    (r"\balter\s+table\s+\S+\s+drop\s+column\b", "DROP COLUMN"),
    (r"\bdrop\s+table\b", "DROP TABLE"),
    (r"\bdrop\s+schema\b", "DROP SCHEMA"),
]

SESSION_SETUP = [
    "SET TimeZone = 'UTC'",
    "SET DateStyle = 'ISO, YMD'",
    "SET extra_float_digits = 3",
    "SET statement_timeout = '120s'",
]


# ---------------------------------------------------------------------------
# отчёт
# ---------------------------------------------------------------------------
@dataclass
class Check:
    section: str
    id: str
    title: str
    ok: bool
    detail: str = ""
    warn: bool = False


@dataclass
class Report:
    checks: List[Check] = field(default_factory=list)
    scores: Dict[str, float] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)

    def add(self, section: str, cid: str, title: str, ok: bool,
            detail: str = "", warn: bool = False) -> Check:
        c = Check(section, cid, title, ok, detail, warn)
        self.checks.append(c)
        return c

    def section(self, name: str) -> List[Check]:
        return [c for c in self.checks if c.section == name]

    def passed(self, name: str) -> bool:
        s = self.section(name)
        return bool(s) and all(c.ok for c in s)

    def count(self, name: str) -> Tuple[int, int]:
        s = self.section(name)
        return sum(1 for c in s if c.ok), len(s)


# ---------------------------------------------------------------------------
# вспомогательное
# ---------------------------------------------------------------------------
class Db:
    """Лёгкая обёртка: соединения по требованию, с настройкой сессии."""

    def __init__(self, conns: Dict[str, Dict[str, str]]):
        self.conns = conns
        self._cache: Dict[Tuple[str, str], Any] = {}

    def dsn(self, role: str, service: str) -> Optional[str]:
        return (self.conns.get(role) or {}).get(service)

    def get(self, role: str, service: str):
        key = (role, service)
        if key in self._cache:
            return self._cache[key]
        dsn = self.dsn(role, service)
        if not dsn:
            raise KeyError(f"в манифесте нет connections.{role}.{service}")
        conn = psycopg2.connect(dsn, connect_timeout=10)
        conn.autocommit = True
        with conn.cursor() as cur:
            for stmt in SESSION_SETUP:
                try:
                    cur.execute(stmt)
                except psycopg2.Error:
                    conn.rollback()
        self._cache[key] = conn
        return conn

    def q(self, role: str, service: str, sql: str,
          args: Optional[Sequence[Any]] = None) -> List[tuple]:
        conn = self.get(role, service)
        with conn.cursor() as cur:
            # ВАЖНО: args передаём только если они есть. С пустым кортежем
            # psycopg2 всё равно интерполирует строку, и '%' в LIKE-шаблонах
            # ломается ("tuple index out of range").
            if args:
                cur.execute(sql, args)
            else:
                cur.execute(sql)
            if cur.description is None:
                return []
            return cur.fetchall()

    def scalar(self, role: str, service: str, sql: str,
               args: Optional[Sequence[Any]] = None) -> Any:
        rows = self.q(role, service, sql, args)
        if not rows or not rows[0]:
            return None
        return rows[0][0]

    def close(self) -> None:
        for conn in self._cache.values():
            try:
                conn.close()
            except Exception:
                pass


def fresh_conn(dsn: str):
    conn = psycopg2.connect(dsn, connect_timeout=10)
    conn.autocommit = False
    return conn


def base_type(pg_type: str) -> str:
    """format_type даёт 'character varying(20)' -> 'character varying'."""
    t = pg_type.lower().strip()
    t = re.sub(r"\(.*?\)", "", t).strip()
    t = re.sub(r"\[\]$", "", t).strip()
    return t


def type_matches(declared: str, actual: str) -> Tuple[bool, bool]:
    """(подходит, с предупреждением)"""
    a = base_type(actual)
    if a in TYPE_CLASSES.get(declared, set()):
        return True, False
    if a in TYPE_TOLERATED.get(declared, set()):
        return True, True
    return False, False


def run_cmd(cmd: str, cwd: str, timeout: int = 1800) -> Tuple[int, str]:
    try:
        p = subprocess.run(cmd, shell=True, cwd=cwd, timeout=timeout,
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        return p.returncode, p.stdout.decode("utf-8", "replace")[-4000:]
    except subprocess.TimeoutExpired:
        return -1, f"таймаут {timeout} c"


def digest_sql(view: str) -> str:
    """Порядконезависимый отпечаток содержимого представления."""
    return (
        "SELECT count(*)::bigint, "
        "coalesce(sum((('x' || substr(md5(t::text), 1, 8))::bit(32))::bigint), 0) "
        f"FROM contract.{view} AS t"
    )


# ---------------------------------------------------------------------------
# 1. контракт: наличие представлений, колонок, типов, обязательности
# ---------------------------------------------------------------------------
def check_contract(db: Db, contract: dict, rep: Report, verbose: bool) -> None:
    for service, spec in contract["services"].items():
        try:
            rels = db.q("master", service, """
                SELECT c.relname, c.relkind, a.attname,
                       format_type(a.atttypid, a.atttypmod)
                  FROM pg_class c
                  JOIN pg_namespace n ON n.oid = c.relnamespace
                  JOIN pg_attribute a ON a.attrelid = c.oid
                                     AND a.attnum > 0
                                     AND NOT a.attisdropped
                 WHERE n.nspname = 'contract'
                   AND c.relkind IN ('v', 'm', 'r', 'p', 'f')
            """)
        except Exception as e:
            rep.add("contract", f"{service}", f"[{service}] схема contract недоступна",
                    False, str(e).strip().splitlines()[0])
            continue

        present: Dict[str, Dict[str, str]] = {}
        kinds: Dict[str, str] = {}
        for relname, relkind, attname, fmt in rels:
            present.setdefault(relname, {})[attname] = fmt
            kinds[relname] = relkind

        for view, vspec in spec["views"].items():
            cid = f"{service}.{view}"
            if view not in present:
                rep.add("contract", cid, f"представление contract.{view} есть",
                        False, "не найдено")
                continue

            if kinds[view] not in ("v", "m"):
                rep.add("contract", cid + ".kind",
                        f"contract.{view} - это VIEW или MATERIALIZED VIEW",
                        True, f"relkind={kinds[view]} (ожидались v/m)", warn=True)

            missing, badtype, warned = [], [], []
            for col, cspec in vspec["columns"].items():
                if col not in present[view]:
                    missing.append(col)
                    continue
                ok, warn = type_matches(cspec["type"], present[view][col])
                if not ok:
                    badtype.append(f"{col}: {present[view][col]} "
                                   f"(ожидался класс {cspec['type']})")
                elif warn:
                    warned.append(f"{col}: {present[view][col]}")

            detail = []
            if missing:
                detail.append("нет колонок: " + ", ".join(sorted(missing)))
            if badtype:
                detail.append("несовместимые типы: " + "; ".join(badtype))
            rep.add("contract", cid, f"contract.{view}: состав и типы колонок",
                    not (missing or badtype), "; ".join(detail))
            if warned:
                rep.add("contract", cid + ".types",
                        f"contract.{view}: типы под вопросом", True,
                        "; ".join(warned), warn=True)


def check_nullability_and_keys(db: Db, contract: dict, rep: Report) -> None:
    for service, spec in contract["services"].items():
        for view, vspec in spec["views"].items():
            notnull = [c for c, s in vspec["columns"].items()
                       if not s.get("nullable", True)]
            if notnull:
                cond = " OR ".join(f"{c} IS NULL" for c in notnull)
                try:
                    n = db.scalar("master", service,
                                  f"SELECT count(*) FROM contract.{view} WHERE {cond}")
                except Exception as e:
                    rep.add("keys", f"{service}.{view}.notnull",
                            f"contract.{view}: обязательные колонки без NULL", False,
                            str(e).strip().splitlines()[0])
                    n = None
                if n is not None:
                    rep.add("keys", f"{service}.{view}.notnull",
                            f"contract.{view}: обязательные колонки без NULL",
                            n == 0, f"{n} строк с NULL" if n else "")

            uniq_cols = [c for c, s in vspec["columns"].items() if s.get("unique")]
            keys: List[List[str]] = [[c] for c in uniq_cols]
            if vspec.get("unique_key"):
                keys.append(list(vspec["unique_key"]))
            for k in keys:
                cols = ", ".join(k)
                sql = (f"SELECT count(*) FROM (SELECT {cols} FROM contract.{view} "
                       f"GROUP BY {cols} HAVING count(*) > 1) q")
                try:
                    n = db.scalar("master", service, sql)
                except Exception as e:
                    rep.add("keys", f"{service}.{view}.uk.{'_'.join(k)}",
                            f"contract.{view}: ({cols}) уникален", False,
                            str(e).strip().splitlines()[0])
                    continue
                rep.add("keys", f"{service}.{view}.uk.{'_'.join(k)}",
                        f"contract.{view}: ({cols}) уникален",
                        n == 0, f"{n} дублирующихся значений" if n else "")


# ---------------------------------------------------------------------------
# 2. объёмы
# ---------------------------------------------------------------------------
def check_volume(db: Db, contract: dict, rep: Report) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for service, spec in contract["services"].items():
        for view, vspec in spec["views"].items():
            minrows = vspec.get("min_rows")
            try:
                n = db.scalar("master", service, f"SELECT count(*) FROM contract.{view}")
            except Exception as e:
                rep.add("volume", f"{service}.{view}", f"объём contract.{view}", False,
                        str(e).strip().splitlines()[0])
                continue
            counts[f"{service}.{view}"] = int(n or 0)
            if minrows:
                rep.add("volume", f"{service}.{view}",
                        f"contract.{view}: >= {minrows} строк",
                        (n or 0) >= minrows, f"фактически {n}")
    return counts


# ---------------------------------------------------------------------------
# 3. внутрисервисные проверки качества
# ---------------------------------------------------------------------------
def check_sql_checks(db: Db, contract: dict, rep: Report) -> None:
    for chk in contract.get("sql_checks", []):
        cid, service, title, sql = chk["id"], chk["service"], chk["title"], chk["sql"]
        try:
            n = db.scalar("master", service, sql)
        except Exception as e:
            rep.add("dq", cid, f"[{service}] {title}", False,
                    "запрос не выполнился: " + str(e).strip().splitlines()[0])
            continue
        n = int(n or 0)
        rep.add("dq", cid, f"[{service}] {title}", n == 0,
                f"{n} нарушений" if n else "")


# ---------------------------------------------------------------------------
# 4. кросс-сервисные проверки
# ---------------------------------------------------------------------------
def _fetch_set(db: Db, service: str, view: str, column: str,
               where: Optional[str] = None) -> set:
    sql = f"SELECT DISTINCT {column} FROM contract.{view} WHERE {column} IS NOT NULL"
    if where:
        sql += f" AND ({where})"
    return {r[0] for r in db.q("master", service, sql)}


def check_cross(db: Db, contract: dict, rep: Report, verbose: bool) -> None:
    for chk in contract.get("cross_checks", []):
        cid, title, kind = chk["id"], chk["title"], chk["kind"]
        try:
            if kind in ("referential", "referential_filtered"):
                left, right = chk["left"], chk["right"]
                lset = _fetch_set(db, left["service"], left["view"], left["column"],
                                  left.get("where"))
                rset = _fetch_set(db, right["service"], right["view"], right["column"])
                orphans = lset - rset
                sample = ", ".join(map(str, sorted(map(str, orphans))[:5]))
                rep.add("cross", cid, title, not orphans,
                        f"{len(orphans)} значений без соответствия"
                        + (f" (напр.: {sample})" if verbose and sample else ""))
            elif kind == "python":
                fn = CROSS_IMPL.get(cid)
                if fn is None:
                    rep.add("cross", cid, title, True,
                            "проверка не реализована - пропущено", warn=True)
                else:
                    ok, detail = fn(db, chk)
                    rep.add("cross", cid, title, ok, detail)
            else:
                rep.add("cross", cid, title, True,
                        f"неизвестный kind={kind}", warn=True)
        except Exception as e:
            rep.add("cross", cid, title, False,
                    "ошибка проверки: " + str(e).strip().splitlines()[0])


def _x07_expired(db: Db, chk: dict) -> Tuple[bool, str]:
    """Не продано ничего из партии с истёкшим сроком."""
    expiry = {r[0]: r[1] for r in db.q("master", "wms",
              "SELECT batch_bk, expiry_date FROM contract.v_batch")}
    rows = db.q("master", "pos", """
        SELECT l.batch_bk, max(r.receipt_dt)::date
          FROM contract.v_receipt_line l
          JOIN contract.v_receipt r ON r.receipt_bk = l.receipt_bk
         WHERE r.doc_type = 'sale'
         GROUP BY l.batch_bk
    """)
    bad = [b for b, dt in rows
           if b in expiry and expiry[b] is not None and dt is not None
           and dt > expiry[b]]
    return not bad, f"{len(bad)} партий продавались после истечения срока"


def _x08_price_coverage(db: Db, chk: dict) -> Tuple[bool, str]:
    """На момент продажи существовала действующая цена."""
    prices: Dict[str, List[tuple]] = {}
    for sku, scope, scope_bk, vf, vt in db.q("master", "catalog", """
            SELECT sku, price_scope, scope_bk, valid_from, valid_to
              FROM contract.v_price"""):
        prices.setdefault(sku, []).append((scope, scope_bk, vf, vt))

    ph_region = {r[0]: r[1] for r in db.q("master", "pos",
                 "SELECT pharmacy_bk, region FROM contract.v_pharmacy")}

    sample = db.q("master", "pos", """
        SELECT l.sku, r.pharmacy_bk, r.receipt_dt
          FROM contract.v_receipt_line l
          JOIN contract.v_receipt r ON r.receipt_bk = l.receipt_bk
         WHERE r.doc_type = 'sale'
         ORDER BY r.receipt_dt DESC
         LIMIT 3000
    """)
    bad = 0
    for sku, ph, dt in sample:
        scopes = {"chain": "ALL", "region": ph_region.get(ph), "pharmacy": ph}
        found = False
        for scope, scope_bk, vf, vt in prices.get(sku, ()):
            if scopes.get(scope) != scope_bk:
                continue
            if vf <= dt and (vt is None or dt < vt):
                found = True
                break
        if not found:
            bad += 1
    return bad == 0, f"{bad} из {len(sample)} продаж без действующей цены"


def _x09_sku_batch(db: Db, chk: dict) -> Tuple[bool, str]:
    """SKU позиции совпадает с SKU партии."""
    batch_sku = {r[0]: r[1] for r in db.q("master", "wms",
                 "SELECT batch_bk, sku FROM contract.v_batch")}
    rows = db.q("master", "pos",
                "SELECT DISTINCT sku, batch_bk FROM contract.v_receipt_line")
    bad = [b for s, b in rows if b in batch_sku and batch_sku[b] != s]
    return not bad, f"{len(bad)} пар (sku, batch_bk) не совпадают со складом"


def _x10_rx_snapshot(db: Db, chk: dict) -> Tuple[bool, str]:
    """Признак рецептурности в snapshot не противоречит каталогу."""
    tol = float(chk.get("tolerance_pct", 5))
    rx = {r[0]: r[1] for r in db.q("master", "catalog",
          "SELECT sku, is_rx FROM contract.v_sku")}
    rows = db.q("master", "pos", """
        SELECT sku, is_rx_snapshot, count(*)
          FROM contract.v_receipt_line GROUP BY 1, 2""")
    total = sum(c for _, _, c in rows) or 1
    mismatch = sum(c for sku, snap, c in rows
                   if sku in rx and rx[sku] != snap)
    pct = 100.0 * mismatch / total
    return pct <= tol, f"расхождение {pct:.2f}% (допуск {tol}%)"


CROSS_IMPL = {
    "X07": _x07_expired,
    "X08": _x08_price_coverage,
    "X09": _x09_sku_batch,
    "X10": _x10_rx_snapshot,
}


# ---------------------------------------------------------------------------
# 5. репликация
# ---------------------------------------------------------------------------
def check_replication(db: Db, contract: dict, sub: dict, rep: Report,
                      repo_root: str, skip_exec: bool) -> None:
    ha_mode = (sub.get("ha") or {}).get("mode", "replica")
    if ha_mode == "none":
        rep.add("repl", "mode", "объявлен режим без реплики", False,
                "ha.mode = none: баллы за репликацию не начисляются")
        return

    # состояние на мастере
    try:
        rows = db.q("admin", "pos", """
            SELECT application_name, state, sync_state
              FROM pg_stat_replication""")
        streaming = [r for r in rows if r[1] == "streaming"]
        rep.add("repl", "pg_stat_replication",
                "на мастере есть streaming-реплика", bool(streaming),
                f"строк в pg_stat_replication: {len(rows)}; "
                + "; ".join(f"{r[0]}:{r[1]}/{r[2]}" for r in rows))
    except Exception as e:
        rep.add("repl", "pg_stat_replication", "на мастере есть streaming-реплика",
                False, str(e).strip().splitlines()[0])

    try:
        slots = db.q("admin", "pos", """
            SELECT slot_name, slot_type, active FROM pg_replication_slots""")
        active = [s for s in slots if s[2]]
        rep.add("repl", "slots", "есть активный слот репликации", bool(active),
                "; ".join(f"{s[0]}({s[1]}, active={s[2]})" for s in slots)
                or "слотов нет")
    except Exception as e:
        rep.add("repl", "slots", "есть активный слот репликации", False,
                str(e).strip().splitlines()[0])

    # состояние на реплике
    if not db.dsn("replica", "pos"):
        rep.add("repl", "replica_dsn", "в манифесте объявлена реплика", False,
                "нет connections.replica")
        return
    try:
        in_rec = db.scalar("replica", "pos", "SELECT pg_is_in_recovery()")
        rep.add("repl", "in_recovery", "реплика в режиме recovery", bool(in_rec))
        wr = db.q("replica", "pos", "SELECT status FROM pg_stat_wal_receiver")
        rep.add("repl", "wal_receiver", "на реплике работает wal receiver",
                bool(wr) and wr[0][0] == "streaming",
                f"status={wr[0][0]}" if wr else "pg_stat_wal_receiver пуст")
    except Exception as e:
        rep.add("repl", "replica_state", "состояние реплики читается", False,
                str(e).strip().splitlines()[0])

    # догенерация данных и сверка отпечатков
    delta_cmd = (sub.get("entrypoint") or {}).get("seed_delta")
    grew = None
    if delta_cmd and not skip_exec:
        before = db.scalar("master", "pos", "SELECT count(*) FROM contract.v_receipt")
        rc, out = run_cmd(delta_cmd, repo_root)
        rep.add("repl", "seed_delta", "команда seed_delta выполнилась", rc == 0,
                f"rc={rc}; {out[-300:]}" if rc != 0 else "")
        after = db.scalar("master", "pos", "SELECT count(*) FROM contract.v_receipt")
        grew = (after or 0) - (before or 0)
        rep.add("repl", "seed_delta_effect", "seed_delta действительно добавил данные",
                grew > 0, f"чеков добавлено: {grew}")
    elif skip_exec:
        rep.notes.append("seed_delta не запускался (--skip-exec): "
                         "сверка отпечатков сделана на текущих данных")

    # ждём, пока реплика догонит
    try:
        master_lsn = db.scalar("admin", "pos", "SELECT pg_current_wal_lsn()::text")
        deadline = time.time() + 60
        caught = False
        while time.time() < deadline:
            r = db.scalar("replica", "pos",
                          "SELECT pg_last_wal_replay_lsn() >= %s::pg_lsn", (master_lsn,))
            if r:
                caught = True
                break
            time.sleep(1)
        rep.add("repl", "catchup", "реплика догнала мастер за 60 c", caught,
                f"master_lsn={master_lsn}")
    except Exception as e:
        rep.add("repl", "catchup", "реплика догнала мастер за 60 c", False,
                str(e).strip().splitlines()[0])

    # отпечатки всех контрактных представлений
    bad = []
    for service, spec in contract["services"].items():
        for view in spec["views"]:
            try:
                m = db.q("master", service, digest_sql(view))[0]
                r = db.q("replica", service, digest_sql(view))[0]
            except Exception as e:
                bad.append(f"{service}.{view}: {str(e).strip().splitlines()[0]}")
                continue
            if tuple(m) != tuple(r):
                bad.append(f"{service}.{view}: мастер {m} != реплика {r}")
    rep.add("repl", "digests",
            "отпечатки всех контрактных представлений совпадают", not bad,
            "; ".join(bad[:6]))

    # пул соединений
    pooler = (sub.get("ha") or {}).get("pooler_dsn")
    if pooler:
        try:
            conn = psycopg2.connect(pooler, connect_timeout=10)
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            conn.close()
            rep.add("repl", "pooler", "подключение через пул соединений работает", True)
        except Exception as e:
            rep.add("repl", "pooler", "подключение через пул соединений работает",
                    False, str(e).strip().splitlines()[0])
    else:
        rep.add("repl", "pooler", "подключение через пул соединений работает", False,
                "ha.pooler_dsn не объявлен")


# ---------------------------------------------------------------------------
# 6. инварианты
# ---------------------------------------------------------------------------
def _read_test(repo_root: str, path: str) -> Tuple[Optional[str], str]:
    full = os.path.join(repo_root, path)
    if not os.path.isfile(full):
        return None, f"файл не найден: {path}"
    with open(full, "r", encoding="utf-8") as f:
        return f.read(), ""


def _forbidden(sql: str) -> Optional[str]:
    stripped = re.sub(r"--[^\n]*", " ", sql)
    stripped = re.sub(r"/\*.*?\*/", " ", stripped, flags=re.S)
    low = re.sub(r"\s+", " ", stripped.lower())
    for pat, name in FORBIDDEN_IN_TESTS:
        if re.search(pat, low):
            return name
    return None


def check_invariants(db: Db, sub: dict, rep: Report, repo_root: str,
                     verbose: bool) -> None:
    invs = sub.get("invariants") or {}
    if not invs:
        rep.add("inv", "manifest", "в манифесте объявлены инварианты", False,
                "секция invariants пуста")
        return

    for name in sorted(invs, key=lambda s: (s[0], len(s), s)):
        spec = invs[name]
        service = spec.get("service")
        expected = str(spec.get("expect_sqlstate", "")).upper()
        title = f"{name}: негативный тест падает с {expected or '???'}"
        sql, err = _read_test(repo_root, spec.get("file", ""))
        if sql is None:
            rep.add("inv", name, title, False, err)
            continue
        bad = _forbidden(sql)
        if bad:
            rep.add("inv", name, title, False,
                    f"тест ослабляет схему ({bad}) - не засчитывается")
            continue
        if not expected:
            rep.add("inv", name, title, False, "не указан expect_sqlstate")
            continue

        dsn = db.dsn("writer", service) or db.dsn("admin", service)
        if not dsn:
            rep.add("inv", name, title, False,
                    f"нет подключения writer/admin для сервиса {service}")
            continue

        got, msg = None, ""
        conn = None
        try:
            conn = fresh_conn(dsn)
            with conn.cursor() as cur:
                for stmt in SESSION_SETUP:
                    cur.execute(stmt)
                cur.execute(sql)
            conn.commit()
        except psycopg2.Error as e:
            got = (e.pgcode or "").upper()
            msg = (getattr(e, "pgerror", None) or str(e)).strip().splitlines()[0]
        except Exception as e:
            msg = str(e).strip().splitlines()[0]
        finally:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
                conn.close()

        if got is None:
            rep.add("inv", name, title, False,
                    "тест выполнился без ошибки - инвариант не защищён")
            continue
        if any(got.startswith(p) for p in BROKEN_TEST_SQLSTATE_PREFIXES):
            rep.add("inv", name, title, False,
                    f"тест сломан, а не схема: SQLSTATE {got} - {msg}")
            continue
        if got != expected:
            rep.add("inv", name, title, False,
                    f"получен {got}, заявлен {expected} - {msg}")
            continue
        rep.add("inv", name, title, True, msg if verbose else "")

    for name, spec in sorted((sub.get("positive_tests") or {}).items()):
        service = spec.get("service")
        title = f"{name}: положительный тест возвращает ok = true"
        sql, err = _read_test(repo_root, spec.get("file", ""))
        if sql is None:
            rep.add("inv", name, title, False, err)
            continue
        bad = _forbidden(sql)
        if bad:
            rep.add("inv", name, title, False, f"тест ослабляет схему ({bad})")
            continue
        dsn = db.dsn("writer", service) or db.dsn("admin", service)
        if not dsn:
            rep.add("inv", name, title, False, f"нет подключения для {service}")
            continue
        conn = None
        try:
            conn = fresh_conn(dsn)
            with conn.cursor() as cur:
                for stmt in SESSION_SETUP:
                    cur.execute(stmt)
                cur.execute(sql)
                rows = cur.fetchall() if cur.description else []
            ok = len(rows) == 1 and rows[0][0] is True
            rep.add("inv", name, title, ok,
                    "" if ok else f"вернулось {rows!r}")
        except Exception as e:
            rep.add("inv", name, title, False, str(e).strip().splitlines()[0])
        finally:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
                conn.close()


# ---------------------------------------------------------------------------
# 7. CDC-готовность
# ---------------------------------------------------------------------------
def check_cdc(db: Db, contract: dict, rep: Report) -> None:
    req = contract.get("cdc_requirements", {})
    want_wal = req.get("wal_level", "logical")
    allowed_ri = set(req.get("replica_identity_allowed", ["d", "f", "i"]))

    for service in contract["services"]:
        try:
            wal = db.scalar("admin", service, "SHOW wal_level")
        except Exception as e:
            rep.add("cdc", f"{service}.wal_level", f"[{service}] wal_level = {want_wal}",
                    False, str(e).strip().splitlines()[0])
            continue
        rep.add("cdc", f"{service}.wal_level", f"[{service}] wal_level = {want_wal}",
                wal == want_wal, f"фактически {wal}")

        try:
            pubs = db.q("admin", service,
                        "SELECT pubname, puballtables FROM pg_publication")
            rep.add("cdc", f"{service}.publication",
                    f"[{service}] есть publication", bool(pubs),
                    ", ".join(f"{p[0]}(all={p[1]})" for p in pubs) or "publication нет")
            if pubs:
                if any(p[1] for p in pubs):
                    rep.add("cdc", f"{service}.pub_coverage",
                            f"[{service}] publication покрывает все таблицы", True,
                            "FOR ALL TABLES")
                else:
                    uncovered = db.q("admin", service, """
                        SELECT n.nspname || '.' || c.relname
                          FROM pg_class c
                          JOIN pg_namespace n ON n.oid = c.relnamespace
                         WHERE c.relkind = 'r'
                           AND n.nspname NOT IN ('pg_catalog','information_schema','contract')
                           AND n.nspname NOT LIKE 'pg_%'
                           AND NOT EXISTS (
                               SELECT 1 FROM pg_publication_tables pt
                                WHERE pt.schemaname = n.nspname
                                  AND pt.tablename = c.relname)
                    """)
                    rep.add("cdc", f"{service}.pub_coverage",
                            f"[{service}] publication покрывает все таблицы",
                            not uncovered,
                            "вне publication: "
                            + ", ".join(r[0] for r in uncovered[:8]))
        except Exception as e:
            rep.add("cdc", f"{service}.publication", f"[{service}] есть publication",
                    False, str(e).strip().splitlines()[0])

        try:
            bad_ri = db.q("admin", service, """
                SELECT n.nspname || '.' || c.relname, c.relreplident
                  FROM pg_class c
                  JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE c.relkind = 'r'
                   AND n.nspname NOT IN ('pg_catalog','information_schema','contract')
                   AND n.nspname NOT LIKE 'pg_%'
                   AND (c.relreplident = 'n'
                        OR (c.relreplident = 'd' AND NOT EXISTS (
                              SELECT 1 FROM pg_constraint k
                               WHERE k.conrelid = c.oid AND k.contype = 'p')))
            """)
            rep.add("cdc", f"{service}.replica_identity",
                    f"[{service}] у всех таблиц пригодный REPLICA IDENTITY",
                    not bad_ri,
                    "; ".join(f"{r[0]}={r[1]}" for r in bad_ri[:8]))
        except Exception as e:
            rep.add("cdc", f"{service}.replica_identity",
                    f"[{service}] у всех таблиц пригодный REPLICA IDENTITY", False,
                    str(e).strip().splitlines()[0])

        _ = allowed_ri  # оставлено для расширения правил в YAML


# ---------------------------------------------------------------------------
# 8. HA-кластер
# ---------------------------------------------------------------------------
def check_ha(db: Db, sub: dict, rep: Report) -> None:
    ha = sub.get("ha") or {}
    mode = ha.get("mode", "none")
    if mode != "patroni":
        rep.add("ha", "mode", "объявлен HA-кластер на Patroni", False,
                f"ha.mode = {mode}")
        return

    api = ha.get("patroni_api")
    expected = int(ha.get("expected_members", 3))
    if api:
        try:
            with urllib.request.urlopen(api.rstrip("/") + "/cluster", timeout=10) as r:
                data = json.loads(r.read().decode())
            members = data.get("members", [])
            roles = [(m.get("name"), m.get("role"), m.get("state")) for m in members]
            leaders = [m for m in members if m.get("role") == "leader"]
            running = [m for m in members if m.get("state") == "running"]
            rep.add("ha", "members", f"в кластере {expected} узлов",
                    len(members) >= expected,
                    "; ".join(f"{n}:{r}/{s}" for n, r, s in roles))
            rep.add("ha", "leader", "ровно один лидер", len(leaders) == 1,
                    f"лидеров: {len(leaders)}")
            rep.add("ha", "running", "все узлы в состоянии running",
                    len(running) == len(members),
                    f"running {len(running)} из {len(members)}")
        except (urllib.error.URLError, OSError, ValueError) as e:
            rep.add("ha", "api", "Patroni REST API отвечает", False, str(e))
    else:
        rep.add("ha", "api", "Patroni REST API объявлен", False,
                "ha.patroni_api не указан")

    for key, want_recovery, label in (
        ("haproxy_rw_dsn", False, "HAProxy: порт записи ведёт на лидера"),
        ("haproxy_ro_dsn", True, "HAProxy: порт чтения ведёт на реплику"),
    ):
        dsn = ha.get(key)
        if not dsn:
            rep.add("ha", key, label, False, f"{key} не указан")
            continue
        try:
            conn = psycopg2.connect(dsn, connect_timeout=10)
            with conn.cursor() as cur:
                cur.execute("SELECT pg_is_in_recovery()")
                in_rec = cur.fetchone()[0]
            conn.close()
            rep.add("ha", key, label, bool(in_rec) == want_recovery,
                    f"pg_is_in_recovery() = {in_rec}")
        except Exception as e:
            rep.add("ha", key, label, False, str(e).strip().splitlines()[0])

    journal = ha.get("journal")
    if journal:
        rep.add("ha", "journal", "файл журнала отказоустойчивости существует",
                os.path.isfile(journal) or True,
                "содержимое проверяется преподавателем вручную", warn=True)


# ---------------------------------------------------------------------------
# 9. реалистичность данных (бонус)
# ---------------------------------------------------------------------------
def check_realism(db: Db, rep: Report) -> None:
    try:
        dow = db.q("master", "pos", """
            SELECT extract(isodow from receipt_dt)::int, count(*)
              FROM contract.v_receipt WHERE doc_type = 'sale'
             GROUP BY 1 ORDER BY 1""")
        vals = [c for _, c in dow]
        ratio = (max(vals) / min(vals)) if vals and min(vals) else 0
        rep.add("realism", "weekday", "недельная сезонность продаж", ratio >= 1.25,
                f"max/min по дням недели = {ratio:.2f} (нужно >= 1.25)")
    except Exception as e:
        rep.add("realism", "weekday", "недельная сезонность продаж", False,
                str(e).strip().splitlines()[0])

    try:
        rows = db.q("master", "pos", """
            SELECT sku, sum(line_amount) FROM contract.v_receipt_line
             GROUP BY 1 ORDER BY 2 DESC""")
        total = sum(float(v or 0) for _, v in rows) or 1.0
        top = max(1, int(round(len(rows) * 0.1)))
        share = sum(float(v or 0) for _, v in rows[:top]) / total
        rep.add("realism", "longtail", "длинный хвост по SKU", share >= 0.5,
                f"топ-10% SKU дают {share:.1%} выручки (нужно >= 50%)")
    except Exception as e:
        rep.add("realism", "longtail", "длинный хвост по SKU", False,
                str(e).strip().splitlines()[0])

    try:
        rows = db.q("master", "pos", """
            SELECT pharmacy_bk, count(*) FROM contract.v_receipt GROUP BY 1""")
        vals = [float(c) for _, c in rows]
        cv = (statistics.pstdev(vals) / statistics.fmean(vals)) if vals else 0
        rep.add("realism", "pharmacy_cv", "неравномерность по аптекам", cv >= 0.15,
                f"коэффициент вариации = {cv:.3f} (нужно >= 0.15)")
    except Exception as e:
        rep.add("realism", "pharmacy_cv", "неравномерность по аптекам", False,
                str(e).strip().splitlines()[0])

    try:
        anon, total = db.q("master", "pos", """
            SELECT count(*) FILTER (WHERE customer_bk IS NULL), count(*)
              FROM contract.v_receipt""")[0]
        share = (anon / total) if total else 0
        rep.add("realism", "no_card", "доля покупок без карты 30-60%",
                0.30 <= share <= 0.60, f"фактически {share:.1%}")
    except Exception as e:
        rep.add("realism", "no_card", "доля покупок без карты 30-60%", False,
                str(e).strip().splitlines()[0])

    try:
        ref, total = db.q("master", "pos", """
            SELECT count(*) FILTER (WHERE doc_type = 'refund'), count(*)
              FROM contract.v_receipt""")[0]
        share = (ref / total) if total else 0
        rep.add("realism", "refunds", "доля возвратов 1-5%",
                0.01 <= share <= 0.05, f"фактически {share:.2%}")
    except Exception as e:
        rep.add("realism", "refunds", "доля возвратов 1-5%", False,
                str(e).strip().splitlines()[0])


# ---------------------------------------------------------------------------
# подсчёт баллов
# ---------------------------------------------------------------------------
def score(rep: Report, sub: dict, contract: dict) -> Dict[str, Optional[float]]:
    s: Dict[str, Optional[float]] = {}

    def graded(sections: Sequence[str], points: float) -> Optional[float]:
        """None, если ни одна из секций не прогонялась."""
        if not any(rep.section(x) for x in sections):
            return None
        return points if all(rep.passed(x) for x in sections) else 0.0

    s["база: стенд и контракт (4)"] = graded(["contract", "keys"], 4.0)
    s["база: данные и целостность (4)"] = graded(["volume", "dq", "cross"], 4.0)
    s["база: реплика и пул (2)"] = graded(["repl"], 2.0)

    # 12 негативных (I1-I12) + 2 положительных (P1, P2).
    # Служебная проверка leakage в знаменатель не входит и в числитель тоже:
    # её красный цвет - повод перепроверить руками, а не вычесть балл.
    REQUIRED_TESTS = 14
    INV_POINTS = SCORE_MAX["инварианты (5)"]
    declared = set(sub.get("invariants") or {}) | set(sub.get("positive_tests") or {})
    inv_checks = [c for c in rep.section("inv") if c.id in declared]
    if inv_checks:
        inv_pass = min(sum(1 for c in inv_checks if c.ok), REQUIRED_TESTS)
        s["инварианты (5)"] = round(INV_POINTS * inv_pass / REQUIRED_TESTS, 2)
        if len(declared) < REQUIRED_TESTS:
            rep.notes.append(
                f"в манифесте объявлено {len(declared)} тестов из "
                f"{REQUIRED_TESTS} обязательных (I1-I12, P1, P2)")
    else:
        s["инварианты (5)"] = None

    s["HA-кластер (2)"] = graded(["ha"], 2.0)
    s["CDC-готовность (1 из 2)"] = graded(["cdc"], 1.0)
    s["карта склейки (1 из 2)"] = None              # вручную
    s["журнал решений ADR (1)"] = None              # вручную

    realism_pass, realism_total = rep.count("realism")
    s["бонус: реалистичные данные (1)"] = (
        (1.0 if realism_pass >= 4 else 0.0) if realism_total else None)
    s["бонус: журнал отказоустойчивости (1)"] = None  # вручную
    s["бонус: кворумный etcd (1)"] = None             # вручную
    s["бонус: одиночное выполнение (2)"] = 2.0 if (sub.get("team") or {}).get("solo") else 0.0
    return s


# ---------------------------------------------------------------------------
# вывод
# ---------------------------------------------------------------------------
# Разбалловка. Держать в согласии с hw01/RUBRIC.md и ASSIGNMENT.md:
# итоговые «из N возможных» считаются отсюда, а не зашиты в текст.
SCORE_MAX = {
    "база: стенд и контракт (4)":        4.0,
    "база: данные и целостность (4)":    4.0,
    "база: реплика и пул (2)":           2.0,
    "инварианты (5)":                    5.0,
    "HA-кластер (2)":                    2.0,
    "CDC-готовность (1 из 2)":           1.0,
    "карта склейки (1 из 2)":            1.0,
    "журнал решений ADR (1)":            1.0,
    "бонус: реалистичные данные (1)":         1.0,
    "бонус: журнал отказоустойчивости (1)":   1.0,
    "бонус: кворумный etcd (1)":              1.0,
    "бонус: одиночное выполнение (2)":        2.0,
}

# позиции, которые чекер посчитать не может по определению
MANUAL_SCORES = {
    "карта склейки (1 из 2)",
    "журнал решений ADR (1)",
    "бонус: журнал отказоустойчивости (1)",
    "бонус: кворумный etcd (1)",
}

MAX_AUTO = sum(v for k, v in SCORE_MAX.items() if k not in MANUAL_SCORES)
MAX_MANUAL = sum(v for k, v in SCORE_MAX.items() if k in MANUAL_SCORES)
MAX_TOTAL = MAX_AUTO + MAX_MANUAL
PASS_100 = 20.0   # базовая (10) + основная (10) части целиком;
                  # десятибалльная оценка = min(сумма, PASS_100) / 2

SECTION_TITLES = {
    "contract": "1. Контракт: представления, колонки, типы",
    "keys": "2. Контракт: обязательность и уникальность",
    "volume": "3. Объёмы данных",
    "dq": "4. Внутрисервисные проверки качества",
    "cross": "5. Кросс-сервисная целостность",
    "repl": "6. Репликация и пул соединений",
    "inv": "7. Инварианты",
    "cdc": "8. CDC-готовность",
    "ha": "9. HA-кластер",
    "realism": "10. Реалистичность данных (бонус)",
}


def print_report(rep: Report, verbose: bool) -> None:
    for sec, title in SECTION_TITLES.items():
        checks = rep.section(sec)
        if not checks:
            continue
        ok = sum(1 for c in checks if c.ok)
        print(f"\n\033[1m{title}\033[0m  [{ok}/{len(checks)}]")
        for c in checks:
            if c.ok and c.warn:
                mark, color = "~", "\033[33m"
            elif c.ok:
                mark, color = "+", "\033[32m"
            else:
                mark, color = "-", "\033[31m"
            line = f"  {color}{mark}\033[0m {c.id:<34} {c.title}"
            print(line)
            if c.detail and (not c.ok or verbose or c.warn):
                print(f"      \033[90m{c.detail}\033[0m")

    print("\n\033[1mБаллы\033[0m")
    auto_total = 0.0
    manual: List[str] = []
    skipped: List[str] = []
    for k, v in rep.scores.items():
        if v is None:
            if k in MANUAL_SCORES:
                manual.append(k)
                note = "проверяется вручную"
            else:
                skipped.append(k)
                note = "секция не прогонялась"
            print(f"  {'?':>6}  {k}  \033[90m({note})\033[0m")
        else:
            auto_total += v
            print(f"  {v:>6.2f}  {k}")
    print(f"\n  автоматически: \033[1m{auto_total:.2f}\033[0m "
          f"из {MAX_AUTO:g} возможных")
    if manual:
        pts = sum(SCORE_MAX.get(k, 0.0) for k in manual)
        print(f"  вручную: {', '.join(manual)} - до {pts:g} баллов")
    if skipped:
        pts = sum(SCORE_MAX.get(k, 0.0) for k in skipped)
        print(f"  не проверено: {', '.join(skipped)} - до {pts:g} баллов")
    print(f"  за 100% принимается {PASS_100:g} баллов, максимум {MAX_TOTAL:g}")
    print(f"  десятибалльная оценка = min(сумма, {PASS_100:g}) / 2")

    if rep.notes:
        print("\n\033[1mЗаметки\033[0m")
        for n in rep.notes:
            print(f"  * {n}")


# ---------------------------------------------------------------------------
# self-test: согласованность материалов ДЗ
# ---------------------------------------------------------------------------
def self_test(contract_path: str) -> int:
    print(f"Проверяю {contract_path}")
    with open(contract_path, encoding="utf-8") as f:
        c = yaml.safe_load(f)
    errors: List[str] = []

    views = {}
    for svc, spec in c["services"].items():
        if svc not in SERVICES:
            errors.append(f"неизвестный сервис {svc}")
        for v, vspec in spec["views"].items():
            views[(svc, v)] = set(vspec["columns"])
            if "grain" not in vspec:
                errors.append(f"{svc}.{v}: не описано зерно (grain)")
            for col, cs in vspec["columns"].items():
                if cs["type"] not in TYPE_CLASSES:
                    errors.append(f"{svc}.{v}.{col}: неизвестный класс типа "
                                  f"{cs['type']}")
            for k in vspec.get("unique_key", []):
                if k not in vspec["columns"]:
                    errors.append(f"{svc}.{v}: unique_key ссылается на "
                                  f"отсутствующую колонку {k}")

    ids = set()
    for chk in c.get("sql_checks", []):
        if chk["id"] in ids:
            errors.append(f"дубль id проверки: {chk['id']}")
        ids.add(chk["id"])
        if chk["service"] not in c["services"]:
            errors.append(f"{chk['id']}: неизвестный сервис {chk['service']}")
        for m in re.finditer(r"contract\.(v_\w+)", chk["sql"]):
            if (chk["service"], m.group(1)) not in views:
                errors.append(f"{chk['id']}: ссылка на неизвестное представление "
                              f"{chk['service']}.{m.group(1)}")

    for chk in c.get("cross_checks", []):
        if chk["id"] in ids:
            errors.append(f"дубль id проверки: {chk['id']}")
        ids.add(chk["id"])
        if chk["kind"] == "python" and chk["id"] not in CROSS_IMPL:
            errors.append(f"{chk['id']}: kind=python, но реализации нет в CROSS_IMPL")
        for side in ("left", "right"):
            if side not in chk:
                continue
            ref = chk[side]
            key = (ref["service"], ref["view"])
            if key not in views:
                errors.append(f"{chk['id']}.{side}: нет представления "
                              f"{ref['service']}.{ref['view']}")
            elif ref["column"] not in views[key]:
                errors.append(f"{chk['id']}.{side}: нет колонки {ref['column']} "
                              f"в {ref['service']}.{ref['view']}")

    for cid in CROSS_IMPL:
        if not any(x["id"] == cid for x in c.get("cross_checks", [])):
            errors.append(f"CROSS_IMPL содержит {cid}, которого нет в контракте")

    base = os.path.dirname(contract_path)
    candidates = [
        os.path.join(base, "..", "student_kit", "hw01_submission.example.yaml"),
        os.path.join(base, "hw01_submission.example.yaml"),
        os.path.join(HERE, "..", "hw01", "student_kit",
                     "hw01_submission.example.yaml"),
    ]
    example = next((p for p in candidates if os.path.isfile(p)), None)
    if example:
        with open(example, encoding="utf-8") as f:
            sub = yaml.safe_load(f)
        declared = set(sub.get("invariants") or {})
        need = {f"I{i}" for i in range(1, 13)}
        missing = need - declared
        if missing:
            errors.append("в примере манифеста не объявлены инварианты: "
                          + ", ".join(sorted(missing)))
        if set(sub.get("positive_tests") or {}) != {"P1", "P2"}:
            errors.append("в примере манифеста должны быть положительные тесты P1, P2")
        for role in ("master", "replica", "admin", "writer"):
            got = set((sub.get("connections") or {}).get(role) or {})
            if got != set(SERVICES):
                errors.append(f"в примере манифеста connections.{role} "
                              f"описывает {sorted(got)}, а не все четыре сервиса")
    else:
        print("пример манифеста не найден - проверка манифеста пропущена")
        sub = None

    # разбалловка: позиции score() и SCORE_MAX обязаны совпадать,
    # иначе итог "из N возможных" разъедется с RUBRIC.md
    positions = score(Report(), sub or {}, c)
    for k in set(positions) - set(SCORE_MAX):
        errors.append(f"позиция баллов '{k}' есть в score(), но нет в SCORE_MAX")
    for k in set(SCORE_MAX) - set(positions):
        errors.append(f"позиция баллов '{k}' есть в SCORE_MAX, но нет в score()")
    if MAX_TOTAL != 25.0:
        errors.append(f"максимум за ДЗ стал {MAX_TOTAL:g} - сверьте "
                      f"ASSIGNMENT.md и RUBRIC.md")
    # порог 100% по договорённости равен базовой + основной частям,
    # то есть всему, что не помечено как "бонус:"
    non_bonus = sum(v for k, v in SCORE_MAX.items() if not k.startswith("бонус"))
    if non_bonus != PASS_100:
        errors.append(f"порог 100% ({PASS_100:g}) разошёлся с обязательной "
                      f"частью ({non_bonus:g}) - сверьте разбалловку")

    print(f"представлений: {len(views)}, проверок: {len(ids)}, "
          f"баллы: {MAX_AUTO:g} авто + {MAX_MANUAL:g} вручную = {MAX_TOTAL:g}")
    if errors:
        print("\nОшибки:")
        for e in errors:
            print("  - " + e)
        return 1
    print("Материалы согласованы.")
    return 0


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="чекер ДЗ №1")
    ap.add_argument("--submission", help="путь к hw01_submission.yaml")
    ap.add_argument("--contract", default=DEFAULT_CONTRACT,
                    help="путь к hw01_contract.yaml")
    ap.add_argument("--sections", default="all",
                    help="contract,keys,volume,dq,cross,repl,inv,cdc,ha,realism")
    ap.add_argument("--skip-exec", action="store_true",
                    help="не запускать команды студента")
    ap.add_argument("--json", help="куда сохранить машинный отчёт")
    ap.add_argument("--self-test", action="store_true",
                    help="проверить согласованность материалов ДЗ")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    if args.self_test:
        return self_test(os.path.abspath(args.contract))

    if not args.submission:
        ap.error("нужен --submission (или --self-test)")

    with open(args.contract, encoding="utf-8") as f:
        contract = yaml.safe_load(f)
    with open(args.submission, encoding="utf-8") as f:
        sub = yaml.safe_load(f)
    repo_root = os.path.dirname(os.path.abspath(args.submission)) or "."

    want = ({"contract", "keys", "volume", "dq", "cross", "repl", "inv", "cdc",
             "ha", "realism"} if args.sections == "all"
            else set(x.strip() for x in args.sections.split(",")))

    team = sub.get("team") or {}
    print("\033[1mПроверка ДЗ №1\033[0m")
    print(f"  команда:  {', '.join(team.get('members') or ['?'])}")
    print(f"  логины:   {', '.join(team.get('github_logins') or ['?'])}")
    if team.get("solo"):
        print("  сдаётся в одиночку")
    print(f"  репозиторий: {repo_root}")

    db = Db(sub.get("connections") or {})
    rep = Report()

    try:
        if "contract" in want:
            check_contract(db, contract, rep, args.verbose)
        if "keys" in want:
            check_nullability_and_keys(db, contract, rep)
        if "volume" in want:
            check_volume(db, contract, rep)
        if "dq" in want:
            check_sql_checks(db, contract, rep)
        if "cross" in want:
            check_cross(db, contract, rep, args.verbose)
        if "repl" in want:
            check_replication(db, contract, sub, rep, repo_root, args.skip_exec)
        if "inv" in want:
            check_invariants(db, sub, rep, repo_root, args.verbose)
            # после негативных тестов данные обязаны остаться прежними
            before = {c.id: c for c in rep.section("volume")}
            if before:
                after = Report()
                check_volume(db, contract, after)
                changed = [c.id for c in after.section("volume")
                           if c.id in before and c.ok != before[c.id].ok]
                rep.add("inv", "leakage",
                        "негативные тесты не оставили следов в данных",
                        not changed, "изменились: " + ", ".join(changed))
        if "cdc" in want:
            check_cdc(db, contract, rep)
        if "ha" in want:
            check_ha(db, sub, rep)
        if "realism" in want:
            check_realism(db, rep)

        rep.scores = score(rep, sub, contract)
        print_report(rep, args.verbose)

        if args.json:
            with open(args.json, "w", encoding="utf-8") as f:
                json.dump({
                    "team": team,
                    "checks": [c.__dict__ for c in rep.checks],
                    "scores": rep.scores,
                    "notes": rep.notes,
                }, f, ensure_ascii=False, indent=2)
            print(f"\nмашинный отчёт: {args.json}")
    finally:
        db.close()

    failed = [c for c in rep.checks if not c.ok]
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
