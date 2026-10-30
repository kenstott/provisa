# Copyright (c) 2026 Kenneth Stott
# Canary: 88e0dfd5-5252-4cb3-83c0-4fa63effc0d4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL catalog proxy.

Intercepts information_schema and pg_catalog queries and answers them
from CompilationContext without the engine round-trip. Uses DuckDB in-memory
as the query engine so clients can send arbitrary JOINs and WHERE clauses.
"""

# Requirements: REQ-127, REQ-128, REQ-363

# complexity-gate: allow-ble=1 reason="classify() fail-opens on unparseable SQL (REQ-127) to a regex-based catalog-name scan so a malformed introspection query is still intercepted rather than crashing the pgwire session"

from __future__ import annotations

import functools
import logging
import re
from typing import Any

from provisa.pgwire.catalog_data import (
    _CATALOG_TABLE_NAMES,
    _INTERCEPT_SCHEMAS,
)
from provisa.pgwire.catalog_rewrite import _rewrite_for_duckdb, next_txid
from provisa.pgwire.catalog_data import (
    _KNOWN_SETTINGS,
    _TYPEINFO_COL_TYPES,
    _TYPEINFO_COLS,
    typeinfo,
)
from provisa.pgwire.catalog_populate import catalog_db_for

log = logging.getLogger(__name__)

_SET_RE = re.compile(r"^\s*SET\b", re.IGNORECASE)
_SHOW_RE = re.compile(r"^\s*SHOW\b", re.IGNORECASE)
_TXN_RE = re.compile(
    r"^\s*(BEGIN|START\s+TRANSACTION|START|COMMIT|END|ROLLBACK|ABORT|DISCARD|RESET|DEALLOCATE|SAVEPOINT|RELEASE)\b",
    re.IGNORECASE,
)

_SHOW_MIN_PARTS = 2

# REQ-2167: a statement that only sleeps, as PostgreSQL's pg_sleep(seconds) does, answering void.
_PG_SLEEP_RE = re.compile(
    r'^\s*SELECT\s+(?:pg_catalog\.)?"?pg_sleep"?\s*\((?P<arg>.*)\)\s*(?:AS\s+(?P<alias>\w+))?\s*;?\s*$',
    re.IGNORECASE | re.DOTALL,
)

_SCALAR_FN_RE = re.compile(
    r"^\s*SELECT\s+(?:pg_catalog\.)?"
    r"(current_user|session_user|current_database\(\)|current_schema\(\)|version\(\)"
    r"|pg_backend_pid\(\)|pg_is_in_recovery\(\)|txid_current\(\)|pg_advisory_unlock_all\(\))\s*$",
    re.IGNORECASE,
)


def _parse_typeinfo_oids(sql: str) -> list[int] | None:
    """Extract OIDs from ANY('{oid,...}'::oid[]) pattern; None if $1 still present."""
    m = re.search(r"ANY\s*\(\s*'\{([^}]*)\}'", sql, re.IGNORECASE)
    if m:
        raw = m.group(1).strip()
        return [int(x) for x in raw.split(",") if x.strip()] if raw else []
    return None


def _handle_typeinfo_tree(oids: list[int]):
    from provisa.executor.result import QueryResult

    rows = []
    _ti = typeinfo()
    for oid in oids:
        info = _ti.get(oid)
        if info is None:
            continue
        ns, name, kind, basetype, elemtype, elemdelim, range_subtype = info
        elem_name = _ti[elemtype][1] if elemtype and elemtype in _ti else None
        base_name = _ti[basetype][1] if basetype and basetype in _ti else None
        range_name = _ti[range_subtype][1] if range_subtype and range_subtype in _ti else None
        rows.append(
            (
                oid,
                ns,
                name,
                kind,
                basetype,
                elemtype,
                elemdelim,
                range_subtype,
                None,
                None,
                0,
                base_name,
                elem_name,
                range_name,
            )
        )
    return QueryResult(rows=rows, column_names=_TYPEINFO_COLS, column_types=_TYPEINFO_COL_TYPES)


_SCALAR_NAMES = frozenset(
    {
        "current_user",
        "session_user",
        "current_database",
        "current_schema",
        "version",
        "pg_backend_pid",
        "pg_is_in_recovery",
        "txid_current",
    }
)


# REQ-2167 (PGWIRE'S CATALOG FUNCTIONS): the catalog functions clients call (psql describe.c,
# pgAdmin, DBeaver) that the catalog's rewrite answers inside a catalog statement. Called on its
# own -- a statement reading no table -- one is answered the same way, not sent to the engine.
_STANDALONE_CATALOG_FUNCTIONS = frozenset(
    {
        "pg_encoding_to_char",
        "has_table_privilege",
        "pg_get_function_arguments",
        "pg_get_function_identity_arguments",
        "pg_get_function_result",
        "pg_get_function_sqlbody",
        "pg_get_functiondef",
        "pg_get_indexdef",
        "pg_get_partition_constraintdef",
        "pg_get_partkeydef",
        "pg_get_ruledef",
        "pg_get_serial_sequence",
        "pg_get_statisticsobjdef_columns",
        "pg_get_statisticsobjdef_expressions",
        "pg_get_triggerdef",
        "pg_get_userbyid",
        "pg_relation_is_publishable",
        "pg_tablespace_location",
        "quote_ident",
        "format_type",
        "pg_table_is_visible",
        "pg_relation_size",
        "pg_total_relation_size",
        "pg_indexes_size",
        # Decision 2b: answered by the catalog's macros (catalog_macros) or its rewrite.
        "getdatabaseencoding",
        "pg_char_to_encoding",
        "pg_database_size",
        "pg_table_size",
        "pg_tablespace_size",
        "pg_column_size",
        "quote_literal",
        "pg_statistics_obj_is_visible",
        "aclexplode",
        "pg_options_to_table",
        "pg_partition_tree",
        "pg_partition_ancestors",
        "pg_show_all_settings",
        "pg_describe_object",
        "pg_identify_object",
        "acldefault",
        "oidvectortypes",
        "pg_cursor",
        "pg_sequence_last_value",
        "setval",
        "plpgsql_validator",
        "fmgr_c_validator",
        "bpchar",
        "has_type_privilege",
        "pg_available_extensions",
        "json_to_record",
        "pg_blocking_pids",
        "pg_current_logfile",
        "pg_get_keywords",
        "pg_get_replica_identity_index",
        "pg_get_viewdef",
        "pg_stat_reset_single_table_counters",
        "ts_token_type",
    }
)


def _calls_a_catalog_function_alone(tree) -> bool:
    import sqlglot.expressions as exp

    # A set-returning one read in FROM is a table only in form.
    if any(
        not (
            isinstance(t.this, exp.Anonymous)
            and t.this.name.lower() in _STANDALONE_CATALOG_FUNCTIONS
        )
        for t in tree.find_all(exp.Table)
    ):
        return False
    return any(
        fn.name.lower() in _STANDALONE_CATALOG_FUNCTIONS for fn in tree.find_all(exp.Anonymous)
    )


#: The types only the catalog has (name, and the object identifier types): a statement reading no
#: table that casts to one is answered by the catalog (REQ-2167).
_CATALOG_ONLY_TYPES = frozenset({"name", "regclass", "regtype", "regnamespace"})


def _casts_to_a_catalog_type_alone(tree) -> bool:
    import sqlglot.expressions as exp

    if any(True for _ in tree.find_all(exp.Table)):
        return False
    return any(
        cast.to.sql(dialect="postgres").lower().removeprefix("pg_catalog.") in _CATALOG_ONLY_TYPES
        for cast in tree.find_all(exp.Cast)
    )


@functools.lru_cache(maxsize=4096)
def classify(sql: str) -> str:  # REQ-127, REQ-128, REQ-363
    """Return 'INTERCEPT' or 'PASS_THROUGH'. A function of the statement text alone — asked on
    every execution of a statement, so the answer is kept per text (the parse is not repeated)."""
    stripped = sql.strip()
    if _SET_RE.match(stripped) or _SHOW_RE.match(stripped) or _TXN_RE.match(stripped):
        return "INTERCEPT"
    if _SCALAR_FN_RE.match(stripped) or _PG_SLEEP_RE.match(stripped):
        return "INTERCEPT"
    try:
        import sqlglot.expressions as exp
        import sqlglot

        tree = sqlglot.parse_one(stripped, read="postgres")
        for tbl in tree.find_all(exp.Table):
            db = tbl.db.lower() if tbl.db else ""
            tname = tbl.name.lower() if tbl.name else ""
            if db in _INTERCEPT_SCHEMAS:
                return "INTERCEPT"
            if not db and tname in _CATALOG_TABLE_NAMES:
                return "INTERCEPT"
        for func in tree.find_all(exp.Anonymous):
            fn = func.name.lower()
            if "current_setting" in fn or "set_config" in fn:
                return "INTERCEPT"
            if fn in _SCALAR_NAMES:
                return "INTERCEPT"
            if any(
                x in fn
                for x in (
                    "obj_description",
                    "col_description",
                    "shobj_description",
                    "pg_get_expr",
                    "pg_stat_get",
                )
            ):
                return "INTERCEPT"
        for col in tree.find_all(exp.Column):
            if col.name.lower() in _SCALAR_NAMES:
                return "INTERCEPT"
        if _calls_a_catalog_function_alone(tree) or _casts_to_a_catalog_type_alone(tree):
            return "INTERCEPT"
        for node in tree.walk():
            if type(node).__name__ in (
                "CurrentUser",
                "SessionUser",
                "CurrentDatabase",
                "CurrentSchema",
            ):
                return "INTERCEPT"
    except Exception:
        lower = stripped.lower()
        for name in _CATALOG_TABLE_NAMES:
            if re.search(r"\b" + re.escape(name) + r"\b", lower):
                return "INTERCEPT"
        for schema in _INTERCEPT_SCHEMAS:
            if schema in lower:
                return "INTERCEPT"
    return "PASS_THROUGH"


def _handle_show(sql: str):
    """Answer SHOW commands without DuckDB."""
    from provisa.executor.result import QueryResult

    normalized = sql.strip().rstrip(";")
    if re.match(r"^\s*SHOW\s+TRANSACTION\s+ISOLATION\s+LEVEL\s*$", normalized, re.IGNORECASE):
        return QueryResult(rows=[("read committed",)], column_names=["transaction_isolation"])
    parts = normalized.split()
    if len(parts) < _SHOW_MIN_PARTS:
        return QueryResult(rows=[], column_names=[])
    setting = parts[1].lower()
    if setting == "all":
        rows = [(k, v) for k, v in _KNOWN_SETTINGS.items()]
        return QueryResult(rows=rows, column_names=["name", "setting"])
    value = _KNOWN_SETTINGS.get(setting, "")
    return QueryResult(rows=[(value,)], column_names=[setting])


#: The schema a session is in, as current_schema() answers it (Flight SQL's GetSessionOptions
#: answers the same, REQ-2175).
CURRENT_SCHEMA = "public"


def _handle_scalar(sql: str, role_id: str, state):
    from provisa.executor.result import QueryResult

    s = sql.strip().lower()
    if "current_user" in s or "session_user" in s:
        return QueryResult(rows=[(role_id,)], column_names=["current_user"])
    if "current_database" in s:
        from provisa.pgwire.catalog_populate import served_database_name

        return QueryResult(rows=[(served_database_name(state),)], column_names=["current_database"])
    if "version()" in s:
        return QueryResult(rows=[("PostgreSQL 14.0 on Provisa",)], column_names=["version"])
    if "current_schema()" in s:
        return QueryResult(rows=[(CURRENT_SCHEMA,)], column_names=["current_schema"])
    if "pg_backend_pid()" in s:
        return QueryResult(rows=[(0,)], column_names=["pg_backend_pid"])
    if "pg_is_in_recovery()" in s:
        # Provisa is never a replica.
        return QueryResult(rows=[(False,)], column_names=["pg_is_in_recovery"])
    if "txid_current()" in s:
        return QueryResult(rows=[(next_txid(),)], column_names=["txid_current"])
    if "pg_advisory_unlock_all()" in s:
        # asyncpg's pool connection-release reset query (Connection.get_reset_query()) always
        # includes this — Provisa never grants real advisory locks, so releasing "all of them" is
        # trivially satisfied. A void-returning function answers with one row, one NULL column,
        # same as _handle_scalar's other functions.
        return QueryResult(rows=[(None,)], column_names=["pg_advisory_unlock_all"])
    return None


def _handle_txid(sql: str):
    """Answer the JDBC/DataGrip status probe combining pg_is_in_recovery + txid_current.

    `SELECT CASE WHEN pg_is_in_recovery() THEN NULL ELSE CAST(...txid_current()...) END
    AS current_txid` resolves to a single-column bigint without reaching the engine.
    """
    from provisa.executor.result import QueryResult

    lower = sql.lower()
    if "pg_is_in_recovery" not in lower or "txid_current" not in lower:
        return None
    m = re.search(r"\bAS\s+(\w+)\s*$", sql.strip(), re.IGNORECASE)
    col = m.group(1) if m else "current_txid"
    return QueryResult(rows=[(next_txid(),)], column_names=[col], column_types=["BIGINT"])


_SET_CONFIG_VALUE_RE = re.compile(
    r"set_config\s*\(\s*'(?:[^']|'')*'\s*,\s*'((?:[^']|'')*)'", re.IGNORECASE
)


def _set_config_value(sql: str) -> str | None:
    """What set_config(name, value, is_local) answers: the value it set, as PostgreSQL does."""
    m = _SET_CONFIG_VALUE_RE.search(sql)
    return m.group(1).replace("''", "'") if m else None


def _handle_current_setting(sql: str):
    """Answer SELECT current_setting(...) [+ set_config(...)] without DuckDB."""
    from provisa.executor.result import QueryResult

    lower = sql.lower()
    if "current_setting" not in lower:
        return None

    # Multi-expression startup query: SELECT current_setting('x') AS a, set_config(...) AS b
    # Detect alias names from the SQL so we return the right column names for asyncpg.
    if "set_config" in lower:
        m1 = re.search(
            r"current_setting\s*\(\s*['\"]([^'\"]+)['\"]\s*\)(?:\s+AS\s+(\w+))?",
            sql,
            re.IGNORECASE,
        )
        m2 = re.search(r"set_config\s*\([^)]+\)(?:\s+AS\s+(\w+))?", sql, re.IGNORECASE)
        col1 = (m1.group(2) or "current_setting") if m1 else "current_setting"
        col2 = (m2.group(1) or "set_config") if m2 else "set_config"
        key = m1.group(1).lower() if m1 else ""
        val1 = _KNOWN_SETTINGS.get(key, "")
        return QueryResult(rows=[(val1, _set_config_value(sql))], column_names=[col1, col2])

    m = re.search(r"current_setting\s*\(\s*['\"]([^'\"]+)['\"]\s*\)", sql, re.IGNORECASE)
    if not m:
        return None
    key = m.group(1).lower()
    value = _KNOWN_SETTINGS.get(key, "")
    return QueryResult(rows=[(value,)], column_names=["current_setting"])


def _declared_types(sql: str, names: list[str], served: list[str]) -> list[str]:  # REQ-2167
    """Each column's type as PostgreSQL 17 declares it where the served value has that type's
    wire form (a catalog column served as text that PostgreSQL declares name keeps its text, sent
    as name); otherwise the type it is served with. Columns pair up by position, or by name where
    the served relation's columns are not PostgreSQL's."""
    from provisa.pgwire.catalog_types import result_types
    from provisa.pgwire.server import same_wire_form

    declared = result_types(sql)
    if len(declared) == len(served):
        pg_types = [pg_type for _, pg_type in declared]
    else:
        by_name = dict(reversed(declared))
        pg_types = [by_name.get(name, "") for name in names]
    return [
        pg_type
        if pg_type and (pg_type == "record" or same_wire_form(pg_type, duck_type))
        else duck_type
        for pg_type, duck_type in zip(pg_types, served)
    ]


def _record_field(value: Any) -> str:
    """One field as record_out writes it: nothing for NULL, quoted where it must be."""
    if value is None:
        return ""
    text = {True: "t", False: "f"}.get(value, str(value)) if isinstance(value, bool) else str(value)
    if text == "" or any(c in text for c in '",\\() \t\n'):
        return '"' + text.replace("\\", "\\\\").replace('"', '""') + '"'
    return text


def _record_text(value: Any) -> Any:
    """A record value (the engine's struct) in PostgreSQL's text form."""
    if not isinstance(value, dict):
        return value
    return "(" + ",".join(_record_field(v) for v in value.values()) + ")"


def _handle_pg_sleep(match: re.Match[str]):  # REQ-2167
    """pg_sleep(seconds): sleeps that long and answers void (empty text); strict, so a NULL
    argument answers NULL without sleeping. A sleep longer than the request has left ends with
    the request's timeout."""
    import threading

    import sqlglot

    from provisa.core import engine_memory
    from provisa.executor.result import QueryResult

    seconds_sql = sqlglot.transpile(
        f"SELECT CAST(({match['arg']}) AS DOUBLE PRECISION)", read="postgres", write="duckdb"
    )[0]
    con = engine_memory.connect()
    try:
        ((seconds,),) = con.execute(seconds_sql).fetchall()
    finally:
        con.close()
    if seconds is not None and seconds > 0:
        # Within the request's own budget (REQ-1905): the sleep is a wait the deadline's cancel
        # wakes, so a CancelRequest or the request timeout ends it early, as it ends a driver's
        # statement; the deadline's own error (57014) follows.
        from provisa.core import request_deadline

        woken = threading.Event()
        with request_deadline.cancel_on_deadline(woken.set):
            woken.wait(seconds)
        request_deadline.check()
    return QueryResult(
        rows=[(None if seconds is None else "",)],
        column_names=[match["alias"] or "pg_sleep"],
        column_types=["void"],
    )


def answer(sql: str, role_id: str, state):  # REQ-532
    """Return a synthetic QueryResult for intercepted catalog/SET/SHOW queries."""
    from provisa.executor.result import QueryResult

    stripped = sql.strip().rstrip(";")

    if _TXN_RE.match(stripped) or _SET_RE.match(stripped):
        return QueryResult(rows=[], column_names=[])

    if _SHOW_RE.match(stripped):
        return _handle_show(stripped)

    sleep = _PG_SLEEP_RE.match(stripped)
    if sleep:
        return _handle_pg_sleep(sleep)

    if _SCALAR_FN_RE.match(stripped):
        result = _handle_scalar(stripped, role_id, state)
        if result is not None:
            return result

    lower_stripped = stripped.lower()
    if "pg_is_in_recovery" in lower_stripped and "txid_current" in lower_stripped:
        result = _handle_txid(stripped)
        if result is not None:
            return result

    if "current_setting" in stripped.lower():
        result = _handle_current_setting(stripped)
        if result is not None:
            return result

    if "set_config" in stripped.lower() and "current_setting" not in stripped.lower():
        from provisa.executor.result import QueryResult

        return QueryResult(
            rows=[(_set_config_value(stripped),)],
            column_names=["set_config"],
            column_types=["VARCHAR"],
        )

    # asyncpg type-introspection recursive CTE. During describe ($1 not yet bound)
    # return schema-only. During execute, return rows from _TYPEINFO for the requested OIDs
    # so asyncpg can cache types and stop introspecting.
    if "typeinfo_tree" in stripped.lower():
        oids = _parse_typeinfo_oids(stripped)
        if oids is None:
            # Describe phase: $1 still present — return schema, 0 rows
            return QueryResult(
                rows=[], column_names=_TYPEINFO_COLS, column_types=_TYPEINFO_COL_TYPES
            )
        return _handle_typeinfo_tree(oids)

    rewritten = stripped
    cur = None
    try:
        # REQ-2167: the role's kept catalog DB, on a cursor of this statement's own.
        cur = catalog_db_for(role_id, state).cursor()
        # Substitute $N params before rewriting so SQLGlot can parse the SQL.
        # Queries with $N::type[] (e.g. asyncpg type introspection) would otherwise
        # fail to parse, preventing table-name rewrites.
        import re as _re

        # Strip $N params AND any trailing PG type cast (e.g. $1::oid[]) so SQLGlot
        # can parse the query without failing on array-type annotations.
        pre_subst = _re.sub(r"\$\d+(?:::[^\s,)]+)?", "NULL", stripped)
        from provisa.pgwire.catalog_populate import served_database_name

        rewritten = _rewrite_for_duckdb(pre_subst, role_id, served_database_name(state))
        cur.execute(rewritten)
        rows = [tuple(r) for r in cur.fetchall()]
        col_names = [desc[0] for desc in (cur.description or [])]
        col_types = _declared_types(
            stripped, col_names, [str(desc[1]) for desc in (cur.description or [])]
        )
        records = [i for i, declared in enumerate(col_types) if declared == "record"]
        if records:
            rows = [
                tuple(_record_text(v) if i in records else v for i, v in enumerate(row))
                for row in rows
            ]
        return QueryResult(rows=rows, column_names=col_names, column_types=col_types)
    except Exception as exc:
        log.error(
            "[CATALOG] DuckDB error sql=%r rewritten=%r: %s", stripped[:200], rewritten[:200], exc
        )
        raise
    finally:
        if cur is not None:
            cur.close()
