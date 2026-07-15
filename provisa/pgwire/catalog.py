# Copyright (c) 2026 Kenneth Stott
# Canary: b2c3d4e5-f6a7-8901-bcde-f12345678901
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

import logging
import re

from provisa.pgwire.catalog_data import (
    _CATALOG_TABLE_NAMES,
    _INTERCEPT_SCHEMAS,
)
from provisa.pgwire.catalog_rewrite import _rewrite_for_duckdb, next_txid
from provisa.pgwire.catalog_data import (
    _KNOWN_SETTINGS,
    _TYPEINFO,
    _TYPEINFO_COL_TYPES,
    _TYPEINFO_COLS,
)
from provisa.pgwire.catalog_populate import _build_catalog_db

log = logging.getLogger(__name__)

_SET_RE = re.compile(r"^\s*SET\b", re.IGNORECASE)
_SHOW_RE = re.compile(r"^\s*SHOW\b", re.IGNORECASE)
_TXN_RE = re.compile(
    r"^\s*(BEGIN|START\s+TRANSACTION|START|COMMIT|ROLLBACK|DISCARD|RESET|DEALLOCATE|SAVEPOINT|RELEASE)\b",
    re.IGNORECASE,
)

_SHOW_MIN_PARTS = 2

_SCALAR_FN_RE = re.compile(
    r"^\s*SELECT\s+(?:pg_catalog\.)?"
    r"(current_user|session_user|current_database\(\)|current_schema\(\)|version\(\)"
    r"|pg_backend_pid\(\)|pg_is_in_recovery\(\)|txid_current\(\))\s*$",
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
    for oid in oids:
        info = _TYPEINFO.get(oid)
        if info is None:
            continue
        ns, name, kind, basetype, elemtype, elemdelim, range_subtype = info
        elem_name = _TYPEINFO[elemtype][1] if elemtype and elemtype in _TYPEINFO else None
        base_name = _TYPEINFO[basetype][1] if basetype and basetype in _TYPEINFO else None
        range_name = (
            _TYPEINFO[range_subtype][1] if range_subtype and range_subtype in _TYPEINFO else None
        )
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


def classify(sql: str) -> str:  # REQ-127, REQ-128, REQ-363
    """Return 'INTERCEPT' or 'PASS_THROUGH'."""
    stripped = sql.strip()
    if _SET_RE.match(stripped) or _SHOW_RE.match(stripped) or _TXN_RE.match(stripped):
        return "INTERCEPT"
    if _SCALAR_FN_RE.match(stripped):
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
        for node in tree.walk():
            if type(node).__name__ in ("CurrentUser", "CurrentDatabase", "CurrentSchema"):
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


def _handle_scalar(sql: str, role_id: str):
    from provisa.executor.result import QueryResult

    s = sql.strip().lower()
    if "current_user" in s or "session_user" in s:
        return QueryResult(rows=[(role_id,)], column_names=["current_user"])
    if "current_database" in s:
        return QueryResult(rows=[("provisa",)], column_names=["current_database"])
    if "version()" in s:
        return QueryResult(rows=[("PostgreSQL 14.0 on Provisa",)], column_names=["version"])
    if "current_schema()" in s:
        return QueryResult(rows=[("public",)], column_names=["current_schema"])
    if "pg_backend_pid()" in s:
        return QueryResult(rows=[(0,)], column_names=["pg_backend_pid"])
    if "pg_is_in_recovery()" in s:
        # Provisa is never a replica.
        return QueryResult(rows=[(False,)], column_names=["pg_is_in_recovery"])
    if "txid_current()" in s:
        return QueryResult(rows=[(next_txid(),)], column_names=["txid_current"])
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
        return QueryResult(rows=[(val1, None)], column_names=[col1, col2])

    m = re.search(r"current_setting\s*\(\s*['\"]([^'\"]+)['\"]\s*\)", sql, re.IGNORECASE)
    if not m:
        return None
    key = m.group(1).lower()
    value = _KNOWN_SETTINGS.get(key, "")
    return QueryResult(rows=[(value,)], column_names=["current_setting"])


def answer(sql: str, role_id: str, state):  # REQ-532
    """Return a synthetic QueryResult for intercepted catalog/SET/SHOW queries."""
    from provisa.executor.result import QueryResult

    stripped = sql.strip().rstrip(";")

    if _TXN_RE.match(stripped) or _SET_RE.match(stripped):
        return QueryResult(rows=[], column_names=[])

    if _SHOW_RE.match(stripped):
        return _handle_show(stripped)

    if _SCALAR_FN_RE.match(stripped):
        result = _handle_scalar(stripped, role_id)
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

        return QueryResult(rows=[("on",)], column_names=["set_config"], column_types=["VARCHAR"])

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

    if "pg_get_keywords" in stripped.lower():
        # pg_get_keywords() is a SRF — rewriter turns it into scalar NULL, breaking FROM clause.
        # DBeaver uses it only for SQL autocomplete keyword exclusion; return empty string.
        return QueryResult(rows=[(None,)], column_names=["string_agg"], column_types=["VARCHAR"])

    rewritten = stripped
    db = None
    try:
        db = _build_catalog_db(role_id, state)
        # Substitute $N params before rewriting so SQLGlot can parse the SQL.
        # Queries with $N::type[] (e.g. asyncpg type introspection) would otherwise
        # fail to parse, preventing table-name rewrites.
        import re as _re

        # Strip $N params AND any trailing PG type cast (e.g. $1::oid[]) so SQLGlot
        # can parse the query without failing on array-type annotations.
        pre_subst = _re.sub(r"\$\d+(?:::[^\s,)]+)?", "NULL", stripped)
        rewritten = _rewrite_for_duckdb(pre_subst, role_id)
        cur = db.execute(rewritten)
        rows = [tuple(r) for r in cur.fetchall()]
        col_names = [desc[0] for desc in (cur.description or [])]
        col_types = [str(desc[1]) for desc in (cur.description or [])]
        return QueryResult(rows=rows, column_names=col_names, column_types=col_types)
    except Exception as exc:
        log.error(
            "[CATALOG] DuckDB error sql=%r rewritten=%r: %s", stripped[:200], rewritten[:200], exc
        )
        raise
    finally:
        if db is not None:
            db.close()
