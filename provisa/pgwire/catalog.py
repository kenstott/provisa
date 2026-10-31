# Copyright (c) 2026 Kenneth Stott
# Canary: 88e0dfd5-5252-4cb3-83c0-4fa63effc0d4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's catalog on a Provisa connection (REQ-2190): which statements are the catalog's, and
their answers. SET, SHOW and transaction control are the session's, answered here; any other is
addressed to the meta domain (catalog_route) and governed as a read of it."""

# Requirements: REQ-127, REQ-128, REQ-363

# complexity-gate: allow-ble=1 reason="classify() fail-opens on unparseable SQL (REQ-127) to a regex-based catalog-name scan so a malformed introspection query is still intercepted rather than crashing the pgwire session"

from __future__ import annotations

import dataclasses
import datetime
import functools
import logging
import re
from typing import Any

from provisa.pgwire.catalog_data import (
    _CATALOG_TABLE_NAMES,
    _INTERCEPT_SCHEMAS,
)
from provisa.pgwire.catalog_route import CatalogRequest, address, next_txid
from provisa.pgwire.catalog_data import (
    _KNOWN_SETTINGS,
    _TYPEINFO_COL_TYPES,
    _TYPEINFO_COLS,
    typeinfo,
)
from provisa.pgwire.catalog_pg17 import PROC_NAMES, TYPES
from provisa.pgwire.constant_arguments import is_constant_sql
from provisa.pgwire.catalog_server_answers import ANSWERED as _SERVER_ANSWERED
from provisa.pgwire.catalog_server_answers import PG_ONLY_TYPE_NAMES, REWRITTEN
from provisa.core.pg_error import PostgresError, postgres_error
from provisa.transpiler.metadata_functions import (
    FORMS,
    READS_THE_SERVER,
    RELATION_FORMS,
    MetadataFunctionRefused,
)
from provisa.transpiler.metadata_settings import relation_names_literal

log = logging.getLogger(__name__)

_SET_RE = re.compile(r"^\s*SET\b", re.IGNORECASE)
_SHOW_RE = re.compile(r"^\s*SHOW\b", re.IGNORECASE)
_TXN_RE = re.compile(
    r"^\s*(BEGIN|START\s+TRANSACTION|START|COMMIT|END|ROLLBACK|ABORT|DISCARD|RESET|DEALLOCATE|SAVEPOINT|RELEASE)\b",
    re.IGNORECASE,
)

_SHOW_MIN_PARTS = 2

#: A call sqlglot reads as syntax of its own (provisa.transpiler.pg_functions.readable).
_UNPARSED_CALL = re.compile(r"\b(convert|json_object)\s*\(")

# REQ-2167: a statement that only sleeps, as PostgreSQL's pg_sleep(seconds) does, answering void.
_PG_SLEEP_RE = re.compile(
    r'^\s*SELECT\s+(?:pg_catalog\.)?"?(?P<name>pg_sleep(?:_for|_until)?)"?\s*\((?P<arg>.*)\)\s*'
    r"(?:AS\s+(?P<alias>\w+))?\s*;?\s*$",
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
        "pg_partition_root",
        *_SERVER_ANSWERED,
        *REWRITTEN,
    }
)


def _reads_only_meta(tree) -> bool:
    """Whether every table ``tree`` reads is meta's: such a statement runs on the control plane."""
    import sqlglot.expressions as exp

    tables = list(tree.find_all(exp.Table))
    return bool(tables) and all((t.db or "").lower() == "meta" for t in tables)


#: What is written in its form in a statement on the control plane.
_FORM_NAMES = frozenset({*(name for name, _ in (*FORMS, *RELATION_FORMS)), *READS_THE_SERVER})


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
#: The types only the catalog DB can hold as a function's argument (REQ-2167).
_FUNCTION_ARGUMENT_TYPES = frozenset({"regproc", "xid", "tid", "oidvector", *PG_ONLY_TYPE_NAMES})


def _casts_to_a_catalog_type_alone(tree) -> bool:
    import sqlglot.expressions as exp

    if any(True for _ in tree.find_all(exp.Table)):
        return False
    types = {
        cast.to.sql(dialect="postgres").lower().removeprefix("pg_catalog.").removesuffix("[]")
        for cast in tree.find_all(exp.Cast)
    }
    if types & _CATALOG_ONLY_TYPES:
        return True
    # A value of a type only the catalog DB holds, given to a function: the engine cannot read the
    # argument. A cast alone is the engine's, which sends the type in its own wire form.
    calls = any(not isinstance(f, exp.Cast) for f in tree.find_all(exp.Func))
    return calls and bool(types & _FUNCTION_ARGUMENT_TYPES)


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
            if fn in _SCALAR_NAMES or (fn in _FORM_NAMES and _reads_only_meta(tree)):
                return "INTERCEPT"  # REQ-2190: left in, it would answer about the control plane
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
        if _UNPARSED_CALL.search(lower) and " from " not in lower:
            return "INTERCEPT"  # REQ-2167: sqlglot reads it as syntax; the catalog DB as a call
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
        from provisa.pgwire.catalog_data import served_database_name

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
        # includes this. A connection's own call releases its locks in the server
        # (server._advisory_answer); answered here it has none: one row, one NULL column, same as
        # _handle_scalar's other functions.
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


#: An object identifier value a statement reads with its name, its oid and name held together.
_NAMED_VALUES = frozenset({"regclass", "regnamespace"})


def _declared_types(sql: str, names: list[str], served: list[str]) -> list[str]:  # REQ-2167
    """Each column's type as PostgreSQL 17 declares it where the served value has that type's
    wire form (a catalog column served as text that PostgreSQL declares name keeps its text, sent
    as name); otherwise the type it is served with. Columns pair up by position, or by name where
    the served relation's columns are not PostgreSQL's."""
    from provisa.pgwire.catalog_types import CATALOG_TYPE_OIDS, result_types
    from provisa.pgwire.server import same_wire_form

    declared = result_types(sql)
    if len(declared) == len(served):
        pg_types = [pg_type for _, pg_type in declared]
    else:
        by_name = dict(reversed(declared))
        pg_types = [by_name.get(name, "") for name in names]
    return [
        pg_type
        if pg_type
        and (
            pg_type == "record"
            or pg_type in CATALOG_TYPE_OIDS
            or pg_type in _NAMED_VALUES
            # A read of an undeclared numeric is floating point (REQ-1961); a catalog's is exact.
            or pg_type == "decimal"
            or same_wire_form(pg_type, duck_type)
        )
        else duck_type
        for pg_type, duck_type in zip(pg_types, served)
    ]


def _record_field(value: Any) -> str:
    """One field as record_out writes it: nothing for NULL, quoted where it must be."""
    if value is None:
        return ""
    if isinstance(value, list):
        text = "{" + ",".join("NULL" if v is None else str(v) for v in value) + "}"
    elif isinstance(value, bool):
        text = "t" if value else "f"
    else:
        text = str(value)
    if text == "" or any(c in text for c in '",\\() \t\n'):
        return '"' + text.replace("\\", "\\\\").replace('"', '""') + '"'
    return text


def _record_text(value: Any) -> Any:
    """A record value (the engine's struct) in PostgreSQL's text form."""
    if isinstance(value, tuple):
        return "(" + ",".join(_record_field(v) for v in value) + ")"
    if not isinstance(value, dict):
        return value
    return "(" + ",".join(_record_field(v) for v in value.values()) + ")"


def _handle_pg_sleep(match: re.Match[str]):  # REQ-2167
    """pg_sleep(seconds), pg_sleep_for(interval) or pg_sleep_until(timestamp with time zone):
    sleeps that long and answers void (empty text); strict, so a NULL argument answers NULL
    without sleeping. A sleep longer than the request has left ends with
    the request's timeout."""
    import threading

    import sqlglot

    from provisa.core import engine_memory
    from provisa.executor.result import QueryResult

    arg = match["arg"]
    if not is_constant_sql(arg):
        raise NotImplementedError(
            f"{match['name']}: an argument that is not a constant expression is not supported"
        )
    seconds = {
        "pg_sleep": f"CAST(({arg}) AS DOUBLE PRECISION)",
        "pg_sleep_for": f"EXTRACT(EPOCH FROM ({arg}))",
        "pg_sleep_until": f"EXTRACT(EPOCH FROM ({arg})) - EXTRACT(EPOCH FROM NOW())",
    }[match["name"]]
    seconds_sql = sqlglot.transpile(f"SELECT {seconds}", read="postgres", write="duckdb")[0]
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
        column_names=[match["alias"] or match["name"]],
        column_types=["void"],
    )


@dataclasses.dataclass(frozen=True)
class CatalogSession:
    """The connection a catalog statement is answered for: what pg_stat_activity lists of it
    (REQ-2167). ``backend_start`` is when the connection was made."""

    pid: int
    application_name: str = ""
    backend_start: datetime.datetime | None = None


def _list_session(cur, role_id: str, session: CatalogSession, sql: str) -> None:
    """Make ``session`` the one row of pg_stat_activity, for this statement's cursor alone: a
    temporary table of the same name shadows the shared, empty one, and goes with the cursor
    (REQ-2167). The row is the session as PostgreSQL shows a backend running a statement."""
    cur.execute(
        "CREATE TEMPORARY TABLE _pg_stat_activity AS SELECT * FROM main._pg_stat_activity LIMIT 0"
    )
    cur.execute(
        "INSERT INTO _pg_stat_activity (datid, datname, pid, usesysid, usename, application_name, "
        "backend_start, xact_start, query_start, state_change, state, query, backend_type) "
        "SELECT d.oid, d.datname, ?, (SELECT r.oid FROM _pg_roles r WHERE r.rolname = ?), ?, ?, ?, "
        "now(), now(), now(), 'active', ?, 'client backend' FROM _pg_database d",
        [session.pid, role_id, role_id, session.application_name, session.backend_start, sql],
    )


#: regproc's oid, a value of which a driver hands over as the function's name.
_REGPROC_OID = 24
_PROC_OIDS = {name: oid for oid, name in PROC_NAMES.items()}
#: PostgreSQL's oids of a value it sends in text as a vector of numbers.
_VECTOR_OIDS = frozenset({22, 30})


def _catalog_request(
    sql: str, role_id: str, state, session: CatalogSession | None
) -> CatalogRequest:
    from provisa.pgwire.catalog_facts import role_facts, row_counts
    from provisa.pgwire.catalog_data import served_database_name
    from provisa.transpiler.metadata_facts import catalog_facts

    connection: dict[str, str] = {}
    if session is not None:
        connection = {
            "pid": str(session.pid),
            "application_name": session.application_name,
            "backend_start": session.backend_start.isoformat() if session.backend_start else "",
        }
    return CatalogRequest(
        role_id=role_id,
        database=served_database_name(state),
        # Only the privilege functions read them, and the model is walked for them.
        facts=role_facts(role_id, state)
        if "_privilege" in sql.lower()
        else catalog_facts(role_id, {}),
        settings=_KNOWN_SETTINGS,
        pid=session.pid if session is not None else 0,
        session={**connection, "query": sql} if "pg_stat_activity" in sql.lower() else connection,
        reltuples=row_counts(role_id, state) if "reltuples" in sql.lower() else {},
    )


def _served_type(oid: int) -> str:
    """The type PostgreSQL said it sent, by the name the catalog's own list gives it."""
    entry = TYPES.get(oid)
    return entry[-1] if entry is not None else "text"


def _as_declared(sql: str, result):
    """``result`` with its columns of the statement as the client wrote it: a catalog relation's
    columns are PostgreSQL 17's types, which a view holds as what it can."""
    from provisa.executor.result import QueryResult

    served = [_served_type(oid) for oid in result.column_type_oids or ()]
    if len(served) != len(result.column_names):
        served = ["text"] * len(result.column_names)
    col_types = _declared_types(sql, result.column_names, served)
    oids = result.column_type_oids or ()
    vectors = [i for i, oid in enumerate(oids) if oid in _VECTOR_OIDS]
    procs = [i for i, oid in enumerate(oids) if oid == _REGPROC_OID]
    records = [i for i, declared in enumerate(col_types) if declared == "record"]
    rows = result.rows
    if vectors or records or procs:
        rows = [
            tuple(
                _record_text(v)
                if i in records
                else [int(n) for n in v.split()]
                if i in vectors and isinstance(v, str)
                else _PROC_OIDS.get(v, 0)
                if i in procs and isinstance(v, str)
                else v
                for i, v in enumerate(row)
            )
            for row in rows
        ]
    return _with_own_oids(
        QueryResult(rows=rows, column_names=result.column_names, column_types=col_types),
        oids,
        served,
    )


def _array_text(value: list) -> str:
    return "{" + ",".join("NULL" if v is None else str(v) for v in value) + "}"


def _with_own_oids(result, oids, served: list[str]):
    """A column of a type the wire has no form of its own for (tsquery, xml[]) is sent as text under
    PostgreSQL's oid, which is what its text form is: the client names it as PostgreSQL does."""
    from provisa.pgwire.server import BVTYPE_TO_PGTYPE, _sql_type_to_bvtype

    declared = result.column_types
    own: list[int | None] = [None] * len(declared)
    for i, oid in enumerate(oids):
        if declared[i] == served[i] and BVTYPE_TO_PGTYPE[_sql_type_to_bvtype(served[i])][0] != oid:
            own[i] = oid
            declared[i] = "text"
    if not any(own):
        return result
    result.column_type_oids = own
    result.rows = [
        tuple(
            _array_text(v) if own[i] is not None and isinstance(v, list) else v
            for i, v in enumerate(row)
        )
        for row in result.rows
    ]
    return result


def _read_meta(sql: str, role_id: str, state, session: CatalogSession | None, run):
    """The statement as a read of the meta domain, governed as any read is (REQ-2190): ``run``
    takes the coroutine onto the connection's loop."""
    from provisa.pgwire._pipeline import _execute_plan, govern_catalog_plan

    request = _catalog_request(sql, role_id, state, session)
    # A $N a value was not bound for (Describe of an unbound statement): any value describes it.
    bare = re.sub(r"\$\d+", "NULL", sql)
    try:
        addressed, asked = address(bare, request)
    except MetadataFunctionRefused as exc:
        raise PostgresError("0A000", str(exc)) from exc
    if asked is not None:
        told = {**request.session, "asked": relation_names_literal(asked)}
        request = dataclasses.replace(request, session=told)

    async def read():
        plan = await govern_catalog_plan(addressed, role_id, request)
        return await _execute_plan(plan)

    try:
        return _as_declared(sql, run(read()))
    except Exception as exc:
        answered = postgres_error(exc)
        if answered is not None:
            raise answered from exc
        log.error("[CATALOG] sql=%r addressed=%r: %s", sql[:200], addressed[:200], exc)
        raise


def answer(sql: str, role_id: str, state, session: CatalogSession | None = None, *, run=None):
    """Return a QueryResult for intercepted catalog/SET/SHOW queries (REQ-532). ``session`` is the
    connection asking, which pg_stat_activity lists; a statement with none lists no session.
    ``run`` runs a coroutine to completion on the connection's own loop."""
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

    return _read_meta(stripped, role_id, state, session, run)
