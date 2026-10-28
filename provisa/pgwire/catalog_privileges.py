# Copyright (c) 2026 Kenneth Stott
# Canary: d55b5c88-ecaf-498c-972f-e19572ce161d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a role holds on the tables it is served, and the defaults a write gives its columns
(REQ-2167, PGWIRE'S CATALOG FUNCTIONS): the relations has_table_privilege and pg_get_expr answer
from, filled from the role's governed grants.

A role holds SELECT on every table the catalog lists for it: the catalog lists what the role reads.
It holds INSERT, UPDATE or DELETE on a table where the table's source takes the operation, the
role holds the ``write`` right and the table carries the role's grant for that operation (the
rule a write is admitted by, compiler/write_admission). A column's default is the value the
role's INSERT grant presets on it."""

# Requirements: REQ-2167

from __future__ import annotations

import re

from provisa.pgwire.catalog_macros import _VISIBLE

_NUMBER = re.compile(r"-?\d+(\.\d+)?")
_NUMERIC_TYPES = frozenset({"smallint", "integer", "bigint", "numeric", "real", "double precision"})

# First free oid for an attrdef row; pg_attrdef oids are not otherwise assigned in the catalog.
_ATTRDEF_OID = 100000


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _default_expression(preset: dict, pg_type: str) -> str:
    """The expression PostgreSQL would store as the column's default for ``preset``."""
    source = preset["source"]
    if source == "now":
        return "now()"
    if source == "session":
        return f"current_setting({_literal(preset['name'])})"
    value = preset["value"]
    if pg_type in _NUMERIC_TYPES and _NUMBER.fullmatch(value):
        return value
    if pg_type == "boolean" and value.lower() in ("true", "false"):
        return value.lower()
    return f"{_literal(value)}::{pg_type}"


def _role_grants(raw_tables: list, role_id: str, can_write: bool):
    """Per raw table, the operations the role may write and its INSERT grant."""
    for tbl in raw_tables:
        grants = {g["operation"]: g for g in tbl.get("write_grants") or [] if g["role"] == role_id}
        takes = frozenset(tbl.get("write_ops") or ())
        operations = frozenset(grants) & takes if can_write else frozenset()
        yield tbl, operations, grants.get("insert") if "insert" in operations else None


def populate_table_privileges(
    db, idx, ctx, raw_tables: list, role_id: str, can_write: bool
) -> None:  # REQ-2167
    """Create ``_pg_table_privilege`` (the role's privileges per table oid) and fill the
    attrdef relation and the default-bearing columns of the attribute and column relations."""
    from provisa.compiler.naming import apply_sql_name
    from provisa.pgwire.catalog_populate import _physical_to_pg_name

    db.execute("CREATE TABLE _pg_table_privilege (relid INTEGER, privilege VARCHAR)")
    rows = [(toid, "SELECT") for (_cat, _sch, _name, _tid, toid) in idx.tables]
    p2s_raw = getattr(ctx, "physical_to_sql", None)
    p2s: dict = p2s_raw if isinstance(p2s_raw, dict) else {}
    types = {(toid, name): col_type for toid, name, col_type, _n, _o in idx.all_cols}
    attrdefs: list[tuple] = []
    served = {tid: toid for (_c, _s, _n, tid, toid) in idx.tables}
    for tbl, operations, insert in _role_grants(raw_tables, role_id, can_write):
        toid = served.get(tbl["id"])
        if toid is None:
            continue  # a table this role is not served holds no privilege here
        rows += [(toid, op.upper()) for op in sorted(operations)]
        for preset in (insert or {}).get("presets") or ():
            column = p2s.get((tbl["id"], preset["column"])) or apply_sql_name(preset["column"])
            attnum = idx.col_attnum.get((toid, column))
            if attnum is None:
                continue
            pg_type = _physical_to_pg_name(types[(toid, column)])
            attrdefs.append((toid, attnum, column, _default_expression(preset, pg_type)))
    if rows:
        db.executemany("INSERT INTO _pg_table_privilege VALUES (?, ?)", rows)
    for n, (toid, attnum, column, expression) in enumerate(attrdefs):
        db.execute(
            "INSERT INTO _pg_attrdef VALUES (?, ?, ?, ?)",
            [_ATTRDEF_OID + n, toid, attnum, expression],
        )
        db.execute(
            "UPDATE _pg_attribute SET atthasdef = true WHERE attrelid = ? AND attnum = ?",
            [toid, attnum],
        )
        schema_table = idx.toid_to_table[toid]
        db.execute(
            "UPDATE _is_columns SET column_default = ? WHERE table_schema = ? "
            "AND table_name = ? AND column_name = ?",
            [expression, schema_table[1], schema_table[2], column],
        )


def _fold(name: str) -> str:
    """An identifier as regclass input reads it: quoted, as written; bare, in lowercase."""
    return (
        f"CASE WHEN starts_with({name}, '\"') THEN replace(trim({name}, '\"'), '\"\"', '\"') "
        f"ELSE lower({name}) END"
    )


_NAME = r"""'^(?:("[^"]*"|[^".]*)\.)?("[^"]*"|[^".]*)$'"""


def _table_oid(table: str) -> str:
    """The oid of the table ``table`` names: an oid as it is, a name resolved on the search path
    (or by its schema), NULL where it names none."""
    schema = f"nullif(regexp_extract(CAST({table} AS VARCHAR), {_NAME}, 1), '')"
    relation = f"regexp_extract(CAST({table} AS VARCHAR), {_NAME}, 2)"
    return (
        f"CASE WHEN typeof({table}) = 'VARCHAR' THEN (SELECT _c.oid FROM _pg_class _c "
        "JOIN _pg_namespace _n ON _n.oid = _c.relnamespace "
        f"WHERE _c.relname = {_fold(relation)} AND (_n.nspname = {_fold(schema)} "
        f"OR ({schema} IS NULL AND _n.nspname IN {_VISIBLE})) "
        "ORDER BY CASE _n.nspname WHEN 'pg_catalog' THEN 0 ELSE 1 END LIMIT 1) "
        f"ELSE try_cast({table} AS BIGINT) END"
    )


def populate_privilege_macros(db, role_id: str) -> None:  # REQ-2167
    """has_table_privilege for the session role: true where it holds any privilege of the list
    (``SELECT``, ``INSERT, UPDATE``), false for one it does not hold (including the ones Provisa
    has no grant of: REFERENCES, TRIGGER, TRUNCATE, and any ``WITH GRANT OPTION``), NULL for a
    table that does not exist or a user other than the session role, whose grants this catalog
    does not hold."""
    held = (
        "(SELECT CASE WHEN _r._k IS NULL THEN NULL ELSE EXISTS (SELECT 1 FROM "
        "_pg_table_privilege _p WHERE _p.relid = _r._k AND list_contains("
        "list_transform(string_split(upper(priv), ','), _s -> trim(_s)), _p.privilege)) END "
        f"FROM (SELECT {_table_oid('tbl')} AS _k) _r)"
    )
    db.execute(
        f"CREATE MACRO has_table_privilege(tbl, priv) AS {held}, "
        f"(usr, tbl, priv) AS CASE WHEN CAST(usr AS VARCHAR) = {_literal(role_id)} "
        f"THEN {held} END"
    )
