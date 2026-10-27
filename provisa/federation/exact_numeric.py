# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Exact reads of a PostgreSQL numeric on DuckDB (REQ-2167).

DuckDB's PostgreSQL reader takes a numeric with no precision as a double, so its value is rounded
before anything Provisa writes can cast it. Registration records such a column as
``numeric(38, s)``, ``s`` the largest scale of its values (``introspect._sized_numeric``). On
DuckDB, a statement reading one is given the table through ``postgres_query``: PostgreSQL itself
casts the column to ``numeric(38, s)``, which DuckDB holds exactly as ``DECIMAL(38, s)``. Only
the columns the statement names are read, as the attached scan reads them. PostgreSQL filters are
already applied in DuckDB on this route (``duckdb_extensions``: filter pushdown is off), so
nothing is read that the scan would not have read.
"""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Callable
from typing import Any

import sqlglot
from sqlglot import expressions as exp

from provisa.core.numeric_type import numeric_size

# The precision registration gives an unsized PostgreSQL numeric (introspect._NUMERIC_PRECISION).
_WIDE_PRECISION = 38
# Source kinds DuckDB reads through its PostgreSQL reader.
_POSTGRES_KINDS = frozenset({"postgresql", "cockroachdb", "yugabytedb", "greenplum"})


@dataclass(frozen=True)
class WideTable:
    """A PostgreSQL-read table with a wide numeric: its registered columns in order, and the
    type each wide one is read as (``numeric(38,s)``)."""

    columns: tuple[str, ...]
    wide: dict[str, str]


Addressed = dict[tuple[str, str, str], WideTable]


# The last build's tables read: (the build's table list, what it holds). One lookup per build.
_BUILT: list[tuple[Any, Addressed]] = []


def wide_tables_of_build(state: Any) -> Addressed:
    """:func:`wide_numeric_tables` of the current build, worked out once per build."""
    tables = state.schema_build_cache.get("tables", [])
    for built, found in _BUILT:
        if built is tables:
            return found
    found = wide_numeric_tables(state)
    _BUILT[:] = [(tables, found)]
    return found


def wide_numeric_tables(state: Any) -> Addressed:
    """``(catalog, schema, table) -> WideTable`` for every registered table of a PostgreSQL-read
    source holding a column registration sized from an unsized numeric."""
    found: Addressed = {}
    for table in state.schema_build_cache.get("tables", []):
        if state.source_types.get(table["source_id"]) not in _POSTGRES_KINDS:
            continue
        wide = {
            c["column_name"]: c["data_type"]
            for c in table["columns"]
            if (numeric_size(c["data_type"] or "") or (None,))[0] == _WIDE_PRECISION
        }
        if wide:
            key = (state.catalog_for(table["source_id"]), table["schema_name"], table["table_name"])
            found[key] = WideTable(tuple(c["column_name"] for c in table["columns"]), wide)
    return found


def _quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _columns_read(tree: exp.Expr, table: exp.Table) -> set[str] | None:  # pyright: ignore[reportPrivateImportUsage]
    """The columns ``tree`` reads of ``table``, or None when it may read every one (a star of
    it, or a column it does not qualify, which may be any table's)."""
    name = table.alias_or_name
    for star in tree.find_all(exp.Star):
        parent = star.parent
        if not isinstance(parent, exp.Column) or parent.table in ("", name):
            return None
    read: set[str] = set()
    for column in tree.find_all(exp.Column):
        if isinstance(column.this, exp.Star):
            continue
        if not column.table:
            return None
        if column.table == name:
            read.add(column.name)
    return read


def _source_select(schema: str, table: str, held: WideTable, names: list[str]) -> str:
    """The PostgreSQL statement that reads ``names`` of ``schema.table``, each wide one cast
    there."""
    selected = ", ".join(
        f"{_quoted(c)}::{held.wide[c]} AS {_quoted(c)}" if c in held.wide else _quoted(c)
        for c in names
    )
    return f"SELECT {selected} FROM {_quoted(schema)}.{_quoted(table)}"


class NotAttached(RuntimeError):
    """A table holding a wide numeric whose remote the engine has not attached."""


def exact_numeric_reads(
    duck_sql: str,
    wide: Addressed,
    remote_of: Callable[[tuple[str, str, str]], tuple[str, str, str] | None],
) -> str:
    """``duck_sql`` with each table holding a wide numeric read through ``postgres_query`` on the
    remote it stands for (``remote_of``: a view's raw attach alias and remote address)."""
    if not wide or not any(f'"{name}"' in duck_sql or name in duck_sql for _, _, name in wide):
        return duck_sql
    tree = sqlglot.parse_one(duck_sql, read="duckdb")
    targets = [
        t
        for t in tree.find_all(exp.Table)
        if (t.catalog, t.db, t.name) in wide and isinstance(t.this, exp.Identifier)
    ]
    if not targets:
        return duck_sql
    for table in targets:
        view = (table.catalog, table.db, table.name)
        held = wide[view]
        remote = remote_of(view)
        if remote is None:
            raise NotAttached(f"{'.'.join(view)} holds an exact numeric but is not attached")
        alias, schema, name = remote
        read = _columns_read(tree, table)
        names = [c for c in held.columns if read is None or c in read]
        source_sql = _source_select(schema, name, held, names).replace("'", "''")
        replacement = sqlglot.parse_one(
            f"SELECT * FROM (SELECT * FROM postgres_query('{alias}', '{source_sql}')) "
            f"AS {_quoted(table.alias_or_name)}",
            read="duckdb",
        ).find(exp.Subquery)
        table.replace(replacement)
    return tree.sql(dialect="duckdb")
