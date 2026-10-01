# Copyright (c) 2026 Kenneth Stott
# Canary: 6e2b9d47-1c5a-4f83-b7e0-8a4d3c9f2e16
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A governed statement's result shape — column names and types — derived from registered metadata.

pgwire's Describe(Statement) answers from this (REQ-589, amended 2026-10-01): the statement is
governed once, and its columns are typed from what the registry already holds — the same per-table
column types the pgwire catalog advertises (``state.schema_build_cache["column_types"]``) — plus
SQLGlot's type annotation for computed expressions. No statement is sent to a source or an engine.

A projected column whose type cannot be derived is an error naming the column; nothing probes for
it. The Execute then encodes its rows as the types described here, so the two agree by construction.
"""

# Requirements: REQ-589

from __future__ import annotations

from typing import Any

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import SqlglotError

# Dialects a registered type string may be spelled in: the registry stores what the bound engine or
# the source reported, or what the config declared.
_TYPE_DIALECTS = ("postgres", "duckdb", "trino", "clickhouse")

_UNDETERMINED = (exp.DataType.Type.UNKNOWN, exp.DataType.Type.NULL)

# Postgres names an unaliased cast after its target type's catalog name.
_PG_TYPNAME = {
    exp.DataType.Type.INT: "int4",
    exp.DataType.Type.BIGINT: "int8",
    exp.DataType.Type.SMALLINT: "int2",
    exp.DataType.Type.DOUBLE: "float8",
    exp.DataType.Type.FLOAT: "float4",
    exp.DataType.Type.DECIMAL: "numeric",
    exp.DataType.Type.BOOLEAN: "bool",
    exp.DataType.Type.TIMESTAMPTZ: "timestamptz",
    exp.DataType.Type.VARCHAR: "varchar",
}


class UnderivableColumn(ValueError):
    """A result column's type cannot be derived from registered metadata."""


def _registered_type(raw: str) -> exp.DataType | None:
    """A registered type string as a SQLGlot type, or None when no dialect parses it (the column
    still describes by its registered spelling; only expressions over it cannot be typed)."""
    for dialect in _TYPE_DIALECTS:
        try:
            return exp.DataType.build(raw, dialect=dialect)
        except (SqlglotError, ValueError):
            continue
    return None


def table_columns(table_id: int, ctx: Any, column_types: dict) -> dict[str, str]:
    """exposed column name -> registered type for one table, as the role sees it — the same
    selection and naming the pgwire catalog publishes (``catalog_populate._build_catalog_index``)."""
    from provisa.compiler.naming import apply_sql_name

    exposed_names = ctx.physical_to_sql
    out: dict[str, str] = {}
    for col in column_types.get(table_id, []):
        physical = col.column_name
        if exposed_names and (table_id, physical) not in exposed_names:
            continue  # not visible to this role
        out[exposed_names.get((table_id, physical)) or apply_sql_name(physical)] = col.data_type
    for virtual in ctx.virtual_columns.get(table_id, {}):
        out[virtual] = "varchar"
    return out


def _resolve_table(tbl: exp.Table, table_map: dict[str, int]) -> int | None:
    qualified = f"{tbl.db}.{tbl.name}" if tbl.db else tbl.name
    table_id = table_map.get(qualified)
    if table_id is None:
        table_id = table_map.get(tbl.name)
    return table_id


def _lookup(columns: dict[str, str], name: str) -> str | None:
    if name in columns:
        return columns[name]
    lowered = name.lower()
    for col, col_type in columns.items():
        if col.lower() == lowered:
            return col_type
    return None


def _pg_column_name(node: exp.Expr) -> str:
    """The name Postgres gives an unaliased result column (its FigureColname rule)."""
    if isinstance(node, exp.Column):
        return node.name
    if isinstance(node, exp.Cast):
        inner = _pg_column_name(node.this)
        if inner != "?column?":
            return inner
        target = node.to
        return _PG_TYPNAME.get(target.this, target.sql(dialect="postgres").lower())
    if isinstance(node, exp.Paren):
        return _pg_column_name(node.this)
    if isinstance(node, exp.Anonymous):
        return node.name.lower()
    if isinstance(node, exp.Case):
        return "case"
    if isinstance(node, exp.Func):
        return node.sql_name().lower()
    return "?column?"


def _untyped_is_text(node: exp.Expr) -> bool:
    """Postgres resolves an unknown-typed literal — a bare NULL, or a parameter with no cast — as
    text in a result column."""
    return isinstance(node, (exp.Null, exp.Placeholder, exp.Parameter))


def derive_result_shape(
    governed_sql: str, table_map: dict[str, int], ctx: Any, column_types: dict
) -> list[tuple[str, str]]:
    """(column name, type) for every result column of ``governed_sql`` (the governed semantic SQL),
    in order. An empty list is a statement that returns no rows (a write without RETURNING).

    ``table_map`` is the governance context's name -> table id resolution, ``ctx`` the role's
    compilation context and ``column_types`` the registry's per-table column types."""
    tree = sqlglot.parse_one(governed_sql, read="postgres")
    if isinstance(tree, (exp.Insert, exp.Update, exp.Delete, exp.Merge)):
        return _returning_shape(tree, table_map, ctx, column_types)
    if not isinstance(tree, exp.Query):
        raise UnderivableColumn(
            f"cannot describe a {type(tree).__name__} statement from registered metadata"
        )
    fast = _single_table_shape(tree, table_map, ctx, column_types)
    if fast is not None:
        return fast
    return _annotated_shape(tree, table_map, ctx, column_types)


def _returning_shape(
    tree: exp.Expr, table_map: dict[str, int], ctx: Any, column_types: dict
) -> list[tuple[str, str]]:
    returning = tree.args.get("returning")
    if returning is None:
        return []
    target = tree.find(exp.Table)
    table_id = _resolve_table(target, table_map) if target is not None else None
    if table_id is None:
        raise UnderivableColumn("cannot describe RETURNING: its target is not a registered table")
    columns = table_columns(table_id, ctx, column_types)
    shape = _plain_projection_shape(returning.expressions, columns)
    if shape is None:
        raise UnderivableColumn(
            "cannot describe RETURNING: only the target table's columns can be typed from "
            f"registered metadata, got {returning.sql(dialect='postgres')!r}"
        )
    return shape


def _plain_projection_shape(
    projections: list[exp.Expr], columns: dict[str, str]
) -> list[tuple[str, str]] | None:
    """The shape when every projection is a column of one table (or ``*``); else None."""
    shape: list[tuple[str, str]] = []
    for sel in projections:
        inner = sel.unalias() if isinstance(sel, exp.Alias) else sel
        if isinstance(inner, exp.Star):
            shape.extend(columns.items())
            continue
        if not isinstance(inner, exp.Column) or isinstance(inner.this, exp.Star):
            return None
        col_type = _lookup(columns, inner.name)
        if col_type is None:
            return None
        shape.append((sel.alias_or_name, col_type))
    return shape


def _single_table_shape(
    tree: exp.Query, table_map: dict[str, int], ctx: Any, column_types: dict
) -> list[tuple[str, str]] | None:
    """The common statement — columns of ONE registered table, any WHERE/ORDER/LIMIT — typed by
    direct registry lookup, with no qualification or annotation pass. None when it does not apply."""
    if not isinstance(tree, exp.Select) or tree.args.get("with_") or tree.args.get("joins"):
        return None
    source = tree.args.get("from_")
    if source is None or not isinstance(source.this, exp.Table):
        return None
    table_id = _resolve_table(source.this, table_map)
    if table_id is None:
        return None
    return _plain_projection_shape(tree.expressions, table_columns(table_id, ctx, column_types))


def _annotated_shape(
    tree: exp.Query, table_map: dict[str, int], ctx: Any, column_types: dict
) -> list[tuple[str, str]]:
    from sqlglot.optimizer.annotate_types import annotate_types
    from sqlglot.optimizer.qualify import qualify
    from sqlglot.optimizer.scope import build_scope
    from sqlglot.schema import MappingSchema

    tree = tree.copy()
    cte_names = {cte.alias for cte in tree.find_all(exp.CTE)}
    # Each registered table reference gets a synthetic schema of its own, so the SQLGlot schema has
    # one uniform depth whether the statement qualified the table or not.
    registered: dict[tuple[str, str], dict[str, str]] = {}
    for tbl in tree.find_all(exp.Table):
        if not tbl.db and tbl.name in cte_names:
            continue
        table_id = _resolve_table(tbl, table_map)
        if table_id is None:
            continue
        db = f"_t{table_id}"
        tbl.set("catalog", None)
        tbl.set("db", exp.to_identifier(db))
        registered[(db, tbl.name)] = table_columns(table_id, ctx, column_types)

    nested: dict[str, Any] = {}
    for (db, name), columns in registered.items():
        nested.setdefault(db, {})[name] = {
            col: _registered_type(raw) or exp.DataType.build("unknown")
            for col, raw in columns.items()
        }
    schema = MappingSchema(nested, dialect="postgres")
    original = list(tree.selects)
    try:
        qualified = qualify(
            tree,
            schema=schema,
            dialect="postgres",
            validate_qualify_columns=False,
            quote_identifiers=False,
            identify=False,
        )
        annotated = annotate_types(qualified, schema=schema, dialect="postgres")
    except SqlglotError as exc:
        raise UnderivableColumn(
            f"cannot describe the statement's result columns from registered metadata: {exc}"
        ) from exc

    root = build_scope(annotated)
    selects = list(annotated.selects)
    # Positions line up with the statement's own projections unless a star was expanded.
    aligned = original if len(original) == len(selects) else None
    shape: list[tuple[str, str]] = []
    for i, sel in enumerate(selects):
        inner = sel.unalias() if isinstance(sel, exp.Alias) else sel
        written = aligned[i] if aligned is not None else sel
        name = written.alias if isinstance(written, exp.Alias) else None
        if not name:
            name = (
                _pg_column_name(written.unalias() if isinstance(written, exp.Alias) else written)
                if aligned is not None
                else sel.alias_or_name
            )

        col_type: str | None = None
        if isinstance(inner, exp.Column) and root is not None:
            source = root.sources.get(inner.table)
            if isinstance(source, exp.Table):
                columns = registered.get((source.db, source.name))
                if columns is not None:
                    col_type = _lookup(columns, inner.name)  # the registry's own spelling
        if col_type is None:
            derived = sel.type
            if derived is not None and derived.this not in _UNDETERMINED:
                col_type = derived.sql(dialect="postgres")
            elif _untyped_is_text(inner):
                col_type = "text"
        if col_type is None:
            raise UnderivableColumn(
                f"cannot describe result column {name!r}: the type of "
                f"{inner.sql(dialect='postgres')} cannot be derived from registered metadata — "
                "CAST the expression to state its type"
            )
        shape.append((name, col_type))
    return shape
