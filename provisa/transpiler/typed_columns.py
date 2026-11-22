# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2e9a41-5c3b-4f86-b0e1-8a4c6f2d9e57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement's column references typed from the registered tables (REQ-2236).

The transpile is text to text. A dialect that writes a construct by its operands' types (Db2 and
SAP HANA: an integer cast, HANA's integer division; provisa/core/sql_integral.py) is handed the
column types of the tables it reads, as a sqlglot schema keyed by the names the statement gives
them, and each column reference whose table the schema holds is annotated with its registered
type. A reference the schema does not resolve is left untyped, and the dialect refuses what it
cannot write without its type."""

# Requirements: REQ-2236

from __future__ import annotations

from collections.abc import Mapping

import sqlglot.expressions as exp
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.scope import Scope, traverse_scope
from sqlglot.schema import MappingSchema

#: The dialects whose forms read their operands' types: they are handed the column types.
TYPED_DIALECTS = frozenset({"db2", "saphana"})

#: A sqlglot nested schema: catalog -> schema -> table -> column -> PostgreSQL type.
ColumnTypes = Mapping[str, Mapping[str, Mapping[str, Mapping[str, str]]]]


def typed(tree: exp.Expr, column_types: ColumnTypes) -> exp.Expr:
    """``tree`` with each column reference typed from ``column_types`` by sqlglot's annotation,
    through derived tables and CTEs. An unqualified reference is read as its scope's one source
    with a column of that name, as PostgreSQL resolves it: it is qualified for the annotation
    and unqualified again after, so the statement is written as it came. ``tree`` is changed in
    place."""
    nested: dict[str, object] = dict(column_types)
    schema = MappingSchema(nested, dialect="postgres")
    qualified: list[exp.Column] = []
    for scope in traverse_scope(tree):
        for column in scope.columns:
            if column.table or _output_alias(scope, column):
                continue
            owner = _owner(scope, column.name, schema)
            if owner is not None:
                column.set("table", exp.to_identifier(owner))
                qualified.append(column)
    annotated = annotate_types(tree, schema=schema, dialect="postgres")
    for column in qualified:
        column.set("table", None)
    return annotated


def _output_alias(scope: Scope, column: exp.Column) -> bool:
    """Whether ``column`` names one of its SELECT's outputs in an ORDER BY, which PostgreSQL reads
    before a source's column of that name."""
    select = scope.expression
    return (
        isinstance(select, exp.Select)
        and column.find_ancestor(exp.Order) is not None
        and any(s.alias == column.name for s in select.selects)
    )


def _owner(scope: Scope, name: str, schema: MappingSchema) -> str | None:
    """The name of the one source of ``scope`` with a column ``name``; None where no source, or
    more than one, has one (PostgreSQL's ambiguous reference)."""
    owners = [
        alias
        for alias, source in scope.sources.items()
        if (isinstance(source, exp.Table) and name in schema.column_names(source))
        or (
            isinstance(source, Scope)
            and isinstance(source.expression, exp.Select)
            and any(s.alias_or_name == name for s in source.expression.selects)
        )
    ]
    return owners[0] if len(owners) == 1 else None
