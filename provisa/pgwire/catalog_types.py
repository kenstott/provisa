# Copyright (c) 2026 Kenneth Stott
# Canary: b0bac4ef-6df2-4879-9f54-7dfbf94069eb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The PostgreSQL types of a catalog statement's result columns (REQ-2167).

A column the statement reads from a pg_catalog or information_schema relation, as it is or
through an alias, a subquery or a CTE, has the type PostgreSQL 17 declares for it
(catalog_pg17.RELATIONS); a column that is a call of a pg_catalog function has the type PostgreSQL
17 says the function returns (catalog_pg17.FUNCTION_RETURNS); any other column has the type its
expression gives."""

from __future__ import annotations

import functools

import sqlglot
import sqlglot.expressions as exp
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.qualify import qualify
from sqlglot.schema import MappingSchema

from provisa.pgwire.catalog_pg17 import FUNCTION_RETURNS, RELATIONS, TYPES

# The types whose text and binary forms are those of the wire type a catalog column is served
# with (text, or a 4-byte integer), named the way the statement's annotation names them; their
# OIDs are PostgreSQL 17's, as RELATIONS records them.
_SAME_WIRE_FORM = frozenset(
    {
        "name",
        '"char"',
        "pg_node_tree",
        "oid",
        "xid",
        "information_schema.sql_identifier",
        "information_schema.character_data",
        "information_schema.yes_or_no",
        "information_schema.cardinal_number",
    }
)

CATALOG_TYPE_OIDS: dict[str, int] = {
    entry[-1]: oid for oid, entry in TYPES.items() if entry[-1] in _SAME_WIRE_FORM
}

_PSEUDO_TYPES = frozenset(entry[-1] for entry in TYPES.values() if entry[3] == "p")

# A function returning a pseudo-type (record, void, anyelement, ...) is left to its expression.
_FUNCTION_TYPES = {
    name: returns for name, returns in FUNCTION_RETURNS.items() if returns not in _PSEUDO_TYPES
}


@functools.cache
def _schema() -> MappingSchema:
    nested: dict[str, object] = {
        schema: {
            relation: {name: pg_type for name, pg_type, _ in columns}
            for (in_schema, relation), columns in RELATIONS.items()
            if in_schema == schema
        }
        for schema, _ in RELATIONS
    }
    return MappingSchema(nested, dialect="postgres")


@functools.lru_cache(maxsize=4096)
def result_types(sql: str) -> tuple[tuple[str, str], ...]:
    """Each result column's name and PostgreSQL type, as PostgreSQL's format_type names it (an
    unknown type is the empty string). An unqualified relation is pg_catalog's, first on PostgreSQL's
    search path."""
    tree = sqlglot.parse_one(sql, read="postgres")
    qualified = qualify(
        tree,
        db="pg_catalog",
        schema=_schema(),
        dialect="postgres",
        validate_qualify_columns=False,
        quote_identifiers=False,
    )
    annotated = annotate_types(qualified, schema=_schema(), dialect="postgres")
    if not isinstance(annotated, exp.Query):
        return ()
    return tuple(
        (select.alias_or_name, _function_type(select.unalias()) or _format_type(select.type))
        for select in annotated.selects
    )


def _function_type(expression: exp.Expr) -> str:
    """The type PostgreSQL 17 says a pg_catalog function call returns; empty when the expression
    is not such a call."""
    if isinstance(expression, exp.Dot):
        expression = expression.expression
    if isinstance(expression, exp.Anonymous):
        name = expression.name.lower()
    elif isinstance(expression, exp.Func):
        name = type(expression).sql_names()[0].lower()
    else:
        return ""
    return _FUNCTION_TYPES.get(name, "")


def _format_type(data_type) -> str:
    if data_type is None or data_type.is_type("unknown"):
        return ""
    return data_type.sql("postgres").lower()
