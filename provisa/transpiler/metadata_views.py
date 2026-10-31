# Copyright (c) 2026 Kenneth Stott
# Canary: e6f3108c-2a97-4d51-b8c0-74a9d1e25b3f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The names of pg_catalog and information_schema in the meta domain (REQ-2190), and how each
view's rows are scoped: what a statement's routing and a role's rights read of the views, apart
from the views themselves (provisa.api._pg_catalog_views)."""

# Requirements: REQ-2190

from __future__ import annotations

from provisa.pgwire.catalog_pg17 import RELATIONS
from provisa.transpiler.metadata_oids import owner_table_sql

_PG = "pg_catalog"
#: PostgreSQL's oid of pg_class, which pg_description names a relation's rows by.
PG_CLASS_OID = 1259
#: The column an information_schema relation's rows are scoped by: it has no oid to read.
SCOPE_COLUMN = "provisa_table_id"


def catalog_view_name(schema: str, name: str) -> str:
    """The meta table that is ``schema.name``, which is all a client's name needs. Each has a
    prefix: an information_schema name is meta's too (domains, roles), and a view named as a
    pg_catalog relation would be that relation to anything naming it unqualified, the system
    catalog coming first on every search path."""
    return f"{'catalog' if schema == _PG else 'is'}_{name}"


CATALOG_VIEW_NAMES: tuple[str, ...] = tuple(
    catalog_view_name(schema, name) for schema, name in sorted(RELATIONS)
)
#: Registered, and read by the forms, beside the relations themselves.
NEUTRAL_VIEW_NAMES: tuple[str, ...] = ("table_details", "column_details", "table_keys")

#: The views of schemas: every role reads meta's domains, whose rows are not scoped.
LISTS_EVERY_SCHEMA: frozenset[str] = frozenset({"catalog_pg_namespace", "is_schemata"})

#: The relations carrying :data:`SCOPE_COLUMN`.
SCOPED_BY_COLUMN: frozenset[str] = frozenset(
    {
        "catalog_pg_tables",
        "catalog_pg_indexes",
        "is_tables",
        "is_columns",
        "is_table_constraints",
        "is_key_column_usage",
        "is_referential_constraints",
        "is_constraint_table_usage",
        "is_constraint_column_usage",
        "is_check_constraints",
    }
)

#: Every view of rows about a registered table, and the expressions over its columns naming the
#: tables a row is about, all of which a role must list to read it. An expression is NULL for a
#: row about no registered table (a system relation, a metric), which every role reads: this is
#: what lets a catalog list itself under a scope that knows only table ids.
CATALOG_ROW_SCOPES: dict[str, tuple[str, ...]] = {
    **{name: (SCOPE_COLUMN,) for name in SCOPED_BY_COLUMN},
    "catalog_pg_class": (owner_table_sql("oid"),),
    "catalog_pg_attribute": (owner_table_sql("attrelid"),),
    "catalog_pg_attrdef": (owner_table_sql("adrelid"),),
    "catalog_pg_index": (owner_table_sql("indrelid"),),
    "catalog_pg_constraint": (
        owner_table_sql("conrelid"),
        f"CASE WHEN confrelid = 0 THEN NULL ELSE {owner_table_sql('confrelid')} END",
    ),
    "catalog_pg_description": (
        f"CASE WHEN classoid = {PG_CLASS_OID} THEN {owner_table_sql('objoid')} END",
    ),
}


def column_descriptions() -> dict[str, dict[str, str]]:
    """What each view's columns are: PostgreSQL's own, the documentation being PostgreSQL's."""
    described = {}
    for (schema, name), columns in sorted(RELATIONS.items()):
        view = catalog_view_name(schema, name)
        described[view] = {
            column: f"{schema}.{name}.{column}, as PostgreSQL 17 defines it"
            for column, _, _ in columns
        }
        if view in SCOPED_BY_COLUMN:
            described[view][SCOPE_COLUMN] = "The registered table this row is about"
    return described


def table_descriptions(own: str) -> dict[str, str]:
    """What each view is, ending as every built-in table's description does (``own``)."""
    return {
        catalog_view_name(schema, name): f"PostgreSQL's {schema}.{name}, of this model{own}"
        for schema, name in sorted(RELATIONS)
    }
