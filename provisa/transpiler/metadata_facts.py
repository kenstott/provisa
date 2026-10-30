# Copyright (c) 2026 Kenneth Stott
# Canary: 7b42c9e5-1d38-4f06-a3c7-e08d52b6f193
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a statement about the model needs that only a model build knows (REQ-2190), as relations
the statement carries: the name each table and column is queried by (the compiler's naming, which
is not SQL), each column's nullability and whether its relation holds it (read from the source
when the model is built; the store keeps neither), and what the role holds on each table (its
grants and what each source takes).

They are computed here from what the build holds, so there is one source; a statement sees only
the rows of tables its role may list. A column whose facts are the defaults has no row: named
``COALESCE(alias, column_name)``, nullable, in its relation."""

# Requirements: REQ-2190

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import sqlglot.expressions as exp

from provisa.transpiler.metadata_oids import schema_oid, table_oid

TABLES = "_provisa_tables"
COLUMNS = "_provisa_columns"
PRIVILEGES = "_provisa_privileges"

_RELATIONS: Mapping[str, tuple[tuple[str, str], ...]] = {
    TABLES: (
        ("table_id", "INTEGER"),
        ("table_oid", "BIGINT"),
        ("schema_name", "TEXT"),
        ("schema_oid", "BIGINT"),
        ("table_name", "TEXT"),
        ("domain_id", "TEXT"),
    ),
    COLUMNS: (
        ("table_id", "INTEGER"),
        ("column_name", "TEXT"),
        ("sql_name", "TEXT"),
        ("not_null", "BOOLEAN"),
        ("outside_relation", "BOOLEAN"),
    ),
    PRIVILEGES: (("table_id", "INTEGER"), ("privilege", "TEXT")),
}


@dataclass(frozen=True)
class CatalogFacts:
    """A role's facts: rows of ``TABLES``, ``COLUMNS`` and ``PRIVILEGES``, and the role."""

    role_id: str
    tables: tuple[tuple[Any, ...], ...] = ()
    columns: tuple[tuple[Any, ...], ...] = ()
    privileges: tuple[tuple[Any, ...], ...] = ()

    def rows(self, relation: str) -> tuple[tuple[Any, ...], ...]:
        return {TABLES: self.tables, COLUMNS: self.columns, PRIVILEGES: self.privileges}[relation]


def table_sql_name(row: Mapping[str, Any], naming_rules: list[dict], domain_prefix: bool) -> str:
    """The name a statement queries ``row``, a raw table of the model, by: what
    schema_gen._assign_names and sql_rewrite.semantic_table_name give it, for a table any role is
    served or none is, since a role lists tables it is not served."""
    from provisa.compiler.naming import (
        active_gql_convention,
        apply_sql_name,
        domain_to_sql_name,
        generate_name,
    )

    alias, name = row.get("alias"), row["table_name"]
    prefix = f"{domain_to_sql_name(row['domain_id'])}__"
    base = name[len(prefix) :] if domain_prefix and not alias and name.startswith(prefix) else name
    return apply_sql_name(
        alias
        or generate_name(
            base,
            naming_rules,
            convention=row.get("gql_naming_convention")
            or row.get("source_gql_naming_convention")
            or active_gql_convention(),
        )
    )


def _column_rows(
    table: Mapping[str, Any],
    types: Iterable[Any],
    outside: Collection[tuple[int, str]],
) -> Iterable[tuple[Any, ...]]:
    from provisa.compiler.naming import apply_sql_name

    required = {t.column_name for t in types if not t.is_nullable}
    for column in table.get("columns", []):
        name, alias = column["column_name"], column.get("alias")
        sql_name = alias or apply_sql_name(name)
        not_null = name in required
        away = (table["id"], name) in outside
        if sql_name != (alias or name) or not_null or away:
            yield (table["id"], name, sql_name, not_null, away)


def role_privileges(
    table_ids: Iterable[int],
    write_grants: Mapping[int, Mapping[str, Any]],
    write_ops: Mapping[int, Collection[str]],
    can_write: bool,
) -> dict[int, frozenset[str]]:
    """What a role holds on the tables it is served, ``table_ids``: SELECT on each, and an
    operation its source takes where the role holds the write right and the table carries its
    grant (compiler/write_admission's rule, read from its governance context)."""
    return {
        table_id: frozenset(
            {
                "SELECT",
                *(
                    operation.upper()
                    for operation in write_grants.get(table_id, ())
                    if can_write and operation in write_ops.get(table_id, ())
                ),
            }
        )
        for table_id in table_ids
    }


def catalog_facts(
    role_id: str,
    raw_tables: Iterable[Mapping[str, Any]],
    column_types: Mapping[int, Iterable[Any]],
    outside_relation: Collection[tuple[int, str]],
    privileges: Mapping[int, Collection[str]],
    *,
    listed: Collection[int] | None,
    naming_rules: list[dict],
    domain_prefix: bool,
) -> CatalogFacts:
    """``role_id``'s facts, from the model's raw tables and what its build read. ``listed`` is the
    table ids the role may list (rights.compute_meta_row_scope; None is all of them)."""
    from provisa.compiler.naming import domain_to_sql_name

    tables: list[tuple[Any, ...]] = []
    columns: list[tuple[Any, ...]] = []
    for table in raw_tables:
        table_id = table["id"]
        if listed is not None and table_id not in listed:
            continue
        domain = table["domain_id"]
        schema = domain_to_sql_name(domain or table["schema_name"] or "public")
        tables.append(
            (
                table_id,
                table_oid(table_id),
                schema,
                schema_oid(schema),
                table_sql_name(table, naming_rules, domain_prefix),
                domain,
            )
        )
        columns.extend(_column_rows(table, column_types.get(table_id, ()), outside_relation))
    return CatalogFacts(
        role_id=role_id,
        tables=tuple(tables),
        columns=tuple(columns),
        privileges=tuple(
            (table_id, privilege)
            for table_id, held in sorted(privileges.items())
            for privilege in sorted(held)
        ),
    )


def _literal(value: Any) -> exp.Expr:
    if value is None:
        return exp.null()
    if isinstance(value, bool):
        return exp.true() if value else exp.false()
    if isinstance(value, int):
        return exp.Literal.number(value)
    return exp.Literal.string(str(value))


def relation(name: str, rows: tuple[tuple[Any, ...], ...]) -> exp.Select:
    """The select a statement's CTE ``name`` holds: ``rows``, each column of its type; none where
    there are no rows."""
    columns = _RELATIONS[name]
    typed = [
        exp.alias_(exp.cast(exp.column(column), sql_type), column) for column, sql_type in columns
    ]
    if not rows:
        return exp.select(
            *(exp.alias_(exp.cast(exp.null(), sql_type), column) for column, sql_type in columns)
        ).where(exp.false())
    values = exp.Values(
        expressions=[exp.Tuple(expressions=[_literal(v) for v in row]) for row in rows],
        alias=exp.TableAlias(
            this=exp.to_identifier("_v"),
            columns=[exp.to_identifier(column) for column, _ in columns],
        ),
    )
    return exp.select(*typed).from_(values)
