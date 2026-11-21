# Copyright (c) 2026 Kenneth Stott
# Canary: 2a7f9c04-e5b1-4d68-8c13-6f0b4d9e7a25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A registered table's definition in its own source, read as a view for lineage (REQ-2230).

A Snowflake Dynamic Table is a table defined by a query that Snowflake runs. Its definition speaks
Snowflake's names: unquoted identifiers folded to upper case, tables by schema and database. The
lineage graph speaks registered names: ``<domain>.<table>`` and each column's SQL name. So the
definition's query is rewritten, on its AST, into the second form and joins the graph as a view
over the tables it reads.

A table it reads is registered only on the same source, in the same schema and database. Any other
keeps the name the definition gives it and is a box outside the model. A definition that will not
parse contributes nothing, as a view that will not parse has no sub-DAG.
"""

# Requirements: REQ-2230

from __future__ import annotations

from dataclasses import dataclass, field

import sqlglot
import sqlglot.errors
import sqlglot.expressions as exp
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from provisa.compiler.naming import domain_to_sql_name
from provisa.lineage.native import sql_column_name

_SNOWFLAKE = "snowflake"
_POSTGRES = "postgres"


@dataclass
class _Table:
    source_id: str
    database: str
    domain_id: str
    schema_name: str
    table_name: str
    definition: str | None
    #: Each column's physical name to the name lineage reads it by.
    columns: dict[str, str] = field(default_factory=dict)


def _tables(rows: list[dict]) -> dict[tuple[str, str, str], _Table]:
    tables: dict[tuple[str, str, str], _Table] = {}
    for row in rows:
        key = (row["source_id"], row["schema_name"], row["table_name"])
        table = tables.setdefault(
            key,
            _Table(
                source_id=row["source_id"],
                database=row["database"],
                domain_id=row["domain_id"],
                schema_name=row["schema_name"],
                table_name=row["table_name"],
                definition=row["source_definition"],
            ),
        )
        table.columns[row["column_name"]] = sql_column_name(row)
    return tables


def _query(definition: str) -> exp.Query | None:
    """The query a CREATE statement defines its table by, identifiers folded as Snowflake folds
    them; None when the statement will not parse or defines none."""
    try:
        tree = sqlglot.parse_one(definition, read=_SNOWFLAKE)
    except sqlglot.errors.SqlglotError:
        return None
    query = tree.expression if isinstance(tree, exp.Create) else None
    if not isinstance(query, exp.Query):
        return None
    normalize_identifiers(query, dialect=_SNOWFLAKE)
    return query


def _quoted(name: str) -> exp.Identifier:
    return exp.to_identifier(name, quoted=True)


def _name_registered_tables(
    query: exp.Query,
    table: _Table,
    tables: dict[tuple[str, str, str], _Table],
) -> dict[str, dict[str, str]]:
    """Rewrite each table ``query`` reads that is registered to its relation; returns the columns
    of those tables by the name the query calls each one."""
    common = {cte.alias for cte in query.find_all(exp.CTE)}
    read: dict[str, dict[str, str]] = {}
    for reference in query.find_all(exp.Table):
        if not reference.db and reference.name in common:
            continue
        if reference.catalog and reference.catalog != table.database:
            continue
        found = tables.get((table.source_id, reference.db or table.schema_name, reference.name))
        if found is None:
            continue
        read[reference.alias_or_name] = found.columns
        reference.set("this", _quoted(found.table_name))
        reference.set("db", _quoted(domain_to_sql_name(found.domain_id)))
        reference.set("catalog", None)
    return read


def _name_columns(query: exp.Query, read: dict[str, dict[str, str]]) -> None:
    """Rewrite each column reference to the name lineage reads it by. A qualifier that is no table
    read here (a common table expression's) is looked up in them all, as an unqualified column is."""
    for column in query.find_all(exp.Column):
        held = read.get(column.table)
        candidates = [held] if held is not None else list(read.values())
        named = next((c[column.name] for c in candidates if column.name in c), None)
        if named is not None:
            column.set("this", _quoted(named))


def _name_outputs(query: exp.Query, outputs: list[str], columns: dict[str, str]) -> None:
    """Alias each output to the column of the Dynamic Table it fills."""
    for projection, output in zip(query.selects, outputs, strict=True):
        named = columns.get(output)
        if named is not None:
            projection.replace(exp.alias_(projection.unalias().copy(), _quoted(named)))


def _as_view(table: _Table, tables: dict[tuple[str, str, str], _Table]) -> str | None:
    query = _query(table.definition or "")
    if query is None:
        return None
    outputs = [projection.alias_or_name for projection in query.selects]
    _name_columns(query, _name_registered_tables(query, table, tables))
    _name_outputs(query, outputs, table.columns)
    return query.sql(dialect=_POSTGRES)


def definition_views(rows: list[dict]) -> list[dict]:
    """Each table with a definition, as the view row lineage reads (``domain_id``, ``table_name``,
    ``view_sql``). ``rows`` hold one row per registered column of every table on those sources."""
    tables = _tables(rows)
    views = []
    for table in tables.values():
        if not table.definition:
            continue
        sql = _as_view(table, tables)
        if sql is not None:
            views.append(
                {"domain_id": table.domain_id, "table_name": table.table_name, "view_sql": sql}
            )
    return views
