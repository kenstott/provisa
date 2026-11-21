# Copyright (c) 2026 Kenneth Stott
# Canary: 4a9c2e7d-6f1b-4d8a-a3e5-9b7c0d2f6a18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The VALUES-CTE rewrite the hot-table cache and the compiler share (REQ-232, REQ-233, REQ-913).

Split out of :mod:`provisa.cache.hot_tables` (REQ-1678) because the compiler needs only this
pure rewrite, and the manager module pulls the file-source and DuckDB connector stack with it.
"""

# Requirements: REQ-232, REQ-233, REQ-913, REQ-1678

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

from provisa.compiler.sql_literals import literal_expr


class HotRows(Protocol):
    """What the rewrite needs from a hot-table entry; ``HotTableEntry`` satisfies it structurally,
    so the compiler and this module never import the cache manager (REQ-1678)."""

    @property
    def column_names(self) -> list[str]: ...

    @property
    def rows(self) -> list[dict]: ...


@dataclass(frozen=True)
class InlineRows:
    """Rows fetched for one statement and substituted into it alone — never held by the hot
    tier (a parameterized fetch, or rows above the hot threshold's reach)."""

    rows: list[dict]
    column_names: list[str]
    #: Each column's IR type, where the read declares it (a registered table's live read). A NULL
    #: in such a column is cast to it: an untyped NULL is the engine's NULL type, and a statement
    #: built over the rows -- a view's store table, created from its probe -- takes that type
    #: (text on DuckDB 2.0, INTEGER on 1.4).
    column_types: Mapping[str, str] = field(default_factory=dict)


def _typed_null(ir_type: str) -> Any:  # noqa: ANN401 — a sqlglot expression
    """A NULL of the IR type ``ir_type``, as a PostgreSQL expression."""
    import sqlglot.expressions as exp

    from provisa.core.ir_types import to_physical

    return exp.cast(exp.Null(), to_physical(ir_type, "postgresql"))


def _cte_literal(value: Any, ir_type: str | None = None) -> Any:  # noqa: ANN401 — any value a hot row holds
    """A hot row's value as a PostgreSQL literal expression. A float is typed DOUBLE PRECISION: a
    bare ``2.0`` is a numeric literal, which the engine reads as a DECIMAL, and a DECIMAL is
    written as the string of its digits (REQ-1867) -- a float column would answer as text. A NULL
    of a column whose IR type is given is cast to it (:attr:`InlineRows.column_types`)."""
    import sqlglot.expressions as exp

    if value is None and ir_type is not None:
        return _typed_null(ir_type)
    literal = literal_expr(value)
    if isinstance(value, float):
        return exp.cast(literal, "DOUBLE PRECISION")
    return literal


def _values_cte(cte_name: str, entry: HotRows) -> Any:  # noqa: ANN401 — a sqlglot CTE
    """The CTE ``"<cte_name>"("<column>", ...) AS (VALUES ...)`` holding ``entry``'s rows, built
    as an expression (REQ-913); with no rows, one that reads none."""
    import sqlglot.expressions as exp

    columns = [exp.to_identifier(c, quoted=True) for c in entry.column_names]
    types = entry.column_types if isinstance(entry, InlineRows) else {}
    if entry.rows:
        values = exp.Values(
            expressions=[
                exp.Tuple(
                    expressions=[_cte_literal(row.get(c), types.get(c)) for c in entry.column_names]
                )
                for row in entry.rows
            ],
            alias=exp.TableAlias(this=exp.to_identifier("_values")),
        )
        # The form the engines have been given: rows read from a derived VALUES table.
        body: exp.Expression = exp.select("*").from_(values)
    else:
        body = exp.select(
            *(_typed_null(types[c]) if c in types else exp.Null() for c in entry.column_names)
        ).where(exp.EQ(this=exp.Literal.number(1), expression=exp.Literal.number(0)))
    return exp.CTE(
        this=body,
        alias=exp.TableAlias(this=exp.to_identifier(cte_name, quoted=True), columns=columns),
    )


def build_values_cte_sql(
    sql: str, table_name: str, entry: HotRows, alias_name: str | None = None
) -> str:  # REQ-232, REQ-233
    """Replace the first table reference matching table_name with a VALUES CTE.

    Works for both FROM and JOIN targets. Merges with any existing WITH clause.

    ``alias_name`` is the name column qualifiers in the surrounding query actually use
    (e.g. the table's registered alias) when it differs from ``table_name`` — callers whose
    upstream semantic-to-physical rewrite already dropped the alias off an unaliased ref
    must pass it through so the fallback alias below matches, not the (now-wrong) physical
    name.

    REQ-233: hot rows are injected verbatim into the CTE on purpose. Column governance
    (RLS / masking / visibility) is applied by Stage 2 (`apply_governance`) to the governed
    SQL that wraps this CTE — the pipeline order is governance → cache/CTE → route — so
    governance filters and masks the CTE rows exactly as it would the live table. Storing
    governed-per-role copies in Redis would be both redundant and a leak risk, so the cache
    holds the raw rows and governance stays at query time.

    REQ-913: structural, AST-only. The table reference is renamed and the CTE, built as an
    expression with its rows as literal nodes, is attached to the query's ``WITH`` node on the
    parsed tree — nothing about it is written as text and read back.
    """
    return build_values_ctes_sql(sql, {table_name: entry}, {table_name: alias_name})


def build_values_ctes_sql(
    sql: str, entries: dict[str, HotRows], alias_names: dict[str, str | None] | None = None
) -> str:  # REQ-232, REQ-233
    """:func:`build_values_cte_sql` for every table of ``entries`` at once: the statement is
    parsed once and written once, whatever the number of tables -- each table rewritten on its
    own re-parsed and re-wrote the whole statement, which for a statement over every label
    (a Cypher ``MATCH (n)``, 216 KB) was most of a request's time."""
    import sqlglot

    if not any(e.column_names for e in entries.values()):
        return sql
    try:
        # REQ-913: the statement reaches this stage as text from the one before it (the stages
        # exchange SQL text); it is read once here and written once below.
        tree = sqlglot.parse_one(sql, dialect="postgres")
    except Exception as exc:
        # A regex fallback can rewrite the wrong occurrence — fail loud on parse error.
        raise ValueError(
            "Failed to parse SQL for hot-table CTE rewrite of "
            f"{sorted(tn for tn, e in entries.items() if e.column_names)!r}"
        ) from exc
    return with_values_ctes(tree, entries, alias_names).sql(dialect="postgres")


def with_values_ctes(
    tree: Any,  # noqa: ANN401 — a sqlglot statement
    entries: dict[str, HotRows],
    alias_names: dict[str, str | None] | None = None,
) -> Any:  # noqa: ANN401
    """``tree``, rewritten in place, with each table of ``entries`` read from its VALUES CTE
    (:func:`build_values_ctes_sql`, on a statement already a tree)."""
    import sqlglot.expressions as exp

    rewritten = {tn: e for tn, e in entries.items() if e.column_names}

    for table_name, entry in rewritten.items():
        cte_name = f"_hot_{table_name}"
        alias_name = (alias_names or {}).get(table_name)

        for tbl in tree.find_all(exp.Table):
            if tbl.name == table_name:
                # Preserve the original table name as an alias when the ref is
                # unaliased, so column qualifiers (e.g. shelter__animalBreeds.name)
                # still resolve after the relation is renamed to the CTE.
                if not tbl.alias:
                    tbl.set(
                        "alias", exp.TableAlias(this=exp.to_identifier(alias_name or table_name))
                    )
                tbl.set("catalog", None)
                tbl.set("db", None)
                tbl.set("this", exp.to_identifier(cte_name, quoted=True))

        # The CTE is built as an expression and attached to the query's WITH: the injection
        # point is chosen structurally, never by matching a leading "WITH" in the query text.
        cte_node = _values_cte(cte_name, entry)
        existing_with = tree.args.get("with_")
        if existing_with is not None:
            existing_with.set("expressions", [cte_node, *existing_with.expressions])
        else:
            tree.set("with_", exp.With(expressions=[cte_node]))
    return tree
