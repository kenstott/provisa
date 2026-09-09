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

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Protocol


class HotRows(Protocol):
    """What the rewrite needs from a hot-table entry; ``HotTableEntry`` satisfies it structurally,
    so the compiler and this module never import the cache manager (REQ-1678)."""

    @property
    def column_names(self) -> list[str]: ...

    @property
    def rows(self) -> list[dict]: ...


def _sql_literal(val) -> str:
    """Render a Python value as a SQL literal (the engine-compatible)."""
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    if isinstance(val, (int, float, Decimal)):
        return str(val)
    if isinstance(val, datetime):
        return f"TIMESTAMP '{val.isoformat()}'"
    if isinstance(val, date):
        return f"DATE '{val}'"
    if isinstance(val, (dict, list)):
        escaped = json.dumps(val).replace("'", "''")
        return f"'{escaped}'"
    escaped = str(val).replace("'", "''")
    return f"'{escaped}'"


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

    REQ-913: structural, AST-only. The table reference is renamed and the CTE is attached
    to the query's ``WITH`` node on the parsed tree — the injection point is never derived
    from SQL text via regex. The CTE definition is built as a self-contained fragment and
    parsed to an AST node (IR construction, not re-deriving the query's structure from text).
    """
    import sqlglot
    import sqlglot.expressions as exp

    if not entry.column_names:
        return sql

    cte_name = f"_hot_{table_name}"
    col_defs = ", ".join(f'"{c}"' for c in entry.column_names)

    if not entry.rows:
        empty_nulls = ", ".join("NULL" for _ in entry.column_names)
        cte_body = f"({col_defs}) AS (SELECT {empty_nulls} WHERE 1=0)"
    else:
        value_rows = [
            "(" + ", ".join(_sql_literal(row.get(c)) for c in entry.column_names) + ")"
            for row in entry.rows
        ]
        cte_body = f"({col_defs}) AS (VALUES {', '.join(value_rows)})"

    cte_sql = f'"{cte_name}"{cte_body}'

    try:
        tree = sqlglot.parse_one(sql, dialect="postgres")
    except Exception as exc:
        # A regex fallback can rewrite the wrong occurrence — fail loud on parse error.
        raise ValueError(
            f"Failed to parse SQL for hot-table CTE rewrite of {table_name!r}"
        ) from exc

    for tbl in tree.find_all(exp.Table):
        if tbl.name == table_name:
            # Preserve the original table name as an alias when the ref is
            # unaliased, so column qualifiers (e.g. shelter__animalBreeds.name)
            # still resolve after the relation is renamed to the CTE.
            if not tbl.alias:
                tbl.set("alias", exp.TableAlias(this=exp.to_identifier(alias_name or table_name)))
            tbl.set("catalog", None)
            tbl.set("db", None)
            tbl.set("this", exp.to_identifier(cte_name, quoted=True))

    # Parse the CTE definition (a self-contained fragment we constructed) into an AST node
    # and attach it to the query's WITH — the injection point is chosen structurally, never
    # by matching a leading "WITH" in the query text.
    cte_fragment = sqlglot.parse_one(f"WITH {cte_sql} SELECT 1", read="postgres")
    cte_node = cte_fragment.args["with_"].expressions[0]
    existing_with = tree.args.get("with_")
    if existing_with is not None:
        existing_with.set("expressions", [cte_node, *existing_with.expressions])
    else:
        tree.set("with_", exp.With(expressions=[cte_node]))
    return tree.sql(dialect="postgres")
