# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""AST-based transform invariants for the optimization/planning transforms (REQ-913).

Locks the no-regex, structure-preserving invariant for the hot-table VALUES-CTE inlining
transform (``rewrite_hot_joins`` / ``build_values_cte_sql``), which previously spliced SQL
text with regexes. A structural transform must not regress to text: it derives the JOIN
scope, alias binding, and WITH injection point from the parsed tree, never from the string.
"""

import sqlglot

from provisa.cache.hot_tables import HotTableEntry, HotTableManager, build_values_cte_sql
from provisa.compiler.sql_gen import CompiledQuery, rewrite_hot_joins


def _mgr(entries: dict[str, HotTableEntry]) -> HotTableManager:
    mgr = HotTableManager(redis_url=None, auto_threshold=1000, max_rows=1000)
    mgr._hot_tables = entries
    return mgr


def _entry(name: str, rows: list[dict], cols: list[str]) -> HotTableEntry:
    return HotTableEntry(
        table_name=name,
        catalog="pg",
        schema="public",
        pk_column="id",
        rows=rows,
        column_names=cols,
    )


def _compiled(sql: str) -> CompiledQuery:
    return CompiledQuery(sql=sql, params=[], root_field="orders", columns=[], sources={"pg"})


def test_hot_cte_output_is_parseable_and_structure_preserved():
    """The rewrite yields a valid tree with the CTE attached and the query body intact."""
    mgr = _mgr({"countries": _entry("countries", [{"id": 1, "code": "US"}], ["id", "code"])})
    sql = (
        'SELECT "t0"."order_id", "t1"."code" FROM "public"."orders" "t0" '
        'LEFT JOIN "public"."countries" "t1" ON "t0"."country_id" = "t1"."id" '
        'WHERE "t0"."order_id" > 10'
    )
    out = rewrite_hot_joins(_compiled(sql), mgr).sql
    tree = sqlglot.parse_one(out, read="postgres")  # parses → structurally valid, not text soup
    # The CTE is a real WITH node on the AST (injection point derived structurally).
    assert tree.args.get("with_") is not None
    cte_names = {c.alias for c in tree.args["with_"].expressions}
    assert "_hot_countries" in cte_names
    # The WHERE survives the rewrite unchanged (structure preserved).
    assert '"t0"."order_id" > 10' in out
    # The physical table reference is gone; the CTE relation took its place.
    assert '"public"."countries"' not in out


def test_hot_inlining_does_not_touch_a_string_literal_matching_the_table_name():
    """A projected string literal equal to the hot table name must never be rewritten.

    A text/regex transform matching the bare token would corrupt the literal; the AST
    transform only rewrites genuine Table nodes.
    """
    mgr = _mgr({"customers": _entry("customers", [{"id": 1}], ["id"])})
    # 'customers' is a STRING LITERAL here; the only real table is orders (not hot).
    sql = 'SELECT \'customers\' AS "label", "id" FROM "public"."orders"'
    out = rewrite_hot_joins(_compiled(sql), mgr).sql
    # No hot Table node present → SQL unchanged, literal intact.
    assert out == sql
    assert "'customers'" in out
    assert "_hot_customers" not in out


def test_build_values_cte_merges_into_existing_with_via_ast():
    """A query that already carries a CTE gets the hot CTE prepended structurally.

    The WITH injection point is chosen on the tree, not by a regex over a leading 'WITH'.
    """
    entry = _entry("countries", [{"id": 1, "code": "US"}], ["id", "code"])
    sql = (
        'WITH "pre" AS (SELECT 1 AS "x") '
        'SELECT "t1"."code" FROM "pre" "p" '
        'LEFT JOIN "public"."countries" "t1" ON "p"."x" = "t1"."id"'
    )
    out = build_values_cte_sql(sql, "countries", entry)
    tree = sqlglot.parse_one(out, read="postgres")
    cte_names = [c.alias for c in tree.args["with_"].expressions]
    # Both CTEs present; the original CTE is retained (no clobber).
    assert "pre" in cte_names
    assert "_hot_countries" in cte_names
