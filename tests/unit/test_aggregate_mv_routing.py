# Copyright (c) 2026 Kenneth Stott
# Canary: b56f4ab6-ab27-4042-b53b-8728140b9315
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A governed aggregate is answered from a view that pre-aggregates its table (REQ-198, REQ-882).

The optimize stage sees the statement governed and lowered: its catalog-physical table names the
view's relation, and the role's row filters and masks are already in it. A filter on a column the
view keeps as a dimension is carried onto the view; one on any other column, or a mask over a
measured column, leaves the statement to its table -- the view was computed over every row.
"""

# Requirements: REQ-198, REQ-882

from __future__ import annotations

from provisa.compiler.sql_types import CompilationContext
from provisa.mv.models import MVDefinition, MVStatus
from provisa.mv.view_reinject import reinject

_VIEW_SQL = (
    'SELECT "region", COUNT(*) AS "n", SUM("amount") AS "total" '
    'FROM "sales_pg"."public"."orders" GROUP BY "region"'
)
_ORDERS = '"sales_pg"."public"."orders"'


def _rollup(sql: str = _VIEW_SQL) -> MVDefinition:
    return MVDefinition(
        id="orders_by_region",
        source_tables=["orders"],
        target_catalog="mat",
        target_schema="mv",
        target_table="orders_by_region",
        sql=sql,
        status=MVStatus.FRESH,
    )


def _answer(sql: str, view: MVDefinition | None = None):
    return reinject(sql, [view or _rollup()], CompilationContext())


def test_a_governed_aggregate_is_read_from_the_view_over_its_table():
    hit = _answer(f'SELECT "region", SUM("amount") FROM {_ORDERS} AS "orders" GROUP BY "region"')
    assert hit is not None and hit.match == "aggregate"
    assert '"mat"."mv"."orders_by_region"' in hit.sql and 'SUM("total")' in hit.sql


def test_a_row_filter_on_a_dimension_is_carried_onto_the_view():
    hit = _answer(f'SELECT SUM("amount") FROM {_ORDERS} WHERE ("region" = $1)')
    assert hit is not None and '"region" = $1' in hit.sql


def test_a_row_filter_or_a_mask_on_anything_else_leaves_the_table():
    assert _answer(f'SELECT SUM("amount") FROM {_ORDERS} WHERE ("owner" = $1)') is None
    masked = f'SELECT SUM(CASE WHEN TRUE THEN 0 ELSE "amount" END) FROM {_ORDERS}'
    assert _answer(masked) is None


def test_a_view_over_another_catalogs_table_of_the_same_name_does_not_answer():
    other = _rollup(_VIEW_SQL.replace('"sales_pg"', '"crm_pg"'))
    assert _answer(f'SELECT SUM("amount") FROM {_ORDERS}', other) is None


def test_a_view_never_answers_its_own_build():
    """Its build is its own definition: answered by the view, it would read back what the view
    held, and an empty first build would keep it empty (the quality demo's gold, REQ-2051)."""
    from provisa.mv.view_reinject import building

    assert _answer(_VIEW_SQL) is not None
    with building("orders_by_region"):
        assert _answer(_VIEW_SQL) is None
        # Another view's build is answered as any statement is.
        with building("another_view"):
            assert _answer(_VIEW_SQL) is not None
    assert _answer(_VIEW_SQL) is not None
