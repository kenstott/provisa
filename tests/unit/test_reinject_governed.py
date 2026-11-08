# Copyright (c) 2026 Kenneth Stott
# Canary: 5d2e8a71-0c4b-4f93-a6e8-9b17c3f40d52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A materialized view still answers a read whose tables stand in it as governed relations
(REQ-039, "ONE CHANGE": no read loses its materialized view).

Each statement here is governed by stage 2 (``apply_governance``) and lowered to catalog-physical,
as the optimize stage sees it: every registered table is a derived table applying the role's row
rule and masks. The view answers it, and what the view answers is what the governed statement
answers over its tables -- the row rule and the masks included -- checked on DuckDB.
"""

# Requirements: REQ-039, REQ-849, REQ-882

from __future__ import annotations

import duckdb
import pytest

from provisa.compiler.sql_rewrite import lower_to_catalog_physical
from provisa.compiler.sql_types import CompilationContext, TableMeta
from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.mv.models import JoinPattern, MVDefinition, MVStatus, TableIdentity
from provisa.mv.view_reinject import reinject
from provisa.security.masking import MaskingRule, MaskType

_AGGREGATE_SQL = (
    'SELECT "region", COUNT(*) AS "n", SUM("amount") AS "total" '
    'FROM "pg"."public"."orders" GROUP BY "region"'
)


def _ctx() -> CompilationContext:
    ctx = CompilationContext()
    for i, name in enumerate(("orders", "customers")):
        ctx.tables[name] = TableMeta(
            table_id=i,
            field_name=name,
            type_name=name.title(),
            source_id="sales-pg",
            catalog_name="pg",
            schema_name="public",
            table_name=name,
            domain_id="sales",
        )
    return ctx


def _gov(
    rules: dict[int, str] | None = None,
    masks: dict[tuple[int, str], MaskingRule] | None = None,
) -> GovernanceContext:
    gov = GovernanceContext()
    gov.table_map = {"public.orders": 0, "orders": 0, "public.customers": 1, "customers": 1}
    gov.all_columns = {
        0: [
            ("id", "integer"),
            ("customer_id", "integer"),
            ("amount", "integer"),
            ("region", "varchar"),
        ],  # fmt: skip
        1: [("id", "integer"), ("name", "varchar"), ("tier", "varchar")],
    }
    gov.rls_rules = dict(rules or {})
    gov.masking_rules = {key: (rule, "varchar") for key, rule in (masks or {}).items()}
    return gov


def _governed(sql: str, gov: GovernanceContext) -> str:
    """``sql`` as the optimize stage reads it: governed, then lowered to catalog-physical."""
    governed = lower_to_catalog_physical(apply_governance(sql, gov), _ctx())
    assert "FROM (SELECT" in governed  # every table stands as its governed relation
    return governed


def _join_view() -> MVDefinition:
    return MVDefinition(
        id="orders_customers",
        source_tables=["orders", "customers"],
        inputs=[
            TableIdentity("sales-pg", "public", "orders"),
            TableIdentity("sales-pg", "public", "customers"),
        ],
        target_catalog="mat",
        target_schema="mv",
        target_table="orders_customers",
        join_pattern=JoinPattern(
            left_table="orders",
            left_column="customer_id",
            right_table="customers",
            right_column="id",
            join_type="left",
        ),
        status=MVStatus.FRESH,
    )


def _aggregate_view() -> MVDefinition:
    return MVDefinition(
        id="orders_by_region",
        source_tables=["orders"],
        target_catalog="mat",
        target_schema="mv",
        target_table="orders_by_region",
        sql=_AGGREGATE_SQL,
        status=MVStatus.FRESH,
    )


@pytest.fixture
def con():
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS pg")
    con.execute("CREATE SCHEMA pg.public")
    con.execute(
        "CREATE TABLE pg.public.orders (id INT, customer_id INT, amount INT, region VARCHAR)"
    )
    con.execute("CREATE TABLE pg.public.customers (id INT, name VARCHAR, tier VARCHAR)")
    con.execute(
        "INSERT INTO pg.public.orders VALUES (1, 10, 5, 'us'), (2, 10, 7, 'eu'), "
        "(3, 11, 9, 'us'), (4, 99, 1, 'eu'), (5, 11, 3, 'us')"
    )
    con.execute("INSERT INTO pg.public.customers VALUES (10, 'ann', 'gold'), (11, 'bo', 'silver')")
    con.execute("ATTACH ':memory:' AS mat")
    con.execute("CREATE SCHEMA mat.mv")
    con.execute(
        "CREATE TABLE mat.mv.orders_customers AS SELECT orders.*, customers.id AS customers__id, "
        "customers.name AS customers__name, customers.tier AS customers__tier "
        "FROM pg.public.orders AS orders LEFT JOIN pg.public.customers AS customers "
        "ON orders.customer_id = customers.id"
    )
    con.execute(f"CREATE TABLE mat.mv.orders_by_region AS {_AGGREGATE_SQL}")
    return con


def _same(con, governed: str, answered: str) -> list[tuple]:
    want = sorted(con.execute(governed).fetchall(), key=repr)
    assert sorted(con.execute(answered).fetchall(), key=repr) == want
    return want


def test_a_governed_read_of_a_view_covered_table_is_answered_by_the_view(con):
    governed = _governed(
        "SELECT region, SUM(amount) AS total FROM public.orders GROUP BY region", _gov()
    )
    hit = reinject(governed, [_aggregate_view()], _ctx())
    assert hit is not None and hit.match == "aggregate"
    assert '"mat"."mv"."orders_by_region"' in hit.sql and "pg" not in hit.sql
    assert _same(con, governed, hit.sql) == [("eu", 8), ("us", 17)]


def test_a_governed_join_is_answered_by_the_view(con):
    governed = _governed(
        "SELECT o.id, c.name FROM public.orders o "
        "LEFT JOIN public.customers c ON o.customer_id = c.id",
        _gov(),
    )
    hit = reinject(governed, [_join_view()], _ctx())
    assert hit is not None and hit.match == "full"
    assert '"mat"."mv"."orders_customers"' in hit.sql and "JOIN" not in hit.sql
    assert len(_same(con, governed, hit.sql)) == 5


def test_the_views_answer_keeps_the_row_rule_and_the_masks(con):
    """The row rule on orders and the mask on customers.name are the relation's; read from the
    view they are carried onto its columns, and a row the join extends with NULLs still reads
    NULL where the mask would give the matched row a value."""
    gov = _gov(
        rules={0: "amount > 4"},
        masks={(1, "name"): MaskingRule(mask_type=MaskType.constant, value="***")},
    )
    governed = _governed(
        "SELECT o.id, c.name FROM public.orders o "
        "LEFT JOIN public.customers c ON o.customer_id = c.id",
        gov,
    )
    hit = reinject(governed, [_join_view()], _ctx())
    assert hit is not None and hit.match == "full"
    assert '"o"."amount" > 4' in hit.sql and "'***'" in hit.sql
    assert _same(con, governed, hit.sql) == [(1, "***"), (2, "***"), (3, "***")]


def test_a_row_rule_on_a_dimension_is_carried_onto_an_aggregate_view(con):
    governed = _governed(
        "SELECT region, SUM(amount) AS total FROM public.orders GROUP BY region",
        _gov(rules={0: "region = 'us'"}),
    )
    hit = reinject(governed, [_aggregate_view()], _ctx())
    assert hit is not None and hit.match == "aggregate"
    assert _same(con, governed, hit.sql) == [("us", 17)]


def test_a_row_rule_the_view_cannot_carry_leaves_the_statement_to_its_tables():
    """The aggregate view summed every row: a rule on a column it does not keep cannot be
    applied to it."""
    governed = _governed(
        "SELECT region, SUM(amount) AS total FROM public.orders GROUP BY region",
        _gov(rules={0: "amount > 4"}),
    )
    assert reinject(governed, [_aggregate_view()], _ctx()) is None


def test_a_rule_on_the_left_joined_table_leaves_the_join_to_its_tables():
    """The join view holds every customer row: a rule restricting which customers join cannot be
    carried onto it."""
    governed = _governed(
        "SELECT o.id, c.name FROM public.orders o "
        "LEFT JOIN public.customers c ON o.customer_id = c.id",
        _gov(rules={1: "tier = 'gold'"}),
    )
    assert reinject(governed, [_join_view()], _ctx()) is None
