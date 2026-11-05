# Copyright (c) 2026 Kenneth Stott
# Canary: 3b0c6f0e-2a47-4f5e-9d1c-7e8a4b6f2c19
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A view never shows a role more than its governed read of the tables (REQ-882, REQ-849,
REQ-1586, REQ-039).

Each statement is governed for a role (``apply_governance``: its row rules and masks), then
handed to the optimize stage's rewrite (``reinject``) as the pipeline hands it. Where a view
answers, the statement is run both ways on DuckDB -- against the tables, and against the view --
and the two answers compared. Where none does, the statement is left to its tables.
"""

# Requirements: REQ-882, REQ-849, REQ-1586, REQ-039

from __future__ import annotations

import duckdb
import pytest

from provisa.compiler.sql_types import CompilationContext, TableMeta
from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.mv.models import JoinPattern, MVDefinition, MVStatus, TableIdentity
from provisa.mv.view_reinject import reinject
from provisa.pgwire._pipeline import _resolve_session_settings
from provisa.security.masking import MaskingRule, MaskType

_ORDERS = '"pg"."public"."orders"'
_CUSTOMERS = '"pg"."public"."customers"'

_ORDER_COLUMNS = [
    ("id", "integer"),
    ("customer_id", "integer"),
    ("region", "varchar"),
    ("amount", "integer"),
    ("flag", "integer"),
]
_CUSTOMER_COLUMNS = [
    ("id", "integer"),
    ("name", "varchar"),
    ("region", "varchar"),
    ("credit", "integer"),
]
_ORDERS_ID, _CUSTOMERS_ID = 1, 2


@pytest.fixture(scope="module")
def con():
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS pg")
    con.execute("CREATE SCHEMA pg.public")
    con.execute(
        "CREATE TABLE pg.public.orders "
        "(id INT, customer_id INT, region VARCHAR, amount INT, flag INT)"
    )
    con.execute(
        "CREATE TABLE pg.public.customers (id INT, name VARCHAR, region VARCHAR, credit INT)"
    )
    con.execute(
        "INSERT INTO pg.public.orders VALUES "
        "(1, 10, 'us', 5, 1), (2, 10, 'us', 7, 0), (3, 11, 'EU', 9, 1), (4, 12, NULL, 3, 1), "
        "(5, 12, NULL, 4, 0), (6, 99, 'apac', 50, 1), (7, 11, 'EU', 2, 0)"
    )
    con.execute(
        "INSERT INTO pg.public.customers VALUES "
        "(10, 'ann', 'us', 10), (11, 'bo', 'EU', 100), (12, 'cy', NULL, 60)"
    )
    con.execute("ATTACH ':memory:' AS mat")
    con.execute("CREATE SCHEMA mat.mv")
    # Each view as its build computes it: over every row of its tables.
    con.execute(f"CREATE TABLE mat.mv.orders_by_region AS {_AGGREGATE_SQL}")
    con.execute(
        "CREATE TABLE mat.mv.orders_customers AS SELECT orders.*, "
        "customers.id AS customers__id, customers.name AS customers__name, "
        "customers.region AS customers__region, customers.credit AS customers__credit "
        "FROM pg.public.orders AS orders LEFT JOIN pg.public.customers AS customers "
        "ON orders.customer_id = customers.id"
    )
    # A join view built before ``orders.flag`` was added: it does not keep that column.
    con.execute(
        "CREATE TABLE mat.mv.orders_customers_drifted AS SELECT orders.id, orders.customer_id, "
        "orders.region, orders.amount, customers.id AS customers__id, "
        "customers.name AS customers__name, customers.region AS customers__region, "
        "customers.credit AS customers__credit "
        "FROM pg.public.orders AS orders LEFT JOIN pg.public.customers AS customers "
        "ON orders.customer_id = customers.id"
    )
    con.execute("CREATE TABLE pg.public.allowed (who VARCHAR, region VARCHAR)")
    con.execute("INSERT INTO pg.public.allowed VALUES ('ann', 'us'), ('bo', 'EU')")
    yield con
    con.close()


_AGGREGATE_SQL = (
    f'SELECT "region", SUM("amount") AS "sum_amount", COUNT(*) AS "n", '
    f'COUNT("amount") AS "n_amount", MIN("amount") AS "min_amount", '
    f'MAX("amount") AS "max_amount" FROM {_ORDERS} GROUP BY "region"'
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


def _join_view(table: str = "orders_customers") -> MVDefinition:
    return MVDefinition(
        id=table,
        source_tables=["orders", "customers"],
        inputs=[
            TableIdentity("sales-pg", "public", "orders"),
            TableIdentity("sales-pg", "public", "customers"),
        ],
        target_catalog="mat",
        target_schema="mv",
        target_table=table,
        join_pattern=JoinPattern(
            left_table="orders",
            left_column="customer_id",
            right_table="customers",
            right_column="id",
            join_type="left",
        ),
        status=MVStatus.FRESH,
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


def _governed(sql: str, rules: dict[int, str] | None = None, masks: dict | None = None) -> str:
    """``sql`` as the role reads it: its row rules and masks in the statement."""
    gov = GovernanceContext(
        rls_rules=rules or {},
        masking_rules=masks or {},
        table_map={
            "orders": _ORDERS_ID,
            "public.orders": _ORDERS_ID,
            "customers": _CUSTOMERS_ID,
            "public.customers": _CUSTOMERS_ID,
        },
        all_columns={_ORDERS_ID: _ORDER_COLUMNS, _CUSTOMERS_ID: _CUSTOMER_COLUMNS},
    )
    return apply_governance(sql, gov)


def _rows(con, sql: str, session: dict[str, str] | None = None) -> list:
    if session is not None:
        sql = _resolve_session_settings(sql, session, "postgres")
    return sorted(con.execute(sql).fetchall(), key=repr)


def _answers_as_its_tables(con, governed: str, view: MVDefinition, **run) -> list:
    """The view answers ``governed``, with the rows its tables give it; those rows."""
    hit = reinject(governed, [view], _ctx())
    assert hit is not None, "the view answers this statement"
    tables = _rows(con, governed, **run)
    assert _rows(con, hit.sql, **run) == tables
    return tables


def _mask(kind: MaskType, data_type: str, **how) -> tuple[MaskingRule, str]:
    return MaskingRule(mask_type=kind, **how), data_type


# -- an aggregate view -----------------------------------------------------------------------------

_BY_REGION = (
    f'SELECT "o"."region", SUM("o"."amount"), COUNT(*), MIN("o"."amount"), MAX("o"."amount") '
    f'FROM {_ORDERS} AS "o" GROUP BY "o"."region"'
)


def test_a_row_rule_on_a_column_the_aggregate_view_does_not_keep_leaves_it_to_its_table(con):
    governed = _governed(_BY_REGION, {_ORDERS_ID: "flag = 1"})
    assert reinject(governed, [_aggregate_view()], _ctx()) is None
    assert _rows(con, governed) == [("EU", 9, 1, 9, 9), ("apac", 50, 1, 50, 50), ("us", 5, 1, 5, 5),
                                    (None, 3, 1, 3, 3)]  # fmt: skip


@pytest.mark.parametrize(
    "rule",
    ["region = 'us'", "region IN ('us', 'apac')", "region <> 'EU'", "region IS NOT NULL"],
)
def test_a_row_rule_on_the_aggregate_views_dimensions_is_answered_from_the_view(con, rule):
    # REQ-882 (amended 2026-11-04, A ROW FILTER ON THE VIEW'S DIMENSIONS): exact, as each row of
    # the view is one combination of its dimensions.
    rows = _answers_as_its_tables(con, _governed(_BY_REGION, {_ORDERS_ID: rule}), _aggregate_view())
    assert "EU" not in {r[0] for r in rows} or rule == "region IS NOT NULL"


def test_a_null_dimension_is_kept_out_by_a_not_equal_rule_both_ways(con):
    # NULL <> 'EU' is not true: the rows of a NULL region are no more the role's in the view.
    rows = _answers_as_its_tables(
        con, _governed(_BY_REGION, {_ORDERS_ID: "region <> 'EU'"}), _aggregate_view()
    )
    assert [r[0] for r in rows] == ["apac", "us"]


def test_a_row_rule_mixing_a_dimension_and_another_column_leaves_it_to_its_table():
    governed = _governed(_BY_REGION, {_ORDERS_ID: "region = 'us' OR flag = 1"})
    assert reinject(governed, [_aggregate_view()], _ctx()) is None


def test_a_row_rule_with_a_subquery_leaves_it_to_its_table():
    rule = "region IN (SELECT a.region FROM pg.public.allowed AS a WHERE a.who = 'ann')"
    governed = _governed(_BY_REGION, {_ORDERS_ID: rule})
    assert reinject(governed, [_aggregate_view()], _ctx()) is None


@pytest.mark.parametrize("value", [0, None], ids=["constant", "null"])
@pytest.mark.parametrize(
    "aggregate",
    ['SUM("o"."amount")', 'COUNT("o"."amount")', 'MIN("o"."amount")', 'AVG("o"."amount")'],
)
def test_a_masked_measure_is_never_read_from_the_views_unmasked_measures(con, aggregate, value):
    sql = f'SELECT "o"."region", {aggregate} FROM {_ORDERS} AS "o" GROUP BY "o"."region"'
    governed = _governed(sql, masks={(_ORDERS_ID, "amount"): _mask(MaskType.constant, "integer",
                                                                   value=value)})  # fmt: skip
    assert reinject(governed, [_aggregate_view()], _ctx()) is None
    # What the role reads is the mask's: over the masked value, not the stored measure.
    masked = {r[1] for r in _rows(con, governed)}
    if aggregate.startswith("COUNT"):
        assert masked == ({1, 2} if value is not None else {0})  # rows per region, or none
    else:
        assert masked <= {0, None}


def test_a_masked_dimension_grouped_on_leaves_it_to_its_table(con):
    governed = _governed(
        _BY_REGION, masks={(_ORDERS_ID, "region"): _mask(MaskType.constant, "varchar", value="X")}
    )
    assert reinject(governed, [_aggregate_view()], _ctx()) is None
    assert _rows(con, governed) == [("X", 80, 7, 2, 50)]


def test_a_masked_dimension_filtered_on_answers_as_its_table(con):
    # The mask lands in the WHERE as the role's value of the column, the same over the view.
    sql = f'SELECT SUM("o"."amount") FROM {_ORDERS} AS "o" WHERE "o"."region" = \'us\''
    governed = _governed(
        sql, masks={(_ORDERS_ID, "region"): _mask(MaskType.constant, "varchar", value="X")}
    )
    hit = reinject(governed, [_aggregate_view()], _ctx())
    if hit is not None:
        assert _rows(con, hit.sql) == _rows(con, governed)
    assert _rows(con, governed) == [(None,)]


def test_a_session_variable_rule_on_a_dimension_is_evaluated_per_query(con):
    governed = _governed(_BY_REGION, {_ORDERS_ID: "region = current_setting('provisa.region')"})
    us = _answers_as_its_tables(con, governed, _aggregate_view(), session={"region": "us"})
    eu = _answers_as_its_tables(con, governed, _aggregate_view(), session={"region": "EU"})
    assert [r[0] for r in us] == ["us"] and [r[0] for r in eu] == ["EU"]
    # A user with no value for the variable reads no row, from the view as from the table.
    assert _answers_as_its_tables(con, governed, _aggregate_view(), session={}) == []


# -- a join view -----------------------------------------------------------------------------------

_JOINED = (
    f'SELECT "o"."id", "o"."amount", "c"."name" FROM {_ORDERS} AS "o" '
    f'LEFT JOIN {_CUSTOMERS} AS "c" ON "o"."customer_id" = "c"."id"'
)


@pytest.mark.parametrize(
    "rules",
    [
        {_CUSTOMERS_ID: "credit >= 60"},
        {_CUSTOMERS_ID: "region <> 'EU'"},
        {_ORDERS_ID: "flag = 1"},
        {_ORDERS_ID: "region <> 'EU'", _CUSTOMERS_ID: "credit < 100"},
    ],
)
def test_a_row_rule_on_a_column_the_join_view_keeps_answers_as_its_tables(con, rules):
    rows = _answers_as_its_tables(con, _governed(_JOINED, rules), _join_view())
    assert len(rows) < 7  # the rule kept some rows out


def test_a_null_joined_column_is_kept_out_by_a_not_equal_rule_both_ways(con):
    rows = _answers_as_its_tables(
        con, _governed(_JOINED, {_CUSTOMERS_ID: "region <> 'EU'"}), _join_view()
    )
    assert [r[0] for r in rows] == [1, 2]  # no EU customer, no NULL region, no unmatched order


def test_a_row_rule_on_a_column_the_join_view_does_not_keep_is_never_answered(con):
    # A join view keeps every column its tables had at its build; one added since is not in it.
    # The rewrite names the column on the view, and the engine refuses it: no row is answered.
    view = _join_view("orders_customers_drifted")
    governed = _governed(_JOINED, {_ORDERS_ID: "flag = 1"})
    hit = reinject(governed, [view], _ctx())
    if hit is not None:
        with pytest.raises(duckdb.BinderException, match="flag"):
            con.execute(hit.sql)


def test_a_row_rule_whose_subquery_reads_the_tables_by_the_statements_names_is_left_alone(con):
    # The rule's subquery names its own read of customers "customers", as the statement names
    # the customers it joins. Read from the view, the subquery's columns were the joined
    # customer's: the role saw every order no larger than its own customer's credit, not
    # than the least credit of all.
    rule = (
        "NOT EXISTS (SELECT 1 FROM pg.public.customers AS customers "
        "WHERE customers.credit < orders.amount)"
    )
    sql = (
        f'SELECT "orders"."id", "customers"."name" FROM {_ORDERS} AS "orders" '
        f'LEFT JOIN {_CUSTOMERS} AS "customers" ON "orders"."customer_id" = "customers"."id"'
    )
    governed = _governed(sql, {_ORDERS_ID: rule})
    assert reinject(governed, [_join_view()], _ctx()) is None
    assert _rows(con, governed) == [(1, "ann"), (2, "ann"), (3, "bo"), (4, "cy"), (5, "cy"),
                                    (7, "bo")]  # fmt: skip


def test_a_row_rule_whose_subquery_reads_by_its_own_names_answers_as_its_tables(con):
    rule = "region IN (SELECT a.region FROM pg.public.allowed AS a WHERE a.who = 'ann')"
    rows = _answers_as_its_tables(con, _governed(_JOINED, {_CUSTOMERS_ID: rule}), _join_view())
    assert [r[0] for r in rows] == [1, 2]


@pytest.mark.parametrize(
    ("table", "column", "data_type", "value"),
    [
        (_CUSTOMERS_ID, "name", "varchar", "X"),
        (_ORDERS_ID, "amount", "integer", 0),
        (_CUSTOMERS_ID, "credit", "integer", None),
    ],
)
def test_a_mask_on_a_column_the_join_view_keeps_answers_as_its_tables(
    con, table, column, data_type, value
):
    sql = _JOINED + ' WHERE "c"."credit" IS NULL OR "c"."credit" > 0'
    masks = {(table, column): _mask(MaskType.constant, data_type, value=value)}
    rows = _answers_as_its_tables(con, _governed(sql, masks=masks), _join_view())
    if column == "name":
        assert {r[2] for r in rows} == {"X"}
    if column == "amount":
        assert {r[1] for r in rows} == {0}


def test_a_mask_on_a_column_the_join_view_does_not_keep_is_never_answered(con):
    sql = _JOINED.replace('"o"."id",', '"o"."id", "o"."flag",')
    masks = {(_ORDERS_ID, "flag"): _mask(MaskType.constant, "integer", value=0)}
    governed = _governed(sql, masks=masks)
    hit = reinject(governed, [_join_view("orders_customers_drifted")], _ctx())
    if hit is not None:
        # The mask is a constant in the column's place: the view answers it as the tables do.
        assert _rows(con, hit.sql) == _rows(con, governed)


def test_a_session_variable_rule_on_a_joined_table_is_evaluated_per_query(con):
    governed = _governed(_JOINED, {_CUSTOMERS_ID: "region = current_setting('provisa.region')"})
    us = _answers_as_its_tables(con, governed, _join_view(), session={"region": "us"})
    eu = _answers_as_its_tables(con, governed, _join_view(), session={"region": "EU"})
    assert [r[0] for r in us] == [1, 2] and [r[0] for r in eu] == [3, 7]
