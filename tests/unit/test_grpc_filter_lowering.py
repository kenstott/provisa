# Copyright (c) 2026 Kenneth Stott
# Canary: 4a8c1e53-9b2d-4f76-8c0a-7d5e3b1f6a48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit: a gRPC request's ``filter`` lowers to the WHERE of the semantic SELECT (REQ-803, REQ-1860).

One lowering for the native servicer (a ``{Type}Filter`` message) and the HTTP gRPC proxy (the
request body's ``filter`` object): the same equality WHERE, the same rule that a filtered field
must be a column the role can read. Filter VALUES are bound parameters — never text in the
statement — so the statement is the request's shape: requests that differ only in their filter
values share one statement (one kept plan) and carry their own values."""

# Requirements: REQ-803, REQ-1860, REQ-1877

from __future__ import annotations

import datetime
import decimal
from types import SimpleNamespace

import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.compiler.sql_types import TableMeta
from provisa.grpc.query_ir import grpc_table_to_semantic_sql, resolve_read_mask

_COLUMNS = [
    ("order_id", "integer"),
    ("amount", "decimal(10,2)"),
    ("region", "varchar"),
    ("shipped", "boolean"),
    ("placed_at", "timestamp"),
    ("seen_at", "timestamp with time zone"),
    ("ship_date", "date"),
    ("ref", "uuid"),
]


def _ctx():
    meta = TableMeta(
        table_id=1,
        field_name="orders",
        type_name="Orders",
        source_id="pg1",
        catalog_name="pg1",
        schema_name="public",
        table_name="orders",
    )
    return SimpleNamespace(tables={"orders": meta}, aggregate_columns={1: list(_COLUMNS)})


def _message(**set_fields):
    """A ``{Type}Filter`` message: every column a field, ``set_fields`` the explicitly-set ones."""
    fields = [SimpleNamespace(name=name) for name in ("order_id", "amount", "region", "hidden")]
    return SimpleNamespace(
        DESCRIPTOR=SimpleNamespace(fields=fields),
        HasField=lambda name: name in set_fields,
        **set_fields,
    )


def _lower(filter_arg, limit: int = 0, paths: list[str] | None = None) -> tuple[str, list]:
    mask = resolve_read_mask(_ctx(), "Orders", paths or [])
    lowered = grpc_table_to_semantic_sql(_ctx(), "Orders", limit, filter_arg, mask)
    assert lowered is not None
    return lowered


def _predicates(sql: str) -> list[str]:
    where = sqlglot.parse_one(sql, read="postgres").args.get("where")
    assert isinstance(where, exp.Where)
    return where.this.sql(dialect="postgres").split(" AND ")


class TestFilterValuesAreBound:
    def test_values_are_placeholders_with_the_values_alongside(self):
        sql, params = _lower({"region": "east", "order_id": 3})
        assert _predicates(sql) == ['"region" = $1', '"order_id" = $2']
        assert params == ["east", 3]
        assert "east" not in sql

    def test_body_filter_and_filter_message_lower_alike(self):
        sql, params = _lower(_message(order_id=3, region="east"))
        assert _predicates(sql) == ['"order_id" = $1', '"region" = $2']
        assert params == [3, "east"]

    def test_values_keep_their_type(self):
        _sql, params = _lower({"shipped": False, "amount": 10.5, "order_id": 7})
        assert params == [False, 10.5, 7]
        assert [type(p) for p in params] == [bool, float, int]

    def test_a_quote_in_a_value_never_reaches_the_statement(self):
        sql, params = _lower({"region": "o'hare' OR '1'='1"})
        assert _predicates(sql) == ['"region" = $1']
        assert params == ["o'hare' OR '1'='1"]

    def test_no_filter_binds_nothing(self):
        assert _lower({}) == _lower(None)
        assert _lower(None)[1] == []


class TestTypedFilterValues:
    """A value that arrives as text for a typed column (the proto filter field of a timestamp,
    date or uuid column is ``string``) is bound and cast to the column's type in the statement,
    so it compares on every engine dialect rather than as text."""

    @pytest.mark.parametrize(
        ("field", "value", "predicate", "bound"),
        [
            ("placed_at", "2026-01-01 08:30:00", '"placed_at" = CAST($1 AS TIMESTAMP)', None),
            (
                "placed_at",
                "2026-01-01T08:30:00",
                '"placed_at" = CAST($1 AS TIMESTAMP)',
                "2026-01-01 08:30:00",  # ISO 'T' separator normalized: not every engine casts it
            ),
            ("seen_at", "2026-01-01 08:30:00+00", '"seen_at" = CAST($1 AS TIMESTAMPTZ)', None),
            ("ship_date", "2026-01-05", '"ship_date" = CAST($1 AS DATE)', None),
            ("ref", "6f1c2a34-0000-4000-8000-000000000001", '"ref" = CAST($1 AS UUID)', None),
            ("amount", 10.5, '"amount" = $1', None),
            ("shipped", True, '"shipped" = $1', None),
            ("order_id", 3, '"order_id" = $1', None),
            ("region", "east", '"region" = $1', None),
        ],
    )
    def test_predicate_and_bound_value(self, field, value, predicate, bound):
        sql, params = _lower({field: value})
        assert _predicates(sql) == [predicate]
        assert params == [value if bound is None else bound]

    def test_typed_filters_compare_on_duckdb(self):
        """The lowered predicates, transpiled for the DuckDB engine and run with the bound values."""
        import duckdb

        from provisa.transpiler.transpile import transpile

        sql, params = _lower(
            {
                "placed_at": "2026-01-01T08:30:00",
                "ship_date": "2026-01-05",
                "amount": 10.5,
                "shipped": True,
                "order_id": 1,
            }
        )
        where = " AND ".join(_predicates(sql))
        con = duckdb.connect()
        try:
            con.execute(
                "CREATE TABLE orders (order_id INTEGER, placed_at TIMESTAMP, ship_date DATE, "
                "amount DECIMAL(10,2), shipped BOOLEAN)"
            )
            con.executemany(
                "INSERT INTO orders VALUES (?, ?, ?, ?, ?)",
                [
                    (
                        1,
                        datetime.datetime(2026, 1, 1, 8, 30),
                        datetime.date(2026, 1, 5),
                        decimal.Decimal("10.50"),
                        True,
                    ),
                    (
                        2,
                        datetime.datetime(2026, 1, 1, 8, 31),
                        datetime.date(2026, 1, 5),
                        decimal.Decimal("10.50"),
                        True,
                    ),
                ],
            )
            statement = transpile(f"SELECT order_id FROM orders WHERE {where}", "duckdb")
            assert con.execute(statement, params).fetchall() == [(1,)]
        finally:
            con.close()

    def test_typed_filters_reach_trino_as_typed_positional_binds(self):
        """Trino has no implicit varchar→timestamp/date comparison: the cast is in the statement
        and the values bind positionally, in order (``executor.trino`` binds with ``?``)."""
        from provisa.compiler.params import bind_positionally
        from provisa.transpiler.transpile import transpile

        sql, params = _lower(
            {
                "placed_at": "2026-01-01T08:30:00",
                "seen_at": "2026-01-01 08:30:00+00",
                "ship_date": "2026-01-05",
                "amount": 10.5,
                "shipped": True,
            }
        )
        where = " AND ".join(_predicates(sql))
        statement, bound = bind_positionally(
            transpile(f"SELECT order_id FROM orders WHERE {where}", "trino"), params, "?"
        )
        assert statement == (
            'SELECT order_id FROM orders WHERE "placed_at" = CAST(? AS TIMESTAMP) '
            'AND "seen_at" = CAST(? AS TIMESTAMP WITH TIME ZONE) '
            'AND "ship_date" = CAST(? AS DATE) AND "amount" = ? AND "shipped" = ?'
        )
        assert bound == ["2026-01-01 08:30:00", "2026-01-01 08:30:00+00", "2026-01-05", 10.5, True]


class TestLimitIsBound:
    """The row limit is a bound value like the filter values (the GraphQL compiler binds it
    too), after them in placeholder order."""

    def test_limit_is_a_placeholder_after_the_filter_values(self):
        sql, params = _lower({"region": "east"}, limit=25)
        assert sql.endswith(" LIMIT $2")
        assert params == ["east", 25]

    def test_limit_alone(self):
        sql, params = _lower(None, limit=25)
        assert sql.endswith(" LIMIT $1")
        assert params == [25]

    def test_no_limit_is_no_clause(self):
        sql, params = _lower(None, limit=0)
        assert "LIMIT" not in sql
        assert params == []


class TestBodyFilterValueMatchesTheColumnType:
    """The HTTP proxy's JSON filter value must be the JSON type the native ``{Type}Filter``
    field carries for that column — a mismatch is rejected by name, never bound as text."""

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("order_id", "3"),  # integer column: a string
            ("order_id", 3.5),  # integer column: a fraction
            ("order_id", True),  # integer column: a boolean is not a number
            ("amount", "10.5"),  # numeric column: a string
            ("amount", False),
            ("shipped", 1),  # boolean column: a number
            ("shipped", "true"),
            ("region", 5),  # text column: a number
            ("placed_at", 20260101),  # timestamp column: its filter value is text
            ("ship_date", True),
        ],
    )
    def test_mismatch_raises_naming_the_field(self, field, value):
        from provisa.grpc.query_ir import FilterError

        with pytest.raises(FilterError, match=field) as failure:
            _lower({field: value})
        assert failure.value.field == field

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("order_id", 3),
            ("amount", 10.5),
            ("amount", 10),  # a whole number is a number
            ("shipped", False),
            ("region", "east"),
            ("placed_at", "2026-01-01 08:30:00"),
            ("ship_date", "2026-01-05"),
        ],
    )
    def test_matching_value_binds(self, field, value):
        _sql, params = _lower({field: value})
        assert params == [value]

    def test_group_by_body_filter_mismatch_raises_naming_the_field(self):
        from provisa.grpc.query_ir import FilterError, grpc_table_to_group_by_graphql_text

        with pytest.raises(FilterError, match="order_id") as failure:
            grpc_table_to_group_by_graphql_text(
                _ctx(), "Orders", ["region"], funcs=["count"], filter_msg={"order_id": "3"}
            )
        assert failure.value.field == "order_id"


class TestStatementIsTheShape:
    """The compiled stage keys its kept plan on the statement text (pgwire.governed_plan)."""

    @staticmethod
    def _key(sql: str) -> str:
        from provisa.pgwire.governed_plan import plan_key

        state = SimpleNamespace(schema_boot_id="boot", schema_version=1)
        return plan_key(state, "compiled", "admin", sql, [])

    def test_different_values_are_one_statement_and_one_plan_key(self):
        lowered = [
            _lower({"order_id": order_id, "region": region}, limit=limit)
            for order_id, region, limit in ((1, "east", 10), (2, "west", 10), (3, "north", 500))
        ]
        assert len({sql for sql, _params in lowered}) == 1
        assert len({self._key(sql) for sql, _params in lowered}) == 1
        assert [params for _sql, params in lowered] == [
            [1, "east", 10],
            [2, "west", 10],
            [3, "north", 500],
        ]

    def test_a_different_shape_is_a_different_plan_key(self):
        shapes = [
            _lower({"order_id": 1}),
            _lower({"region": "east"}),  # another filtered field
            _lower({"order_id": 1, "region": "east"}),  # more fields
            _lower({"order_id": 1}, paths=["order_id"]),  # same filter, a read_mask
            _lower({"order_id": 1}, paths=["order_id", "region"]),  # another read_mask
            _lower(
                {"order_id": 1}, limit=11
            ),  # a limit (its value is bound; its presence is shape)
            _lower(None),  # no filter
        ]
        assert len({self._key(sql) for sql, _params in shapes}) == len(shapes)


@pytest.mark.parametrize("filter_arg", [{"hidden": 1}, _message(hidden=1)])
def test_filter_on_a_field_the_role_cannot_read_raises_naming_the_field(filter_arg):
    """Body or message alike, Query or GroupBy alike — a filter must not probe a column outside
    the role's schema (GitHub issue 131)."""
    from provisa.grpc.query_ir import FilterError, grpc_table_to_group_by_graphql_text

    with pytest.raises(FilterError, match="hidden") as failure:
        grpc_table_to_semantic_sql(_ctx(), "Orders", 10, filter_arg)
    assert failure.value.field == "hidden"
    with pytest.raises(FilterError, match="hidden") as failure:
        grpc_table_to_group_by_graphql_text(
            _ctx(), "Orders", ["region"], funcs=["count"], filter_msg=filter_arg
        )
    assert failure.value.field == "hidden"


@pytest.mark.parametrize("value", [None, ["east"], {"eq": "east"}])
def test_body_filter_value_that_is_not_a_scalar_raises_naming_the_field(value):
    from provisa.grpc.query_ir import FilterError

    with pytest.raises(FilterError, match="region") as failure:
        grpc_table_to_semantic_sql(_ctx(), "Orders", 10, {"region": value})
    assert failure.value.field == "region"
