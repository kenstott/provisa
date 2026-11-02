# Copyright (c) 2026 Kenneth Stott
# Canary: 74eac6fa-6c29-4e0a-ad78-615506de5fca
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1867 (amended 2026-11-02): GraphQL and the shared JSON response path write exact values.

A DECIMAL is the JSON string of its exact digits; PostgreSQL's infinite dates and timestamps are
"infinity" / "-infinity"; only a json or jsonb column (or a relationship the statement builds as
JSON) becomes a JSON object or array, and text stays a string. What JSON cannot carry is answered
exactly or refused by name, never a 500. With the four serializer and cache defects J494 found
(a to-one truncation warning, a null to-one parent, list elements keyed by id(), a cache hit that
answered differently from its miss).
"""

from __future__ import annotations

import json
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace

import pyarrow as pa
import pytest

from provisa.api.json_response import dumps
from provisa.compiler.sql_types import ColumnRef
from provisa.core.ir_types import infinite_date, infinite_timestamp, interval_value
from provisa.core.json_refusal import JsonRefused, JsonResponseRefused, JsonValueRefused
from provisa.executor.result import QueryResult
from provisa.executor.serialize import result_records
from tests.unit.json_encoder.rows import encoded, serialize_aggregate, serialize_rows


def _col(
    field,
    nested_in=None,
    cardinality=None,
    *,
    json_=False,
    json_text=(),
    is_agg=False,
    integer=False,
):
    return ColumnRef(
        alias=None,
        column=field,
        field_name=field,
        nested_in=nested_in,
        cardinality=cardinality,
        is_agg=is_agg,
        json=json_,
        json_text=frozenset(json_text),
        integer=integer,
    )


def _rows(rows, columns, root="t"):
    return serialize_rows(rows, columns, root)["data"][root]


def _document(rows, columns, root="t") -> bytes:
    """The response body's bytes as GraphQL answers the field (the encoder's rows, embedded)."""
    return b'{"data":{"' + root.encode() + b'":' + encoded(rows, columns) + b"}}"


def _body(rows, columns, root="t"):
    """The response body's bytes, parsed back: what the client reads."""
    return json.loads(_document(rows, columns, root))["data"][root]


# --- Decision 1: a DECIMAL is the JSON string of its exact digits --------------------------------


class TestDecimal:
    def test_digits_past_a_float_are_kept(self):
        got = _body([(Decimal("1234567890123456.78"),)], [_col("amount")])
        assert got == [{"amount": "1234567890123456.78"}]

    def test_a_whole_decimal_past_2_53_is_exact(self):
        got = _body([(Decimal("9007199254740993"),)], [_col("n")])
        assert got == [{"n": "9007199254740993"}]

    def test_nothing_fails_past_2_64(self):
        big = Decimal(2**64) * 1000 + 1
        assert _body([(big,)], [_col("n")]) == [{"n": str(2**64 * 1000 + 1)}]

    def test_positional_never_an_exponent(self):
        got = _rows(
            [(Decimal("0E-7"), Decimal("1E+3"), Decimal("1.23E+5"))],
            [_col("zero"), _col("k"), _col("big")],
        )
        assert got == [{"zero": "0.0000000", "k": "1000", "big": "123000"}]

    def test_an_aggregate_is_exact(self):
        got = serialize_aggregate(
            [(Decimal("100.10"),)], [_col("amount", nested_in="aggregate.sum")], "t"
        )
        assert got["data"]["t"]["aggregate"]["sum"]["amount"] == "100.10"

    def test_a_decimal_the_response_class_meets_is_exact(self):
        assert json.loads(dumps({"v": Decimal("0.10000000000000000001")})) == {
            "v": "0.10000000000000000001"
        }


# --- Decision 2: PostgreSQL's infinite dates and timestamps --------------------------------------


class TestInfinity:
    def test_infinite_date_and_timestamp_are_written_as_pgwire_writes_them(self):
        row = (
            infinite_date(1),
            infinite_date(-1),
            infinite_timestamp(1, aware=True),
            infinite_timestamp(-1, aware=False),
        )
        cols = [_col("d1"), _col("d2"), _col("ts1"), _col("ts2")]
        assert _body([row], cols) == [
            {"d1": "infinity", "d2": "-infinity", "ts1": "infinity", "ts2": "-infinity"}
        ]

    def test_infinity_is_not_the_last_real_date(self):
        from datetime import date

        got = _rows([(date(9999, 12, 31),), (infinite_date(1),)], [_col("d")])
        assert got == [{"d": "9999-12-31"}, {"d": "infinity"}]

    def test_the_response_class_writes_infinity(self):
        assert json.loads(dumps([infinite_date(-1), infinite_timestamp(1, aware=True)])) == [
            "-infinity",
            "infinity",
        ]


# --- Decision 3: only JSON-typed columns are JSON ------------------------------------------------


class TestOnlyJsonColumnsAreJson:
    def test_text_that_reads_as_json_stays_a_string(self):
        got = _body([("[1,2]", '{"a": 1}')], [_col("name"), _col("note")])
        assert got == [{"name": "[1,2]", "note": '{"a": 1}'}]

    def test_a_json_column_is_its_value(self):
        got = _body([('{"a": [1, 2]}',)], [_col("prefs", json_=True)])
        assert got == [{"prefs": {"a": [1, 2]}}]

    def test_a_json_scalar_string_is_its_string(self):
        assert _body([('"hello"',)], [_col("prefs", json_=True)]) == [{"prefs": "hello"}]

    def test_a_string_inside_a_json_value_is_not_parsed_again(self):
        got = _body([('{"a": "[1]"}',)], [_col("prefs", json_=True)])
        assert got == [{"prefs": {"a": "[1]"}}]

    def test_a_relationship_built_as_text_by_the_engine_is_parsed(self):
        # Trino's json_format makes a nested list text inside its parent's JSON (transpile.py).
        value = json.dumps([{"id": 1, "items": json.dumps([{"sku": "[x]"}])}])
        col = _col(
            "orders", cardinality="one-to-many", json_=True, json_text={"items"}, is_agg=True
        )
        got = _body([(7, value)], [_col("id"), col])
        assert got == [{"id": 7, "orders": [{"id": 1, "items": [{"sku": "[x]"}]}]}]

    def test_records_read_the_engines_column_types(self):
        result = QueryResult(
            rows=[('{"a": 1}', '{"a": 1}')],
            column_names=["doc", "text"],
            column_types=["jsonb", "varchar"],
        )
        assert result_records(result) == [{"doc": {"a": 1}, "text": '{"a": 1}'}]


class TestJsonEdges:
    """What JSON cannot carry is answered exactly or refused by name, never a 500."""

    @pytest.mark.parametrize("token", ["NaN", "Infinity", "-Infinity"])
    def test_a_non_finite_number_is_refused_naming_the_field(self, token):
        with pytest.raises(JsonValueRefused) as refused:
            _rows([(f'{{"x": {token}}}',)], [_col("prefs", json_=True)])
        assert refused.value.params["field"] == "prefs"
        assert token in str(refused.value)
        assert isinstance(refused.value, JsonRefused)  # the app answers the family as 422

    def test_an_int_past_64_bits_is_written_with_all_its_digits(self):
        text = '{"n": 123456789012345678901234567890, "m": -18446744073709551617}'
        body = _document([(text,)], [_col("prefs", json_=True)], "t")
        assert b"123456789012345678901234567890" in body
        assert json.loads(body)["data"]["t"] == [
            {"prefs": {"n": 123456789012345678901234567890, "m": -18446744073709551617}}
        ]

    def test_an_int_past_64_bits_at_the_root_is_exact(self):
        body = dumps({"data": [{"n": 2**70}]})
        assert json.loads(body) == {"data": [{"n": 2**70}]}

    def test_a_lone_surrogate_is_refused_naming_the_field(self):
        with pytest.raises(JsonValueRefused) as refused:
            _rows([('{"k": "\\ud800"}',)], [_col("prefs", json_=True)])
        assert refused.value.params["field"] == "prefs"

    def test_a_lone_surrogate_in_a_key_is_refused(self):
        with pytest.raises(JsonValueRefused):
            _rows([('{"\\udc00": 1}',)], [_col("prefs", json_=True)])

    def test_a_surrogate_pair_is_its_character(self):
        got = _body([('{"k": "\\ud83d\\ude00"}',)], [_col("prefs", json_=True)])
        assert got == [{"prefs": {"k": "\U0001f600"}}]

    def test_nesting_past_the_encoder_is_refused_by_name(self):
        text = "[" * 300 + "]" * 300
        with pytest.raises(JsonResponseRefused) as refused:
            _document([(text,)], [_col("prefs", json_=True)], "t")
        assert isinstance(refused.value, JsonRefused)

    def test_nesting_past_the_parser_is_refused_naming_the_field(self):
        text = "[" * 200_000 + "]" * 200_000
        with pytest.raises(JsonValueRefused) as refused:
            _rows([(text,)], [_col("prefs", json_=True)])
        assert refused.value.params["field"] == "prefs"

    def test_json_text_that_is_not_json_is_refused_naming_the_field(self):
        with pytest.raises(JsonValueRefused) as refused:
            _rows([("{nope",)], [_col("prefs", json_=True)])
        assert refused.value.params["field"] == "prefs"


# --- The compiler marks which columns are JSON ---------------------------------------------------


def _ctx(customer_columns=None):
    """``customer_columns``: the customers table's column entries, in place of its plain ones (a
    column entry may name a JSON path, as a registration's ``path`` does)."""
    from provisa.compiler import naming as _naming
    from provisa.compiler.context import build_context
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.compiler.schema_gen import SchemaInput, generate_schema

    def col(name, data_type):
        return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=True)

    def table(tid, name, cols):
        return {
            "id": tid,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": name,
            "columns": [
                {**c, "visible_to": ["admin"]}
                if isinstance(c, dict)
                else {"column_name": c, "visible_to": ["admin"]}
                for c in cols
            ],
            "write_ops": [],
            "write_returns_rows": True,
            "write_refused_forms": [],
        }

    _naming.configure(gql="snake")
    si = SchemaInput(
        tables=[
            table(
                1,
                "customers",
                customer_columns or ["id", "name", "prefs", "visits", "balance", "score"],
            ),
            table(2, "orders", ["id", "customer_id", "note", "meta"]),
        ],
        relationships=[
            {
                "id": "r1",
                "source_table_id": 1,
                "target_table_id": 2,
                "source_column": "id",
                "target_column": "customer_id",
                "cardinality": "one-to-many",
            },
            {
                "id": "r2",
                "source_table_id": 2,
                "target_table_id": 1,
                "source_column": "customer_id",
                "target_column": "id",
                "cardinality": "many-to-one",
            },
        ],
        column_types={
            1: [
                col("id", "integer"),
                col("name", "text"),
                col("prefs", "jsonb"),
                col("visits", "hugeint"),
                col("balance", "numeric(38,0)"),
                col("score", "double precision"),
            ],
            2: [
                col("id", "integer"),
                col("customer_id", "integer"),
                col("note", "text"),
                col("meta", "json"),
            ],
        },
        naming_rules=[],
        role={"id": "admin", "capabilities": [], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
    )
    generate_schema(si)
    return build_context(si)


class TestCompilerMarksJson:
    def test_json_and_jsonb_columns_are_json_and_text_is_not(self):
        from graphql import parse

        from provisa.compiler.sql_gen import compile_query

        (compiled,) = compile_query(parse("{ customers { id name prefs } }"), _ctx(), None)
        marks = {c.field_name: c.json for c in compiled.columns}
        assert marks == {"id": False, "name": False, "prefs": True}

    def test_a_relationship_built_as_json_names_its_json_text_paths(self):
        """The nested relationship inside it is a path whose string is engine text (Trino's
        json_format); a json column inside it is embedded as JSON and is no such path."""
        from graphql import parse

        from provisa.compiler.sql_gen import compile_query

        doc = parse("{ customers { id orders { id note meta customer { name } } } }")
        (compiled,) = compile_query(doc, _ctx(), None)
        orders = next(c for c in compiled.columns if c.field_name == "orders")
        assert orders.nested_in is None and orders.is_agg  # one JSON value for the field
        assert orders.json and orders.json_text == frozenset({"customer"})

    def test_a_jsonb_string_inside_a_relationship_stays_its_string(self):
        from graphql import parse

        from provisa.compiler.sql_gen import compile_query

        doc = parse("{ customers { id orders { meta } } }")
        (compiled,) = compile_query(doc, _ctx(), None)
        value = json.dumps([{"meta": "[1]"}, {"meta": {"a": "[2]"}}])
        got = json.loads(_document([(1, value)], compiled.columns, "t"))
        assert got["data"]["t"] == [{"id": 1, "orders": [{"meta": "[1]"}, {"meta": {"a": "[2]"}}]}]


# --- Defect 4: the many-to-one truncation warning ------------------------------------------------


class TestToOneTruncation:
    def _cols(self):
        return [
            _col("id"),
            _col("a", nested_in="owner", cardinality="many-to-one"),
            _col("b", nested_in="owner", cardinality="many-to-one"),
        ]

    def test_a_repeated_row_with_a_null_two_column_to_one_is_not_warned(self):
        shaped = serialize_rows([(1, None, None), (1, None, None)], self._cols(), "t")
        assert "extensions" not in shaped

    def test_a_repeated_row_with_the_same_to_one_is_not_warned(self):
        shaped = serialize_rows([(1, "x", "y"), (1, "x", "y")], self._cols(), "t")
        assert "extensions" not in shaped

    def test_a_repeated_row_with_another_to_one_is_warned(self):
        shaped = serialize_rows([(1, "x", "y"), (1, "x", "z")], self._cols(), "t")
        assert [w["path"] for w in shaped["extensions"]["warnings"]] == ["owner"]

    def test_a_nested_to_one_inside_is_not_its_parents_difference(self):
        cols = [
            _col("id"),
            _col("a", nested_in="owner", cardinality="many-to-one"),
            _col("c", nested_in="owner.sub", cardinality="many-to-one"),
        ]
        shaped = serialize_rows([(1, "x", "q"), (1, "x", "q")], cols, "t")
        assert "extensions" not in shaped


@pytest.fixture
def pipeline(monkeypatch):
    from provisa.api.data import endpoint
    from provisa.pgwire import _pipeline
    from provisa.transpiler.router import Route

    results: list = []

    async def _govern(sql, role_id, **kwargs):
        return SimpleNamespace(
            route=Route.DIRECT,
            cache_hit=None,
            materialize=None,
            sources=frozenset({"sales-pg"}),
            source_id="sales-pg",
            dialect="postgres",
            route_reason="t",
            sql=sql,
        )

    async def _execute(plan, state):
        return results.pop(0)

    monkeypatch.setattr(_pipeline, "_govern_and_route_compiled", _govern)
    monkeypatch.setattr(_pipeline, "_execute_plan", _execute)
    monkeypatch.setattr(endpoint, "note_request_route", lambda route: None)
    monkeypatch.setattr(endpoint, "_record_per_source_stats", lambda *a, **k: None)
    return results


async def test_the_truncation_warning_reaches_the_request(pipeline):
    from provisa.api.data import endpoint
    from provisa.compiler.directives import CacheHint
    from provisa.compiler.sql_types import CompiledQuery
    from provisa.core.statement_warnings import collecting
    from provisa.executor.serialize import TO_ONE_TRUNCATED

    cols = [
        _col("id"),
        _col("a", nested_in="owner", cardinality="many-to-one"),
    ]
    compiled = CompiledQuery(
        sql="SELECT 1",
        params=[],
        root_field="orders",
        columns=cols,
        sources={"sales-pg"},
        canonical_field="orders",
    )
    pipeline.append(QueryResult(rows=[(1, "x"), (1, "y")], column_names=["id", "owner__a"]))
    with collecting() as warned:
        _, field_rows, _, _ = await endpoint._execute_one_field(
            compiled,
            SimpleNamespace(tables={}),
            SimpleNamespace(),
            "analyst",
            "json",
            delivery=None,
            steward_hint=None,
            query_session_props=None,
            cache_hint=CacheHint(opt_in=False, ttl=None, debug_trace=False),
        )
    assert json.loads(field_rows.body) == [{"id": 1, "owner": {"a": "x"}}]
    assert [(w.code, w.params) for w in warned] == [(TO_ONE_TRUNCATED, {"path": "owner"})]


# --- Defect 5: a repeated root row whose to-one parent is null -----------------------------------


def test_a_null_to_one_parent_with_a_nested_to_one_does_not_fail():
    cols = [
        _col("id"),
        _col("a", nested_in="rel", cardinality="many-to-one"),
        _col("c", nested_in="rel.sub", cardinality="many-to-one"),
    ]
    shaped = serialize_rows([(1, None, None), (1, None, None)], cols, "t")
    assert shaped == {"data": {"t": [{"id": 1, "rel": None}]}}


def test_a_null_to_one_parent_that_a_later_row_fills_is_warned():
    cols = [
        _col("id"),
        _col("a", nested_in="rel", cardinality="many-to-one"),
        _col("c", nested_in="rel.sub", cardinality="many-to-one"),
    ]
    shaped = serialize_rows([(1, None, None), (1, "x", "y")], cols, "t")
    assert [w["path"] for w in shaped["extensions"]["warnings"]] == ["rel", "rel.sub"]


# --- Defect 6: one-to-many elements keyed by their place, not id(list) ---------------------------


def test_list_elements_are_keyed_by_their_place_not_the_lists_address(monkeypatch):
    """Two orders' item lists hold an equal item. CPython can give a later list the address of
    one already dropped; every list here answers the same id() to stand for that, and each
    order still keeps its own item."""
    import builtins

    import provisa.executor.serialize as serialize

    monkeypatch.setattr(serialize, "id", lambda _obj: 0, raising=False)
    assert getattr(serialize, "id") is not builtins.id
    cols = [_col("id"), _col("sku", nested_in="items", cardinality="one-to-many")]
    got = _rows([(1, "a"), (2, "a")], cols)
    assert got == [{"id": 1, "items": [{"sku": "a"}]}, {"id": 2, "items": [{"sku": "a"}]}]


def test_a_list_put_in_place_of_another_keeps_its_own_elements():
    """A parent field and a child relationship sharing a response key (owner.pets): each row
    puts a new list in place of the last; that list's element is its own."""
    cols = [
        _col("id"),
        _col("pets", nested_in="owner", cardinality="many-to-one"),
        _col("name", nested_in="owner.pets", cardinality="one-to-many"),
    ]
    got = _rows([(1, "p", "rex"), (1, "p", "rex")], cols)
    assert got == [{"id": 1, "owner": {"pets": [{"name": "rex"}]}}]


# --- Defect 7: a response-cache hit answers as its miss ------------------------------------------


def _arrow_ipc_entry(table: pa.Table) -> dict:
    from provisa.cache.raw_sql import arrow_entry

    return arrow_entry(table.schema, table.to_batches())


def test_an_arrow_cache_hit_reads_an_interval_as_the_miss_does():
    from provisa.cache.raw_sql import entry_as_result

    mdn = pa.array([pa.MonthDayNano([14, 3, 5_000_000_000])], type=pa.month_day_nano_interval())
    hit = entry_as_result(_arrow_ipc_entry(pa.table({"wait": mdn})), None)
    assert _body(hit.rows, [_col("wait")]) == [{"wait": "P1Y2M3DT5S"}]


def test_an_arrow_cache_hit_reads_an_infinite_date_as_the_miss_does():
    from provisa.cache.raw_sql import entry_as_result

    days = pa.array([2**31 - 1, -(2**31 - 1), 0], type=pa.int32()).cast(pa.date32())
    hit = entry_as_result(_arrow_ipc_entry(pa.table({"d": days})), None)
    assert _body(hit.rows, [_col("d")]) == [
        {"d": "infinity"},
        {"d": "-infinity"},
        {"d": "1970-01-01"},
    ]


def test_a_rows_cache_hit_keeps_infinities_and_intervals():
    from provisa.cache.codec import decode_cache_payload, encode_cache_payload
    from provisa.cache.raw_sql import entry_as_result, rows_entry

    row = (
        infinite_date(1),
        infinite_timestamp(-1, aware=True),
        infinite_timestamp(1, aware=False),
        interval_value(14, 3, 5_000_000),
        timedelta(days=2),
        Decimal("12345678901234567890.12"),
    )
    cols = [_col(c) for c in ("d", "tz", "ts", "i", "td", "n")]
    miss = _body([row], cols)
    entry = decode_cache_payload(
        encode_cache_payload(rows_entry([row], [c.field_name for c in cols]))
    )
    hit = entry_as_result(entry, None)
    assert _body(hit.rows, cols) == miss
    assert miss == [
        {
            "d": "infinity",
            "tz": "-infinity",
            "ts": "infinity",
            "i": "P1Y2M3DT5S",
            "td": "P2D",
            "n": "12345678901234567890.12",
        }
    ]


# --- A float a hot row or an action's row holds stays a float ------------------------------------


def test_a_float_in_a_values_cte_is_read_as_a_double_not_a_decimal():
    """Hot-table and action rows reach the engine as a VALUES CTE. A bare ``2.0`` is a numeric
    literal the engine reads as a DECIMAL, which would answer as the string "2.0"."""
    import duckdb
    import sqlglot

    from provisa.cache.values_cte import build_values_cte_sql

    entry = SimpleNamespace(column_names=["id", "bonus"], rows=[{"id": 1, "bonus": 2.0}])
    sql = build_values_cte_sql('SELECT "id", "bonus" FROM "t"', "t", entry)
    con = duckdb.connect()
    rel = con.sql(sqlglot.transpile(sql, read="postgres", write="duckdb")[0])
    assert [str(t) for t in rel.types] == ["INTEGER", "DOUBLE"]
    result = QueryResult(rows=rel.fetchall(), column_names=["id", "bonus"])
    assert result_records(result) == [{"id": 1, "bonus": 2.0}]


# --- The other JSON surfaces write the same values -----------------------------------------------


def test_ndjson_writes_the_values_the_json_answer_writes():
    from provisa.executor.formats.ndjson import rows_to_ndjson

    row = (Decimal("1234567890123456.78"), infinite_date(1), '{"a": 1}', '{"a": 1}')
    cols = [_col("amount"), _col("day"), _col("note"), _col("prefs", json_=True)]
    line = json.loads(rows_to_ndjson([row], cols))
    assert line == {
        "amount": "1234567890123456.78",
        "day": "infinity",
        "note": '{"a": 1}',
        "prefs": {"a": 1},
    }


async def test_mcp_run_sql_writes_the_values_the_json_answer_writes(monkeypatch):
    import provisa.pgwire._pipeline as pipeline
    from provisa.api.mcp import tools

    answered = QueryResult(
        rows=[(Decimal("1234567890123456.78"), infinite_date(-1), '{"a": 1}', '{"a": 1}')],
        column_names=["amount", "day", "note", "prefs"],
        column_types=["numeric", "date", "text", "jsonb"],
    )

    async def _batch(sql, role, state, **_kw):
        return answered

    monkeypatch.setattr(pipeline, "execute_sql_batch", _batch)
    monkeypatch.setattr(tools, "require_role", lambda role, state: role)
    monkeypatch.setattr(tools, "_max_rows", lambda: 100)
    got = await tools.run_sql(SimpleNamespace(), "analyst", "SELECT 1")
    assert got["rows"] == [
        {"amount": "1234567890123456.78", "day": "-infinity", "note": '{"a": 1}', "prefs": {"a": 1}}
    ]


# --- J497b decision 1: PostgreSQL's json and jsonb are read as the text PostgreSQL wrote ----------

#: jsonb values whose text a re-parse would misread: a bare JSON string reads as the value it
#: spells ("5" as 5, "[1]" as a list), and a number's digits past a float are lost.
_PG_JSON_TEXTS = ['"5"', '"[1]"', '"hello"', "[1]", '{"a": 1.10}', "0.12345678901234567890123"]


@pytest.fixture
def pg_cursor(embedded_postgres):
    import psycopg2

    con = psycopg2.connect(embedded_postgres.dsn())
    try:
        yield con.cursor()
    finally:
        con.close()


class TestPostgresJsonIsText:
    @pytest.mark.parametrize("pg_type", ["json", "jsonb"])
    def test_the_pg_engine_reads_json_as_postgresqls_text(self, pg_cursor, pg_type):
        from provisa.federation.pg_runtime import _read_as_postgresql_writes

        _read_as_postgresql_writes(pg_cursor)
        literals = ", ".join(f"%s::{pg_type}" for _ in _PG_JSON_TEXTS)
        pg_cursor.execute(f"SELECT {literals}", _PG_JSON_TEXTS)
        got = pg_cursor.fetchone()
        pg_cursor.execute(f"SELECT {literals.replace(pg_type, f'{pg_type}::text')}", _PG_JSON_TEXTS)
        assert got == pg_cursor.fetchone()  # what PostgreSQL itself writes for each value

    def test_the_pg_engine_reads_a_json_array_as_postgresqls_texts(self, pg_cursor):
        from provisa.federation.pg_runtime import _read_as_postgresql_writes

        _read_as_postgresql_writes(pg_cursor)
        pg_cursor.execute("""SELECT ARRAY['"5"'::jsonb, '{"a": 1}'::jsonb]""")
        assert pg_cursor.fetchone() == (['"5"', '{"a": 1}'],)

    def test_a_root_jsonb_string_read_by_the_pg_engine_answers_as_its_string(self, pg_cursor):
        from provisa.federation.pg_runtime import _read_as_postgresql_writes

        _read_as_postgresql_writes(pg_cursor)
        pg_cursor.execute("""SELECT '"5"'::jsonb, '"[1]"'::jsonb, '"hello"'::jsonb""")
        cols = [_col(c, json_=True) for c in ("a", "b", "c")]
        assert _body([pg_cursor.fetchone()], cols) == [{"a": "5", "b": "[1]", "c": "hello"}]

    async def test_the_direct_driver_reads_json_as_postgresqls_text(self, embedded_postgres):
        from provisa.executor.drivers.postgresql import PostgreSQLDriver
        from tests.embedded_pg import DATABASE, PASSWORD, ROLE

        driver = PostgreSQLDriver()
        await driver.connect("127.0.0.1", embedded_postgres.port, DATABASE, ROLE, PASSWORD)
        try:
            literals = ", ".join("$%d::jsonb" % (i + 1) for i in range(len(_PG_JSON_TEXTS)))
            result = await driver.execute(f"SELECT {literals}", list(_PG_JSON_TEXTS))
        finally:
            await driver.close()
        assert list(result.rows[0]) == [
            '"5"',
            '"[1]"',
            '"hello"',
            "[1]",
            '{"a": 1.10}',
            "0.12345678901234567890123",
        ]

    def test_pgwire_writes_the_text_postgresql_sent(self, pg_cursor):
        from buenavista.core import BVType
        from buenavista.postgres import BVTYPE_TO_PGTYPE

        from provisa.federation.pg_runtime import _read_as_postgresql_writes

        _read_as_postgresql_writes(pg_cursor)
        pg_cursor.execute("""SELECT '"5"'::jsonb, '{"a": 1.10}'::json""")
        jsonb, json_ = pg_cursor.fetchone()
        assert BVTYPE_TO_PGTYPE[BVType.JSONB][1](jsonb) == '"5"'
        assert BVTYPE_TO_PGTYPE[BVType.JSON][1](json_) == '{"a": 1.10}'

    def test_flight_sends_the_text_postgresql_sent(self, pg_cursor):
        from provisa.executor.formats.arrow import rows_to_arrow_table
        from provisa.federation.pg_runtime import _read_as_postgresql_writes

        _read_as_postgresql_writes(pg_cursor)
        pg_cursor.execute("""SELECT '"5"'::jsonb UNION ALL SELECT '{"a": 1.10}'::jsonb""")
        table = rows_to_arrow_table(pg_cursor.fetchall(), [_col("doc")], result_types=["jsonb"])
        assert table.column(0).to_pylist() == ['"5"', '{"a": 1.10}']


# --- J497b decision 2: a number inside JSON text keeps its digits ---------------------------------


class TestJsonNumbersAreExact:
    def test_a_number_past_a_floats_digits_is_written_with_all_of_them(self):
        body = _document([('{"x": 0.12345678901234567890123}',)], [_col("p", json_=True)], "t")
        assert b"0.12345678901234567890123" in body
        got = json.loads(body)["data"]["t"][0]["p"]["x"]
        assert got == pytest.approx(0.12345678901234567890123, rel=1e-14)

    def test_a_root_json_number_is_written_with_its_digits(self):
        body = _document([("3.14159265358979323846264",)], [_col("p", json_=True)], "t")
        assert json.loads(body, parse_float=Decimal)["data"]["t"] == [
            {"p": Decimal("3.14159265358979323846264")}
        ]

    def test_a_numbers_own_spelling_is_kept(self):
        body = _document([("[1.10, 1E+5, -0.0, 7]",)], [_col("p", json_=True)], "t")
        assert b"[1.10,1E+5,-0.0,7]" in body

    def test_a_number_is_still_a_number_where_json_is_written_by_the_standard_library(self):
        (record,) = _rows([('{"x": 1.25}',)], [_col("p", json_=True)])
        again = json.loads(json.dumps(record, default=str))
        assert again["p"]["x"] == pytest.approx(1.25, rel=1e-14)

    def test_elements_that_differ_past_a_floats_digits_are_two_elements(self):
        cols = [
            _col("id"),
            _col("n", nested_in="items", cardinality="one-to-many", json_=True),
        ]
        rows = [(1, "0.10000000000000000001"), (1, "0.10000000000000000002")]
        body = _document(rows, cols, "t")
        assert b"0.10000000000000000001" in body and b"0.10000000000000000002" in body

    def test_ndjson_writes_a_json_number_with_its_digits(self):
        from provisa.executor.formats.ndjson import rows_to_ndjson

        text = rows_to_ndjson([('{"x": 0.12345678901234567890123}',)], [_col("p", json_=True)])
        assert "0.12345678901234567890123" in text


# --- J497b decision 3: an integer column is a JSON number, by its engine type ---------------------

_HUGE = 170141183460469231731687303715884105727  # HUGEINT's largest


class TestIntegersAreNumbers:
    @pytest.mark.parametrize(
        "engine_type",
        [
            "int2",
            "int4",
            "int8",
            "smallint",
            "integer",
            "bigint",
            "TINYINT",
            "HUGEINT",
            "UHUGEINT",
            "UBIGINT",
        ],
    )
    def test_records_write_an_integer_column_as_a_number(self, engine_type):
        result = QueryResult(
            rows=[(Decimal(_HUGE),)], column_names=["n"], column_types=[engine_type]
        )
        assert json.loads(dumps(result_records(result))) == [{"n": _HUGE}]

    def test_a_decimal_column_with_no_fraction_stays_a_string(self):
        result = QueryResult(
            rows=[(Decimal(_HUGE),)], column_names=["n"], column_types=["DECIMAL(38,0)"]
        )
        assert json.loads(dumps(result_records(result))) == [{"n": str(_HUGE)}]

    def test_a_duckdb_hugeint_read_through_arrow_is_a_number(self):
        import duckdb

        from provisa.executor.drivers.duckdb_driver import _arrow_result

        con = duckdb.connect()
        # The driver reads the result through Arrow (J496), where a HUGEINT is its own bytes.
        res = con.execute(
            f"SELECT {_HUGE}::HUGEINT AS h, 12::DECIMAL(38,0) AS d, DATE '2020-01-01' AS day"
        )
        types = [str(d[1]) for d in res.description]
        result = _arrow_result(res)
        result.column_types = types
        assert json.loads(dumps(result_records(result))) == [
            {"h": _HUGE, "d": "12", "day": "2020-01-01"}
        ]

    def test_an_integer_column_in_graphql_is_a_number(self):
        got = _body([(Decimal(_HUGE), Decimal(_HUGE))], [_col("h", integer=True), _col("d")])
        assert got == [{"h": _HUGE, "d": str(_HUGE)}]

    def test_the_compiler_marks_integer_columns_by_their_registered_type(self):
        from graphql import parse

        from provisa.compiler.sql_gen import compile_query

        doc = parse("{ customers { id visits balance score orders { id } } }")
        (compiled,) = compile_query(doc, _ctx(), None)
        marks = {c.field_name: c.integer for c in compiled.columns if c.nested_in is None}
        assert marks == {
            "id": True,
            "visits": True,
            "balance": False,
            "score": False,
            "orders": False,
        }

    def test_an_integer_aggregate_of_an_integer_column_is_a_number(self):
        from graphql import parse

        from provisa.compiler.sql_gen import compile_query

        doc = parse(
            "{ customers_aggregate { aggregate { count sum { visits balance } "
            "max { visits } avg { visits } } } }"
        )
        (compiled,) = compile_query(doc, _ctx(), None)
        marks = {(c.nested_in, c.field_name): c.integer for c in compiled.columns}
        assert marks[("aggregate.sum", "visits")] is True
        assert marks[("aggregate.max", "visits")] is True
        assert marks[("aggregate.sum", "balance")] is False
        assert marks[("aggregate.avg", "visits")] is False
        got = json.loads(
            dumps(
                serialize_aggregate(
                    [tuple(Decimal(_HUGE) for _ in compiled.columns)], compiled.columns, "t"
                )
            )
        )["data"]["t"]["aggregate"]
        assert got["sum"] == {"visits": _HUGE, "balance": str(_HUGE)}


# --- J497b decision 4: a ->> leaf is text, as PostgreSQL's ->> returns text -----------------------


def test_a_json_path_leaf_is_text_even_when_its_value_is_an_array():
    from graphql import parse

    from provisa.compiler.sql_gen import compile_query

    ctx = _ctx(customer_columns=["id", {"column_name": "prefs", "alias": "tags", "path": "tags"}])
    (compiled,) = compile_query(parse("{ customers { id tags } }"), ctx, None)
    assert "->>'tags'" in compiled.sql
    tags = next(c for c in compiled.columns if c.field_name == "tags")
    assert not tags.json
    # PostgreSQL's ->> of an array-valued key is the array's text
    got = _body([(1, '["a", "b"]')], compiled.columns)
    assert got == [{"id": 1, "tags": '["a", "b"]'}]


# --- J497b decision 5: a keyset cursor carries a DECIMAL sort key exactly -------------------------


class TestDecimalCursor:
    def _connection(self, value):
        from provisa.compiler.sql_types import CompiledQuery
        from provisa.executor.serialize import serialize_connection

        compiled = CompiledQuery(
            sql="",
            params=[],
            root_field="t",
            columns=[_col("amount"), _col("id")],
            sources=set(),
            sort_columns=["amount", "id"],
            page_size=1,
        )
        return serialize_connection([(value, 7)], compiled)["data"]["t"]

    def test_the_cursor_holds_the_decimals_exact_digits(self):
        from provisa.compiler.cursor import decode_cursor

        page = self._connection(Decimal("12345678901234567.89"))
        values = decode_cursor(page["edges"][0]["cursor"])
        assert values == [Decimal("12345678901234567.89"), 7]
        assert isinstance(values[0], Decimal)
        assert page["edges"][0]["node"]["amount"] == "12345678901234567.89"

    def test_the_keyset_comparison_binds_the_decimal(self):
        from provisa.compiler.cursor import apply_cursor_pagination
        from provisa.compiler.params import ParamCollector

        page = self._connection(Decimal("0.10000000000000000001"))
        collector = ParamCollector()
        where, _, _ = apply_cursor_pagination(
            {"first": 1, "after": page["pageInfo"]["endCursor"]}, ["amount", "id"], collector, None
        )
        assert where == '("amount", "id") > ($1, $2)'
        assert collector.params == [Decimal("0.10000000000000000001"), 7]
        assert isinstance(collector.params[0], Decimal)

    def test_a_cursor_with_no_decimal_is_the_list_it_was(self):
        from provisa.compiler.cursor import decode_cursor, encode_cursor

        assert decode_cursor(encode_cursor([42, "x"])) == [42, "x"]
        assert json.loads(__import__("base64").b64decode(encode_cursor([42, "x"]))) == [42, "x"]
