# Copyright (c) 2026 Kenneth Stott
# Canary: a7c2e915-4d38-4b06-9f1e-63b0d8a52c47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Arrow type each read path gives each kind of column (REQ-1961).

A governed read reaches a client as Arrow by one of four paths, and each typed its columns its
own way. This file states, column kind by column kind, what each path answers — captured from
the code as it stood before the paths were brought to one rule, so that every change to a path
shows here as a changed expectation and nothing changes unseen.

The four paths:

- **row stream** — a source read as rows and packed into Arrow as it streams
  (``federation.runtime_support.arrow_batches_from_rows``; Flight over a direct route). ON THE
  ONE RULE since REQ-1961: given the governed statement's result shape, each column is read as
  its registered type; the "today" cases below are what it does without a shape;
- **materialised** — a whole row result turned into a table
  (``executor.formats.arrow.rows_to_arrow_table``; Flight's buffered answers). ON THE ONE RULE
  since REQ-1961, as the row stream is;
- **engine stream** — the engine's own Arrow reader
  (``federation.runtime_support.declared_arrow_stream``; Flight and Airport over an engine
  route). ON THE ONE RULE since REQ-1961: a registered column the engine typed otherwise is
  cast to its registered type; a stream in which nothing differs is passed through;
- **Airport** — a direct scan typed from the driver's column types
  (``api.airport.query._direct_typed_schema``).

Where the paths DISAGREE today (the findings; the one rule's answer is the registered type):

1. A numeric column that is not a registered column (a computed one): every path gives
   ``decimal128(p, s)`` when the result's own column type carries the size, and ``double``
   when it does not (#202: it was ``decimal128(38, s)`` with ``s`` read off the first batch's
   values on the row stream, and ``double`` whatever the driver said on the other two).
2. A 32-bit integer column: the row stream and the materialised path give ``int64`` (inferred
   from Python ints); Airport gives ``int32`` (from the driver's type).
3. A result with no rows: the row stream and the materialised path give every column the
   ``null`` type; Airport gives the same types as a result with rows.
"""

# Requirements: REQ-1961

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pyarrow as pa
import pytest

from provisa.api.airport.query import _direct_typed_schema
from provisa.compiler.result_shape import RegisteredType
from provisa.compiler.sql_gen import ColumnRef
from provisa.executor.formats.arrow import rows_to_arrow_table
from provisa.federation.runtime_support import arrow_batches_from_rows

# One result, a column of every kind: its name, the type name a Postgres driver reports for it
# and the one DuckDB reports, and two values.
_COLUMNS = [
    ("i", "int4", "INTEGER", [1, 2]),
    ("b", "int8", "BIGINT", [10**12, None]),
    ("money", "numeric", "DECIMAL(18,2)", [Decimal("12.50"), Decimal("0.10")]),
    ("ratio", "numeric", "DECIMAL", [Decimal("0.125"), Decimal("3")]),
    ("f", "float8", "DOUBLE", [1.5, 2.5]),
    ("t", "text", "VARCHAR", ["a", "b"]),
    ("ts", "timestamp", "TIMESTAMP", [dt.datetime(2026, 1, 2, 3, 4, 5), None]),
    ("d", "date", "DATE", [dt.date(2026, 1, 2), dt.date(2026, 1, 3)]),
    ("ok", "bool", "BOOLEAN", [True, False]),
]
_NAMES = [c[0] for c in _COLUMNS]
_ROWS = [tuple(c[3][r] for c in _COLUMNS) for r in range(2)]
_DRIVER_TYPES = {
    "postgres": [c[1] for c in _COLUMNS],
    "duckdb": [c[2] for c in _COLUMNS],
    "none": None,
}

# The columns every path agrees on, whatever reported their types.
_AGREED = {
    "b": pa.int64(),
    "f": pa.float64(),
    "t": pa.string(),
    "ts": pa.timestamp("us"),
    "d": pa.date32(),
    "ok": pa.bool_(),
}
_NULLS = dict.fromkeys(_NAMES, pa.null())


class _Stream:
    """A lazy row result, as a direct-route driver hands one over."""

    def __init__(self, rows: list[tuple], types: list[str] | None) -> None:
        self._rows = rows
        self.column_names = _NAMES
        self.column_types = types

    def iter_rows(self):
        return iter(self._rows)


def _types(schema: pa.Schema) -> dict[str, pa.DataType]:
    return {field.name: field.type for field in schema}


# --- the row stream --------------------------------------------------------------------------------

# The same result's columns as the model registers them: what the governed statement's result
# shape (compiler.result_shape) says each one is.
_REGISTERED = [
    (name, RegisteredType(declared))
    for name, declared in [
        ("i", "integer"),
        ("b", "bigint"),
        ("money", "numeric(18,2)"),
        ("ratio", "numeric"),
        ("f", "double"),
        ("t", "varchar"),
        ("ts", "timestamp"),
        ("d", "date"),
        ("ok", "boolean"),
    ]
]
# The one rule (REQ-1961): the registered type, on every path. An integer is 32 bits; a numeric
# is exact where the model records its size and floating point where it does not.
_BY_THE_RULE = {
    **_AGREED,
    "i": pa.int32(),
    "money": pa.decimal128(18, 2),
    "ratio": pa.float64(),
}


@pytest.mark.parametrize("reported", ["postgres", "duckdb", "none"])
@pytest.mark.parametrize("rows", [_ROWS, []], ids=["rows", "no rows"])
def test_the_row_stream_reads_each_column_as_its_registered_type(reported, rows):
    """Whatever the driver reported, whatever the first rows held, and with no rows at all."""
    schema, batches = arrow_batches_from_rows(
        _Stream(rows, _DRIVER_TYPES[reported]), shape=_REGISTERED
    )
    assert _types(schema) == _BY_THE_RULE
    for batch in batches:
        assert batch.schema == schema


def test_an_undeclared_numeric_is_floating_point_and_says_what_that_costs():
    """0.1 + 0.2 as decimals is exactly 0.3. Read as floating point it is approximately 0.3 —
    here the double Arrow's column-wise cast produces, which is not even the double nearest
    0.3. Declare the column's precision and scale for exact values."""
    rows = [(Decimal("0.1") + Decimal("0.2"),)]
    exact = Decimal("0.3")

    class _One(_Stream):
        def __init__(self) -> None:
            self._rows, self.column_names, self.column_types = rows, ["v"], ["numeric"]

    schema, batches = arrow_batches_from_rows(_One(), shape=[("v", RegisteredType("numeric"))])
    (value,) = next(batches).column(0).to_pylist()
    assert schema.field("v").type == pa.float64()
    assert isinstance(value, float) and value == pytest.approx(0.3, abs=1e-15)
    assert Decimal(value) != exact, "a double cannot hold 0.3"
    schema, batches = arrow_batches_from_rows(
        _One(), shape=[("v", RegisteredType("numeric(10,1)"))]
    )
    assert schema.field("v").type == pa.decimal128(10, 1)
    assert next(batches).column(0).to_pylist() == [exact]


def test_values_that_cannot_be_read_as_the_registered_type_are_refused_naming_the_column():
    """Three digits after the point in a column registered with two: never rounded silently.
    The refusal is one of the read refusals every surface answers in one place, with a stable
    code, what it is about, and what a registrar does about it."""
    from provisa.core.read_refusal import ReadRefused
    from provisa.federation.runtime_support import ColumnTypeMismatch

    rows = [tuple(Decimal("12.505") if n == "money" else v for n, v in zip(_NAMES, _ROWS[0]))]
    with pytest.raises(ColumnTypeMismatch) as refused:
        _schema, batches = arrow_batches_from_rows(_Stream(rows, None), shape=_REGISTERED)
        list(batches)
    assert isinstance(refused.value, ReadRefused)
    assert refused.value.code == "query.column_type_mismatch"
    assert refused.value.params == {
        "column": "money",
        "registered": "decimal128(18, 2)",
        "found": "decimal128(38, 3)",
    }
    assert "Register the column with the type its source returns" in str(refused.value)


def test_a_column_the_shape_does_not_type_keeps_the_type_its_values_give_it():
    """No registered type by that name, or JSON (REQ-1965: a JSON column's read form is its
    reader's): read off the values, as without a shape."""
    shape = [
        ("t", RegisteredType("json")),
        ("ok", RegisteredType("boolean")),
    ]  # "b" is not in it
    schema, _batches = arrow_batches_from_rows(_Stream(_ROWS, None), shape=shape)
    types = _types(schema)
    assert types["i"] == pa.int64() and types["b"] == pa.int64() and types["t"] == pa.string()
    assert types["ok"] == pa.bool_()


def test_a_computed_column_keeps_the_type_its_engine_gives_it():
    """The model declares columns, not expressions. ``amount * amount`` over numeric(18,2) is
    four digits after the point on the engine; SQLGlot annotates it ``DECIMAL(18, 2)``. Typing
    it by that annotation would refuse a correct answer, so a computed column is not typed by
    the shape: the size its engine reports for it decides."""
    rows = [(Decimal("156.2500"), Decimal("12.50"))]

    class _Two(_Stream):
        def __init__(self) -> None:
            self._rows, self.column_names = rows, ["sq", "amount"]
            self.column_types = ["DECIMAL(18,4)", "DECIMAL(18,2)"]

    shape = [("sq", "DECIMAL(18, 2)"), ("amount", RegisteredType("numeric(18,2)"))]
    schema, batches = arrow_batches_from_rows(_Two(), shape=shape)
    assert schema.field("sq").type == pa.decimal128(18, 4)
    assert schema.field("amount").type == pa.decimal128(18, 2)
    assert next(batches).column(0).to_pylist() == [Decimal("156.2500")]


# --- a computed decimal with no reported size (#202) -----------------------------------------------


class _Quotients(_Stream):
    """A computed decimal as a PostgreSQL direct route reports it: ``numeric``, no size. The
    later rows have more digits after the point than the first."""

    def __init__(self, declared: str | None = "numeric") -> None:
        self._rows = [(Decimal("1.5"),), (Decimal("0.333333"),), (Decimal("0.3"),)]
        self.column_names = ["q"]
        self.column_types = [declared] if declared else None


@pytest.mark.parametrize("declared", ["numeric", None])
def test_a_later_batch_with_more_digits_than_the_first_does_not_fail_the_read(declared):
    """#202: the column was typed decimal128(38, 1) by its first batch, and the second batch's
    0.333333 failed the read with "Rescaling Decimal value would cause data loss". A decimal
    with no size is floating point — the rule for a registered numeric with none — so its type
    depends on no row; each value is its nearest double."""
    schema, batches = arrow_batches_from_rows(_Quotients(declared), batch_rows=1)
    assert schema.field("q").type == pa.float64()
    assert [b.column(0).to_pylist() for b in batches] == [[1.5], [0.333333], [0.3]]


def test_a_computed_decimal_is_kept_exact_by_stating_its_size():
    """The statement's CAST carries a size, and the column is read exact at it — whatever the
    driver reports (PostgreSQL still says ``numeric``) and however the rows are batched."""
    from provisa.compiler.result_shape import StatedType

    shape = [("q", StatedType("DECIMAL(12, 6)"))]
    schema, batches = arrow_batches_from_rows(_Quotients(), batch_rows=1, shape=shape)
    assert schema.field("q").type == pa.decimal128(12, 6)
    assert [b.column(0).to_pylist() for b in batches] == [
        [Decimal("1.500000")],
        [Decimal("0.333333")],
        [Decimal("0.300000")],
    ]


def test_the_shape_says_which_columns_the_statement_states_the_type_of():
    from provisa.compiler.result_shape import StatedType
    from tests.unit.test_result_shape import _shape

    shape = dict(
        _shape(
            "SELECT CAST(o.amount / 3 AS NUMERIC(12, 6)) AS q, o.amount / 3 AS loose, "
            "CAST(o.amount AS DOUBLE PRECISION) AS f FROM sales.orders o"
        )
    )
    assert isinstance(shape["q"], StatedType) and shape["q"] == "DECIMAL(12, 6)"
    assert not isinstance(shape["loose"], (StatedType, RegisteredType))
    assert isinstance(shape["f"], StatedType)  # stated, and not a sized numeric: not held to it


def test_every_path_types_an_unsized_computed_decimal_alike():
    """The materialised table and Airport follow the row stream: exact where the driver reports
    a size, floating point where it does not. (The engine's own stream always carries a size.)"""
    from provisa.api.airport.query import _typed_batches_from_rows

    refs = [ColumnRef(alias=None, column="q", field_name="q", nested_in=None)]
    rows = list(_Quotients().iter_rows())
    for declared, expected in (("numeric", pa.float64()), ("DECIMAL(12,6)", pa.decimal128(12, 6))):
        stream_schema, _ = arrow_batches_from_rows(_Quotients(declared))
        table = rows_to_arrow_table(rows, refs, result_types=[declared])
        airport = _direct_typed_schema(["q"], [declared])
        (batch,) = _typed_batches_from_rows(_Quotients(declared), airport)
        assert (
            stream_schema.field("q").type
            == table.schema.field("q").type
            == airport.field("q").type
            == batch.schema.field("q").type
            == expected
        )


def test_a_registered_column_is_typed_beside_a_column_whose_type_cannot_be_derived():
    """Each column on its own: the underivable one keeps its values' type, the registered one
    beside it is read as the model declares."""
    from provisa.compiler.result_shape import Underived

    rows = [("x", Decimal("12.50"))]

    class _Two(_Stream):
        def __init__(self) -> None:
            self._rows, self.column_names, self.column_types = rows, ["d", "amount"], None

    shape = [("d", Underived("why")), ("amount", RegisteredType("numeric(18,2)"))]
    schema, _batches = arrow_batches_from_rows(_Two(), shape=shape)
    assert schema.field("d").type == pa.string()
    assert schema.field("amount").type == pa.decimal128(18, 2)


def test_the_shape_marks_which_result_columns_are_registered_columns():
    from tests.unit.test_result_shape import _shape

    shape = dict(
        _shape(
            "SELECT o.order_id, o.amount AS total, o.amount * o.amount AS sq, "
            "sum(o.amount) OVER () AS running FROM sales.orders o"
        )
    )
    assert isinstance(shape["order_id"], RegisteredType)
    assert isinstance(shape["total"], RegisteredType) and shape["total"] == "numeric(18,2)"
    assert not isinstance(shape["sq"], RegisteredType)
    assert not isinstance(shape["running"], RegisteredType)
    (star,) = [_shape("SELECT * FROM sales.orders")]
    assert all(isinstance(declared, RegisteredType) for _name, declared in star)


@pytest.mark.parametrize(
    "reported, money",
    [
        # The size comes from the RESULT's column type, when it carries one...
        ("duckdb", pa.decimal128(18, 2)),
        # ...and a decimal with no size is floating point (#202), never sized by the rows.
        ("postgres", pa.float64()),
        ("none", pa.float64()),
    ],
)
def test_the_row_stream_without_a_shape(reported, money):
    """A read that is not a governed plan's (the engine seam's own row adapter) has no shape."""
    schema, _batches = arrow_batches_from_rows(_Stream(_ROWS, _DRIVER_TYPES[reported]))
    assert _types(schema) == {
        **_AGREED,
        "i": pa.int64(),  # finding 2: inferred, not the column's 32 bits
        "money": money,  # finding 1
        "ratio": pa.float64(),  # finding 1: no size reported
    }


@pytest.mark.parametrize("reported", ["postgres", "duckdb", "none"])
def test_the_row_stream_of_a_result_with_no_rows_without_a_shape(reported):
    schema, _batches = arrow_batches_from_rows(_Stream([], _DRIVER_TYPES[reported]))
    assert _types(schema) == _NULLS  # finding 3


# --- the materialised table ------------------------------------------------------------------------


def _column_refs() -> list[ColumnRef]:
    return [ColumnRef(alias=None, column=n, field_name=n, nested_in=None) for n in _NAMES]


@pytest.mark.parametrize("rows", [_ROWS, []], ids=["rows", "no rows"])
def test_the_materialised_table_reads_each_registered_column_as_its_registered_type(rows):
    """ON THE ONE RULE since REQ-1961: the same answer as the row stream, with or without rows."""
    table = rows_to_arrow_table(rows, _column_refs(), shape=_REGISTERED)
    assert _types(table.schema) == _BY_THE_RULE
    if rows:
        assert table.column("money").to_pylist() == [Decimal("12.50"), Decimal("0.10")]


def test_the_materialised_table_matches_the_shape_by_the_results_own_names():
    """A compiled statement's table is named by its fields; its shape by the SQL result's names."""
    refs = [ColumnRef(alias=None, column="amt", field_name="totalAmount", nested_in=None)]
    table = rows_to_arrow_table(
        [(Decimal("12.50"),)],
        refs,
        shape=[("amt", RegisteredType("numeric(18,2)"))],
        result_names=["amt"],
    )
    assert _types(table.schema) == {"totalAmount": pa.decimal128(18, 2)}


def test_the_materialised_table_without_a_shape():
    """A result that is not a governed statement's (a command's rows) has no shape; with no
    driver types either, a decimal has no size and is floating point."""
    assert _types(rows_to_arrow_table(_ROWS, _column_refs()).schema) == {
        **_AGREED,
        "i": pa.int64(),  # finding 2
        "money": pa.float64(),  # finding 1
        "ratio": pa.float64(),
    }


def test_the_materialised_table_of_a_result_with_no_rows_without_a_shape():
    assert _types(rows_to_arrow_table([], _column_refs()).schema) == _NULLS  # finding 3


# --- the engine's own stream ------------------------------------------------------------------------


def _engine_stream(rows: bool = True):
    """As an engine reads the columns: its integer 64-bit, its decimals sized its own way."""
    schema = pa.schema(
        [("i", pa.int64()), ("money", pa.decimal128(38, 2)), ("ratio", pa.decimal128(38, 3))]
    )
    data = [[1, 2], [Decimal("12.50"), Decimal("0.10")], [Decimal("0.500"), Decimal("0.250")]]
    batches = [
        pa.RecordBatch.from_arrays(
            [pa.array(c, t) for c, t in zip(data, schema.types)], schema=schema
        )
    ]
    return schema, iter(batches if rows else [])


@pytest.mark.parametrize("rows", [True, False], ids=["rows", "no rows"])
def test_the_engine_stream_reads_each_registered_column_as_its_registered_type(rows):
    from provisa.federation.runtime_support import declared_arrow_stream

    schema, batches = declared_arrow_stream(*_engine_stream(rows), _REGISTERED)
    assert _types(schema) == {
        "i": pa.int32(),
        "money": pa.decimal128(18, 2),
        "ratio": pa.float64(),
    }
    got = list(batches)
    assert all(batch.schema == schema for batch in got) and len(got) == (1 if rows else 0)
    if rows:
        assert got[0].column(1).to_pylist() == [Decimal("12.50"), Decimal("0.10")]
        assert got[0].column(2).to_pylist() == [0.5, 0.25]


def test_an_engine_stream_already_as_declared_is_passed_through_untouched():
    from provisa.federation.runtime_support import declared_arrow_stream

    schema = pa.schema([("i", pa.int32()), ("sq", pa.decimal128(18, 4))])
    batches = iter([])
    shape = [("i", RegisteredType("integer")), ("sq", "DECIMAL(18, 2)")]  # sq is computed
    same_schema, same_batches = declared_arrow_stream(schema, batches, shape)
    assert same_schema is schema and same_batches is batches
    assert declared_arrow_stream(schema, batches, None) == (schema, batches)


def test_an_engine_value_that_is_not_the_registered_type_is_refused_naming_the_column():
    from provisa.federation.runtime_support import ColumnTypeMismatch, declared_arrow_stream

    schema = pa.schema([("i", pa.int64())])
    batch = pa.RecordBatch.from_arrays([pa.array([2**40], pa.int64())], schema=schema)
    _schema, batches = declared_arrow_stream(
        schema, iter([batch]), [("i", RegisteredType("integer"))]
    )
    with pytest.raises(ColumnTypeMismatch) as refused:
        list(batches)
    assert refused.value.params == {"column": "i", "registered": "int32", "found": "int64"}


def test_the_engine_streams_of_flight_and_airport_are_read_as_declared():
    import inspect

    from provisa.api.airport import query
    from provisa.api.flight import server

    assert (
        "declared_arrow_stream(\n                arrow_schema, batch_gen, self._result_shape_of(plan)"
        in inspect.getsource(server)
    )
    assert inspect.getsource(query).count("= _declared_engine_stream(state, plan)") == 2


# --- Airport ---------------------------------------------------------------------------------------


@pytest.mark.parametrize("reported", ["postgres", "duckdb"])
def test_airport_reads_each_registered_column_as_its_registered_type(reported):
    """ON THE ONE RULE since REQ-1961: the schema a scan advertises and streams is the model's
    for a registered column, whatever the driver reported, with or without rows."""
    from provisa.api.airport.query import _typed_batches_from_rows
    from provisa.federation.runtime_support import read_targets

    declared = read_targets(_NAMES, _REGISTERED)
    typed = _direct_typed_schema(_NAMES, _DRIVER_TYPES[reported], declared)
    assert _types(typed) == _BY_THE_RULE
    (batch,) = _typed_batches_from_rows(
        _Stream(_ROWS, _DRIVER_TYPES[reported]), typed, declared=declared
    )
    assert batch.schema == typed
    assert batch.column(_NAMES.index("money")).to_pylist() == [Decimal("12.50"), Decimal("0.10")]


def test_airport_refuses_values_that_are_not_the_registered_type():
    from provisa.api.airport.query import _typed_batches_from_rows
    from provisa.federation.runtime_support import ColumnTypeMismatch, read_targets

    declared = read_targets(_NAMES, _REGISTERED)
    typed = _direct_typed_schema(_NAMES, _DRIVER_TYPES["postgres"], declared)
    rows = [tuple(Decimal("12.505") if n == "money" else v for n, v in zip(_NAMES, _ROWS[0]))]
    with pytest.raises(ColumnTypeMismatch) as refused:
        list(_typed_batches_from_rows(_Stream(rows, None), typed, declared=declared))
    assert refused.value.column == "money"


@pytest.mark.parametrize("reported", ["postgres", "duckdb"])
def test_airport_without_declared_types(reported):
    """A column that is not a registered column is typed from the driver's column type alone:
    a decimal exact where the driver reports its size, floating point where it does not — the
    row stream's rule."""
    assert _types(_direct_typed_schema(_NAMES, _DRIVER_TYPES[reported])) == {
        **_AGREED,
        "i": pa.int32(),
        "money": pa.decimal128(18, 2) if reported == "duckdb" else pa.float64(),
        "ratio": pa.float64(),
    }


def test_airports_scan_takes_its_declared_types_from_the_plans_result_shape():
    import inspect

    from provisa.api.airport import query

    source = inspect.getsource(query)
    assert "read_targets(column_names, None if kept is None else kept.now())" in source
    # its definition, the stream, and both schema probes
    assert source.count("_declared_types(plan, ") == 4


def test_airport_refuses_a_scan_whose_driver_reports_no_types():
    with pytest.raises(ValueError, match="cannot form a byte-stable schema"):
        _direct_typed_schema(_NAMES, None)


# --- what the paths agree on -----------------------------------------------------------------------


def test_the_columns_every_path_types_alike():
    """bigint, double, text, timestamp, date and boolean: one answer on all three paths that
    type a result, which is what the one rule must keep."""
    row_stream, _ = arrow_batches_from_rows(_Stream(_ROWS, _DRIVER_TYPES["postgres"]))
    materialised = rows_to_arrow_table(_ROWS, _column_refs()).schema
    airport = _direct_typed_schema(_NAMES, _DRIVER_TYPES["postgres"])
    for name, arrow_type in _AGREED.items():
        answers = {
            row_stream.field(name).type,
            materialised.field(name).type,
            airport.field(name).type,
        }
        assert answers == {arrow_type}, name


# --- the one rule ----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "registered, arrow_type",
    [
        ("integer", pa.int32()),
        ("int4", pa.int32()),
        ("smallint", pa.int16()),
        ("bigint", pa.int64()),
        ("real", pa.float32()),
        ("double precision", pa.float64()),
        ("numeric(18,2)", pa.decimal128(18, 2)),
        ("decimal(10,2)", pa.decimal128(10, 2)),
        ("numeric(50,4)", pa.decimal256(50, 4)),
        ("numeric", pa.float64()),
        ("decimal", pa.float64()),
        ("character varying(40)", pa.string()),
        ("timestamp without time zone", pa.timestamp("us")),
        ("boolean", pa.bool_()),
        ("json", pa.string()),
        ("array(varchar)", pa.string()),  # REQ-1965: nested is JSON
    ],
)
def test_the_arrow_type_of_a_registered_type(registered, arrow_type):
    from provisa.core.catalog_types import column_arrow_type

    assert column_arrow_type(registered) == arrow_type


@pytest.mark.parametrize("registered", [None, ""])
def test_a_column_with_no_registered_type_has_no_arrow_type_of_its_own(registered):
    from provisa.core.catalog_types import column_arrow_type

    assert column_arrow_type(registered) is None


@pytest.mark.parametrize("registered", ["USER-DEFINED", "hierarchyid"])
def test_a_type_naming_no_ir_type_is_read_as_text(registered):
    from provisa.core.catalog_types import column_arrow_type

    assert column_arrow_type(registered) == pa.string()  # REQ-2171


def test_flights_direct_stream_is_typed_by_the_plans_result_shape():
    """The call site: the governed statement's result shape, kept with the statement, is what
    the row adapter is handed."""
    import inspect

    from provisa.api.flight import server

    source = inspect.getsource(server)
    assert "return None if kept is None else kept.now()" in source  # no loop crossing
    assert "stream, shape=self._result_shape_of(plan)" in source  # the direct stream
    assert source.count("shape=self._result_shape_of(plan)") == 3  # and both buffered reads


# --- the catalog lists what a read returns -----------------------------------------------------------


def test_the_type_a_catalog_lists_for_a_column_is_the_type_a_read_of_it_returns():
    """REQ-1961's sentence, on every read path: one registered table, listed by the Flight
    catalog and read through the row stream, the materialised table, the engine's stream and an
    Airport scan."""
    from provisa.api.airport.query import _typed_batches_from_rows
    from provisa.api.flight.catalog import (
        CatalogColumn,
        CatalogTable,
        catalog_table_to_arrow_schema,
    )
    from provisa.federation.runtime_support import declared_arrow_stream, read_targets

    listed = _types(
        catalog_table_to_arrow_schema(
            CatalogTable(
                domain_id="d",
                table_name="t",
                description="",
                columns=[CatalogColumn(n, str(declared), True, "") for n, declared in _REGISTERED],
            )
        )
    )
    assert listed == _BY_THE_RULE

    row_stream, _ = arrow_batches_from_rows(_Stream(_ROWS, None), shape=_REGISTERED)
    materialised = rows_to_arrow_table(_ROWS, _column_refs(), shape=_REGISTERED).schema
    declared = read_targets(_NAMES, _REGISTERED)
    airport = _direct_typed_schema(_NAMES, _DRIVER_TYPES["postgres"], declared)
    (airport_batch,) = _typed_batches_from_rows(_Stream(_ROWS, None), airport, declared=declared)
    engine_schema, _batches = declared_arrow_stream(*_engine_stream(), _REGISTERED)
    for read in (row_stream, materialised, airport, airport_batch.schema):
        assert _types(read) == listed
    assert _types(engine_schema) == {name: listed[name] for name in engine_schema.names}


def test_a_numeric_read_as_the_floating_point_its_statement_states_is_typed_without_rows():
    """A numeric the model records no size for is read through CAST(... AS DOUBLE PRECISION), so
    the shape states its type: it is float64 whether or not rows arrive. The integration lane
    read a result of no rows with it as Arrow null (main 15bd4bf1b)."""
    from provisa.compiler.result_shape import StatedType

    class _Ledger(_Stream):
        def __init__(self, rows: list[tuple]) -> None:
            super().__init__(rows, ["int4", "float8"])
            self.column_names = ["id", "ratio"]

    shape = [("id", RegisteredType("integer")), ("ratio", StatedType("DOUBLE PRECISION"))]
    none = arrow_batches_from_rows(_Ledger([]), shape=shape)[0]
    some = arrow_batches_from_rows(_Ledger([(1, 0.5)]), shape=shape)[0]
    assert _types(none) == _types(some) == {"id": pa.int32(), "ratio": pa.float64()}
