# Copyright (c) 2026 Kenneth Stott
# Canary: 4b7e1c90-3f2a-4d58-9e61-a0c8d5b27f34
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1965 (amended 2026-11-11, TYPED ARRAYS ARE AN IR TYPE): a typed array is read in the
engine's own array type where the engine holds one (PostgreSQL, DuckDB), as its JSON form where
it holds none (Trino), and pgwire describes and sends it as the PostgreSQL array either way:
_int4 (1007), _text (1009), _float8 (1022), in PostgreSQL's array text. The statement is DBeaver's
#57 (scripts/conformance/bi_tools/dbeaver.yaml) over conformance.types_fixture."""

# Requirements: REQ-1965

from __future__ import annotations

import struct
from types import SimpleNamespace

import duckdb
import psycopg
import pyarrow as pa
import pytest
from buenavista.postgres import BVTYPE_TO_OID

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.native_nested import NestedColumn, mark_native_nested_reads
from provisa.compiler.result_shape import derive_result_shape, registered_type
from provisa.transpiler.transpile import transpile
from tests.unit.pgwire_encoder.python_ref import query_result, wired_encode

pytestmark = pytest.mark.unit

_DBEAVER_57 = "SELECT tf.* FROM conformance.types_fixture AS tf"
_PHYSICAL = 'SELECT "tf"."c_int4_array", "tf"."c_text_array", "tf"."c_float8_array" FROM "conformance"."types_fixture" AS "tf"'
_NESTED = {
    ("conformance", "types_fixture"): {
        "c_int4_array": NestedColumn("integer[]", 1, "integer"),
        "c_text_array": NestedColumn("text[]", 1, "text"),
        "c_float8_array": NestedColumn("double precision[]", 1, "double"),
    }
}
_NAMES = ["c_int4_array", "c_text_array", "c_float8_array"]
_OIDS = [1007, 1009, 1022]  # _int4, _text, _float8: what PostgreSQL 17 describes
_TEXT = [b"{1,2,3}", b"{a,b,c}", b"{1.5,2.5}"]  # array_out's text of the fixture's row


def _ctx() -> SimpleNamespace:
    meta = SimpleNamespace(
        table_id=1,
        domain_id="conformance",
        schema_name="conformance",
        table_name="types_fixture",
        original_table_name="types_fixture",
        display_name="types_fixture",
        field_name="conformance__types_fixture",
    )
    return SimpleNamespace(
        tables={"types_fixture": meta},
        physical_to_sql={},
        virtual_columns={},
        row_virtual_columns={},
        columns_outside_relation=set(),
        nested_columns=_NESTED,
    )


_COLUMNS = {
    1: [
        ColumnMetadata(
            column_name=name, data_type="json", is_nullable=True, native_type=nested.native_type
        )
        for name, nested in _NESTED[("conformance", "types_fixture")].items()
    ]
}


def _read_shape() -> list[tuple[str, str]]:
    """DBeaver #57's shape as a read types it (pgwire's ``_KeptResultShape``)."""
    tables = {"conformance.types_fixture": 1, "types_fixture": 1}
    return derive_result_shape(_DBEAVER_57, tables, _ctx(), _COLUMNS, each=True)


def _cells(data_rows: bytes) -> list[list[bytes | None]]:
    """Each DataRow's cells."""
    rows, at = [], 0
    while at < len(data_rows):
        assert data_rows[at : at + 1] == b"D"
        (length,) = struct.unpack("!i", data_rows[at + 1 : at + 5])
        end, at = at + 1 + length, at + 5
        (count,) = struct.unpack("!h", data_rows[at : at + 2])
        at += 2
        cells: list[bytes | None] = []
        for _ in range(count):
            (size,) = struct.unpack("!i", data_rows[at : at + 4])
            at += 4
            cells.append(None if size < 0 else data_rows[at : at + size])
            at += max(size, 0)
        assert at == end
        rows.append(cells)
    return rows


def _sent(batch: pa.RecordBatch, column_types: list[str]) -> tuple[list[int], list[list]]:
    """The OIDs pgwire's RowDescription names and the text cells its DataRows send."""
    shape = _read_shape()
    qr = query_result(batch.schema, [], column_types, None, sql=_DBEAVER_57, read_shape=shape)
    oids = [
        qr.column_type_oid(i) or BVTYPE_TO_OID[qr.column(i)[1]] for i in range(qr.column_count())
    ]
    out, _, _ = wired_encode(
        batch.schema, [batch], column_types, None, sql=_DBEAVER_57, read_shape=shape
    )
    return oids, _cells(out)


def test_dbeaver_57_reads_each_typed_array_as_its_array_type():
    assert _read_shape() == [
        ("c_int4_array", "integer[]"),
        ("c_text_array", "text[]"),
        ("c_float8_array", "double[]"),
    ]


# --- DuckDB: held natively ------------------------------------------------------------------


def test_duckdb_reads_a_typed_array_natively_and_pgwire_sends_it_as_the_array():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA conformance")
    con.execute(
        "CREATE TABLE conformance.types_fixture "
        "(c_int4_array INTEGER[], c_text_array VARCHAR[], c_float8_array DOUBLE[])"
    )
    con.execute("INSERT INTO conformance.types_fixture VALUES ([1,2,3], ['a','b','c'], [1.5,2.5])")
    sql = transpile(mark_native_nested_reads(_PHYSICAL, _NESTED), "duckdb", generated=False)
    assert "TO_JSON" not in sql.upper()

    relation = con.sql(sql)
    column_types = [str(t) for t in relation.types]
    (batch,) = relation.to_arrow_table().to_batches()

    assert batch.column(0).to_pylist() == [[1, 2, 3]]  # the engine's own list: no JSON text
    assert _sent(batch, column_types) == (_OIDS, [_TEXT])


def test_duckdb_keeps_an_array_the_wire_has_no_array_type_for_as_json():
    nested = {("s", "t"): {"v": NestedColumn("timestamp[]", 1, "timestamp")}}
    sql = transpile(
        mark_native_nested_reads('SELECT "t"."v" FROM "s"."t" AS "t"', nested),
        "duckdb",
        generated=False,
    )
    assert "TO_JSON(" in sql.upper()


# --- PostgreSQL: held natively --------------------------------------------------------------


def test_postgresql_reads_a_typed_array_natively_and_pgwire_sends_it_as_the_array(
    embedded_postgres,
):
    fixture = (
        "(SELECT ARRAY[1,2,3]::int4[] AS c_int4_array, ARRAY['a','b','c']::text[] AS c_text_array, "
        "ARRAY[1.5,2.5]::float8[] AS c_float8_array)"
    )
    sql = transpile(mark_native_nested_reads(_PHYSICAL, _NESTED), "postgres", generated=False)
    assert "TO_JSONB" not in sql.upper()
    with psycopg.connect(embedded_postgres.dsn()) as con:
        statement = sql.replace('"conformance"."types_fixture"', fixture)
        row = con.execute(statement).fetchone()  # type: ignore[misc]
    assert row == ([1, 2, 3], ["a", "b", "c"], [1.5, 2.5])  # the source's own arrays

    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([row[0]], pa.list_(pa.int32())),
            pa.array([row[1]], pa.list_(pa.string())),
            pa.array([row[2]], pa.list_(pa.float64())),
        ],
        names=_NAMES,
    )
    assert _sent(batch, ["_int4", "_text", "_float8"]) == (_OIDS, [_TEXT])


def test_postgresql_reads_a_float_array_with_nan_as_postgresql_does(embedded_postgres):
    nested = {("s", "t"): {"v": NestedColumn("double precision[]", 1, "double")}}
    sql = transpile(
        mark_native_nested_reads('SELECT "t"."v" FROM "s"."t" AS "t"', nested),
        "postgres",
        generated=False,
    )
    fixture = "(SELECT ARRAY[1.5, 'NaN']::float8[] AS v)"
    with psycopg.connect(embedded_postgres.dsn()) as con:
        (value,) = con.execute(sql.replace('"s"."t"', fixture)).fetchone()  # type: ignore[misc]
    assert value[0] == 1.5 and value[1] != value[1]  # NaN, as PostgreSQL answers it


# --- Trino: held as JSON, converted back ----------------------------------------------------


def test_a_json_only_engine_reads_the_json_form_and_pgwire_sends_it_as_the_array():
    sql = transpile(mark_native_nested_reads(_PHYSICAL, _NESTED), "trino", generated=False)
    assert sql.upper().count("AS JSON)") == 3

    batch = pa.RecordBatch.from_arrays(
        [pa.array(["[1,2,3]"]), pa.array(['["a","b","c"]']), pa.array(["[1.5,2.5]"])],
        names=_NAMES,
    )
    assert _sent(batch, ["json", "json", "json"]) == (_OIDS, [_TEXT])


def test_a_json_only_engine_sends_a_decimal_array_exactly():
    shape = [("v", registered_type("numeric(10,2)[]", "numeric(10,2)[]"))]
    batch = pa.RecordBatch.from_arrays([pa.array(['["12.30", null]'])], names=["v"])
    out, types, _ = wired_encode(batch.schema, [batch], ["json"], None, read_shape=shape)
    assert types == ["DECIMALARRAY"]
    assert _cells(out) == [[b"{12.30,NULL}"]]
