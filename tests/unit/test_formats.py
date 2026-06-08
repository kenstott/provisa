# Copyright (c) 2026 Kenneth Stott
# Canary: 8944fb3e-8cd6-47dc-92a1-d3deb680a931
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for output format serializers."""

import json
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import io

from provisa.compiler.sql_gen import ColumnRef
from provisa.executor.formats.ndjson import rows_to_ndjson
from provisa.executor.formats.tabular import rows_to_csv, rows_to_parquet
from provisa.executor.formats.arrow import rows_to_arrow_ipc, rows_to_arrow_table


def _cols():
    return [
        ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
        ColumnRef(alias=None, column="amount", field_name="amount", nested_in=None),
        ColumnRef(alias=None, column="name", field_name="name", nested_in=None),
    ]


ROWS = [
    (1, Decimal("19.99"), "Alice"),
    (2, Decimal("29.99"), "Bob"),
    (3, Decimal("0"), "Carol"),
]


class TestNDJSON:
    def test_basic(self):
        result = rows_to_ndjson(ROWS, _cols())
        lines = result.strip().split("\n")
        assert len(lines) == 3
        first = json.loads(lines[0])
        assert first["id"] == 1
        assert first["amount"] == 19.99
        assert first["name"] == "Alice"

    def test_empty(self):
        result = rows_to_ndjson([], _cols())
        assert result == ""

    def test_nested_columns(self):
        cols = [
            ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
            ColumnRef(alias="t1", column="name", field_name="name", nested_in="customer"),
        ]
        rows = [(1, "Alice")]
        result = rows_to_ndjson(rows, cols)
        obj = json.loads(result.strip())
        assert obj["id"] == 1
        assert obj["customer.name"] == "Alice"


class TestCSV:
    def test_basic(self):
        result = rows_to_csv(ROWS, _cols())
        lines = [l.strip() for l in result.strip().splitlines()]
        assert lines[0] == "id,amount,name"
        assert "Alice" in lines[1]
        assert len(lines) == 4  # header + 3 rows

    def test_empty(self):
        result = rows_to_csv([], _cols())
        lines = [l.strip() for l in result.strip().splitlines()]
        assert len(lines) == 1  # header only

    def test_json_string_cells_flattened(self):
        cols = [
            ColumnRef(alias=None, column="status", field_name="status", nested_in=None),
            ColumnRef(alias=None, column="user", field_name="user", nested_in=None),
            ColumnRef(alias=None, column="pet", field_name="pet", nested_in=None),
        ]
        rows = [
            ("closed", '{"name":"Alice Nguyen"}', '{"species":"lion","price":1600.0,"animalBreed":{"careLevel":"expert","avgLifespanYears":18}}'),
            ("open",   '{"name":"Bob Martinez"}', '{"species":"cat","price":380.0,"animalBreed":{"careLevel":"moderate","avgLifespanYears":15}}'),
        ]
        result = rows_to_csv(rows, cols)
        lines = [l.strip() for l in result.strip().splitlines()]
        headers = lines[0].split(",")
        assert "status" in headers
        assert "user.name" in headers
        assert "pet.species" in headers
        assert "pet.animalBreed.careLevel" in headers
        assert "pet.animalBreed.avgLifespanYears" in headers
        assert "user" not in headers
        assert "pet" not in headers
        row1 = dict(zip(headers, lines[1].split(",")))
        assert row1["user.name"] == "Alice Nguyen"
        assert row1["pet.species"] == "lion"
        assert row1["pet.animalBreed.careLevel"] == "expert"

    def test_dict_cells_flattened(self):
        cols = [
            ColumnRef(alias=None, column="status", field_name="status", nested_in=None),
            ColumnRef(alias=None, column="user", field_name="user", nested_in=None),
        ]
        rows = [("open", {"name": "Alice", "age": 30})]
        result = rows_to_csv(rows, cols)
        lines = [l.strip() for l in result.strip().splitlines()]
        headers = lines[0].split(",")
        assert "user.name" in headers
        assert "user.age" in headers
        assert "user" not in headers


class TestParquet:
    def test_basic(self):
        data = rows_to_parquet(ROWS, _cols())
        assert len(data) > 0
        # Read back
        table = pq.read_table(io.BytesIO(data))
        assert table.num_rows == 3
        assert "id" in table.column_names
        assert "amount" in table.column_names

    def test_empty(self):
        data = rows_to_parquet([], _cols())
        table = pq.read_table(io.BytesIO(data))
        assert table.num_rows == 0


class TestArrowIPC:
    def test_basic(self):
        data = rows_to_arrow_ipc(ROWS, _cols())
        assert len(data) > 0
        reader = pa.ipc.open_stream(data)
        table = reader.read_all()
        assert table.num_rows == 3
        assert "id" in table.column_names

    def test_empty(self):
        data = rows_to_arrow_ipc([], _cols())
        reader = pa.ipc.open_stream(data)
        table = reader.read_all()
        assert table.num_rows == 0


class TestArrowTable:
    def test_basic(self):
        table = rows_to_arrow_table(ROWS, _cols())
        assert isinstance(table, pa.Table)
        assert table.num_rows == 3
        assert table.column("id").to_pylist() == [1, 2, 3]
