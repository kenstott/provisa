# Copyright (c) 2026 Kenneth Stott
# Canary: 77492e02-f8b2-4742-9344-5ab1323efab4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Extended unit tests for the executor serialization layer, redirect logic,
output formats, and driver registry helpers.

Complements the existing test_serialize.py and test_formats.py test files with
additional edge-case and behavioural coverage not present elsewhere.
"""

from __future__ import annotations

import io
import json
from datetime import date, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from provisa.compiler.sql_gen import ColumnRef
from provisa.executor.serialize import (
    _convert_value,
    serialize_aggregate,
    serialize_connection,
    serialize_rows,
)
from provisa.executor.redirect import (
    DEFAULT_THRESHOLD,
    DEFAULT_TTL,
    RedirectConfig,
    _serialize_for_redirect,
    should_redirect,
)
from provisa.executor.trino import QueryResult
from provisa.executor.formats.arrow import rows_to_arrow_ipc, rows_to_arrow_table
from provisa.executor.formats.ndjson import rows_to_ndjson
from provisa.executor.formats.tabular import rows_to_csv, rows_to_parquet
from provisa.executor.drivers.registry import (
    available_drivers,
    create_driver,
    has_driver,
)
from provisa.executor.drivers.base import DirectDriver


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _col(field_name: str, nested_in: str | None = None) -> ColumnRef:
    return ColumnRef(alias=None, column=field_name, field_name=field_name, nested_in=nested_in)


def _cols(*specs) -> list[ColumnRef]:
    """Build a list of ColumnRef from (field_name, nested_in?) pairs."""
    result = []
    for spec in specs:
        if isinstance(spec, str):
            result.append(_col(spec))
        else:
            result.append(_col(*spec))
    return result


def _result(n_rows: int, ncols: int = 3) -> QueryResult:
    return QueryResult(
        rows=[tuple(range(ncols)) for _ in range(n_rows)],
        column_names=[f"c{i}" for i in range(ncols)],
    )


def _redirect_config(enabled: bool = True, threshold: int = 10) -> RedirectConfig:
    return RedirectConfig(
        enabled=enabled,
        threshold=threshold,
        bucket="test-bucket",
        endpoint_url="http://localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        ttl=3600,
    )


# ---------------------------------------------------------------------------
# _convert_value
# ---------------------------------------------------------------------------

class TestConvertValue:
    def test_decimal_whole_number_becomes_int(self):
        assert _convert_value(Decimal("5")) == 5
        assert isinstance(_convert_value(Decimal("5")), int)

    def test_decimal_fractional_becomes_float(self):
        v = _convert_value(Decimal("3.14"))
        assert isinstance(v, float)
        assert abs(v - 3.14) < 1e-9

    def test_decimal_zero_becomes_int(self):
        assert _convert_value(Decimal("0")) == 0
        assert isinstance(_convert_value(Decimal("0")), int)

    def test_date_uses_isoformat(self):
        d = date(2024, 6, 15)
        assert _convert_value(d) == "2024-06-15"

    def test_datetime_uses_isoformat(self):
        dt = datetime(2024, 6, 15, 10, 30, 0)
        assert _convert_value(dt) == "2024-06-15T10:30:00"

    def test_string_unchanged(self):
        assert _convert_value("hello") == "hello"

    def test_int_unchanged(self):
        assert _convert_value(42) == 42

    def test_none_unchanged(self):
        assert _convert_value(None) is None

    def test_bool_unchanged(self):
        assert _convert_value(True) is True

    def test_float_unchanged(self):
        assert _convert_value(1.5) == 1.5


# ---------------------------------------------------------------------------
# serialize_rows — additional edge cases
# ---------------------------------------------------------------------------

class TestSerializeRowsEdgeCases:
    def test_isoformat_date_in_row(self):
        columns = _cols("id", "created_at")
        rows = [(1, date(2024, 1, 1))]
        result = serialize_rows(rows, columns, "items")
        assert result["data"]["items"][0]["created_at"] == "2024-01-01"

    def test_isoformat_datetime_in_row(self):
        columns = _cols("ts")
        rows = [(datetime(2024, 3, 15, 9, 0, 0),)]
        result = serialize_rows(rows, columns, "events")
        assert result["data"]["events"][0]["ts"] == "2024-03-15T09:00:00"

    def test_none_values_preserved(self):
        columns = _cols("id", "name")
        rows = [(1, None)]
        result = serialize_rows(rows, columns, "users")
        assert result["data"]["users"][0]["name"] is None

    def test_root_field_name_used_as_key(self):
        columns = _cols("id")
        result = serialize_rows([(1,)], columns, "my_custom_field")
        assert "my_custom_field" in result["data"]

    def test_deep_nested_path(self):
        """Dotted nest paths like 'a.b' produce nested dict { a: { b: { field: val } } }."""
        columns = [
            _col("id"),
            ColumnRef(alias=None, column="city", field_name="city", nested_in="address.location"),
        ]
        rows = [(1, "London")]
        result = serialize_rows(rows, columns, "users")
        row = result["data"]["users"][0]
        assert row["address"]["location"]["city"] == "London"

    def test_multiple_rows_multiple_nested_groups(self):
        columns = [
            _col("id"),
            ColumnRef(alias=None, column="name", field_name="name", nested_in="customer"),
            ColumnRef(alias=None, column="title", field_name="title", nested_in="product"),
        ]
        rows = [(1, "Alice", "Widget"), (2, "Bob", "Gadget")]
        result = serialize_rows(rows, columns, "orders")
        rows_out = result["data"]["orders"]
        assert rows_out[0]["customer"] == {"name": "Alice"}
        assert rows_out[0]["product"] == {"title": "Widget"}
        assert rows_out[1]["customer"] == {"name": "Bob"}

    def test_output_shape_is_list(self):
        columns = _cols("id")
        result = serialize_rows([(1,), (2,)], columns, "things")
        assert isinstance(result["data"]["things"], list)

    def test_decimal_19_99_serialized_as_float(self):
        columns = _cols("amount")
        rows = [(Decimal("19.99"),)]
        result = serialize_rows(rows, columns, "items")
        assert result["data"]["items"][0]["amount"] == 19.99


# ---------------------------------------------------------------------------
# serialize_aggregate — additional coverage
# ---------------------------------------------------------------------------

class TestSerializeAggregateEdgeCases:
    def test_empty_agg_rows(self):
        agg_columns = [_col("count", "aggregate")]
        result = serialize_aggregate(
            agg_rows=[],
            agg_columns=agg_columns,
            nodes_rows=None,
            nodes_columns=None,
            root_field="items_aggregate",
        )
        data = result["data"]["items_aggregate"]
        assert data["aggregate"] == {}

    def test_custom_agg_alias(self):
        agg_columns = [
            ColumnRef(alias=None, column="count", field_name="count", nested_in="summary"),
        ]
        agg_rows = [(7,)]
        result = serialize_aggregate(
            agg_rows=agg_rows,
            agg_columns=agg_columns,
            nodes_rows=None,
            nodes_columns=None,
            root_field="x",
            agg_alias="summary",
        )
        assert result["data"]["x"]["summary"]["count"] == 7

    def test_nodes_empty_list(self):
        agg_columns = [_col("count", "aggregate")]
        result = serialize_aggregate(
            agg_rows=[(0,)],
            agg_columns=agg_columns,
            nodes_rows=[],
            nodes_columns=[_col("id")],
            root_field="stuff_aggregate",
        )
        assert result["data"]["stuff_aggregate"]["nodes"] == []

    def test_aggregate_min_max_nested(self):
        agg_columns = [
            ColumnRef(alias=None, column="amount", field_name="amount", nested_in="aggregate.min"),
            ColumnRef(alias=None, column="amount", field_name="amount", nested_in="aggregate.max"),
        ]
        agg_rows = [(Decimal("1.00"), Decimal("99.00"))]
        result = serialize_aggregate(
            agg_rows=agg_rows,
            agg_columns=agg_columns,
            nodes_rows=None,
            nodes_columns=None,
            root_field="orders_aggregate",
        )
        data = result["data"]["orders_aggregate"]
        assert data["aggregate"]["min"]["amount"] == 1.0
        assert data["aggregate"]["max"]["amount"] == 99.0


# ---------------------------------------------------------------------------
# serialize_connection — Relay-style pagination
# ---------------------------------------------------------------------------

class TestSerializeConnection:
    def _compiled(
        self,
        rows_count: int,
        page_size: int | None = None,
        is_backward: bool = False,
        has_cursor: bool = False,
        sort_columns: list[str] | None = None,
    ):
        """Build a minimal CompiledQuery for serialize_connection tests."""
        from provisa.compiler.sql_gen import CompiledQuery

        cols = [_col("id"), _col("name")]
        return CompiledQuery(
            sql="SELECT id, name FROM t",
            params=[],
            root_field="items",
            columns=cols,
            sources={"pg"},
            is_connection=True,
            is_backward=is_backward,
            sort_columns=sort_columns or ["id"],
            page_size=page_size,
            has_cursor=has_cursor,
        )

    def _rows(self, n: int) -> list[tuple]:
        return [(i, f"item_{i}") for i in range(1, n + 1)]

    def test_edges_contain_node_and_cursor(self):
        compiled = self._compiled(3, page_size=3)
        result = serialize_connection(self._rows(3), compiled)
        edges = result["data"]["items"]["edges"]
        assert len(edges) == 3
        for edge in edges:
            assert "node" in edge
            assert "cursor" in edge

    def test_page_info_structure(self):
        compiled = self._compiled(2, page_size=2)
        result = serialize_connection(self._rows(2), compiled)
        page_info = result["data"]["items"]["pageInfo"]
        assert "hasNextPage" in page_info
        assert "hasPreviousPage" in page_info
        assert "startCursor" in page_info
        assert "endCursor" in page_info

    def test_has_next_page_when_more_rows(self):
        """Extra row beyond page_size means hasNextPage=True."""
        compiled = self._compiled(5, page_size=3)
        # Provide 4 rows (page_size+1) to signal there are more
        result = serialize_connection(self._rows(4), compiled)
        assert result["data"]["items"]["pageInfo"]["hasNextPage"] is True

    def test_no_next_page_when_at_page_size(self):
        compiled = self._compiled(3, page_size=3)
        result = serialize_connection(self._rows(3), compiled)
        assert result["data"]["items"]["pageInfo"]["hasNextPage"] is False

    def test_empty_edges_null_cursors(self):
        compiled = self._compiled(0, page_size=10)
        result = serialize_connection([], compiled)
        page_info = result["data"]["items"]["pageInfo"]
        assert result["data"]["items"]["edges"] == []
        assert page_info["startCursor"] is None
        assert page_info["endCursor"] is None

    def test_backward_pagination_reverses_rows(self):
        compiled = self._compiled(3, page_size=3, is_backward=True)
        rows = self._rows(3)  # ids: 1,2,3
        result = serialize_connection(rows, compiled)
        edges = result["data"]["items"]["edges"]
        # After reversal, first edge should have id=3
        assert edges[0]["node"]["id"] == 3

    def test_has_cursor_sets_has_previous_page(self):
        compiled = self._compiled(2, page_size=2, has_cursor=True)
        result = serialize_connection(self._rows(2), compiled)
        assert result["data"]["items"]["pageInfo"]["hasPreviousPage"] is True

    def test_no_cursor_means_no_previous_page(self):
        compiled = self._compiled(2, page_size=2, has_cursor=False)
        result = serialize_connection(self._rows(2), compiled)
        assert result["data"]["items"]["pageInfo"]["hasPreviousPage"] is False

    def test_node_fields_match_columns(self):
        compiled = self._compiled(1, page_size=5)
        result = serialize_connection([(10, "hello")], compiled)
        node = result["data"]["items"]["edges"][0]["node"]
        assert node["id"] == 10
        assert node["name"] == "hello"

    def test_root_field_used_as_key(self):
        compiled = self._compiled(1, page_size=5)
        result = serialize_connection(self._rows(1), compiled)
        assert "items" in result["data"]


# ---------------------------------------------------------------------------
# Redirect — should_redirect logic (additional scenarios)
# ---------------------------------------------------------------------------

class TestShouldRedirectAdditional:
    def test_exactly_at_threshold_not_redirected(self):
        assert not should_redirect(_result(10), _redirect_config(threshold=10))

    def test_one_above_threshold_redirected(self):
        assert should_redirect(_result(11), _redirect_config(threshold=10))

    def test_disabled_no_redirect_regardless_of_count(self):
        assert not should_redirect(_result(10_000), _redirect_config(enabled=False))

    def test_force_bypasses_threshold(self):
        assert should_redirect(_result(1), _redirect_config(threshold=1000), force=True)

    def test_force_with_disabled_still_blocked(self):
        assert not should_redirect(
            _result(100), _redirect_config(enabled=False), force=True,
        )

    def test_pre_approved_blocks_regardless_of_count(self):
        assert not should_redirect(
            _result(5000), _redirect_config(),
            table_governance={42: "pre-approved"},
            target_table_ids=[42],
        )

    def test_pre_approved_blocks_force(self):
        assert not should_redirect(
            _result(1), _redirect_config(),
            table_governance={1: "pre-approved"},
            target_table_ids=[1],
            force=True,
        )

    def test_governance_with_no_target_ids_redirects(self):
        """table_governance populated but no target_table_ids — redirect allowed."""
        assert should_redirect(
            _result(100), _redirect_config(),
            table_governance={1: "pre-approved"},
            target_table_ids=None,
        )

    def test_target_ids_but_no_governance_redirects(self):
        assert should_redirect(
            _result(100), _redirect_config(),
            table_governance=None,
            target_table_ids=[1],
        )

    def test_registry_required_does_not_block_redirect(self):
        assert should_redirect(
            _result(100), _redirect_config(),
            table_governance={1: "registry-required"},
            target_table_ids=[1],
        )

    def test_mixed_one_pre_approved_blocks_all(self):
        assert not should_redirect(
            _result(100), _redirect_config(),
            table_governance={1: "registry-required", 2: "pre-approved"},
            target_table_ids=[1, 2],
        )


# ---------------------------------------------------------------------------
# RedirectConfig — from_env and defaults
# ---------------------------------------------------------------------------

class TestRedirectConfig:
    def test_from_env_defaults(self):
        config = RedirectConfig.from_env()
        assert config.enabled is False
        assert config.threshold == DEFAULT_THRESHOLD
        assert config.ttl == DEFAULT_TTL
        assert config.bucket == "provisa-results"
        assert config.region == "us-east-1"
        assert config.default_format == "parquet"

    def test_from_env_respects_env_vars(self, monkeypatch):
        monkeypatch.setenv("PROVISA_REDIRECT_ENABLED", "true")
        monkeypatch.setenv("PROVISA_REDIRECT_THRESHOLD", "500")
        monkeypatch.setenv("PROVISA_REDIRECT_TTL", "7200")
        monkeypatch.setenv("PROVISA_REDIRECT_BUCKET", "my-bucket")
        config = RedirectConfig.from_env()
        assert config.enabled is True
        assert config.threshold == 500
        assert config.ttl == 7200
        assert config.bucket == "my-bucket"

    def test_default_threshold_value(self):
        assert DEFAULT_THRESHOLD == 1000

    def test_default_ttl_value(self):
        assert DEFAULT_TTL == 3600


# ---------------------------------------------------------------------------
# _serialize_for_redirect — JSON and NDJSON paths (no S3)
# ---------------------------------------------------------------------------

class TestSerializeForRedirect:
    def _qr(self) -> QueryResult:
        return QueryResult(
            rows=[(1, "Alice", 99.5), (2, "Bob", 12.3)],
            column_names=["id", "name", "score"],
        )

    def test_json_format_produces_json_bytes(self):
        body, ctype, ext = _serialize_for_redirect(self._qr(), None, "json")
        assert ctype == "application/json"
        assert ext == ".json"
        data = json.loads(body)
        assert len(data) == 2
        assert data[0]["id"] == 1

    def test_ndjson_format_one_line_per_row(self):
        body, ctype, ext = _serialize_for_redirect(self._qr(), None, "ndjson")
        assert ctype == "application/x-ndjson"
        assert ext == ".ndjson"
        lines = body.decode().strip().split("\n")
        assert len(lines) == 2
        first = json.loads(lines[0])
        assert first["name"] == "Alice"

    def test_unknown_format_falls_back_to_ndjson(self):
        body, ctype, ext = _serialize_for_redirect(self._qr(), None, "zzz_unknown")
        # Falls back to NDJSON
        assert ext == ".ndjson"
        lines = body.decode().strip().split("\n")
        assert len(lines) == 2

    def test_csv_format_with_columns(self):
        cols = _cols("id", "name", "score")
        qr = self._qr()
        body, ctype, ext = _serialize_for_redirect(qr, cols, "csv")
        assert ctype == "text/csv"
        assert ext == ".csv"
        lines = body.decode().strip().splitlines()
        assert lines[0] == "id,name,score"
        assert len(lines) == 3  # header + 2 data rows

    def test_parquet_format_with_columns(self):
        cols = _cols("id", "name", "score")
        qr = self._qr()
        body, ctype, ext = _serialize_for_redirect(qr, cols, "parquet")
        assert ctype == "application/vnd.apache.parquet"
        assert ext == ".parquet"
        assert len(body) > 0
        table = pq.read_table(io.BytesIO(body))
        assert table.num_rows == 2

    def test_arrow_format_with_columns(self):
        cols = _cols("id", "name", "score")
        qr = self._qr()
        body, ctype, ext = _serialize_for_redirect(qr, cols, "arrow")
        assert ctype == "application/vnd.apache.arrow.stream"
        assert ext == ".arrow"
        reader = pa.ipc.open_stream(io.BytesIO(body))
        tbl = reader.read_all()
        assert tbl.num_rows == 2


# ---------------------------------------------------------------------------
# Driver registry — additional coverage
# ---------------------------------------------------------------------------

class TestDriverRegistryAdditional:
    def test_has_driver_postgresql(self):
        assert has_driver("postgresql") is True

    def test_has_driver_unknown_returns_false(self):
        assert has_driver("nonexistent_db_type") is False

    def test_create_driver_returns_direct_driver_instance(self):
        driver = create_driver("postgresql")
        assert isinstance(driver, DirectDriver)

    def test_create_driver_raises_key_error_for_unknown(self):
        with pytest.raises(KeyError, match="No direct driver"):
            create_driver("unknown_db_type")

    def test_available_drivers_is_list(self):
        drivers = available_drivers()
        assert isinstance(drivers, list)

    def test_available_drivers_contains_postgresql(self):
        assert "postgresql" in available_drivers()

    def test_available_drivers_excludes_unknown(self):
        assert "cassandra" not in available_drivers()

    def test_mysql_aliases_supported(self):
        """singlestore and mariadb use the MySQL driver factory."""
        if not has_driver("mysql"):
            pytest.skip("aiomysql not installed")
        assert has_driver("singlestore")
        assert has_driver("mariadb")

    def test_has_driver_duckdb(self):
        if not has_driver("duckdb"):
            pytest.skip("duckdb not installed")
        assert has_driver("duckdb") is True

    def test_create_driver_duckdb(self):
        if not has_driver("duckdb"):
            pytest.skip("duckdb not installed")
        driver = create_driver("duckdb")
        assert isinstance(driver, DirectDriver)


# ---------------------------------------------------------------------------
# Output formats — additional edge cases
# ---------------------------------------------------------------------------

class TestNDJSONEdgeCases:
    def test_single_row(self):
        cols = _cols("x")
        result = rows_to_ndjson([(42,)], cols)
        obj = json.loads(result.strip())
        assert obj["x"] == 42

    def test_nested_column_key(self):
        cols = [ColumnRef(alias=None, column="n", field_name="n", nested_in="rel")]
        result = rows_to_ndjson([(5,)], cols)
        obj = json.loads(result.strip())
        assert obj["rel.n"] == 5

    def test_decimal_serialized_as_number(self):
        cols = _cols("amount")
        result = rows_to_ndjson([(Decimal("9.99"),)], cols)
        obj = json.loads(result.strip())
        assert obj["amount"] == 9.99

    def test_trailing_newline_when_nonempty(self):
        cols = _cols("x")
        result = rows_to_ndjson([(1,)], cols)
        assert result.endswith("\n")

    def test_empty_no_trailing_newline(self):
        cols = _cols("x")
        result = rows_to_ndjson([], cols)
        assert result == ""


class TestCSVEdgeCases:
    def test_decimal_converted_to_float(self):
        cols = _cols("amount")
        result = rows_to_csv([(Decimal("3.14"),)], cols)
        lines = result.strip().splitlines()
        assert "3.14" in lines[1]

    def test_nested_column_qualified_name(self):
        cols = [ColumnRef(alias=None, column="city", field_name="city", nested_in="address")]
        result = rows_to_csv([("London",)], cols)
        header = result.splitlines()[0]
        assert "address.city" in header

    def test_multiple_rows_correct_count(self):
        cols = _cols("id")
        result = rows_to_csv([(i,) for i in range(5)], cols)
        lines = result.strip().splitlines()
        assert len(lines) == 6  # header + 5 rows


class TestParquetEdgeCases:
    def test_roundtrip_values(self):
        cols = _cols("id", "score")
        rows = [(1, 9.5), (2, 8.2)]
        data = rows_to_parquet(rows, cols)
        table = pq.read_table(io.BytesIO(data))
        assert table.column("id").to_pylist() == [1, 2]

    def test_nested_col_name_in_parquet(self):
        cols = [ColumnRef(alias=None, column="name", field_name="name", nested_in="cust")]
        data = rows_to_parquet([("Alice",)], cols)
        table = pq.read_table(io.BytesIO(data))
        assert "cust.name" in table.column_names


class TestArrowEdgeCases:
    def test_roundtrip_ipc(self):
        cols = _cols("id", "val")
        rows = [(1, "a"), (2, "b")]
        data = rows_to_arrow_ipc(rows, cols)
        reader = pa.ipc.open_stream(io.BytesIO(data))
        tbl = reader.read_all()
        assert tbl.num_rows == 2
        assert tbl.column("id").to_pylist() == [1, 2]

    def test_arrow_table_column_names(self):
        cols = _cols("x", "y", "z")
        rows = [(1, 2, 3)]
        tbl = rows_to_arrow_table(rows, cols)
        assert isinstance(tbl, pa.Table)
        assert tbl.column_names == ["x", "y", "z"]

    def test_decimal_converted_in_arrow(self):
        cols = _cols("amount")
        rows = [(Decimal("7.77"),)]
        tbl = rows_to_arrow_table(rows, cols)
        assert abs(tbl.column("amount").to_pylist()[0] - 7.77) < 1e-9

    def test_empty_arrow_table(self):
        cols = _cols("id")
        tbl = rows_to_arrow_table([], cols)
        assert tbl.num_rows == 0
        assert "id" in tbl.column_names
