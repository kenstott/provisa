# Copyright (c) 2026 Kenneth Stott
# Canary: e5f6a7b8-c9d0-1234-efab-234567890004
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for ProvisaFlightServer pure-Python helpers and rows→Arrow conversion.

Moved from tests/integration/test_arrow_flight_integration.py Tier 1 section.
These tests require no running infrastructure.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

pa = pytest.importorskip("pyarrow")

from provisa.api.flight.server import ProvisaFlightServer  # noqa: E402
from provisa.executor.formats.arrow import rows_to_arrow_table  # noqa: E402
from provisa.compiler.sql_gen import ColumnRef  # noqa: E402

pytestmark = [pytest.mark.asyncio]


class TestFlightServerBuildCatalogTable:
    """Unit tests for the Arrow table builders (no server needed)."""

    async def test_build_catalog_table_produces_correct_schema(self):
        cat1 = MagicMock()
        cat1.domain_id = "sales"
        cat1.table_name = "orders"
        cat1.description = "Order records"

        cat2 = MagicMock()
        cat2.domain_id = "crm"
        cat2.table_name = "customers"
        cat2.description = "Customer data"

        table = ProvisaFlightServer._build_catalog_table([cat1, cat2])
        assert "schema_name" in table.schema.names
        assert "table_name" in table.schema.names
        assert "description" in table.schema.names
        assert table.num_rows == 2

    async def test_build_catalog_table_domain_filter(self):
        cat1 = MagicMock()
        cat1.domain_id = "sales"
        cat1.table_name = "orders"
        cat1.description = ""
        cat2 = MagicMock()
        cat2.domain_id = "crm"
        cat2.table_name = "customers"
        cat2.description = ""

        table = ProvisaFlightServer._build_catalog_table([cat1, cat2], domain_filter="sales")
        assert table.num_rows == 1
        assert table.column("schema_name")[0].as_py() == "sales"

    async def test_build_columns_table_structure(self):
        col = MagicMock()
        col.name = "id"
        col.data_type = "integer"
        col.is_nullable = False
        col.description = "Primary key"

        cat = MagicMock()
        cat.columns = [col]

        table = ProvisaFlightServer._build_columns_table(cat)
        assert "column_name" in table.schema.names
        assert "data_type" in table.schema.names
        assert "is_nullable" in table.schema.names
        assert "description" in table.schema.names
        assert table.num_rows == 1


class TestRowsToArrowTable:
    """Unit tests for the rows → Arrow table conversion used by Flight."""

    async def test_basic_conversion(self):
        columns = [
            ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
            ColumnRef(alias=None, column="amount", field_name="amount", nested_in=None),
        ]
        rows = [(1, 100), (2, 200), (3, 300)]
        table = rows_to_arrow_table(rows, columns)
        assert isinstance(table, pa.Table)
        assert table.num_rows == 3
        assert "id" in table.schema.names
        assert "amount" in table.schema.names

    async def test_empty_rows_gives_empty_table(self):
        columns = [
            ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
        ]
        table = rows_to_arrow_table([], columns)
        assert table.num_rows == 0

    async def test_nested_column_uses_dotted_name(self):
        from provisa.executor.formats.arrow import _column_names

        columns = [
            ColumnRef(alias=None, column="name", field_name="name", nested_in="customer"),
        ]
        names = _column_names(columns)
        assert names == ["customer.name"]

    async def test_decimal_converted_to_float(self):
        columns = [
            ColumnRef(alias=None, column="amount", field_name="amount", nested_in=None),
        ]
        rows = [(Decimal("123.45"),)]
        table = rows_to_arrow_table(rows, columns)
        val = table.column("amount")[0].as_py()
        assert isinstance(val, float)
        assert abs(val - 123.45) < 0.001

    async def test_schema_field_names_match_query_columns(self):
        """Returned schema field names match the queried ColumnRef field names."""
        columns = [
            ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
            ColumnRef(alias=None, column="region", field_name="region", nested_in=None),
            ColumnRef(alias=None, column="amount", field_name="amount", nested_in=None),
        ]
        rows = [(1, "us-east", 500), (2, "eu-west", 200)]
        table = rows_to_arrow_table(rows, columns)
        schema_names = table.schema.names
        for col in columns:
            assert col.field_name in schema_names
