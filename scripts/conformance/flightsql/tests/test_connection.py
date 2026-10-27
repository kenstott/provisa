# Copyright (c) 2026 Kenneth Stott
# Canary: abbdc69d-548d-48e4-a9c4-5c06e556d209
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The suite's connection tests, against Provisa (REQ-2167)."""

# Requirements: REQ-2167

import copy

import adbc_drivers_validation.tests.connection
import pytest
from adbc_drivers_validation import model
from adbc_drivers_validation.tests.connection import TestConnection as _SuiteConnection

from . import provisa

#: The fixture model's schema: its domain, where every table the job declares is listed.
FIXTURE_SCHEMA = "conformance"
#: The columns every model table carries besides its own (fixtures.LINK_COLUMNS).
LINK_COLUMNS = ("_name_", "_domain_")


def _in_fixture_schema(driver: model.DriverQuirks) -> model.DriverQuirks:
    """``driver`` with the fixture's schema as the one a test looks for the fixture table in: the
    suite's would be in the current schema (public); a model's tables are in their domains'."""
    features = driver.features.model_copy()
    # The suite's DriverFeatures answers current_schema from this private attribute.
    features._current_schema = FIXTURE_SCHEMA
    adapted = copy.copy(driver)
    adapted.features = features
    return adapted


def _columns(objects: list[dict]) -> list[tuple]:
    return [
        (obj["catalog_name"], schema["db_schema_name"], table["table_name"], column["column_name"])
        for obj in objects
        for schema in obj["catalog_db_schemas"]
        for table in schema["db_schema_tables"]
        for column in table["table_columns"]
    ]


class TestConnection(_SuiteConnection):
    """The suite's connection tests, the get_objects ones adapted to the fixture model
    (by_design/flight-sql.yaml): its table is in the fixture's schema, and listed with the link
    columns every model table carries after its own."""

    @pytest.fixture(scope="class")
    @classmethod
    def get_objects_table(cls, driver: model.DriverQuirks):
        """The table the suite would ingest, declared in the fixture model with the same columns
        and rows."""
        yield (driver.features.current_catalog, FIXTURE_SCHEMA, "getobjects_fixture")

    def test_get_objects_column_filter_schema(self, conn, driver, get_objects_table) -> None:
        super().test_get_objects_column_filter_schema(
            conn, _in_fixture_schema(driver), get_objects_table
        )

    def test_get_objects_column_xdbc(self, conn, driver, get_objects_table) -> None:
        super().test_get_objects_column_xdbc(conn, _in_fixture_schema(driver), get_objects_table)

    def test_get_objects_column_filter_table_name(self, conn, driver, get_objects_table) -> None:
        """The suite's assertions, with the link columns after the table's own."""
        table_id = get_objects_table
        objects = conn.adbc_get_objects(depth="columns", table_name_filter=table_id[-1])
        columns = _columns(objects.read_all().to_pylist())
        assert sorted(set(columns)) == sorted(columns)
        assert columns == [(*table_id, c) for c in ("ints", "strs", *LINK_COLUMNS)]

    def test_get_table_schema_schema(self, conn, driver) -> None:
        """The suite's assertion on the table it would create in the secondary schema, which the
        fixture model declares there (spam INTEGER, eggs TEXT), with the link columns after its
        own."""
        schema = conn.adbc_get_table_schema(
            "test_get_table_schema_schema", db_schema_filter=driver.features.secondary_schema
        )
        assert schema.names == ["spam", "eggs", *LINK_COLUMNS]

    def test_get_objects_column_filter_table(self, conn, driver, get_objects_table) -> None:
        """The suite's assertions, in the fixture's schema, with the link columns after the
        table's own."""
        table_id = get_objects_table
        objects = conn.adbc_get_objects(
            depth="columns",
            catalog_filter=driver.features.current_catalog,
            db_schema_filter=FIXTURE_SCHEMA,
            table_name_filter=table_id[-1],
        )
        columns = _columns(objects.read_all().to_pylist())
        assert sorted(set(columns)) == sorted(columns)
        assert columns == [(*table_id, c) for c in ("ints", "strs", *LINK_COLUMNS)]


def pytest_generate_tests(metafunc) -> None:
    return adbc_drivers_validation.tests.connection.generate_tests([provisa.get_quirks()], metafunc)
