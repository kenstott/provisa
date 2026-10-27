# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The suite's bulk-ingest tests, against Provisa (REQ-2167), adapted to the fixture model.

Nothing is defined through a query protocol, so each table a test would create is declared in
the fixture model, seeded empty and writable (fixtures.ingest_tables). Where creating it is the
test's setup -- its first ingest, in mode create (replace for test_many_columns) -- that ingest
is made in mode create_append, which appends to the declared table; the test's other ingests,
and every temporary one, are made as the test makes them. A temporary ingest lands in the
session's own temporary table, the one object a session creates (by_design/flight-sql.yaml)."""

# Requirements: REQ-2167

import adbc_driver_manager.dbapi
import adbc_drivers_validation.tests.ingest
import pytest
from adbc_drivers_validation.tests.ingest import TestIngest as _SuiteIngest

from . import provisa

#: The tests whose setup creates their table, and the mode that setup ingests in.
_SETUP_CREATES = {
    "test_create": "create",
    "test_create_dictionary": "create",
    "test_append": "create",
    "test_create_conflict": "create",
    "test_append_schema_mismatch": "create",
    "test_create_multiple_batches": "create",
    "test_create_large_batch": "create",
    "test_create_long_values": "create",
    "test_schema": "create",
    "test_ingest_then_query": "create",
    "test_temporary": "create",
    "test_many_columns": "replace",
}


class TestIngest(_SuiteIngest):
    @pytest.fixture(autouse=True)
    def _setup_appends_to_the_declared_table(self, request, monkeypatch):
        """The test's first ingest into a table, when it creates the table as setup, appends to
        the table the fixture model declares instead."""
        setup_mode = _SETUP_CREATES.get(request.node.originalname)
        if setup_mode is None:
            return
        ingest = adbc_driver_manager.dbapi.Cursor.adbc_ingest
        created: set[str] = set()

        def _ingest(cursor, table_name, data, mode="create", **options):
            if not options.get("temporary") and mode == setup_mode and table_name not in created:
                created.add(table_name)
                mode = "create_append"
            return ingest(cursor, table_name, data, mode=mode, **options)

        monkeypatch.setattr(adbc_driver_manager.dbapi.Cursor, "adbc_ingest", _ingest)

    def test_temporary(self, driver, conn_factory, query) -> None:
        """The suite's test, reading the ingested table where the fixture model declares it."""
        super().test_temporary(provisa.in_fixture_schema(driver), conn_factory, query)


def pytest_generate_tests(metafunc) -> None:
    return adbc_drivers_validation.tests.ingest.generate_tests([provisa.get_quirks()], metafunc)
