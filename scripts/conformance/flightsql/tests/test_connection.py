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

import adbc_drivers_validation.tests.connection
import pytest
from adbc_drivers_validation import model
from adbc_drivers_validation.tests.connection import TestConnection as _SuiteConnection

from . import provisa


class TestConnection(_SuiteConnection):
    @pytest.fixture(scope="class")
    @classmethod
    def get_objects_table(cls, driver: model.DriverQuirks):
        """The table the suite would ingest, declared in the fixture model's conformance domain
        with the same columns and rows (adapted, by_design/flight-sql.yaml). The suite's would be
        in the current schema; a model's tables are in their domains'."""
        yield (driver.features.current_catalog, "conformance", "getobjects_fixture")


def pytest_generate_tests(metafunc) -> None:
    return adbc_drivers_validation.tests.connection.generate_tests([provisa.get_quirks()], metafunc)
