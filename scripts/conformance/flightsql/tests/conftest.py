# Copyright (c) 2026 Kenneth Stott
# Canary: 71c30bcc-8b12-465e-8785-1187cbd95487
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The suite's fixtures, with Provisa as the backend and the ADBC Flight SQL driver as the driver
(REQ-2167)."""

# Requirements: REQ-2167

from __future__ import annotations

import adbc_driver_flightsql
import adbc_drivers_validation.model
import adbc_drivers_validation.tests.conftest
import pytest
from adbc_drivers_validation.tests.conftest import (  # noqa: F401
    conn,
    conn_factory,
    db_kwargs,
    manual_test,
    noci,
    pytest_collection_modifyitems,
)

from . import provisa


def pytest_addoption(parser):
    adbc_drivers_validation.tests.conftest.pytest_addoption(parser)


@pytest.fixture(scope="session")
def driver() -> adbc_drivers_validation.model.DriverQuirks:
    return provisa.get_quirks()


@pytest.fixture(scope="session")
def driver_path() -> str:
    return adbc_driver_flightsql._driver_path()
