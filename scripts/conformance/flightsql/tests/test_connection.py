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
from adbc_drivers_validation.tests.connection import TestConnection  # noqa: F401

from . import provisa


def pytest_generate_tests(metafunc) -> None:
    return adbc_drivers_validation.tests.connection.generate_tests([provisa.get_quirks()], metafunc)
