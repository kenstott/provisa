# Copyright (c) 2026 Kenneth Stott
# Canary: d5703c46-cdc2-40b3-8923-2ea6d2d943ba
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The suite's query tests, against Provisa (REQ-2167)."""

# Requirements: REQ-2167

import adbc_drivers_validation.tests.query
from adbc_drivers_validation.tests.query import TestQuery  # noqa: F401

from . import provisa


def pytest_generate_tests(metafunc) -> None:
    return adbc_drivers_validation.tests.query.generate_tests([provisa.get_quirks()], metafunc)
