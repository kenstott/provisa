# Copyright (c) 2026 Kenneth Stott
# Canary: 8f1b3d47-0a6c-4e29-b5d8-3c7e9a2f6b10
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ASGI entry point: the deployed ``main:app`` with the test-only driver SQL tap.

``main`` is imported first, so the app is built exactly as deployed; the tap then wraps the
source-driver and engine terminals (``tests.driver_sql_tap``) and writes to
``$PROVISA_TEST_DRIVER_SQL_TAP``. Nothing it wraps has run yet — the servers start in the lifespan."""

import os

from main import app  # noqa: F401  (the ASGI app uvicorn serves)

from tests import driver_sql_tap

driver_sql_tap.install(os.environ["PROVISA_TEST_DRIVER_SQL_TAP"])
