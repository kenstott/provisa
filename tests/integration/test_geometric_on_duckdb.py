# Copyright (c) 2026 Kenneth Stott
# Canary: 7a4b4485-8f32-4c0f-b244-cd135b0a970c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The geometric forms over columns on DuckDB, against the PostgreSQL 17 recording (REQ-2171)."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import duckdb
import pytest

from tests.integration.geometric_engines import on_columns
from tests.unit.pgwire.geometric_replay import BY_FUNCTION

pytestmark = [pytest.mark.integration]


@pytest.mark.parametrize("function", sorted(BY_FUNCTION))
def test_geometric_columns_on_duckdb(function: str):
    connection = duckdb.connect()
    on_columns(function, "duckdb", lambda sql: [r[0] for r in connection.execute(sql).fetchall()])
