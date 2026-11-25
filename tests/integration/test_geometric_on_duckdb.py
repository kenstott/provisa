# Copyright (c) 2026 Kenneth Stott
# Canary: e35a71c9-08b4-4d6e-8f23-b16d4c09a7f2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The geometric forms on DuckDB, against the PostgreSQL 17 recording (REQ-2171)."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import duckdb
import pytest

from tests.integration.geometric_engines import check, on_columns
from tests.unit.pgwire.geometric_replay import BY_FUNCTION

pytestmark = [pytest.mark.integration]


@pytest.mark.parametrize("function", sorted(BY_FUNCTION))
def test_geometric_forms_on_duckdb(function: str):
    connection = duckdb.connect()
    run = lambda sql: connection.execute(sql).fetchone()[0]  # noqa: E731
    assert check(function, "duckdb", run) == []


@pytest.mark.parametrize("function", sorted(BY_FUNCTION))
def test_geometric_columns_on_duckdb(function: str):
    connection = duckdb.connect()
    on_columns(function, "duckdb", lambda sql: [r[0] for r in connection.execute(sql).fetchall()])
