# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DuckDB 2.0's remote_pushdown optimizer (new in 2.0; 1.5.5 had none) plans a statement over a
large constant list for minutes once a PostgreSQL catalog is attached: Cypher's
``UNWIND range(1000000, 2000000)`` took 73 s to plan, twice (bind and run), where it answers in
0.2 s without it -- the Cypher TCK and Cypher HTTP reads past their deadline. Every DuckDB Provisa
opens has it off, so a statement is planned as on 1.5.5 (REQ-1142)."""

# Requirements: REQ-1142, REQ-2006

from __future__ import annotations

import time

import pytest

from provisa.core import engine_memory
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime
from provisa.federation.native_backend import libpq_dsn

pytestmark = pytest.mark.unit

_DISABLED = "SELECT current_setting('disabled_optimizers')"


@pytest.mark.parametrize("extra", [None, {"memory_limit": "256MiB", "threads": 2}])
def test_every_database_provisa_opens_plans_without_remote_pushdown(extra):
    con = engine_memory.connect(extra=extra)
    try:
        (disabled,) = engine_memory.first_row(con.execute(_DISABLED))
        assert "remote_pushdown" in disabled.split(",")
        # a cursor is another connection to the same database: the setting is the database's
        (disabled,) = engine_memory.first_row(con.cursor().execute(_DISABLED))
        assert "remote_pushdown" in disabled.split(",")
    finally:
        con.close()


def test_a_large_constant_list_is_planned_promptly_with_the_control_plane_attached(control_plane):
    rt = DuckDBFederationRuntime()
    try:
        rt.attach_control_plane(libpq_dsn(control_plane.org().engine.url))
        started = time.monotonic()
        rows = rt.run_sync(
            'WITH _w0 AS (SELECT _uw0."i" AS "i" FROM UNNEST(GENERATE_SERIES(1, 200000)) '
            'AS _uw0(i) LIMIT 3000) SELECT SUM(_w0."i") AS "sum(i)" FROM _w0'
        ).rows
        took = time.monotonic() - started
    finally:
        rt.close()
    assert rows == [(4501500,)]
    # 0.03 s planned as 1.5.5 did; with remote_pushdown, seconds.
    assert took < 2.0, took
