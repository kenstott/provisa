# Copyright (c) 2026 Kenneth Stott
# Canary: 3d8e2f41-9c6a-4b57-a1e0-6f2b7c9d0e84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An error PostgreSQL raises through the pg engine's runtime reaches the client with PostgreSQL's
SQLSTATE, not XX000. The runtime reaches PostgreSQL through psycopg2, whose error carries the code
as ``pgcode``; pgwire read only ``sqlstate`` (polyfill triage 2026-11-22, G1: 9 grid rows). Real
PostgreSQL, the real runtime, pgwire's own mapping."""

# Requirements: REQ-2167

from __future__ import annotations

import tempfile

import pytest

pytestmark = [pytest.mark.integration]

pgserver = pytest.importorskip("pixeltable_pgserver")

# The 9 grid rows of G1, each with the SQLSTATE PostgreSQL raises it with
# (function_support.json detail.postgres).
_RAISED = [
    ("SELECT array_append('{{1,2},{3,4}}'::integer[], 3)", "22000"),
    ("SELECT array_cat('{{1,2},{3,4}}'::integer[], '{1,2,3}'::integer[])", "2202E"),
    ("SELECT array_fill(3, NULL::integer[])", "22004"),
    ("SELECT array_fill(3, NULL::integer[], '{1,2,3}')", "22004"),
    ("SELECT array_positions('{{1,2},{3,4}}'::integer[], 3)", "0A000"),
    ("SELECT array_prepend(3, '{{1,2},{3,4}}'::integer[])", "22000"),
    ("SELECT array_remove('{{1,2},{3,4}}'::integer[], 3)", "0A000"),
    ("SELECT oid('-9223372036854775808'::bigint)", "22003"),
    ("SELECT trim_array('{2147483647,-2147483648}'::integer[], 3)", "2202E"),
]


@pytest.fixture(scope="module")
def runtime():
    from provisa.federation.pg_runtime import PgFederationRuntime

    server = pgserver.get_server(tempfile.mkdtemp(prefix="provisa_pg_runtime_sqlstate_"))
    rt = PgFederationRuntime(engine_dsn=server.get_uri())
    try:
        yield rt
    finally:
        rt.close()


@pytest.mark.parametrize(("sql", "sqlstate"), _RAISED, ids=[s for s, _ in _RAISED])
def test_postgresql_s_error_keeps_its_sqlstate(runtime, sql, sqlstate):
    from provisa.pgwire.server import _sqlstate_of

    with pytest.raises(Exception) as raised:
        runtime.run_sync(sql).rows
    assert _sqlstate_of(raised.value) == sqlstate, repr(raised.value)
