# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table the DuckDB engine lands into a Postgres store, in a schema the store did not hold yet,
is read back through the engine (REQ-2108, REQ-848).

The landing writes through the store's own connection; the engine reads the store through its
ATTACH, whose catalog of the store is cached once read. A schema created by the landing was
missing from it, so the read failed "schema ... does not exist" -- a view's first DIRECT build,
staged into its org's schema (test_view_input_refusal_e2e). Self-provisioned embedded Postgres;
no docker."""

from __future__ import annotations

import tempfile

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.local_platform,
    pytest.mark.asyncio(loop_scope="session"),
]

pgserver = pytest.importorskip("pixeltable_pgserver")

from provisa.federation.duckdb_runtime import DuckDBFederationRuntime  # noqa: E402


@pytest.fixture(scope="module")
def store_uri():
    return pgserver.get_server(tempfile.mkdtemp(prefix="provisa_pg_new_schema_")).get_uri()


async def test_a_table_landed_in_a_new_schema_is_read_through_the_engine(store_uri):
    rt = DuckDBFederationRuntime(materialize_dsn=store_uri)
    store = rt.ensure_materialize_attached()
    columns = [("id", "integer"), ("amount", "integer")]
    # The engine has read the store's catalog: an existing schema's table.
    await rt.land_table(schema="public", table="seen", columns=columns, rows=[])
    assert (await rt.run(f'SELECT COUNT(*) FROM "{store}"."public"."seen"')).rows[0][0] == 0

    await rt.land_table(
        schema="org_new_mv_cache",
        table="mv_constants__direct",
        columns=columns,
        rows=[{"id": 1, "amount": 10}],
    )
    read = await rt.run(
        f'SELECT id, amount FROM "{store}"."org_new_mv_cache"."mv_constants__direct"'
    )
    assert [tuple(r) for r in read.rows] == [(1, 10)]
