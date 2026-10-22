# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The pg engine's reconcile keeps a landed replica whose key is not built yet (REQ-2015, REQ-846).

The pg engine builds a replica with no key (pg_statement_copy) and keys it once it has landed
(PostgresIndexer). A schema rebuild in between ran the reconcile, which read the missing key as
drift and dropped and re-created the table empty while its record still said built: in
test_replica_convergence_e2e both servers recorded the build (5 rows copied) and the replica held
0, until the wait for its rows timed out."""

# Requirements: REQ-2015, REQ-846

from __future__ import annotations

import pytest

from provisa.federation.pg_runtime import PgFederationRuntime

pytestmark = pytest.mark.unit

_SCHEMA = "org_t_replicas"
_COLUMNS = [("id", "integer"), ("amount", "double")]


@pytest.fixture
def runtime(embedded_postgres):
    rt = PgFederationRuntime(engine_dsn=embedded_postgres.dsn())
    yield rt
    rt.close()


def _landed_without_key(runtime) -> None:
    cur = runtime._con.cursor()  # noqa: SLF001 - the engine's own connection, as a build writes
    cur.execute(f'DROP SCHEMA IF EXISTS "{_SCHEMA}" CASCADE')
    cur.execute(f'CREATE SCHEMA "{_SCHEMA}"')
    cur.execute(f'CREATE TABLE "{_SCHEMA}"."orders" ("id" integer, "amount" double precision)')
    cur.execute(f'INSERT INTO "{_SCHEMA}"."orders" SELECT g, g FROM generate_series(1, 5) g')


def _rows(runtime) -> int:
    cur = runtime._con.cursor()  # noqa: SLF001
    cur.execute(f'SELECT COUNT(*) FROM "{_SCHEMA}"."orders"')
    return cur.fetchone()[0]


async def test_a_landed_replica_whose_key_is_not_built_yet_keeps_its_rows(runtime):
    _landed_without_key(runtime)
    kept = await runtime.reconcile_replica(
        schema=_SCHEMA, table="orders", columns=_COLUMNS, pk_columns=["id"]
    )
    assert kept == "kept"
    assert _rows(runtime) == 5


async def test_a_replica_whose_columns_changed_is_still_recreated(runtime):
    _landed_without_key(runtime)
    recreated = await runtime.reconcile_replica(
        schema=_SCHEMA,
        table="orders",
        columns=[*_COLUMNS, ("note", "text")],
        pk_columns=["id"],
    )
    assert recreated == "recreated"
    assert _rows(runtime) == 0
