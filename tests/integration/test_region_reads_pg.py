# Copyright (c) 2026 Kenneth Stott
# Canary: 6e6ccc5a-0050-4a96-9284-eb0cf2e54464
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The pg engine reads a table another region keeps from that region's replicas store through
postgres_fdw (REQ-1922), against a real PostgreSQL: the table is imported read only into
``region_<id>__<schema>``, a read there answers from the replica that region built, the store's
address is set afresh on each import, and a replica whose columns changed is read as it is now."""

# Requirements: REQ-1922

from __future__ import annotations

import os
import uuid

import psycopg2
import pytest
import sqlalchemy as sa

pytestmark = [pytest.mark.integration]


@pytest.fixture
def eu(docker_postgres):
    """The eu region's replicas store — a second database on the same server, reached by the
    engine's own server over postgres_fdw — holding one built replica of ``crm.public.orders``;
    and a pg engine runtime on the ``provisa`` database."""
    from provisa.federation.pg_runtime import PgFederationRuntime

    password = os.environ.get("PG_PASSWORD", "provisa")
    host, port = docker_postgres["host"], docker_postgres["port"]
    store_db = f"eu_{uuid.uuid4().hex[:8]}"
    schema = "org_acme_replicas"
    admin = sa.create_engine(
        f"postgresql+psycopg://provisa:{password}@{host}:{port}/provisa",
        isolation_level="AUTOCOMMIT",
    )
    with admin.connect() as conn:
        conn.execute(sa.text(f'CREATE DATABASE "{store_db}"'))
    store = sa.create_engine(f"postgresql+psycopg://provisa:{password}@{host}:{port}/{store_db}")
    with store.begin() as conn:
        conn.execute(sa.text(f'CREATE SCHEMA "{schema}"'))
        conn.execute(sa.text(f'CREATE TABLE "{schema}".orders (id int, region text)'))
        conn.execute(sa.text(f"INSERT INTO \"{schema}\".orders VALUES (1, 'eu'), (2, 'eu')"))
    runtime = PgFederationRuntime(
        engine_dsn=f"postgresql://provisa:{password}@{host}:{port}/provisa"
    )
    # The engine's own server dials the store: inside its container it listens on 5432.
    dsn = f"postgresql://provisa:{password}@localhost:5432/{store_db}"
    yield runtime, store, dsn, schema
    cur = runtime._con.cursor()
    cur.execute('DROP SERVER IF EXISTS "region_eu" CASCADE')
    cur.execute(f'DROP SCHEMA IF EXISTS "region_eu__{schema}" CASCADE')
    runtime.close()
    store.dispose()
    with admin.connect() as conn:
        conn.execute(sa.text(f'DROP DATABASE "{store_db}" WITH (FORCE)'))
    admin.dispose()


def _rows(runtime, sql: str) -> list[tuple]:
    cur = runtime._con.cursor()
    cur.execute(sql)
    return cur.fetchall()


def test_the_pg_engine_reads_another_regions_replica_through_postgres_fdw(eu):
    runtime, _store, dsn, schema = eu
    catalog, local, table = runtime.attach_region_table("eu", dsn, schema, "orders")
    assert (local, table) == (f"region_eu__{schema}", "orders")
    assert _rows(
        runtime, f'SELECT id, region FROM "{catalog}"."{local}"."{table}" ORDER BY id'
    ) == [
        (1, "eu"),
        (2, "eu"),
    ]
    # Read only: the region's replicas are its own to build.
    with pytest.raises(psycopg2.Error, match="does not allow inserts"):
        _rows(runtime, f'INSERT INTO "{local}"."{table}" VALUES (3, \'us\') RETURNING id')


def test_a_replica_whose_columns_changed_is_read_as_it_is_now(eu):
    runtime, store, dsn, schema = eu
    runtime.attach_region_table("eu", dsn, schema, "orders")
    with store.begin() as conn:
        conn.execute(sa.text(f'ALTER TABLE "{schema}".orders ADD COLUMN total numeric'))
        conn.execute(sa.text(f'UPDATE "{schema}".orders SET total = id * 10'))
    _catalog, local, table = runtime.attach_region_table("eu", dsn, schema, "orders")
    assert _rows(runtime, f'SELECT id, total FROM "{local}"."{table}" ORDER BY id') == [
        (1, 10),
        (2, 20),
    ]


def test_a_store_that_moved_is_dialed_at_its_new_address(eu):
    runtime, _store, dsn, schema = eu
    stale = dsn.replace("@localhost:5432/", "@127.0.0.1:5432/")
    runtime.attach_region_table("eu", stale, schema, "orders")
    runtime.attach_region_table("eu", dsn, schema, "orders")
    options = _rows(runtime, "SELECT srvoptions FROM pg_foreign_server WHERE srvname = 'region_eu'")
    assert "host=localhost" in options[0][0]
    assert "updatable=false" in options[0][0]
