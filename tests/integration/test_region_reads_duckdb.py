# Copyright (c) 2026 Kenneth Stott
# Canary: e267dd17-a754-423b-84a2-b861a4f7dd47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DuckDB reads a table another region keeps from that region's replicas store (REQ-1922): the
store is attached read-only under ``region_<id>``, and a read routed there answers from the
replica that region built — against a real PostgreSQL store."""

# Requirements: REQ-1922

from __future__ import annotations

import os
import uuid

import duckdb
import pytest
import sqlalchemy as sa

pytestmark = [pytest.mark.integration]


@pytest.fixture
def eu_replicas(docker_postgres):
    """The eu region's replicas store, holding one built replica of ``crm.public.orders``."""
    schema = f"org_reg{uuid.uuid4().hex[:8]}_replicas"
    url = (
        f"postgresql://provisa:{os.environ.get('PG_PASSWORD', 'provisa')}@"
        f"{docker_postgres['host']}:{docker_postgres['port']}/provisa"
    )
    engine = sa.create_engine(url.replace("postgresql://", "postgresql+psycopg://"))
    with engine.begin() as conn:
        conn.execute(sa.text(f'CREATE SCHEMA "{schema}"'))
        conn.execute(sa.text(f'CREATE TABLE "{schema}".crm__public__orders (id int, region text)'))
        conn.execute(
            sa.text(f"INSERT INTO \"{schema}\".crm__public__orders VALUES (1, 'eu'), (2, 'eu')")
        )
    yield url, schema
    with engine.begin() as conn:
        conn.execute(sa.text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


def test_a_node_reads_another_regions_replica_through_its_attached_store(eu_replicas):
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    url, schema = eu_replicas
    runtime = DuckDBFederationRuntime.__new__(DuckDBFederationRuntime)
    runtime._con = duckdb.connect()
    runtime._region_stores = set()
    # The org environment's name for eu's store (replica_address.region_read_name).
    alias = runtime.attach_region_store("org_acme__region_eu", url)
    assert alias == "org_acme__region_eu"
    assert runtime.attach_region_store("org_acme__region_eu", url) == alias  # attached once
    rows = runtime._con.execute(
        f'SELECT id, region FROM "{alias}"."{schema}".crm__public__orders ORDER BY id'
    ).fetchall()
    assert rows == [(1, "eu"), (2, "eu")]
    # Read only: the region's replicas are its own to build.
    with pytest.raises(duckdb.Error):
        runtime._con.execute(
            f'INSERT INTO "{alias}"."{schema}".crm__public__orders VALUES (3, \'us\')'
        )
