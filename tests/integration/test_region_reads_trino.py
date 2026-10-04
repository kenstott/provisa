# Copyright (c) 2026 Kenneth Stott
# Canary: 375b6867-2fd7-41e2-a4ee-9940b5b74443
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Trino reads a store other than the control plane through a catalog of that store (REQ-1048,
REQ-1922), against a real coordinator and a real PostgreSQL store: another region's replicas
store as ``org_<org>__region_<id>``, and an org's own store as ``org_<org>__store`` — where its
replicas and views are written, so where Trino reads them, not the ``provisa_admin`` catalog."""

# Requirements: REQ-1048, REQ-1922

from __future__ import annotations

import os
import uuid
from types import SimpleNamespace

import pytest
import sqlalchemy as sa
import trino

from provisa.core.region_stores import ForeignRegion

pytestmark = [pytest.mark.integration]

_ORG = "acme"


@pytest.fixture
def store(docker_postgres):
    """A schema in a PostgreSQL store holding one built replica of ``crm.public.orders``, and the
    store's DSN as the coordinator reaches it (``postgres:5432`` on the stack's network)."""
    schema = f"org_reg{uuid.uuid4().hex[:8]}_replicas"
    password = os.environ.get("PG_PASSWORD", "provisa")
    app_url = (
        f"postgresql+psycopg://provisa:{password}@"
        f"{docker_postgres['host']}:{docker_postgres['port']}/provisa"
    )
    engine = sa.create_engine(app_url)
    with engine.begin() as conn:
        conn.execute(sa.text(f'CREATE SCHEMA "{schema}"'))
        conn.execute(sa.text(f'CREATE TABLE "{schema}".crm__public__orders (id int, region text)'))
        conn.execute(
            sa.text(f"INSERT INTO \"{schema}\".crm__public__orders VALUES (1, 'eu'), (2, 'eu')")
        )
    yield SimpleNamespace(
        schema=schema,
        app_url=sa.make_url(app_url),
        engine_dsn=f"postgresql://provisa:{password}@postgres:5432/provisa",
    )
    with engine.begin() as conn:
        conn.execute(sa.text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


@pytest.fixture
def trino_state(store):
    """The Trino backend with an org's terminal bound to the stack's coordinator."""
    from provisa.federation.engine import build_engine
    from provisa.federation.trino_lifecycle import ENGINE_USER, engine_source

    conn = trino.dbapi.connect(
        host="localhost",
        port=int(os.environ["TRINO_PORT"]),
        user=ENGINE_USER,
        source=engine_source(_ORG),
        http_scheme="http",
    )
    state = SimpleNamespace(
        org_id=_ORG,
        engine_conn=conn,
        engine_conn_kwargs={},
        tenant_engine=SimpleNamespace(url=store.app_url),
    )
    backend = build_engine("trino").backend
    yield backend, state, conn
    cur = conn.cursor()
    for catalog in backend.__dict__.get("_store_catalogs", {}):
        cur.execute(f"DROP CATALOG IF EXISTS {catalog}")
        cur.fetchall()
    conn.close()


def _rows(conn, sql: str) -> list:
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


def test_trino_reads_another_regions_replica_through_a_catalog_of_its_store(store, trino_state):
    backend, state, conn = trino_state
    region = ForeignRegion("eu", store.engine_dsn, None)  # type: ignore[arg-type]
    catalog, schema, table = backend.region_read_address(
        state, region, store.schema, "crm__public__orders"
    )
    # A read that finds the replica built attaches it (registers the catalog).
    backend.attach_region_read(state, region, store.schema, "crm__public__orders", ("h", "[]"))
    assert (catalog, schema, table) == (
        f"org_{_ORG}__region_eu",
        store.schema,
        "crm__public__orders",
    )
    assert _rows(
        conn, f'SELECT id, region FROM {catalog}."{store.schema}".crm__public__orders ORDER BY id'
    ) == [[1, "eu"], [2, "eu"]]


def test_trino_reads_an_orgs_own_store_through_its_catalog(store, trino_state, monkeypatch):
    backend, state, conn = trino_state
    monkeypatch.setattr(
        "provisa.storage.byo.org_store_dsn",
        lambda org: store.engine_dsn if org == _ORG else None,
    )
    catalog, _schema = backend.materialize_store_target(state, _ORG)
    assert catalog == f"org_{_ORG}__store"
    assert _rows(conn, f'SELECT count(*) FROM {catalog}."{store.schema}".crm__public__orders') == [
        [2]
    ]
