# Copyright (c) 2026 Kenneth Stott
# Canary: 8f2c6a15-3e9b-4d70-b1a4-5c7d0e2f9a63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a source created through the admin API can be replicated and read (REQ-1695).

A source created through the admin API keeps its password in the org's vault and carries a
``${secret:...}`` reference to it. Building its replica has to resolve that reference, under the
org the source belongs to, at the point the build runs — the build is not always reached through
a path that already bound the org's vault. Before the fix every read after replication was turned
on answered 500: ``Cannot resolve ${secret:source_src_password}: no organization is bound to this
context``.

A real server per engine (pg and DuckDB) over the test's own source Postgres: the source and its
table are created through ``/admin/graphql``, read live, set to replicate through the admin API,
and read again — from the replica.
"""

# Requirements: REQ-1695, REQ-826

from __future__ import annotations

import tempfile
from pathlib import Path

import httpx
import pytest
import yaml

from tests.helpers import registered_id, release_field

from tests.integration.test_pg_engine_landing_never_writes_source_e2e import (
    _ROLE,
    _ROWS,
    _SourceAndEngine,
)

pytestmark = [pytest.mark.integration]

pytest.importorskip("duckdb")

_ID_AMOUNT = [{"id": i, "amount": amount} for i, amount, _note in _ROWS]


def _config(engine: str) -> dict:
    return {
        "federation_engine": engine,
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "shop", "description": "Shop"}],
        "roles": [
            {
                "id": _ROLE,
                "capabilities": [
                    "source_registration",
                    "table_registration",
                    "query_development",
                    "access_config",
                ],
                "domain_access": ["*"],
            }
        ],
        "sources": [],
        "tables": [],
    }


@pytest.fixture
def databases():
    pg = _SourceAndEngine()
    pg.start()
    try:
        yield pg
    finally:
        pg.stop()


def _server(pg: _SourceAndEngine, engine: str, workdir: str):
    from tests.integration.isolated_server import IsolatedServer

    path = Path(workdir) / "config.yaml"
    path.write_text(yaml.safe_dump(_config(engine)))
    if engine == "pg":
        control_plane = pg.url(pg.engine_port, "provisa", "+psycopg")
        return IsolatedServer(
            "api_source_replication_pg",
            engine="pg",
            config=str(path),
            control_plane="postgres",
            env={
                "PLATFORM_DATABASE_URL": control_plane,
                "TENANT_DATABASE_URL": control_plane,
                "PROVISA_MATERIALIZE_URL": pg.url(pg.engine_port, "provisa"),
                "PROVISA_REDIS_EMBEDDED": "1",
            },
        )
    return IsolatedServer(
        "api_source_replication_duckdb",
        engine="duckdb",
        config=str(path),
        control_plane="sqlite",
        materialize_store_url=f"duckdb:///{Path(workdir) / 'materialize.duckdb'}",
    )


def _admin(srv, mutation: str) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": "mutation { " + mutation + " { success message } }"},
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 10,
    )
    assert response.status_code == 200, response.text
    assert "errors" not in response.json(), response.text
    (result,) = response.json()["data"].values()
    assert result["success"], result["message"]
    return result


def _read(srv) -> httpx.Response:
    return httpx.post(
        f"{srv.base_url}/data/graphql",
        json={"query": "{ orders { id amount } }"},
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 10,
    )


@pytest.mark.parametrize("engine", ["pg", "duckdb"])
def test_a_source_created_through_the_admin_api_is_replicated_and_read(databases, engine):
    pg = databases
    with tempfile.TemporaryDirectory() as workdir:
        srv = _server(pg, engine, workdir)
        try:
            srv.start()
            _admin(
                srv,
                'createSource(input: {id: "src", type: "postgresql", host: "127.0.0.1", '
                f'port: {pg.source_port}, database: "shop", username: "provisa", '
                'password: "provisa"})',
            )
            registered = _admin(
                srv,
                'registerTable(input: {sourceId: "src", domainId: "shop", schemaName: "public", '
                'tableName: "orders", columns: ['
                '{name: "id", visibleTo: ["org_admin"], dataType: "integer", isPrimaryKey: true}, '
                '{name: "amount", visibleTo: ["org_admin"], dataType: "double"}]})',
            )
            # REQ-1921: registered through the admin, it starts as draft; released, it is read.
            _admin(srv, release_field(registered_id(registered["message"])))
            live = _read(srv)
            assert live.status_code == 200, live.text
            assert sorted(live.json()["data"]["orders"], key=lambda r: r["id"]) == _ID_AMOUNT

            # REQ-1907: a replicated table on the ttl change signal needs its landing TTL.
            _admin(srv, 'updateSourceCache(sourceId: "src", cacheEnabled: true, cacheTtl: 3600)')
            _admin(srv, 'updateSourceReplicate(sourceId: "src", replicate: 0)')
            replicated = _read(srv)
            assert replicated.status_code == 200, replicated.text
            assert sorted(replicated.json()["data"]["orders"], key=lambda r: r["id"]) == _ID_AMOUNT

            # The read is the replica's: a later change in the source is not seen.
            pg.update_source("UPDATE orders SET amount = 999 WHERE id = 1")
            again = _read(srv)
            assert again.status_code == 200, again.text
            assert sorted(again.json()["data"]["orders"], key=lambda r: r["id"]) == _ID_AMOUNT
        finally:
            srv.stop_process()
