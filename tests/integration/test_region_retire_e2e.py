# Copyright (c) 2026 Kenneth Stott
# Canary: 86b309c9-7d0e-4763-ba24-193938c1886a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a copy left in a region that is no longer its table's home is retired and
dropped (REQ-1921, REQ-1922).

Two real pg-engine nodes of one org, one in region ``eu`` and one in ``us``, over one SOURCE
Postgres and an ENGINE Postgres holding the shared model store and each region's own store
(engine, replicas, state and record: databases ``eu_store`` / ``us_store``), with a Redis as each
region's cache. A replicated table naming no region is built in both regions. Then:

* it names ``eu``: the copy us built goes from us's store (retired, then dropped after the grace),
  eu's stays and answers; a read on us is answered from eu's replica in place and builds no copy
  there;
* it names ``us``: the copy eu built goes from eu's store, us builds its own and answers, and a
  read on eu is answered from us's replica the same way.

The table's region is written in the model store as the operator's change (the admin has no
region field on this branch); the write advances the model stamp, so every node reloads.
"""

# Requirements: REQ-1921, REQ-1922

from __future__ import annotations

import base64
import os
import subprocess
import tempfile
import time
from pathlib import Path

import httpx
import psycopg
import pytest
import yaml

from tests.integration.test_pg_engine_landing_never_writes_source_e2e import (
    _ROLE,
    _ROWS,
    _SourceAndEngine,
)

pytestmark = [pytest.mark.integration]

_NAME = "region_retire"
_REPLICA = f'"org_{_NAME}_replicas"."src__public__orders"'
_RELOAD_S, _TIMEOUT_S = 0.5, 3
_ID_AMOUNT = [(i, amount) for i, amount, _note in _ROWS]
_MASTER_KEY = base64.b64encode(os.urandom(32)).decode()


class _Stack:
    """The source and engine Postgres, each region's store database, and a Redis."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        self.pg = _SourceAndEngine()
        (self.redis_port,) = lease_ports(1)
        self._redis = f"provisa-itest-regretire-redis-{os.getpid()}"

    def start(self) -> None:
        self.pg.start()
        with psycopg.connect(self.pg.url(self.pg.engine_port, "provisa"), autocommit=True) as c:
            for region in ("eu", "us"):
                c.execute(f"CREATE DATABASE {region}_store")
        subprocess.run(
            ["docker", "run", "-d", "--rm", "--name", self._redis]
            + ["-p", f"127.0.0.1:{self.redis_port}:6379", "redis:7-alpine"],
            check=True,
            capture_output=True,
        )

    def stop(self) -> None:
        subprocess.run(["docker", "rm", "-f", self._redis], capture_output=True)
        self.pg.stop()

    def store_url(self, region: str, driver: str = "") -> str:
        return self.pg.url(self.pg.engine_port, f"{region}_store", driver)

    def replica_rows(self, region: str) -> int | None:
        """The row count of ``region``'s copy, or None when its store has no such table."""
        with psycopg.connect(self.store_url(region), autocommit=True) as conn:
            if conn.execute("SELECT to_regclass(%s)", (_REPLICA,)).fetchone()[0] is None:
                return None
            return conn.execute(f"SELECT COUNT(*) FROM {_REPLICA}").fetchone()[0]

    def set_region(self, region: str | None) -> None:
        """The operator's change of the table's region, in the shared model store."""
        with psycopg.connect(self.pg.url(self.pg.engine_port, "provisa"), autocommit=True) as c:
            (schema,) = c.execute(
                "SELECT table_schema FROM information_schema.tables "
                "WHERE table_name = 'registered_tables' AND table_schema LIKE %s",
                (f"org_{_NAME}%",),
            ).fetchone()
            updated = c.execute(
                f'UPDATE "{schema}".registered_tables SET region = %s WHERE table_name = %s',
                (region, "orders"),
            )
            assert updated.rowcount == 1


def _config(stack: _Stack) -> dict:
    stores = []
    regions = []
    for region in ("eu", "us"):
        stores += [
            {"id": f"{region}-pg", "url": stack.store_url(region), "kind": "pg"},
            {"id": f"{region}-redis", "url": f"redis://127.0.0.1:{stack.redis_port}/0"},
        ]
        regions.append(
            {
                "id": region,
                "engine": f"{region}-pg",
                "replicas": f"{region}-pg",
                "views": f"{region}-pg",
                "cache": f"{region}-redis",
                "state": f"{region}-pg",
                "record": f"{region}-pg",
            }
        )
    return {
        "federation_engine": "pg",
        "platform": {
            "regions": [
                {"id": "eu", "address": "https://eu.example.com"},
                {"id": "us", "address": "https://us.example.com"},
            ]
        },
        "stores": stores,
        "regions": regions,
        "server": {
            "config_reload_interval": _RELOAD_S,
            "limits": {
                "request_timeout": _TIMEOUT_S,
                "request_timeouts": {"flight": _TIMEOUT_S, "pgwire": _TIMEOUT_S},
            },
        },
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
                    "observability",
                ],
                "domain_access": ["*"],
            }
        ],
        "sources": [],
        "tables": [],
    }


def _server(stack: _Stack, region: str, workdir: str):
    from tests.integration.isolated_server import IsolatedServer

    path = Path(workdir) / "config.yaml"
    path.write_text(yaml.safe_dump(_config(stack)))
    control_plane = stack.pg.url(stack.pg.engine_port, "provisa", "+psycopg")
    return IsolatedServer(
        _NAME,
        engine="pg",
        config=str(path),
        control_plane="postgres",
        env={
            "PLATFORM_DATABASE_URL": control_plane,
            "TENANT_DATABASE_URL": control_plane,
            "PROVISA_REGION": region,
            # The nodes of one deployment hold one master key (secrets are written under it).
            "PROVISA_ENCRYPTION_KEY": _MASTER_KEY,
            "PROVISA_REDIS_EMBEDDED": "1",
        },
    )


def _admin(srv, document: str) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": document},
        headers={"X-Provisa-Role": _ROLE},
        timeout=60,
    )
    assert response.status_code == 200, response.text
    assert "errors" not in response.json(), response.text
    return response.json()["data"]


def _mutate(srv, mutation: str) -> None:
    (result,) = _admin(srv, "mutation { " + mutation + " { success message } }").values()
    assert result["success"], result["message"]


def _read(srv) -> list[tuple]:
    response = httpx.post(
        f"{srv.base_url}/data/graphql",
        json={"query": "{ orders { id amount } }"},
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 10,
    )
    assert response.status_code == 200, response.text
    assert "errors" not in response.json(), response.text
    return sorted((row["id"], row["amount"]) for row in response.json()["data"]["orders"])


def _wait(condition, *, seconds: float, what: str) -> None:
    deadline = time.monotonic() + seconds
    while not condition():
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        time.sleep(0.25)


@pytest.fixture
def stack():
    s = _Stack()
    s.start()
    try:
        yield s
    finally:
        s.stop()


def test_a_copy_in_a_region_no_longer_its_tables_home_is_retired_and_dropped(stack):
    with tempfile.TemporaryDirectory() as dir_eu, tempfile.TemporaryDirectory() as dir_us:
        eu, us = _server(stack, "eu", dir_eu), _server(stack, "us", dir_us)
        try:
            eu.start()
            us.start()
            _mutate(
                eu,
                'createSource(input: {id: "src", type: "postgresql", host: "127.0.0.1", '
                f'port: {stack.pg.source_port}, database: "shop", username: "provisa", '
                'password: "provisa"})',
            )
            _mutate(
                eu,
                # No region: chosen explicitly (left out, a new table starts in the connected one).
                'registerTable(input: {sourceId: "src", domainId: "shop", schemaName: "public", '
                'tableName: "orders", region: null, columns: ['
                '{name: "id", visibleTo: ["org_admin"], dataType: "integer", isPrimaryKey: true}, '
                '{name: "amount", visibleTo: ["org_admin"], dataType: "double"}]})',
            )
            _mutate(eu, 'updateSourceCache(sourceId: "src", cacheEnabled: true, cacheTtl: 3600)')
            _mutate(eu, 'updateSourceReplicate(sourceId: "src", replicate: 0)')

            # No region: every region keeps its own copy.
            for region in ("eu", "us"):
                _wait(
                    lambda r=region: stack.replica_rows(r) == len(_ROWS),
                    seconds=90,
                    what=f"{region}'s copy",
                )

            # The table names eu: us's copy goes, eu's stays; eu answers, and us answers from
            # eu's replica in place, building no copy of its own for the read.
            stack.set_region("eu")
            _wait(lambda: stack.replica_rows("us") is None, seconds=90, what="us's copy dropped")
            assert stack.replica_rows("eu") == len(_ROWS)
            assert _read(eu) == _ID_AMOUNT
            assert _read(us) == _ID_AMOUNT
            time.sleep(2 * _RELOAD_S + 2)  # a build the read asked for would have started
            assert stack.replica_rows("us") is None

            # It moves to us: eu's copy goes, us builds its own; us answers, and eu answers from
            # us's replica in place.
            stack.set_region("us")
            _wait(lambda: stack.replica_rows("eu") is None, seconds=90, what="eu's copy dropped")
            _wait(lambda: stack.replica_rows("us") == len(_ROWS), seconds=90, what="us's new copy")
            assert _read(us) == _ID_AMOUNT
            assert _read(eu) == _ID_AMOUNT
            time.sleep(2 * _RELOAD_S + 2)
            assert stack.replica_rows("eu") is None
        except AssertionError as failed:
            raise AssertionError(f"{failed}\n{_logged_errors(eu, us)}") from failed
        finally:
            eu.stop_process()
            us.stop_process()


def _logged_errors(*servers) -> str:
    """The errors each node logged, to say why a wait timed out."""
    out = []
    for srv in servers:
        lines = Path(srv._stderr_file.name).read_text(errors="replace").splitlines()
        blocks, block = [], None
        for ln in lines:
            if ln.startswith("Traceback") or " ERROR " in ln:
                block = [ln]
                blocks.append(block)
            elif block is not None:
                block.append(ln)
                if ln and not ln.startswith((" ", "Traceback", "During", "The above")):
                    block = None
        shown = "\n".join("\n".join(b[-25:]) for b in blocks[-6:])
        out.append(f"--- {srv.base_url}: {len(lines)} lines\n{shown}")
    return "\n".join(out)
