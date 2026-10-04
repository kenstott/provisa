# Copyright (c) 2026 Kenneth Stott
# Canary: 1851721a-6fdf-4c4f-bfe7-9889452268f2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a table its region reads in place is read in place by the org's other regions
too, under the reader's governance (REQ-1921, REQ-1922).

Two real DuckDB-engine nodes of one org, one in region ``eu`` and one in ``us``, over a shared
model store in Postgres. A CSV source's table names ``eu``; eu's DuckDB scans the file in place,
so eu keeps no replica of it. A read on us goes to the file itself — never refused for a replica
that was never meant to exist, never copied into us — with the row-level rule applied for the
reader: each region sees only its own rows (``provisa.region``)."""

# Requirements: REQ-1921, REQ-1922

from __future__ import annotations

import base64
import os
import tempfile
import time
from pathlib import Path

import httpx
import psycopg
import pytest
import yaml

from tests.integration.test_pg_engine_landing_never_writes_source_e2e import _SourceAndEngine

pytestmark = [pytest.mark.integration]

pytest.importorskip("duckdb")

_NAME = "region_read_in_place"
_ROLE = "org_admin"
_MASTER_KEY = base64.b64encode(os.urandom(32)).decode()
_RELOAD_S, _TIMEOUT_S = 0.5, 20


def _config(stack: "_Stack", files: Path, work: Path) -> dict:
    v = {"visible_to": [_ROLE]}
    stores, regions = [], []
    for region in ("eu", "us"):
        stores += [
            {"id": f"{region}-duck", "url": f"duckdb:///{work / region}.duckdb", "kind": "duckdb"},
            {"id": f"{region}-pg", "url": stack.url(stack.engine_port, f"{region}_store")},
            {"id": f"{region}-redis", "url": f"redis://127.0.0.1:{stack.redis_port}/0"},
        ]
        regions.append(
            {
                "id": region,
                "engine": f"{region}-duck",
                "replicas": f"{region}-pg",
                "views": f"{region}-pg",
                "cache": f"{region}-redis",
                "state": f"{region}-pg",
                "record": f"{region}-pg",
            }
        )
    return {
        "federation_engine": "duckdb",
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
            "limits": {"request_timeout": _TIMEOUT_S},
        },
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "sales", "description": "Sales"}],
        "roles": [
            {
                "id": _ROLE,
                "capabilities": ["query_development", "observability"],
                "domain_access": ["*"],
            }
        ],
        "sources": [{"id": "orders_csv", "type": "csv", "path": str(files)}],
        "tables": [
            {
                "source_id": "orders_csv",
                "domain_id": "sales",
                "schema": "main",
                "table": "orders",
                "region": "eu",
                "columns": [
                    {"name": "id", "data_type": "integer", "is_primary_key": True, **v},
                    {"name": "amount", "data_type": "integer", **v},
                    {"name": "region", "data_type": "varchar", **v},
                ],
            }
        ],
        "relationships": [],
        # Each region's readers see that region's rows (provisa.region is the answering node's).
        "rls_rules": [
            {
                "table_id": "orders",
                "role_id": _ROLE,
                "filter": "region = current_setting('provisa.region')",
            }
        ],
        "functions": [],
        "webhooks": [],
    }


class _Stack(_SourceAndEngine):
    """The engine Postgres (the shared model store and each region's stores) and a Redis."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        super().__init__()
        (self.redis_port,) = lease_ports(1)
        self._redis = f"provisa-itest-readinplace-redis-{os.getpid()}"

    def start(self) -> None:
        import subprocess

        super().start()
        with psycopg.connect(self.url(self.engine_port, "provisa"), autocommit=True) as conn:
            for region in ("eu", "us"):
                conn.execute(f"CREATE DATABASE {region}_store")
        subprocess.run(
            ["docker", "run", "-d", "--rm", "--name", self._redis]
            + ["-p", f"127.0.0.1:{self.redis_port}:6379", "redis:7-alpine"],
            check=True,
            capture_output=True,
        )

    def stop(self) -> None:
        import subprocess

        subprocess.run(["docker", "rm", "-f", self._redis], capture_output=True)
        super().stop()

    def replicas_tables(self, region: str) -> list[str]:
        """Every table in ``region``'s replicas store: a copy of the files table would be one."""
        with psycopg.connect(self.url(self.engine_port, f"{region}_store")) as conn:
            return [
                r[0]
                for r in conn.execute(
                    "SELECT table_schema || '.' || table_name FROM information_schema.tables "
                    "WHERE table_schema LIKE '%%_replicas'"
                )
            ]


def _server(stack: _Stack, region: str, config: Path):
    from tests.integration.isolated_server import IsolatedServer

    control_plane = stack.url(stack.engine_port, "provisa", "+psycopg")
    return IsolatedServer(
        _NAME,
        engine="duckdb",
        config=str(config),
        control_plane="postgres",
        env={
            "PLATFORM_DATABASE_URL": control_plane,
            "TENANT_DATABASE_URL": control_plane,
            "PROVISA_REGION": region,
            "PROVISA_ENCRYPTION_KEY": _MASTER_KEY,
            "PROVISA_REDIS_EMBEDDED": "1",
        },
    )


def _read(srv) -> tuple[int, object]:
    response = httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": 'SELECT "id", "region" FROM "orders" ORDER BY "id"', "role": _ROLE},
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 10,
    )
    if response.status_code != 200:
        return response.status_code, response.text
    rows = response.json()["data"]["sql"]
    return 200, [(int(r["id"]), r["region"]) for r in rows]


def _until_answered(srv) -> list[tuple[int, str]]:
    deadline, last = time.monotonic() + 90, None
    while time.monotonic() < deadline:
        status, last = _read(srv)
        if status == 200 and last:
            return last  # type: ignore[return-value]
        time.sleep(1)
    raise AssertionError(f"{srv.base_url} never answered: {last}")


@pytest.fixture
def stack():
    s = _Stack()
    s.start()
    try:
        yield s
    finally:
        s.stop()


def test_a_table_its_region_reads_in_place_is_read_in_place_by_another_region(stack):
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        files = work / "orders.csv"
        files.write_text("id,amount,region\n1,10,eu\n2,20,us\n3,30,us\n4,40,eu\n")
        config = work / "config.yaml"
        config.write_text(yaml.safe_dump(_config(stack, files, work)))
        eu, us = _server(stack, "eu", config), _server(stack, "us", config)
        try:
            eu.start()
            us.start()
            # The home region reads its files in place; each reader sees its region's rows.
            assert _until_answered(eu) == [(1, "eu"), (4, "eu")]
            # The other region reads the same files in place — no replica asked of eu, none
            # built in us — under its own rule.
            assert _until_answered(us) == [(2, "us"), (3, "us")]
            time.sleep(2 * _RELOAD_S + 2)  # a build a read asked for would have started
            assert stack.replicas_tables("eu") == []
            assert stack.replicas_tables("us") == []
        finally:
            eu.stop_process()
            us.stop_process()
