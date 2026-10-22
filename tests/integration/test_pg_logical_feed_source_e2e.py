# Copyright (c) 2026 Kenneth Stott
# Canary: 695ee239-fdc2-4269-825c-2cc4cda925f1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a PostgreSQL table on the native change signal follows its database's logical
replication (REQ-929).

A pg-engine server reads a table of a PostgreSQL run with ``wal_level=logical``. The source is
registered through the admin API on the native signal and its table set to be served from its
replica. The first read builds the replica. A row inserted at the source then reaches reads over
/data/sql through a rebuild the slot asked for -- the read itself never asks, the table being
fresh by its feed. The slot and its publication stand on the source while the table follows it,
and are dropped once the source is set off the native signal.
"""

# Requirements: REQ-929, REQ-1861

from __future__ import annotations

import tempfile
import time
from pathlib import Path

import httpx
import psycopg
import pytest
import yaml

from provisa.subscriptions import pg_logical
from tests.embedded_pg import DATABASE, PASSWORD, ROLE, start_local_postgres
from tests.helpers import registered_id, release_field

pytestmark = [pytest.mark.integration]

_NAME = "pg_logical_feed"
_ADMIN = "org_admin"
# The org the server runs as (IsolatedServer(_NAME)), its prod environment, source shop_pg.
_SLOT = pg_logical.slot_name(_NAME, None, "shop_pg")


def _config() -> dict:
    return {
        "federation_engine": "pg",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ADMIN}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "shop", "description": "Shop"}],
        "roles": [],
        "sources": [],
        "tables": [],
    }


@pytest.fixture
def source_pg():
    pg = start_local_postgres(settings={"wal_level": "logical", "synchronous_commit": "on"})
    try:
        with psycopg.connect(pg.dsn(), autocommit=True) as conn:
            conn.execute("CREATE TABLE orders (id integer PRIMARY KEY, name text)")
            conn.execute("INSERT INTO orders VALUES (1, 'first')")
        yield pg
    finally:
        pg.stop()


@pytest.fixture
def server(source_pg):
    from tests.integration.isolated_server import IsolatedServer

    store = start_local_postgres()
    try:
        with tempfile.TemporaryDirectory() as workdir:
            path = Path(workdir) / "config.yaml"
            path.write_text(yaml.safe_dump(_config()))
            srv = IsolatedServer(
                _NAME,
                engine="pg",
                config=str(path),
                control_plane="own",
                # The engine runs on the store this test starts, beside its sources: left to the
                # control plane, it would run in the stack's container, where 127.0.0.1 is not them.
                env={"PROVISA_MATERIALIZE_URL": store.dsn(), "PROVISA_ENGINE_URL": store.dsn()},
            )
            try:
                srv.start()
                yield srv
            finally:
                srv.stop_process()
    finally:
        store.stop()


def _mutate(srv, mutation: str) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": "mutation { " + mutation + " { success message } }"},
        headers={"X-Provisa-Role": _ADMIN},
        timeout=60,
    )
    assert response.status_code == 200, response.text
    (result,) = response.json()["data"].values()
    assert result["success"], result["message"]
    return result


def _source_input(pg, signal: str) -> str:
    return (
        f'{{id: "shop_pg", type: "postgresql", host: "127.0.0.1", port: {pg.port}, '
        f'database: "{DATABASE}", username: "{ROLE}", password: "{PASSWORD}", '
        f'changeSignal: "{signal}", cacheTtl: 3600}}'
    )


def _read(srv) -> list[int]:
    response = httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": "SELECT id, name FROM orders ORDER BY id", "role": _ADMIN},
        timeout=120,
    )
    assert response.status_code == 200, f"{response.text}\n{srv.dump_stderr_debug()[-6000:]}"
    body = response.json()
    rows = body.get("data") or body.get("rows") or []
    if isinstance(rows, dict):
        rows = next(iter(rows.values()))
    return [int(r["id"]) for r in rows]


def _held(pg) -> tuple[int, int] | None:
    """Provisa's slot and publication on the source: (slots, publications) of that name."""
    with psycopg.connect(pg.dsn(), autocommit=True) as conn:
        return conn.execute(
            "SELECT (SELECT count(*) FROM pg_replication_slots WHERE slot_name = %s), "
            "(SELECT count(*) FROM pg_publication WHERE pubname = %s)",
            (_SLOT, _SLOT),
        ).fetchone()


def _wait(condition, what: str, srv) -> None:
    deadline = time.monotonic() + 90
    while not condition():
        assert time.monotonic() < deadline, f"{what}\n{srv.dump_stderr_debug()[-6000:]}"
        time.sleep(0.5)


def test_a_native_table_follows_its_databases_logical_replication(server, source_pg):
    _mutate(server, f"createSource(input: {_source_input(source_pg, 'native')})")
    registered = _mutate(
        server,
        'registerTable(input: {sourceId: "shop_pg", domainId: "shop", schemaName: "public", '
        'tableName: "orders", columns: ['
        '{name: "id", visibleTo: ["org_admin"], dataType: "integer", isPrimaryKey: true}, '
        '{name: "name", visibleTo: ["org_admin"], dataType: "varchar"}]})',
    )
    _mutate(server, release_field(registered_id(registered["message"])))
    # Served from its replica: reads never reach the source live.
    _mutate(server, 'updateSourceReplicate(sourceId: "shop_pg", replicate: 0)')

    _wait(lambda: _read(server) == [1], "the first read never answered from the replica", server)
    _wait(lambda: _held(source_pg) == (1, 1), "the slot was never made on the source", server)

    with psycopg.connect(source_pg.dsn(), autocommit=True) as conn:
        conn.execute("INSERT INTO orders VALUES (2, 'second')")
    _wait(lambda: _read(server) == [1, 2], "an inserted row never reached the replica", server)

    _mutate(server, f"updateSource(input: {_source_input(source_pg, 'ttl')})")
    _wait(lambda: _held(source_pg) == (0, 0), "the slot outlived the native signal", server)


def _file_config(pg, signal: str) -> dict:
    """The deployment's file: the source and its table declared in it, on ``signal``."""
    config = _config()
    config["sources"] = [
        {
            "id": "shop_pg",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": pg.port,
            "database": DATABASE,
            "username": ROLE,
            "password": PASSWORD,
            "change_signal": signal,
            "cache_ttl": 3600,
            "replicate": 0,
        }
    ]
    config["tables"] = [
        {
            "source_id": "shop_pg",
            "domain_id": "shop",
            "schema": "public",
            "table": "orders",
            "columns": [
                {"name": "id", "data_type": "integer", "is_primary_key": True,
                 "visible_to": [_ADMIN]},
                {"name": "name", "data_type": "varchar", "visible_to": [_ADMIN]},
            ],
        }
    ]  # fmt: skip
    return config


def _boot(workdir: str, config: dict, store) -> object:
    from tests.integration.isolated_server import IsolatedServer

    path = Path(workdir) / "config.yaml"
    path.write_text(yaml.safe_dump(config))
    srv = IsolatedServer(
        _NAME,
        engine="pg",
        config=str(path),
        control_plane="own",
        # The engine runs on the store this test starts, beside its sources: left to the
        # control plane, it would run in the stack's container, where 127.0.0.1 is not them.
        env={"PROVISA_MATERIALIZE_URL": store.dsn(), "PROVISA_ENGINE_URL": store.dsn()},
    )
    srv.start()
    return srv


def test_a_slot_left_by_a_config_edited_while_stopped_is_dropped_at_the_next_start(source_pg):
    """The file sets the source off the native signal while no server runs: the next server's
    first wiring finds the slot no native table follows and drops it with its publication."""
    store = start_local_postgres()
    try:
        with tempfile.TemporaryDirectory() as workdir:
            first = _boot(workdir, _file_config(source_pg, "native"), store)
            try:
                _wait(lambda: _held(source_pg) == (1, 1), "the slot was never made", first)
            finally:
                first.stop_process()
            second = _boot(workdir, _file_config(source_pg, "ttl"), store)
            try:
                _wait(lambda: _held(source_pg) == (0, 0), "the abandoned slot stayed", second)
            finally:
                second.stop_process()
    finally:
        store.stop()
