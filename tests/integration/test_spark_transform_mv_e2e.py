# Copyright (c) 2026 Kenneth Stott
# Canary: 4c7e2a91-6b38-4f05-9d1a-e83b5f0c2d74
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A view's table transform run on Spark (REQ-2225, amended 2026-11-23): a materialized view over a
call of a Spark command given a governed table, landed as any view is and refreshed on its schedule
and when that table changes, never run on a read.

The ``test`` instance only: a Provisa server on DuckDB with its own control plane, serving Arrow
Flight over TLS at this machine's address; a one-node Spark Connect server in a container; the
Spark connector's host, joined with a script whose @query reads its table argument as a DataFrame
(REQ-2270); and a PostgreSQL of the test's own holding ``ledger.clips``, replicated, on the
``signal`` change signal.

- The view lands the rows a live call of the command returns.
- A row added to ``ledger.clips`` and signalled is in the view once its replica lands: the change
  refreshed the view.
- A view with a short interval refreshes again with nothing changed: its schedule refreshed it.
- outsider, whose domains leave out ``ledger``, may call the command and read the view's domain; it
  gets the refusal a live call gets, naming the table."""

# Requirements: REQ-2225, REQ-2226, REQ-2270

from __future__ import annotations

import json
import time

import httpx
import psycopg
import pytest
import yaml

from provisa.codesource.manifest import manifest_hash
from tests.embedded_pg import DATABASE, PASSWORD, ROLE, start_local_postgres
from tests.integration.test_spark_connector_e2e import (  # noqa: F401  # fixtures used by name
    _PROJECT_MANIFEST,
    _TABLE_SCRIPT,
    client_python,
    cluster,
    flight_tls,
    spark_host,
)

pytestmark = [pytest.mark.integration]

_ADMIN = "org_admin"
#: Calls the command and reads the views' domain; ledger, the argument table's, is not among its.
_OUTSIDER = "outsider"
_ROLES = (_ADMIN, _OUTSIDER)
_CALL = "clips_on_the_cluster('ledger.clips')"
_LIVE = f"SELECT c.id, c.region FROM {_CALL} AS c ORDER BY c.id"
#: Refreshed only when its input changes (or on a schedule far past this test).
_ON_CHANGE = "spark_clips"
#: Refreshed on a short schedule.
_SCHEDULED = "spark_clips_scheduled"
_SCHEDULE_S = 30
_START = [(1, "emea"), (2, "amer"), (3, "apac")]


@pytest.fixture(scope="module")
def source_pg():
    pg = start_local_postgres()
    try:
        with psycopg.connect(pg.dsn(), autocommit=True) as conn:
            conn.execute("CREATE TABLE clips (id bigint PRIMARY KEY, region text)")
            for clip in _START:
                conn.execute("INSERT INTO clips VALUES (%s, %s)", clip)
        yield pg
    finally:
        pg.stop()


def _config() -> dict:
    return {
        "federation_engine": "duckdb",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ADMIN}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [
            {"id": "fx", "description": "The Spark command and the views over it."},
            {"id": "ledger", "description": "The table the command is given."},
        ],
        "roles": [
            {
                "id": _OUTSIDER,
                "description": "Calls the Spark command and reads fx; cannot read ledger.",
                "parents": ["data_writer"],
                "capabilities": ["usage", "query_development", "command_execution", "full_results"],
                "domain_access": ["fx"],
            }
        ],
        "sources": [],
        "tables": [],
    }


@pytest.fixture(scope="module")
def provisa_server(tmp_path_factory, spark_host, flight_tls):  # noqa: F811  (the fixtures)
    """A Provisa server serving Arrow Flight over TLS at this machine's address, the connector's
    block and image labels stood in for (tests/integration/spark_connector_app.py)."""
    from tests.integration.isolated_server import IsolatedServer

    address, cert, key = flight_tls
    work = tmp_path_factory.mktemp("spark-transform-mv")
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(_config()))
    row = {
        "name": "spark",
        "git": "https://git.example/spark.git",
        "ref": "main",
        "commit": "a" * 40,
        "language": "python",
        "image": "registry.example/connectors/spark",
        "digest": "sha256:" + "b" * 64,
        "code_hash": spark_host.hashed,
        "arch": "amd64",
        "requires": [{"name": "script", "multiline": True, "script": True}, {"name": "cluster"}],
    }
    labels = {
        "code_hash": spark_host.hashed,
        "manifest_hash": manifest_hash(_PROJECT_MANIFEST),
        "runtime": "python",
    }
    srv = IsolatedServer(
        "spark_transform_mv",
        engine="duckdb",
        config=str(path),
        control_plane="own",
        materialize_store_url=f"duckdb:///{work / 'store.duckdb'}",
        await_flight=True,
        app="tests.integration.spark_connector_app:app",
        env={
            "PROVISA_RUNTIME_DEPS_CACHE": str(tmp_path_factory.mktemp("runtime-deps")),
            "PROVISA_HOSTNAME": address,
            "PROVISA_FLIGHT_CERT": str(cert),
            "PROVISA_FLIGHT_KEY": str(key),
            "PROVISA_TEST_SPARK_CONNECTOR": json.dumps(row),
            "PROVISA_TEST_SPARK_LABELS": json.dumps(labels),
        },
    )
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


def _admin(srv, mutation: str) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": "mutation { " + mutation + " { success message } }"},
        headers={"X-Provisa-Role": _ADMIN},
        timeout=srv.request_timeout + 600,
    )
    assert response.status_code == 200, response.text
    assert "errors" not in response.json(), response.text
    (result,) = response.json()["data"].values()
    assert result["success"], f"{result['message']}\n{srv.dump_stderr_debug()[-12000:]}"
    return result


def _column(name: str, data_type: str) -> str:
    readers = ", ".join(f'"{r}"' for r in _ROLES)
    return f'{{name: "{name}", visibleTo: [{readers}], dataType: "{data_type}"}}'


def _view(srv, name: str, interval: int) -> None:
    columns = ", ".join([_column("id", "bigint"), _column("region", "text")])
    sql = f"SELECT c.id, c.region FROM {_CALL} AS c"
    _admin(
        srv,
        'registerTable(input: {sourceId: "__derived__", domainId: "fx", schemaName: "views", '
        f'tableName: "{name}", alias: "{name}", viewSql: {json.dumps(sql)}, '
        f"materialize: true, mvRefreshInterval: {interval}, columns: [{columns}]}})",
    )


@pytest.fixture(scope="module")
def views(provisa_server, spark_host, cluster, source_pg):  # noqa: F811  (the fixtures)
    """``ledger.clips`` (replicated, on the signal change signal), the Spark source and its
    command registered, and both views over its call built."""
    srv = provisa_server
    _admin(
        srv,
        f'createSource(input: {{id: "ledger_pg", type: "postgresql", host: "127.0.0.1", '
        f'port: {source_pg.port}, database: "{DATABASE}", username: "{ROLE}", '
        f'password: "{PASSWORD}", changeSignal: "signal"}})',
    )
    columns = ", ".join(
        [
            f'{{name: "id", visibleTo: ["{_ADMIN}", "{_OUTSIDER}"], dataType: "bigint", '
            "isPrimaryKey: true}",
            _column("region", "text"),
        ]
    )
    _admin(
        srv,
        'registerTable(input: {sourceId: "ledger_pg", domainId: "ledger", schemaName: "public", '
        f'tableName: "clips", columns: [{columns}]}})',
    )
    _admin(srv, 'updateSourceReplicate(sourceId: "ledger_pg", replicate: 0)')
    joining = {
        "connector_join": {
            "connector": "spark",
            "host": "127.0.0.1",
            "port": spark_host.port,
            "pairing_code": spark_host.pairing,
        },
        "settings": {"script": _TABLE_SCRIPT, "cluster": cluster},
    }
    _admin(
        srv,
        f'createSource(input: {{id: "spark_jobs", type: "spark", host: "", port: 0, '
        f'database: "", cacheTtl: 300, mappingJson: {json.dumps(json.dumps(joining))}}})',
    )
    response = httpx.post(
        f"{srv.base_url}/admin/actions/functions",
        json={
            "name": "clips_on_the_cluster",
            "sourceId": "spark_jobs",
            "functionName": "clips_on_the_cluster",
            "domainId": "fx",
            "visibleTo": list(_ROLES),
        },
        headers={"X-Provisa-Role": _ADMIN},
        timeout=srv.request_timeout + 120,
    )
    assert response.status_code == 200, response.text
    _view(srv, _ON_CHANGE, 3600)
    _view(srv, _SCHEDULED, _SCHEDULE_S)
    for name in (_ON_CHANGE, _SCHEDULED):
        _admin(srv, f'refreshMv(mvId: "view-{name}")')
    return srv


def _sql(srv, role: str, sql: str) -> httpx.Response:
    return httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": sql},
        headers={"X-Provisa-Role": role},
        timeout=srv.request_timeout + 300,
    )


def _rows(srv, role: str, sql: str) -> list[tuple[int, str]]:
    response = _sql(srv, role, sql)
    assert response.status_code == 200, f"{response.text}\n{srv.dump_stderr_debug()[-6000:]}"
    return [(int(r["id"]), r["region"]) for r in response.json()["data"]["sql"]]


def _view_rows(srv, name: str) -> list[tuple[int, str]]:
    return _rows(srv, _ADMIN, f"SELECT id, region FROM {name} ORDER BY id")


def _mv(srv, name: str) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": "{ mvList { id status lastRefreshAt lastError } }"},
        headers={"X-Provisa-Role": _ADMIN},
        timeout=srv.request_timeout + 10,
    )
    assert response.status_code == 200, response.text
    (found,) = [m for m in response.json()["data"]["mvList"] if m["id"] == f"view-{name}"]
    return found


def test_the_view_lands_what_a_live_call_returns(views):
    live = _rows(views, _ADMIN, _LIVE)
    assert live == _START
    assert _view_rows(views, _ON_CHANGE) == live
    assert _view_rows(views, _SCHEDULED) == live


def test_a_role_that_cannot_read_the_argument_table_gets_the_refusal_a_live_call_gets(views):
    """A live call runs the job, and the job's read of the table is refused as the role (REQ-2270):
    the call fails with that refusal. A read of the view runs no job; the view's definition is
    judged for the role, its call's table argument as its input, and refused in the same words."""
    live = _sql(views, _OUTSIDER, _LIVE)
    stored = _sql(views, _OUTSIDER, f"SELECT id, region FROM {_ON_CHANGE} ORDER BY id")
    assert live.status_code != 200, live.text
    assert stored.status_code == 403, stored.text
    refusal = stored.json()["detail"]
    assert "clips" in refusal, stored.text
    # As the live call's JSON body spells it.
    assert json.dumps(refusal)[1:-1] in live.text, (live.text, stored.text)


def test_a_change_to_the_argument_table_refreshes_the_view(views, source_pg):
    built = _mv(views, _ON_CHANGE)["lastRefreshAt"]
    assert built is not None
    with psycopg.connect(source_pg.dsn(), autocommit=True) as conn:
        conn.execute("INSERT INTO clips VALUES (4, 'emea')")
    signalled = httpx.post(
        f"{views.base_url}/data/sources/ledger_pg/signal",
        headers={"X-Provisa-Role": _ADMIN},
        timeout=60,
    )
    assert signalled.status_code == 202, signalled.text
    expected = [*_START, (4, "emea")]
    deadline = time.monotonic() + 300
    while _view_rows(views, _ON_CHANGE) != expected:
        assert time.monotonic() < deadline, (
            f"the change never reached the view: {_mv(views, _ON_CHANGE)}\n"
            f"{views.dump_stderr_debug()[-12000:]}"
        )
        time.sleep(2)
    assert _mv(views, _ON_CHANGE)["lastRefreshAt"] > built
    assert _rows(views, _ADMIN, _LIVE) == expected


def test_the_view_refreshes_on_its_schedule(views):
    before = _mv(views, _SCHEDULED)["lastRefreshAt"]
    assert before is not None
    deadline = time.monotonic() + 3 * _SCHEDULE_S + 240
    while True:
        now = _mv(views, _SCHEDULED)
        if now["lastRefreshAt"] is not None and now["lastRefreshAt"] > before:
            break
        assert time.monotonic() < deadline, (
            f"no scheduled refresh: {now}\n{views.dump_stderr_debug()[-12000:]}"
        )
        time.sleep(3)
    assert now["lastError"] is None, now
