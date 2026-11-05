# Copyright (c) 2026 Kenneth Stott
# Canary: 4b8e1f62-3a7d-4c95-9e20-6d1a5c8b7f34
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a MongoDB column registered at a path in its document, read by SQL through its governed
relation, on DuckDB and on Trino (REQ-039).

The perf bench's order_docs registers customer_tier at customer_profile.tier. Stage 2 stands every
registered table as the role's governed relation, which reads a path column by its own name: every
engine's relation of the table but one exposes it so (DuckDB's attach names the nested field's
column apart, #266). Trino's MongoDB connector reads the document as it is, a nested document as a
ROW column, so on Trino the routed statement reads the column at its path. A statement that does
not read a path column does not read it at all (on Trino it is no column of the table: the bench's
federated_join failed with COLUMN_NOT_FOUND, J670).

Everything here is the ``test`` instance: a MongoDB this module starts on a leased port and
removes, and an isolated server on each engine. Each answer is checked against the documents."""

# Requirements: REQ-039

from __future__ import annotations

import os
import subprocess
import time

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.requires_trino, pytest.mark.integration]

_ROLE = "org_admin"
_ANALYST = "itest_path_analyst"
_DB = "pathdocs"
_DOCS = [
    {
        "_id": 100 + i,
        "order_id": i,
        "status": "open" if i % 2 else "closed",
        "customer_profile": {"tier": ("gold", "silver", "bronze")[i % 3], "since": 2000 + i},
        "shipping": {"carrier": "ups", "address": {"city": f"city{i}"}},
    }
    for i in range(1, 13)
]


def _expected(doc: dict) -> dict:
    """A document as the table's columns read it."""
    return {
        "order_id": doc["order_id"],
        "status": doc["status"],
        "customer_tier": doc["customer_profile"]["tier"],
        "city": doc["shipping"]["address"]["city"],
        "doc_key": doc["_id"],
    }


_ROWS = sorted((_expected(d) for d in _DOCS), key=lambda r: r["order_id"])


class _Mongo:
    """A MongoDB on a leased port, reachable from this process and from Trino in the stack."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.port,) = lease_ports(1)
        self._name = f"provisa-itest-mongo-paths-{os.getpid()}"
        # One address for both readers: this process, and Trino in the test stack's container,
        # which cannot dial the host's loopback. The default bridge's gateway is a host address.
        self.host = subprocess.run(
            ["docker", "network", "inspect", "bridge"]
            + ["--format", "{{(index .IPAM.Config 0).Gateway}}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    @property
    def uri(self) -> str:
        return f"mongodb://{self.host}:{self.port}/?directConnection=true"

    def start(self) -> None:
        import pymongo
        from pymongo.errors import PyMongoError

        subprocess.run(
            ["docker", "run", "-d", "--rm", "--memory", "512m", "--name", self._name]
            + ["-p", f"{self.host}:{self.port}:27017", "mongo:7"],
            check=True,
            capture_output=True,
        )
        deadline = time.monotonic() + 120
        with pymongo.MongoClient(self.uri, serverSelectionTimeoutMS=120_000) as client:
            while True:
                try:
                    client.admin.command("ping")
                    break
                except PyMongoError:
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(1)
            client[_DB]["order_docs"].insert_many([dict(d) for d in _DOCS])

    def stop(self) -> None:
        subprocess.run(["docker", "rm", "-f", self._name], capture_output=True)


@pytest.fixture(scope="module")
def mongo():
    m = _Mongo()
    try:
        m.start()
        yield m
    finally:
        m.stop()


def _config(mongo: _Mongo, engine: str) -> dict:
    both = [_ROLE, _ANALYST]

    def col(name: str, data_type: str, roles: list[str], **extra) -> dict:
        return {"name": name, "data_type": data_type, "visible_to": roles, **extra}

    return {
        "federation_engine": engine,
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "docs", "description": "path columns e2e"}],
        "roles": [
            {
                "id": _ANALYST,
                "parents": ["analyst"],
                "description": "Test role.",
                "capabilities": ["query_development", "usage"],
                "domain_access": ["docs"],
            }
        ],
        "sources": [
            {"id": "pathdocs", "type": "mongodb", "host": mongo.uri, "port": 0, "database": _DB}
        ],
        "tables": [
            {
                "source_id": "pathdocs",
                "domain_id": "docs",
                "schema": _DB,
                "table": "order_docs",
                "columns": [
                    col("order_id", "integer", both, is_primary_key=True),
                    col("status", "varchar", both),
                    col("customer_tier", "varchar", both, path="customer_profile.tier"),
                    # The analyst is not served the city.
                    col("city", "varchar", [_ROLE], path="shipping.address.city"),
                    col("doc_key", "integer", both, path="_id"),
                ],
            }
        ],
    }


@pytest.fixture(scope="module", params=["duckdb", "trino"])
def server(mongo, tmp_path_factory, request):
    from tests.integration.isolated_server import IsolatedServer

    engine = request.param
    work = tmp_path_factory.mktemp(f"mongo_paths_{engine}")
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(_config(mongo, engine)))
    if engine == "duckdb":
        srv = IsolatedServer(
            "mongo_path_columns",
            engine="duckdb",
            config=str(path),
            control_plane="own",
            materialize_store_url=f"duckdb:///{work / 'store.duckdb'}",
        )
    else:
        # Trino from the test stack, with its Postgres control plane.
        srv = IsolatedServer("mongo_path_columns_trino", engine="trino", config=str(path))
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


def _rows(server, sql: str, role: str = _ROLE) -> list[dict]:
    resp = httpx.post(
        f"{server.base_url}/data/sql",
        json={"sql": sql},
        headers={"X-Provisa-Role": role},
        timeout=server.request_timeout + 30,
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["data"]["sql"]


def _only(rows: list[dict], *names: str) -> list[dict]:
    return [{n: r[n] for n in names} for r in rows]


def test_a_read_path_column_answers_its_value_in_the_document(server):
    rows = _rows(server, "SELECT order_id, customer_tier, city FROM order_docs ORDER BY order_id")
    assert rows == _only(_ROWS, "order_id", "customer_tier", "city")


def test_a_filter_on_a_path_column_reads_its_value_in_the_document(server):
    rows = _rows(
        server, "SELECT order_id FROM order_docs WHERE customer_tier = 'gold' ORDER BY order_id"
    )
    assert rows == [{"order_id": r["order_id"]} for r in _ROWS if r["customer_tier"] == "gold"]


def test_a_one_key_path_reads_a_top_level_field(server):
    rows = _rows(server, "SELECT doc_key, status FROM order_docs ORDER BY doc_key")
    assert rows == _only(_ROWS, "doc_key", "status")


def test_a_star_reads_every_served_column_at_its_path(server):
    assert _rows(server, "SELECT * FROM order_docs ORDER BY order_id") == _ROWS
    served = _rows(server, "SELECT * FROM order_docs ORDER BY order_id", role=_ANALYST)
    assert served == _only(_ROWS, "order_id", "status", "customer_tier", "doc_key")


def test_a_read_of_no_path_column_answers(server):
    rows = _rows(server, "SELECT status, count(*) AS n FROM order_docs GROUP BY status ORDER BY 1")
    assert rows == [
        {"status": s, "n": sum(1 for r in _ROWS if r["status"] == s)} for s in ("closed", "open")
    ]
