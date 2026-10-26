# Copyright (c) 2026 Kenneth Stott
# Canary: b8194dca-ab05-4c3e-a5d2-510fd27e6f03
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: Azure Cosmos DB for NoSQL as a source (REQ-1982), on the Cosmos DB Linux emulator.

Everything here is the ``test`` instance: an emulator this module starts on a leased port and
removes, and an isolated DuckDB server. Tables:

* ``customers``: a container table read whole, a column masked for the analyst;
* ``orders``: a container table whose partition key ``customerId`` a steward made required, so a
  read gives it and reads that one partition;
* ``big_orders``: a query table over ``orders``, its ``@min`` a required argument sent as a
  query parameter.

Search (vector, full text) is left to the spike's answer (test_cosmosdb_emulator_search_spike)."""

# Requirements: REQ-1982, REQ-1986

from __future__ import annotations

import json
import os
import subprocess
import time

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration]

_ROLE = "org_admin"
_ANALYST = "itest_analyst"
_IMAGE = "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview"
# The emulator's published, well-known account key (not a secret).
_KEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
_CUSTOMERS = [
    {"id": "c1", "name": "Ada", "email": "ada@example.test"},
    {"id": "c2", "name": "Grace", "email": "grace@example.test"},
]
_ORDERS = [
    {"id": "o1", "customerId": "c1", "total": 120.5, "placed": "2025-03-02"},
    {"id": "o2", "customerId": "c1", "total": 15.0, "placed": "2025-03-05"},
    {"id": "o3", "customerId": "c2", "total": 300.0, "placed": "2025-04-01"},
]


class _Emulator:
    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.port,) = lease_ports(1)
        self._name = f"provisa-itest-cosmos-{os.getpid()}"

    @property
    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.port}/"

    def start(self) -> None:
        from azure.cosmos import CosmosClient, PartitionKey

        subprocess.run(
            ["docker", "run", "-d", "--rm", "--memory", "2g", "--name", self._name]
            + ["-p", f"127.0.0.1:{self.port}:8081", _IMAGE],
            check=True,
            capture_output=True,
        )
        # The client's constructor already asks the account for its endpoints, so it is made inside
        # the wait: until the emulator listens, the port's proxy resets the connection.
        deadline = time.monotonic() + 300
        while True:
            try:
                client = CosmosClient(self.endpoint, credential=_KEY)
                db = client.create_database_if_not_exists("shop")
                break
            except Exception:  # allow-blind-except: polling the emulator until it answers
                if time.monotonic() > deadline:
                    raise
                time.sleep(3)
        customers = db.create_container_if_not_exists("customers", PartitionKey(path="/id"))
        orders = db.create_container_if_not_exists("orders", PartitionKey(path="/customerId"))
        for doc in _CUSTOMERS:
            customers.upsert_item(dict(doc))
        for doc in _ORDERS:
            orders.upsert_item(dict(doc))

    def stop(self) -> None:
        subprocess.run(["docker", "rm", "-f", self._name], capture_output=True)


@pytest.fixture(scope="module")
def emulator():
    e = _Emulator()
    try:
        e.start()
        yield e
    finally:
        e.stop()


def _col(name: str, data_type: str, roles: list[str], **kw) -> dict:
    return {"name": name, "data_type": data_type, "visible_to": roles, **kw}


def _param(name: str, data_type: str, kind: str) -> dict:
    return {
        "name": f"_nf_{name}",
        "data_type": data_type,
        "native_filter_type": kind,
        "native_filter_required": False,
        "required_by_steward": True,
        "visible_to": [],
    }


@pytest.fixture(scope="module")
def server(emulator, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    both = [_ROLE, _ANALYST]
    cfg = {
        "federation_engine": "duckdb",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "shop", "description": "cosmos e2e"}],
        "roles": [
            {
                "id": _ANALYST,
                "parents": ["analyst"],
                "description": "Test role.",
                "capabilities": ["query_development"],
                "domain_access": ["shop"],
            }
        ],
        "sources": [
            {
                "id": "cosmos",
                "type": "cosmosdb",
                "host": emulator.endpoint,
                "database": "shop",
                "password": _KEY,
                "cache_ttl": 60,
            }
        ],
        "tables": [
            {
                "source_id": "cosmos",
                "domain_id": "shop",
                "schema": "shop",
                "table": "customers",
                "cache_ttl": 60,
                "columns": [
                    _col("id", "varchar", both),
                    _col("name", "varchar", both),
                    _col(
                        "email",
                        "varchar",
                        both,
                        masks=[
                            {"to": [_ROLE], "mask": None},
                            {"to": ["*"], "mask": {"type": "constant", "value": "***"}},
                        ],
                    ),
                ],
            },
            {
                "source_id": "cosmos",
                "domain_id": "shop",
                "schema": "shop",
                "table": "orders",
                "cache_ttl": 60,
                "columns": [
                    _col("id", "varchar", both),
                    _col("customerId", "varchar", both),
                    _col("total", "double", both),
                    _col("placed", "date", both),
                    _param("customerId", "varchar", "partition_key"),
                ],
            },
            {
                "source_id": "cosmos",
                "domain_id": "shop",
                "schema": "shop",
                "table": "big_orders",
                "cache_ttl": 60,
                "query_template": json.dumps(
                    {
                        "container": "orders",
                        "query": "SELECT c.id, c.customerId, c.total FROM c WHERE c.total >= @min",
                    }
                ),
                "columns": [
                    _col("id", "varchar", both),
                    _col("customerId", "varchar", both),
                    _col("total", "double", both),
                    _param("min", "double", "query_arg"),
                ],
            },
        ],
        # The join the tests make is a registered relationship: a join outside the registered
        # ones is refused to a role that may not ignore them (REQ-603).
        "relationships": [
            {
                "id": "order-customer",
                "source_table_id": "orders",
                "target_table_id": "customers",
                "source_column": "customerId",
                "target_column": "id",
                "cardinality": "many-to-one",
            }
        ],
    }
    work = tmp_path_factory.mktemp("cosmos_e2e")
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(cfg))
    srv = IsolatedServer(
        "cosmosdb_source",
        engine="duckdb",
        config=str(path),
        control_plane="own",
        materialize_store_url=f"duckdb:///{work / 'store.duckdb'}",
    )
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


def _sql(server, sql: str, role: str = _ROLE) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}/data/sql",
        json={"sql": sql},
        headers={"X-Provisa-Role": role},
        timeout=server.request_timeout + 30,
    )


def _rows(resp: httpx.Response) -> list[dict]:
    assert resp.status_code == 200, resp.text
    return resp.json()["data"]["sql"]


def test_a_container_table_is_read_whole(server):
    rows = _rows(_sql(server, "SELECT id, name FROM customers ORDER BY id"))
    assert rows == [{"id": "c1", "name": "Ada"}, {"id": "c2", "name": "Grace"}]


def test_a_partition_key_parameter_reads_that_partition_and_dates_read_as_dates(server):
    rows = _rows(
        _sql(server, "SELECT id, total, placed FROM orders WHERE _nf_customerId = 'c1' ORDER BY id")
    )
    assert [r["id"] for r in rows] == ["o1", "o2"]
    assert rows[0]["placed"] == "2025-03-02"


def test_a_read_without_the_required_partition_key_is_refused_naming_it(server):
    resp = _sql(server, "SELECT id FROM orders")
    assert resp.status_code != 200
    assert "orders" in resp.text and "customerId" in resp.text


def test_a_query_table_sends_its_argument_as_a_parameter(server):
    rows = _rows(_sql(server, "SELECT id FROM big_orders WHERE _nf_min = 100 ORDER BY id"))
    assert [r["id"] for r in rows] == ["o1", "o3"]


def test_a_cosmos_table_joins_another(server):
    rows = _rows(
        _sql(
            server,
            "SELECT c.name, o.total FROM orders o JOIN customers c ON c.id = o.customerId "
            "WHERE o._nf_customerId = 'c2'",
        )
    )
    assert rows == [{"name": "Grace", "total": 300.0}]


def test_a_mask_applies_to_cosmos_rows(server):
    rows = _rows(_sql(server, "SELECT email FROM customers ORDER BY id", role=_ANALYST))
    assert {r["email"] for r in rows} == {"***"}
