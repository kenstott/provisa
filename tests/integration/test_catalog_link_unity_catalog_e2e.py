# Copyright (c) 2026 Kenneth Stott
# Canary: 5b3f8e14-9a27-4d60-8c51-a2e7d4f0b938
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2122 e2e: a table takes its descriptions from Unity Catalog, and the export then writes
them onto the Unity Catalog table the steward picked.

The run's catalog (provisa_e2e_*) holds the customer's own Delta table for sales.orders, with a
comment and column comments. Through the production admin API, the pick list finds it, the offer
reads it, the apply writes what was accepted and links the table, the next publish writes the
table's comment and its Provisa address (a table property) onto the picked table, and a table
locked to Unity Catalog keeps the comment the catalog holds. The catalog is dropped after the run.

Without the warehouse's credentials the test fails, never skips (REQ-2119).
"""

# Requirements: REQ-2122

from __future__ import annotations

import asyncio
import os
import uuid
from typing import Any

import httpx
import pytest

from tests.integration.metadata_export_server import DOMAIN, published_server, save_targets

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_warehouse,
    pytest.mark.asyncio(loop_scope="session"),
]

ORG = "catlink_unity_catalog_e2e"
CATALOG = "unity_catalog"
TABLE_COMMENT = "Every order a customer placed, as the lakehouse team keeps it"
AMOUNT_COMMENT = "The order's total, tax included"
PROVISA_DESCRIPTION = "Orders placed"
_CREDENTIALS = ("DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")


def _credentials() -> dict[str, str]:
    missing = [name for name in _CREDENTIALS if not os.environ.get(name)]
    assert not missing, f"the Unity Catalog link e2e needs {', '.join(missing)}"
    return {name: os.environ[name] for name in _CREDENTIALS}


def _rows(creds: dict[str, str], sql: str) -> list[tuple]:
    from databricks import sql as dbsql

    from provisa.federation.databricks_tls import databricks_tls_kwargs

    conn = dbsql.connect(
        server_hostname=creds["DATABRICKS_SERVER_HOSTNAME"],
        http_path=creds["DATABRICKS_HTTP_PATH"],
        access_token=creds["DATABRICKS_TOKEN"],
        **databricks_tls_kwargs(),
    )
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            return [tuple(r) for r in cur.fetchall()] if cur.description else []
        finally:
            cur.close()
    finally:
        conn.close()


async def _candidate(client: httpx.AsyncClient, table_id: int, name: str) -> dict[str, Any]:
    for _ in range(30):
        answer = await client.get(
            f"/admin/tables/{table_id}/catalog/candidates", params={"search": name}
        )
        assert answer.status_code == 200, answer.text
        (listing,) = answer.json()["catalogs"]
        assert "error" not in listing, listing
        for asset in listing["assets"]:
            if asset["name"].endswith(f".{name}"):
                return asset
        await asyncio.sleep(5)
    raise AssertionError(f"Unity Catalog never listed {name}")


async def _publish(client: httpx.AsyncClient) -> None:
    published = await client.post("/admin/metadata-export/publish")
    assert published.status_code == 200, published.text
    (outcome,) = published.json()["targets"]
    assert outcome["ok"], outcome["errors"]


async def test_a_table_takes_its_descriptions_from_unity_catalog_and_the_export_writes_the_pick():
    creds = _credentials()
    run = uuid.uuid4().hex[:8]
    catalog = f"provisa_e2e_{run}"
    customer = f"orders_{run}"
    dsn = (
        f"databricks://token:{creds['DATABRICKS_TOKEN']}@{creds['DATABRICKS_SERVER_HOSTNAME']}"
        f"?http_path={creds['DATABRICKS_HTTP_PATH']}&catalog={catalog}"
    )
    try:
        async with published_server(
            ORG, engine="databricks", env={"PROVISA_ENGINE_URL": dsn}, with_view=False
        ) as client:
            # A member is replicated into Delta on its first read, in the run's catalog the
            # engine makes; the product's views read the replicas.
            for table in ("orders", "customers"):
                read = await client.post(
                    "/data/sql",
                    json={"sql": f'SELECT id FROM "{DOMAIN}"."{table}"', "role": "org_admin"},
                )
                assert read.status_code == 200, read.text
            # The customer's table lives in a schema of that catalog.
            _rows(creds, f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`lakehouse`")
            _rows(
                creds,
                f"CREATE TABLE `{catalog}`.`lakehouse`.`{customer}` "
                f"(id INT COMMENT 'The order''s number', amount DECIMAL(10, 2) "
                f"COMMENT '{AMOUNT_COMMENT.replace(chr(39), chr(39) * 2)}') "
                f"COMMENT '{TABLE_COMMENT}'",
            )
            await save_targets(client, [{"id": CATALOG, "provider": "unity_catalog"}])
            tables = (
                await client.post("/admin/graphql", json={"query": "{ tables { id tableName } }"})
            ).json()["data"]["tables"]
            (table_id,) = [int(t["id"]) for t in tables if t["tableName"] == "orders"]

            asset = await _candidate(client, table_id, customer)
            assert asset["ref"] == f"{catalog}.lakehouse.{customer}"
            offer = (
                await client.get(
                    f"/admin/tables/{table_id}/catalog/{CATALOG}/offer",
                    params={"ref": asset["ref"]},
                )
            ).json()
            assert offer["description"]["catalog"] == TABLE_COMMENT
            by_column = {c["name"]: c["description"] for c in offer["catalog_columns"]}
            assert by_column["amount"] == AMOUNT_COMMENT

            applied = await client.post(
                f"/admin/tables/{table_id}/catalog/{CATALOG}/apply",
                json={"ref": asset["ref"], "columns": {"amount": "amount"}},
            )
            assert applied.status_code == 200, applied.text

            await _publish(client)
            (comment,) = _rows(
                creds,
                f"SELECT comment FROM `{catalog}`.information_schema.tables "
                f"WHERE table_schema = 'lakehouse' AND table_name = '{customer}'",
            )
            assert comment[0] == PROVISA_DESCRIPTION
            properties = dict(
                _rows(creds, f"SHOW TBLPROPERTIES `{catalog}`.`lakehouse`.`{customer}`")
            )
            assert properties["provisa.uri"].endswith("/tables/orders")

            locked = await client.put(
                f"/admin/tables/{table_id}/catalog/lock", json={"catalog": CATALOG}
            )
            assert locked.status_code == 200, locked.text
            _rows(
                creds,
                f"COMMENT ON TABLE `{catalog}`.`lakehouse`.`{customer}` IS 'Edited in Unity Catalog'",
            )
            await _publish(client)
            (kept,) = _rows(
                creds,
                f"SELECT comment FROM `{catalog}`.information_schema.tables "
                f"WHERE table_schema = 'lakehouse' AND table_name = '{customer}'",
            )
            assert kept[0] == "Edited in Unity Catalog"
    finally:
        _rows(creds, f"DROP CATALOG IF EXISTS `{catalog}` CASCADE")
