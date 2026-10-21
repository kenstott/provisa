# Copyright (c) 2026 Kenneth Stott
# Canary: 0d6a2f95-3e71-4b84-a6c9-7f1e8b2d5c03
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2122 e2e: a table takes its descriptions from Snowflake Horizon, and the export then writes
them onto the Snowflake table the steward picked.

The run's own database holds the customer's table for sales.orders, with a comment and column
comments. Through the production admin API, the pick list finds it, the offer reads it, the
apply writes what was accepted and links the table, the next publish writes the table's comment
onto the picked table and tags it with its Provisa address, and a table locked to Horizon keeps
the comment Snowflake holds. The database, share and listing are dropped after the run.

Without the account's credentials the test fails, never skips (REQ-2119).
"""

# Requirements: REQ-2122

from __future__ import annotations

import asyncio
import os
import uuid
from typing import Any
from urllib.parse import quote

import httpx
import pytest

from tests.integration.metadata_export_server import DOMAIN, published_server, save_targets

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_warehouse,
    pytest.mark.asyncio(loop_scope="session"),
]

ORG = "catlink_horizon_e2e"
CATALOG = "horizon"
TABLE_COMMENT = "Every order a customer placed, as the warehouse team keeps it"
AMOUNT_COMMENT = "The order total, tax included"
PROVISA_DESCRIPTION = "Orders placed"
_CREDENTIALS = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_WAREHOUSE")


def _credentials() -> dict[str, str]:
    missing = [name for name in _CREDENTIALS if not os.environ.get(name)]
    assert not missing, f"the Snowflake Horizon link e2e needs {', '.join(missing)}"
    return {name: os.environ[name] for name in _CREDENTIALS}


def _rows(creds: dict[str, str], sql: str) -> list[dict[str, Any]]:
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=creds["SNOWFLAKE_ACCOUNT"],
        user=creds["SNOWFLAKE_USER"],
        password=creds["SNOWFLAKE_PASSWORD"],
        warehouse=creds["SNOWFLAKE_WAREHOUSE"],
    )
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            if not cur.description:
                return []
            names = [d[0].lower() for d in cur.description]
            return [dict(zip(names, row, strict=True)) for row in cur.fetchall()]
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
    raise AssertionError(f"Snowflake never listed {name}")


async def _publish(client: httpx.AsyncClient) -> None:
    published = await client.post("/admin/metadata-export/publish")
    assert published.status_code == 200, published.text
    (outcome,) = published.json()["targets"]
    assert outcome["ok"], outcome["errors"]


async def test_a_table_takes_its_descriptions_from_horizon_and_the_export_writes_the_pick():
    creds = _credentials()
    run = uuid.uuid4().hex[:10].upper()
    database = f"PROVISA_E2E_{run}"
    customer = f"ORDERS_{run}"
    product = f"provisa_e2e_{run.lower()}"
    _rows(creds, f"CREATE DATABASE {database}")
    try:
        _rows(creds, f"CREATE SCHEMA {database}.WAREHOUSE")
        _rows(
            creds,
            f"CREATE TABLE {database}.WAREHOUSE.{customer} "
            f"(ID INT COMMENT 'The order number', AMOUNT NUMBER(10, 2) COMMENT '{AMOUNT_COMMENT}') "
            f"COMMENT = '{TABLE_COMMENT}'",
        )
        dsn = (
            f"snowflake://{quote(creds['SNOWFLAKE_USER'], safe='')}:"
            f"{quote(creds['SNOWFLAKE_PASSWORD'], safe='')}@{creds['SNOWFLAKE_ACCOUNT']}/"
            f"{database}?warehouse={quote(creds['SNOWFLAKE_WAREHOUSE'], safe='')}"
        )
        async with published_server(
            ORG,
            engine="snowflake",
            env={"PROVISA_ENGINE_URL": dsn},
            with_view=False,
            product=product,
        ) as client:
            for table in ("orders", "customers"):
                read = await client.post(
                    "/data/sql",
                    json={"sql": f'SELECT id FROM "{DOMAIN}"."{table}"', "role": "org_admin"},
                )
                assert read.status_code == 200, read.text
            await save_targets(client, [{"id": CATALOG, "provider": "snowflake_horizon"}])
            tables = (
                await client.post("/admin/graphql", json={"query": "{ tables { id tableName } }"})
            ).json()["data"]["tables"]
            (table_id,) = [int(t["id"]) for t in tables if t["tableName"] == "orders"]

            asset = await _candidate(client, table_id, customer)
            assert asset["ref"] == f"{database}.WAREHOUSE.{customer}"
            offer = (
                await client.get(
                    f"/admin/tables/{table_id}/catalog/{CATALOG}/offer",
                    params={"ref": asset["ref"]},
                )
            ).json()
            assert offer["description"]["catalog"] == TABLE_COMMENT
            by_column = {c["name"]: c["description"] for c in offer["catalog_columns"]}
            assert by_column["AMOUNT"] == AMOUNT_COMMENT
            assert {c["column"]: c["catalog_column"] for c in offer["columns"]}[
                "amount"
            ] == "AMOUNT"

            applied = await client.post(
                f"/admin/tables/{table_id}/catalog/{CATALOG}/apply",
                json={"ref": asset["ref"], "columns": {"amount": "AMOUNT"}},
            )
            assert applied.status_code == 200, applied.text

            await _publish(client)
            (shown,) = _rows(creds, f"SHOW TABLES LIKE '{customer}' IN SCHEMA {database}.WAREHOUSE")
            assert shown["comment"] == PROVISA_DESCRIPTION
            (tagged,) = _rows(
                creds,
                f"SELECT SYSTEM$GET_TAG('{database}.PROVISA_GOVERNANCE.PROVISA_URI', "
                f"'{database}.WAREHOUSE.{customer}', 'table') AS uri",
            )
            assert tagged["uri"].endswith("/tables/orders")

            locked = await client.put(
                f"/admin/tables/{table_id}/catalog/lock", json={"catalog": CATALOG}
            )
            assert locked.status_code == 200, locked.text
            _rows(
                creds,
                f"ALTER TABLE {database}.WAREHOUSE.{customer} SET COMMENT = 'Edited in Snowflake'",
            )
            await _publish(client)
            (kept,) = _rows(creds, f"SHOW TABLES LIKE '{customer}' IN SCHEMA {database}.WAREHOUSE")
            assert kept["comment"] == "Edited in Snowflake"
    finally:
        for stmt in (
            f'DROP LISTING IF EXISTS "provisa_{product}_listing"',
            f'DROP SHARE IF EXISTS "provisa_{product}_share"',
            f"DROP DATABASE IF EXISTS {database}",
        ):
            _rows(creds, stmt)
