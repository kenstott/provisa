# Copyright (c) 2026 Kenneth Stott
# Canary: 3a8e6c19-5d42-4b70-9e13-f7c2b0d5a684
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2122 e2e: a table takes its descriptions from a real DataHub, and the export then writes
onto the dataset the steward picked.

DataHub already holds the customer's own dataset for the physical table behind sales.orders, as
its ingestion leaves it: a description, column descriptions, an owner and a tag. Through the
production admin API, as the table editor calls it, the pick list preselects that dataset, the
apply writes what was accepted and links the table, and the export writes the table's
description into the dataset's editable aspects and its Provisa address into its custom
properties, without a dataset of the export's own. A table locked to DataHub keeps DataHub's
description through an export. Every assertion reads back from GMS or Provisa.
"""

# Requirements: REQ-2122

from __future__ import annotations

import asyncio
import json
import os
from typing import Any
from urllib.parse import quote

import httpx
import pytest

from tests.integration.metadata_export_server import (
    SCHEMA,
    SOURCE,
    published_server,
    save_targets,
)

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.requires_datahub,
    pytest.mark.asyncio(loop_scope="session"),
]

ORG = "catlink_datahub_e2e"
CATALOG = "datahub"
CATALOG_DESCRIPTION = "Every order a customer placed, one row per order"
AMOUNT_DESCRIPTION = "The order's total, tax included"
PROVISA_DESCRIPTION = "Orders placed"


def _gms() -> str:
    return f"http://localhost:{os.environ['DATAHUB_GMS_PORT']}"


def _customer_urn(database: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:postgres,{database}.{SCHEMA}.orders,PROD)"


async def _propose(entity_type: str, urn: str, aspect: str, value: dict[str, Any]) -> None:
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.post(
            f"{_gms()}/aspects?action=ingestProposal",
            json={
                "proposal": {
                    "entityType": entity_type,
                    "entityUrn": urn,
                    "changeType": "UPSERT",
                    "aspectName": aspect,
                    "aspect": {"contentType": "application/json", "value": json.dumps(value)},
                }
            },
        )
    assert response.status_code < 400, response.text


async def _aspect(urn: str, aspect: str) -> dict[str, Any] | None:
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.get(
            f"{_gms()}/aspects/{quote(urn, safe='')}", params={"aspect": aspect, "version": 0}
        )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    (value,) = response.json()["aspect"].values()
    return value


async def _customer_dataset(database: str) -> str:
    """The customer's own dataset, as its Postgres ingestion leaves it."""
    urn = _customer_urn(database)
    await _propose("tag", "urn:li:tag:pii", "tagProperties", {"name": "pii"})
    await _propose(
        "dataset",
        urn,
        "datasetProperties",
        {
            "name": "orders",
            "qualifiedName": f"{database}.{SCHEMA}.orders",
            "description": CATALOG_DESCRIPTION,
            "customProperties": {"ingested_by": "postgres"},
        },
    )
    await _propose(
        "dataset",
        urn,
        "schemaMetadata",
        {
            "schemaName": f"{database}.{SCHEMA}.orders",
            "platform": "urn:li:dataPlatform:postgres",
            "version": 0,
            "hash": "",
            "platformSchema": {"com.linkedin.schema.OtherSchema": {"rawSchema": ""}},
            "fields": [
                {
                    "fieldPath": "id",
                    "nativeDataType": "integer",
                    "type": {"type": {"com.linkedin.schema.NumberType": {}}},
                },
                {
                    "fieldPath": "amount",
                    "nativeDataType": "numeric",
                    "description": AMOUNT_DESCRIPTION,
                    "type": {"type": {"com.linkedin.schema.NumberType": {}}},
                },
                {
                    "fieldPath": "ssn",
                    "nativeDataType": "text",
                    "type": {"type": {"com.linkedin.schema.StringType": {}}},
                    "globalTags": {"tags": [{"tag": "urn:li:tag:pii"}]},
                },
            ],
        },
    )
    await _propose(
        "dataset",
        urn,
        "ownership",
        {
            "owners": [{"owner": "urn:li:corpuser:ann", "type": "DATAOWNER"}],
            "lastModified": {"time": 0, "actor": "urn:li:corpuser:ingestion"},
        },
    )
    return urn


async def _orders(client: httpx.AsyncClient) -> dict[str, Any]:
    answer = await client.post(
        "/admin/graphql",
        json={
            "query": "{ tables { id tableName description columns { columnName description } } }"
        },
    )
    assert answer.status_code == 200, answer.text
    (orders,) = [t for t in answer.json()["data"]["tables"] if t["tableName"] == "orders"]
    return orders


async def _candidates(client: httpx.AsyncClient, table_id: int, urn: str) -> dict[str, Any]:
    """The pick list, once DataHub's search index holds the dataset (it is written by the
    change-log consumer after the ingest returns)."""
    for _ in range(60):
        answer = await client.get(f"/admin/tables/{table_id}/catalog/candidates")
        assert answer.status_code == 200, answer.text
        (listing,) = answer.json()["catalogs"]
        assert "error" not in listing, listing
        if any(a["ref"] == urn for a in listing["assets"]):
            return listing
        await asyncio.sleep(2)
    raise AssertionError(f"DataHub's search never listed {urn}")


async def _publish(client: httpx.AsyncClient) -> None:
    published = await client.post("/admin/metadata-export/publish")
    assert published.status_code == 200, published.text
    (outcome,) = published.json()["targets"]
    assert outcome["ok"], outcome["errors"]


async def test_a_table_takes_its_descriptions_from_datahub_and_the_export_writes_the_pick():
    urn = await _customer_dataset(os.environ["PG_DATABASE"])
    async with published_server(ORG) as client:
        await save_targets(
            client,
            [{"id": CATALOG, "provider": "datahub", "endpoint": _gms(), "timeout_seconds": 60}],
        )
        table_id = int((await _orders(client))["id"])

        listing = await _candidates(client, table_id, urn)
        assert listing["preselected"] == urn

        offer = (
            await client.get(
                f"/admin/tables/{table_id}/catalog/{CATALOG}/offer", params={"ref": urn}
            )
        ).json()
        assert offer["description"] == {
            "current": PROVISA_DESCRIPTION,
            "catalog": CATALOG_DESCRIPTION,
        }
        assert {c["column"]: c["catalog_column"] for c in offer["columns"]}["amount"] == "amount"
        assert offer["owners"] == ["ann"]
        by_column = {c["name"]: c for c in offer["catalog_columns"]}
        assert by_column["ssn"]["tags"] == [{"name": "pii", "tag": "pii"}]

        applied = await client.post(
            f"/admin/tables/{table_id}/catalog/{CATALOG}/apply",
            json={
                "ref": urn,
                "columns": {"amount": "amount"},
                "tags": [{"column": "ssn", "tag": "pii"}],
            },
        )
        assert applied.status_code == 200, applied.text
        columns = {c["columnName"]: c["description"] for c in (await _orders(client))["columns"]}
        assert columns["amount"] == AMOUNT_DESCRIPTION

        # The export writes onto the picked dataset: its edited description becomes Provisa's,
        # its custom properties carry the Provisa address beside the ingestion's own, and no
        # dataset of the export's own is made for the table.
        await client.put(
            "/admin/metadata-export", json={"enabled": True, "targets": [{"catalog": CATALOG}]}
        )
        await _publish(client)
        edited = await _aspect(urn, "editableDatasetProperties")
        assert edited is not None and edited["description"] == PROVISA_DESCRIPTION
        properties = await _aspect(urn, "datasetProperties")
        assert properties is not None
        assert properties["description"] == CATALOG_DESCRIPTION
        assert properties["customProperties"]["ingested_by"] == "postgres"
        assert properties["customProperties"]["provisaUri"].endswith("/tables/orders")
        own = f"urn:li:dataset:(urn:li:dataPlatform:provisa,{SOURCE}.{SCHEMA}.orders,PROD)"
        assert await _aspect(own, "datasetProperties") is None

        # Locked to DataHub, the table keeps DataHub's description through an export.
        locked = await client.put(
            f"/admin/tables/{table_id}/catalog/lock", json={"catalog": CATALOG}
        )
        assert locked.status_code == 200, locked.text
        await _propose(
            "dataset", urn, "editableDatasetProperties", {"description": "Edited in DataHub"}
        )
        await _publish(client)
        kept = await _aspect(urn, "editableDatasetProperties")
        assert kept is not None and kept["description"] == "Edited in DataHub"

        refreshed = (await client.get(f"/admin/tables/{table_id}/catalog/{CATALOG}/offer")).json()
        assert refreshed["description"]["catalog"] == "Edited in DataHub"
