# Copyright (c) 2026 Kenneth Stott
# Canary: 8e4b1d27-6c90-4a35-b7f2-1d9c5e3a8f60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2122 e2e: a table takes its descriptions from Amazon DataZone, and the data product export
then publishes the asset the steward picked as the member.

The domain's producer project holds the customer's own Glue table asset for sales.orders, made
for the run (provisa_e2e_*) with a description and its columns' business descriptions. Through
the production admin API, the pick list finds it, the offer reads it, the apply writes what was
accepted and links the table, and the next publish revises the picked asset (its own forms kept,
Provisa's address added, the table's description written) and lists it in the product. Everything
the run made is removed after it.

Without the domain's ids and the CI user's keys the test fails, never skips (REQ-2119).
"""

# Requirements: REQ-2122

from __future__ import annotations

import asyncio
import json
import os
import uuid
from typing import Any

import httpx
import pytest

from provisa.api.metadata_export.datazone import TABLE_FORM
from tests.integration.metadata_export_server import PRODUCT_FIELDS, published_server, save_targets

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_warehouse,
    pytest.mark.asyncio(loop_scope="session"),
]

ORG = "catlink_datazone_e2e"
REGION = "us-east-1"
CATALOG = "datazone"
TABLE_DESCRIPTION = "Every order a customer placed, as the lake team keeps it"
AMOUNT_DESCRIPTION = "The order's total, tax included"
PROVISA_DESCRIPTION = "Orders placed"
_CREDENTIALS = (
    "PROVISA_E2E_DATAZONE_AWS_ACCESS_KEY_ID",
    "PROVISA_E2E_DATAZONE_AWS_SECRET_ACCESS_KEY",
    "PROVISA_E2E_DATAZONE_DOMAIN_ID",
    "PROVISA_E2E_DATAZONE_PROJECT_ID",
)


def _credentials() -> dict[str, str]:
    missing = [name for name in _CREDENTIALS if not os.environ.get(name)]
    assert not missing, f"the DataZone link e2e needs {', '.join(missing)}"
    return {name: os.environ[name] for name in _CREDENTIALS}


def _client(creds: dict[str, str]) -> Any:
    import boto3

    return boto3.client(
        "datazone",
        region_name=REGION,
        aws_access_key_id=creds["PROVISA_E2E_DATAZONE_AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=creds["PROVISA_E2E_DATAZONE_AWS_SECRET_ACCESS_KEY"],
    )


def _customer_asset(creds: dict[str, str], name: str) -> str:
    """The customer's own Glue table asset, as a DataZone data source would leave it."""
    client = _client(creds)
    created = client.create_asset(
        domainIdentifier=creds["PROVISA_E2E_DATAZONE_DOMAIN_ID"],
        owningProjectIdentifier=creds["PROVISA_E2E_DATAZONE_PROJECT_ID"],
        name=name,
        description=TABLE_DESCRIPTION,
        typeIdentifier="amazon.datazone.GlueTableAssetType",
        formsInput=[
            {
                "formName": "GlueTableForm",
                "typeIdentifier": "amazon.datazone.GlueTableFormType",
                "content": json.dumps(
                    {
                        "catalogId": "000000000000",
                        "region": REGION,
                        "databaseName": name,
                        "tableName": "orders",
                        "columns": [
                            {"columnName": "id", "dataType": "int"},
                            {"columnName": "amount", "dataType": "decimal(10,2)"},
                        ],
                    }
                ),
            },
            {
                "formName": "ColumnBusinessMetadataForm",
                "typeIdentifier": "amazon.datazone.ColumnBusinessMetadataFormType",
                "content": json.dumps(
                    {
                        "columnsBusinessMetadata": [
                            {"name": "amount", "description": AMOUNT_DESCRIPTION}
                        ]
                    }
                ),
            },
        ],
    )
    return created["id"]


async def _candidate(client: httpx.AsyncClient, table_id: int, name: str) -> dict[str, Any]:
    """The customer's asset in the pick list, once DataZone's search index holds it."""
    for _ in range(60):
        answer = await client.get(
            f"/admin/tables/{table_id}/catalog/candidates", params={"search": name}
        )
        assert answer.status_code == 200, answer.text
        (listing,) = answer.json()["catalogs"]
        assert "error" not in listing, listing
        for asset in listing["assets"]:
            if asset["name"] == f"{name}.orders":
                return asset
        await asyncio.sleep(5)
    raise AssertionError(f"DataZone's search never listed {name}")


async def test_a_member_takes_its_descriptions_from_datazone_and_is_published_as_the_pick():
    creds = _credentials()
    run = f"provisa_e2e_{uuid.uuid4().hex[:8]}"
    product_name = f"{PRODUCT_FIELDS['name']} {run}"
    scope = {"domainIdentifier": creds["PROVISA_E2E_DATAZONE_DOMAIN_ID"]}
    dz = _client(creds)
    asset_id = _customer_asset(creds, run)
    product_id: str | None = None
    try:
        async with published_server(
            ORG, with_view=False, product=run, product_name=product_name
        ) as client:
            await save_targets(
                client,
                [
                    {
                        "id": CATALOG,
                        "provider": "datazone",
                        "aws_region": REGION,
                        "datazone_domain_id": creds["PROVISA_E2E_DATAZONE_DOMAIN_ID"],
                        "datazone_project_id": creds["PROVISA_E2E_DATAZONE_PROJECT_ID"],
                        "aws_access_key_id": creds["PROVISA_E2E_DATAZONE_AWS_ACCESS_KEY_ID"],
                        "aws_secret_access_key": creds[
                            "PROVISA_E2E_DATAZONE_AWS_SECRET_ACCESS_KEY"
                        ],
                        "timeout_seconds": 120,
                    }
                ],
            )
            tables = (
                await client.post("/admin/graphql", json={"query": "{ tables { id tableName } }"})
            ).json()["data"]["tables"]
            (table_id,) = [int(t["id"]) for t in tables if t["tableName"] == "orders"]

            asset = await _candidate(client, table_id, run)
            assert asset["ref"] == asset_id
            offer = (
                await client.get(
                    f"/admin/tables/{table_id}/catalog/{CATALOG}/offer",
                    params={"ref": asset_id},
                )
            ).json()
            assert offer["description"]["catalog"] == TABLE_DESCRIPTION
            by_column = {c["name"]: c["description"] for c in offer["catalog_columns"]}
            assert by_column["amount"] == AMOUNT_DESCRIPTION

            applied = await client.post(
                f"/admin/tables/{table_id}/catalog/{CATALOG}/apply",
                json={"ref": asset_id, "columns": {"amount": "amount"}},
            )
            assert applied.status_code == 200, applied.text

            published = await client.post("/admin/metadata-export/publish")
            assert published.status_code == 200, published.text
            (outcome,) = published.json()["targets"]
            assert outcome["ok"], outcome["errors"]

        revised = dz.get_asset(**scope, identifier=asset_id)
        assert revised["description"] == PROVISA_DESCRIPTION
        forms = {f["formName"]: f for f in revised["formsOutput"]}
        assert "GlueTableForm" in forms and TABLE_FORM in forms
        assert json.loads(forms[TABLE_FORM]["content"])["provisaUri"].endswith("/tables/orders")
        found = [
            h["dataProductItem"]
            for h in dz.search(
                **scope,
                owningProjectIdentifier=creds["PROVISA_E2E_DATAZONE_PROJECT_ID"],
                searchScope="DATA_PRODUCT",
                searchText=product_name,
            )["items"]
            if h.get("dataProductItem", {}).get("name") == product_name
        ]
        assert found, f"the data product {product_name} was not published"
        product_id = found[0]["id"]
        product = dz.get_data_product(**scope, identifier=product_id)
        assert asset_id in {i["identifier"] for i in product["items"]}
    finally:
        if product_id is not None:
            dz.delete_data_product(**scope, identifier=product_id)
        dz.delete_asset(**scope, identifier=asset_id)
