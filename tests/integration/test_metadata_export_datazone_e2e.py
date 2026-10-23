# Copyright (c) 2026 Kenneth Stott
# Canary: d140beb9-fe4d-4705-8eb7-84f0cb3cf3d3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2118 e2e: a Provisa server publishes a data product to Amazon DataZone.

The project's DataZone domain (provisa-e2e-datazone) and its producer project
(provisa-e2e-producer) are the target. The target is saved and published through the admin API;
the data product, named for the run, and its two member assets are then read back from DataZone
with the CI user's own keys, and removed.

Without the domain's ids and the CI user's keys the test fails, never skips: a target with no
end-to-end proof is not complete (REQ-2119).
"""

# Requirements: REQ-2118, REQ-2119

from __future__ import annotations

import json
import os
import time
import uuid

import pytest

from provisa.api.metadata_export.datazone import PRODUCT_FORM, TABLE_FORM, TABLE_TYPE
from tests.integration.metadata_export_server import (
    PRODUCT_FIELDS,
    publish,
    published_server,
    refused,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_warehouse,
    pytest.mark.asyncio(loop_scope="session"),
]

ORG = "mdx_datazone_e2e"
REGION = "us-east-1"
_CREDENTIALS = (
    "PROVISA_E2E_DATAZONE_AWS_ACCESS_KEY_ID",
    "PROVISA_E2E_DATAZONE_AWS_SECRET_ACCESS_KEY",
    "PROVISA_E2E_DATAZONE_DOMAIN_ID",
    "PROVISA_E2E_DATAZONE_PROJECT_ID",
)


def _credentials() -> dict[str, str]:
    missing = [name for name in _CREDENTIALS if not os.environ.get(name)]
    assert not missing, f"the DataZone e2e needs {', '.join(missing)}"
    return {name: os.environ[name] for name in _CREDENTIALS}


def _client(creds: dict[str, str]):
    import boto3

    return boto3.client(
        "datazone",
        region_name=REGION,
        aws_access_key_id=creds["PROVISA_E2E_DATAZONE_AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=creds["PROVISA_E2E_DATAZONE_AWS_SECRET_ACCESS_KEY"],
    )


def _listing(client, scope: dict, product_id: str, name: str) -> dict | None:
    """The product's catalog listing, which DataZone creates after the publish returns: waited
    for, up to two minutes."""
    deadline = time.monotonic() + 120
    while True:
        for hit in client.search_listings(**scope, searchText=name)["items"]:
            listing = hit.get("dataProductListing")
            if listing and listing["entityId"] == product_id:
                return listing
        if time.monotonic() > deadline:
            return None
        time.sleep(5)


def _read_back(creds: dict[str, str], name: str) -> tuple[dict | None, list[dict], dict | None]:
    """The run's data product, its member assets and its listing, then the product removed
    (the assets stay: they are the members', revised each run)."""
    client = _client(creds)
    scope = {"domainIdentifier": creds["PROVISA_E2E_DATAZONE_DOMAIN_ID"]}
    # DataZone's search index has a new product some seconds after it is created: waited for,
    # up to two minutes.
    deadline = time.monotonic() + 120
    while True:
        hits = client.search(
            **scope,
            owningProjectIdentifier=creds["PROVISA_E2E_DATAZONE_PROJECT_ID"],
            searchScope="DATA_PRODUCT",
            searchText=name,
        )["items"]
        found = [
            h["dataProductItem"] for h in hits if h.get("dataProductItem", {}).get("name") == name
        ]
        if found or time.monotonic() > deadline:
            break
        time.sleep(5)
    if not found:
        return None, [], None
    product = client.get_data_product(**scope, identifier=found[0]["id"])
    assets = [client.get_asset(**scope, identifier=i["identifier"]) for i in product["items"]]
    listing = _listing(client, scope, product["id"], name)
    if listing is not None:
        client.create_listing_change_set(
            **scope, entityIdentifier=product["id"], entityType="DATA_PRODUCT", action="UNPUBLISH"
        )
    client.delete_data_product(**scope, identifier=product["id"])
    return product, assets, listing


@pytest.fixture(scope="module")
async def published():
    creds = _credentials()
    product = f"provisa_e2e_{uuid.uuid4().hex[:8]}"
    name = f"{PRODUCT_FIELDS['name']} {product}"
    async with published_server(ORG, with_view=False, product=product, product_name=name) as client:
        result = await publish(
            client,
            {
                "provider": "datazone",
                "aws_region": REGION,
                "datazone_domain_id": creds["PROVISA_E2E_DATAZONE_DOMAIN_ID"],
                "datazone_project_id": creds["PROVISA_E2E_DATAZONE_PROJECT_ID"],
                "aws_access_key_id": creds["PROVISA_E2E_DATAZONE_AWS_ACCESS_KEY_ID"],
                "aws_secret_access_key": creds["PROVISA_E2E_DATAZONE_AWS_SECRET_ACCESS_KEY"],
                "timeout_seconds": 120,
            },
        )
    found, assets, listing = _read_back(creds, name)
    yield result, found, assets, listing


async def test_the_publish_reports_the_product_and_its_assets(published):
    result = published[0]
    assert result["ok"], refused(result)
    assert result["published"] == {"data_products": 1, "data_assets": 2}


async def test_the_product_is_listed_for_subscription(published):
    listing = published[3]
    assert listing is not None, "the data product was not listed in the catalog"


async def test_the_product_carries_its_fields_in_its_form(published):
    found = published[1]
    assert found is not None, "no DataZone data product was found"
    assert found["description"] == PRODUCT_FIELDS["purpose"]
    (form,) = [f for f in found["formsOutput"] if f["formName"] == PRODUCT_FORM]
    content = json.loads(form["content"])
    assert content["usage"] == PRODUCT_FIELDS["usage"]
    assert content["version"] == PRODUCT_FIELDS["version"]


async def test_each_member_is_a_provisa_table_asset(published):
    assets = published[2]
    assert len(assets) == 2
    assert {a["typeIdentifier"] for a in assets} == {TABLE_TYPE}
    for asset in assets:
        (form,) = [f for f in asset["formsOutput"] if f["formName"] == TABLE_FORM]
        assert json.loads(form["content"])["provisaUri"] == asset["externalIdentifier"]
