# Copyright (c) 2026 Kenneth Stott
# Canary: cd241716-a559-48cf-9abd-4702543270db
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2118 e2e: a Provisa server publishes a data product to Microsoft Purview's Unified Catalog.

The project's Purview account (provisa-e2e-purview) holds the governance domain
provisa-e2e-sales, which the product's domain is named for. The product's owner role has one
holder: a user of the Entra tenant, found through Graph. The target is saved and published through
the admin API: the Data Map through Atlas, then the Unified Catalog product over the Data Map's
entities. The product and its data assets are read back from the Unified Catalog, then deleted.

Without the account's endpoint and the CI app's credentials the test fails, never skips: a target
with no end-to-end proof is not complete (REQ-2119).
"""

# Requirements: REQ-2118, REQ-2119

from __future__ import annotations

import os
import uuid

import httpx
import pytest

from provisa.api.metadata_export.purview import API_VERSION, token_oid
from provisa.api.metadata_export.refs import term_uri
from tests.integration.metadata_export_server import (
    CLASS_TERM,
    PRODUCT_FIELDS,
    publish,
    published_server,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_warehouse,
    pytest.mark.asyncio(loop_scope="session"),
]

ORG = "mdx_purview_e2e"
DOMAIN = "provisa-e2e-sales"
OWNER = "provisa-e2e-owner"
_CREDENTIALS = (
    "PROVISA_E2E_PURVIEW_ENDPOINT",
    "AZURE_TENANT_ID",
    "AZURE_CLIENT_ID",
    "AZURE_CLIENT_SECRET",
)


def _credentials() -> dict[str, str]:
    missing = [name for name in _CREDENTIALS if not os.environ.get(name)]
    assert not missing, f"the Purview e2e needs {', '.join(missing)}"
    return {name: os.environ[name] for name in _CREDENTIALS}


def _token(creds: dict[str, str], scope: str) -> str:
    response = httpx.post(
        f"https://login.microsoftonline.com/{creds['AZURE_TENANT_ID']}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": creds["AZURE_CLIENT_ID"],
            "client_secret": creds["AZURE_CLIENT_SECRET"],
            "scope": scope,
        },
        timeout=60,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def _catalog(creds: dict[str, str]) -> str:
    return (
        f"https://{creds['AZURE_TENANT_ID']}-api.purview-service.microsoft.com"
        "/datagovernance/catalog"
    )


def _a_tenant_user(creds: dict[str, str]) -> tuple[str, str]:
    """A user of the tenant, as Graph has it: (address, object id)."""
    headers = {"Authorization": f"Bearer {_token(creds, 'https://graph.microsoft.com/.default')}"}
    listed = httpx.get(
        "https://graph.microsoft.com/v1.0/users",
        params={"$top": "1", "$select": "id,userPrincipalName"},
        headers=headers,
        timeout=60,
    )
    listed.raise_for_status()
    (user,) = listed.json()["value"]
    return user["userPrincipalName"], user["id"]


async def _hold_owner_role(client: httpx.AsyncClient, email: str) -> None:
    """A member holding the product's owner role, with ``email`` in the directory: the grant
    through the admin API, the address as a member's sign-in records it."""
    import asyncpg

    granted = await client.post(
        f"/admin/users/{OWNER}/assignments",
        json={"role_id": PRODUCT_FIELDS["owner_role"], "domain_id": "*"},
    )
    assert granted.status_code == 200, granted.text
    conn = await asyncpg.connect(
        host=os.environ["PG_HOST"],
        port=int(os.environ["PG_PORT"]),
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
        database=os.environ["PG_DATABASE"],
        timeout=15,
    )
    try:
        await conn.execute(
            f'INSERT INTO "org_{ORG}".user_directory (user_id, email) VALUES ($1, $2) '
            "ON CONFLICT (user_id) DO UPDATE SET email = EXCLUDED.email",
            OWNER,
            email,
        )
    finally:
        await conn.close()


def _term(creds: dict[str, str], name: str) -> tuple[dict | None, list[dict]]:
    """The org's Unified Catalog term ``name``, by the id its Provisa URI derives, and its data
    asset relationships. Read before the run's data assets are removed; the term stays, it is
    the org's."""
    headers = {"Authorization": f"Bearer {_token(creds, 'https://purview.azure.net/.default')}"}
    params = {"api-version": API_VERSION}
    term_id = str(uuid.uuid5(uuid.NAMESPACE_URL, term_uri(ORG, name)))
    with httpx.Client(headers=headers, timeout=60) as http:
        got = http.get(f"{_catalog(creds)}/terms/{term_id}", params=params)
        if got.status_code == 404:
            return None, []
        got.raise_for_status()
        related = http.get(
            f"{_catalog(creds)}/terms/{term_id}/relationships",
            params={**params, "entityType": "DATAASSET"},
        )
        related.raise_for_status()
    return got.json(), related.json().get("value", [])


def _read_back(creds: dict[str, str], name: str) -> tuple[dict | None, list[dict], list[str]]:
    """The Unified Catalog product named ``name``, its related data assets and their Data Map
    guids, then all of them deleted."""
    headers = {"Authorization": f"Bearer {_token(creds, 'https://purview.azure.net/.default')}"}
    params = {"api-version": API_VERSION}
    base = _catalog(creds)
    with httpx.Client(headers=headers, timeout=60) as http:
        listed = http.post(f"{base}/dataProducts/query", params=params, json={"nameKeyword": name})
        listed.raise_for_status()
        found = [p for p in listed.json().get("value", []) if p["name"] == name]
        if not found:
            return None, [], []
        product = found[0]
        related = http.get(
            f"{base}/dataProducts/{product['id']}/relationships",
            params={**params, "entityType": "DATAASSET"},
        )
        related.raise_for_status()
        assets = related.json().get("value", [])
        guids = []
        for asset in assets:
            got = http.get(f"{base}/dataAssets/{asset['entityId']}", params=params)
            got.raise_for_status()
            guids.append(got.json()["source"]["assetId"])
        for asset in assets:
            http.delete(
                f"{base}/dataProducts/{product['id']}/relationships",
                params={**params, "entityType": "DATAASSET", "entityId": asset["entityId"]},
            )
        http.delete(f"{base}/dataProducts/{product['id']}", params=params)
        for asset in assets:
            http.delete(f"{base}/dataAssets/{asset['entityId']}", params=params)
    return product, assets, guids


@pytest.fixture(scope="module")
async def published():
    creds = _credentials()
    product = f"provisa_e2e_{uuid.uuid4().hex[:8]}"
    name = f"{PRODUCT_FIELDS['name']} {product}"
    email, oid = _a_tenant_user(creds)
    async with published_server(
        ORG, with_view=False, product=product, domain=DOMAIN, product_name=name
    ) as client:
        await _hold_owner_role(client, email)
        # The run's product carries the run's name, so it is found and deleted by it.
        result = await publish(
            client,
            {
                "provider": "purview",
                "endpoint": creds["PROVISA_E2E_PURVIEW_ENDPOINT"],
                "auth_mode": "entra",
                "entra_tenant_id": creds["AZURE_TENANT_ID"],
                "entra_client_id": creds["AZURE_CLIENT_ID"],
                "entra_client_secret": creds["AZURE_CLIENT_SECRET"],
                "timeout_seconds": 120,
            },
        )
    term, term_assets = _term(creds, CLASS_TERM)
    found, assets, guids = _read_back(creds, name)
    yield result, found, assets, guids, oid, term, term_assets


async def test_the_publish_reports_the_product_and_its_assets(published):
    result = published[0]
    assert result["ok"], result["errors"]
    assert result["published"]["data_products"] == 1
    assert result["published"]["data_assets"] == 2


async def test_the_product_is_in_its_governance_domain_with_its_owner(published):
    found, oid = published[1], published[4]
    assert found is not None, "no Unified Catalog data product was found"
    assert found["contacts"]["owner"][0]["id"] == oid
    assert PRODUCT_FIELDS["purpose"] in found["description"]
    assert found["businessUse"] == PRODUCT_FIELDS["usage"]
    assert found["status"] == "PUBLISHED"


async def test_each_member_is_a_data_asset_over_its_data_map_entity(published):
    assets, guids = published[2], published[3]
    assert len(assets) == 2 and len(set(guids)) == 2


async def test_the_class_term_is_in_its_domain_and_related_to_its_tables_asset(published):
    """REQ-1387/1389: the Class term lands in the Unified Catalog glossary, in its domain's
    governance domain, and is related to the data asset of the table it grounds, as Provisa's
    own relationship."""
    result, term, term_assets = published[0], published[5], published[6]
    assert term is not None, "the Unified Catalog has no term for the class term"
    assert term["name"] == CLASS_TERM and term["status"] == "PUBLISHED"
    assert result["published"]["glossary_term"] == 2
    assert len(term_assets) == 1
    provisa = token_oid(_token(_credentials(), "https://purview.azure.net/.default"))
    assert term_assets[0]["systemData"]["createdBy"] == provisa
