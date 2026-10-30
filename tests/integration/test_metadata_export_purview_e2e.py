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
entities. The product, its data assets and the org's terms are read back from the Unified Catalog.

REQ-254: the run leaves nothing in the account. Everything it publishes is named for the run --
the org (so the Data Map glossary), the source (so the Data Map entities and the data assets over
them), the product and the terms -- and the fixture's teardown, which runs however the test ends,
deletes all of it by those names: the Unified Catalog relationships, data product, terms and data
assets, then the Data Map entities, glossary terms and glossary. Every delete is attempted and
every failure reported together; then the account is read again for each of the run's objects and
the teardown fails if any is still there. The governance domain is the account's, which the test
only reads, and stays.

Without the account's endpoint and the CI app's credentials the test fails, never skips: a target
with no end-to-end proof is not complete (REQ-2119).
"""

# Requirements: REQ-2118, REQ-2119, REQ-254

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass

import httpx
import pytest

from provisa.api.metadata_export.atlas import (
    CLUSTER,
    PROVISA_COLUMN_TYPE,
    PROVISA_DATA_PRODUCT_TYPE,
    PROVISA_SOURCE_TYPE,
    PROVISA_TABLE_TYPE,
)
from provisa.api.metadata_export.purview import API_VERSION
from tests.integration.metadata_export_server import (
    CLASS_TERM,
    GLOSSARY_TERM,
    PRODUCT_FIELDS,
    SOURCE,
    approve_terms,
    model_config,
    publish,
    published_server,
    refused,
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
PURVIEW = "https://purview.azure.net/.default"
ATLAS = "/api/atlas/v2"
# The Atlas pass's published counts of the entity kinds teardown names (AtlasEntity.kind).
_DATA_MAP_KINDS = ("source", "table", "column", "data_product")


@dataclass(frozen=True)
class _Run:
    """The names one run publishes under: each carries the run's tag (REQ-254), so teardown
    finds exactly the run's own objects and no other run's."""

    tag: str

    @classmethod
    def new(cls) -> _Run:
        return cls(uuid.uuid4().hex[:8])

    @property
    def org(self) -> str:
        return f"{ORG}_{self.tag}"

    @property
    def source(self) -> str:
        return f"{SOURCE}-{self.tag}"

    @property
    def product(self) -> str:
        return f"provisa_e2e_{self.tag}"

    @property
    def name(self) -> str:
        return f"{PRODUCT_FIELDS['name']} {self.product}"

    @property
    def glossary_term(self) -> str:
        return f"{GLOSSARY_TERM} {self.product}"

    @property
    def class_term(self) -> str:
        return f"{CLASS_TERM} {self.product}"

    @property
    def terms(self) -> tuple[str, str]:
        return (self.glossary_term, self.class_term)

    @property
    def glossary(self) -> str:
        """The qualifiedName of the org's Data Map glossary (atlas._glossary_qn)."""
        return f"provisa_glossary.{self.org}@{CLUSTER}"

    def data_map_entities(self) -> list[tuple[str, str]]:
        """(type, qualifiedName) of every Data Map entity the publish makes, from the model the
        server is given, by the Atlas adapter's addressing (atlas._qualified): the source, each
        table, each column, the data product."""
        config = model_config(
            "trino",
            with_view=False,
            product=self.product,
            domain=DOMAIN,
            product_name=self.name,
            source=self.source,
            terms=self.terms,
        )
        names = [(PROVISA_SOURCE_TYPE, f"{self.source}@{CLUSTER}")]
        for table in config["tables"]:
            path = f"{table['source_id']}.{table['schema_name']}.{table['table_name']}"
            names.append((PROVISA_TABLE_TYPE, f"{path}@{CLUSTER}"))
            names.extend(
                (PROVISA_COLUMN_TYPE, f"{path}.{column['name']}@{CLUSTER}")
                for column in table["columns"]
            )
        names.append((PROVISA_DATA_PRODUCT_TYPE, f"data-product.{self.product}@{CLUSTER}"))
        return names


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


def _purview(creds: dict[str, str]) -> httpx.Client:
    """A client of the account's data plane, signed in as the CI app."""
    return httpx.Client(headers={"Authorization": f"Bearer {_token(creds, PURVIEW)}"}, timeout=60)


def _catalog(creds: dict[str, str]) -> str:
    return (
        f"https://{creds['AZURE_TENANT_ID']}-api.purview-service.microsoft.com"
        "/datagovernance/catalog"
    )


def _data_map(creds: dict[str, str]) -> str:
    return f"{creds['PROVISA_E2E_PURVIEW_ENDPOINT'].rstrip('/')}{ATLAS}"


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


async def _hold_owner_role(client: httpx.AsyncClient, org: str, email: str) -> None:
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
            f'INSERT INTO "org_{org}".user_directory (user_id, email) VALUES ($1, $2) '
            "ON CONFLICT (user_id) DO UPDATE SET email = EXCLUDED.email",
            OWNER,
            email,
        )
    finally:
        await conn.close()


def _listed(http: httpx.Client, url: str, **params) -> list[dict]:
    """Every item of a paged Unified Catalog listing."""
    items: list[dict] = []
    response = http.get(url, params={"api-version": API_VERSION, **params})
    while True:
        response.raise_for_status()
        page = response.json()
        items.extend(page.get("value", []))
        if not page.get("nextLink"):
            return items
        response = http.get(page["nextLink"])


def _domain_id(http: httpx.Client, creds: dict[str, str]) -> str:
    domains = {d["name"]: d["id"] for d in _listed(http, f"{_catalog(creds)}/businessdomains")}
    return domains[DOMAIN]


def _named(http: httpx.Client, creds: dict[str, str], kind: str, names: set[str]) -> list[dict]:
    """The Unified Catalog ``kind`` (dataProducts, terms) of DOMAIN named one of ``names``: the
    service gives each an id of its own, so the run's are found by the run's names."""
    url = f"{_catalog(creds)}/{kind}"
    return [
        item
        for item in _listed(http, url, domainId=_domain_id(http, creds))
        if item["name"] in names
    ]


def _related(http: httpx.Client, url: str, category: str) -> list[dict]:
    """The relationships of category ``category`` of the Unified Catalog object at ``url``."""
    return _listed(http, f"{url}/relationships", entityType=category)


def _term(creds: dict[str, str], name: str) -> tuple[dict | None, list[dict]]:
    """The run's Unified Catalog term ``name`` in DOMAIN and its data asset relationships."""
    with _purview(creds) as http:
        found = _named(http, creds, "terms", {name})
        if not found:
            return None, []
        (term,) = found
        return term, _related(http, f"{_catalog(creds)}/terms/{term['id']}", "DATAASSET")


def _read_back(creds: dict[str, str], name: str) -> tuple[dict | None, list[dict], list[str]]:
    """The Unified Catalog product the publish wrote, found by name in DOMAIN as the adapter finds
    it; its related data assets and their Data Map guids."""
    base = _catalog(creds)
    with _purview(creds) as http:
        named = _named(http, creds, "dataProducts", {name})
        if not named:
            return None, [], []
        (product,) = named
        assets = _related(http, f"{base}/dataProducts/{product['id']}", "DATAASSET")
        guids = []
        for asset in assets:
            got = http.get(
                f"{base}/dataAssets/{asset['entityId']}", params={"api-version": API_VERSION}
            )
            got.raise_for_status()
            guids.append(got.json()["source"]["assetId"])
    return product, assets, guids


class _Teardown:
    """Every delete of the run's objects, each attempted whatever the others answered; what
    failed is kept, never dropped, and the teardown fails on it once all were tried."""

    def __init__(self) -> None:
        self.failures: list[str] = []

    def step(self, what: str, call, ok: tuple[int, ...] = (200, 204)) -> httpx.Response | None:
        """``call()`` -- one request -- recorded as a failure of ``what`` unless it answers one of
        ``ok``; None when it failed."""
        try:
            response = call()
        except httpx.HTTPError as exc:
            self.failures.append(f"{what}: {exc!r}")
            return None
        if response.status_code not in ok:
            self.failures.append(f"{what}: HTTP {response.status_code}: {response.text[:500]}")
            return None
        return response

    def read(self, what: str, call):
        """A read the deletes depend on; its failure is recorded and None returned."""
        try:
            return call()
        except (httpx.HTTPError, KeyError, ValueError) as exc:
            self.failures.append(f"{what}: {exc!r}")
            return None


# The status the service requires before a delete: the Unified Catalog deletes a draft, so a
# published product or term is unpublished first, as its documented procedure does
# (https://learn.microsoft.com/en-us/purview/how-to-create-manage-glossary-terms).
_DRAFT = "DRAFT"
# The relationship categories the adapter writes (purview.py): a product's to its members' data
# assets; a term's to the Class term's table's data asset and to other terms. Each is listed and
# every relationship in it removed before its object is deleted.
_PRODUCT_CATEGORIES = ("DATAASSET",)
_TERM_CATEGORIES = ("DATAASSET", "TERM")


def _unpublish_and_delete(
    t: _Teardown, http: httpx.Client, url: str, item: dict, what: str, categories: tuple[str, ...]
) -> None:
    """Remove the relationships of ``item`` (a Unified Catalog product or term at ``url``) in
    ``categories``, unpublish it, delete it."""
    params = {"api-version": API_VERSION}
    for category in categories:
        related = t.read(
            f"{what}: list its {category} relationships", lambda: _related(http, url, category)
        )
        for rel in related or []:
            t.step(
                f"{what}: remove its {category} relationship to {rel['entityId']}",
                lambda rel=rel: http.delete(
                    f"{url}/relationships",
                    params={
                        **params,
                        "entityType": category,
                        "entityId": rel["entityId"],
                        **(
                            {"relationshipType": rel["relationshipType"]}
                            if rel.get("relationshipType")
                            else {}
                        ),
                    },
                ),
            )
    if str(item.get("status", "")).upper() != _DRAFT:
        t.step(
            f"{what}: unpublish",
            lambda: http.put(url, params=params, json={**item, "status": _DRAFT}),
        )
    t.step(f"{what}: delete", lambda: http.delete(url, params=params))


def _data_map_guids(t: _Teardown, http: httpx.Client, creds: dict[str, str], run: _Run) -> dict:
    """The live Data Map guid of each of the run's entities, by (type, qualifiedName); one the
    Data Map does not hold is absent."""
    guids: dict[tuple[str, str], str] = {}
    for type_name, qn in run.data_map_entities():
        response = t.step(
            f"Data Map: look up {type_name} {qn}",
            lambda type_name=type_name, qn=qn: http.get(
                f"{_data_map(creds)}/entity/uniqueAttribute/type/{type_name}",
                params={
                    "attr:qualifiedName": qn,
                    "minExtInfo": "true",
                    "ignoreRelationships": "true",
                },
            ),
            ok=(200, 404),
        )
        if response is not None and response.status_code == 200:
            entity = response.json()["entity"]
            if entity.get("status") != "DELETED":
                guids[(type_name, qn)] = entity["guid"]
    return guids


def _data_map_glossary(t: _Teardown, http: httpx.Client, creds: dict[str, str], run: _Run):
    """The run's Data Map glossary (the org's, named for the run), or None."""
    response = t.step("Data Map: list glossaries", lambda: http.get(f"{_data_map(creds)}/glossary"))
    if response is None:
        return None
    found = [g for g in response.json() or [] if g.get("qualifiedName") == run.glossary]
    return found[0] if found else None


def _data_assets(t: _Teardown, http: httpx.Client, creds: dict[str, str], guids: list[str]):
    """The Unified Catalog data assets over the run's Data Map entities."""
    if not guids:
        return []
    response = t.step(
        "Unified Catalog: query the data assets over the run's Data Map entities",
        lambda: http.post(
            f"{_catalog(creds)}/dataAssets/query",
            params={"api-version": API_VERSION},
            json={"sourceAssetIds": sorted(guids)},
        ),
    )
    return [] if response is None else response.json().get("value", [])


def _tear_down(creds: dict[str, str], run: _Run, result: dict | None) -> list[str]:
    """Delete everything the run published to the account, every delete attempted; what failed,
    then what the account still holds of the run's (each a message)."""
    t = _Teardown()
    base = _catalog(creds)
    params = {"api-version": API_VERSION}
    with _purview(creds) as http:
        guids = _data_map_guids(t, http, creds, run)
        if result is not None and result.get("ok"):
            # A publish that succeeded made every one of these, and no other entity: one not
            # found, or a count apart, means teardown's names no longer match the adapter's, and
            # whatever it made instead would be left behind.
            expected = run.data_map_entities()
            missing = sorted(qn for type_name, qn in expected if (type_name, qn) not in guids)
            if missing:
                t.failures.append(f"Data Map: the publish's entities not found by name: {missing}")
            made = sum(result["published"].get(k, 0) for k in _DATA_MAP_KINDS)
            if made != len(expected):
                t.failures.append(
                    f"Data Map: the publish made {made} entities, teardown names {len(expected)}"
                )
        assets = _data_assets(t, http, creds, list(guids.values()))

        # The Unified Catalog first: its objects refer to the Data Map's.
        for product in (
            t.read(
                "Unified Catalog: list the run's data products",
                lambda: _named(http, creds, "dataProducts", {run.name}),
            )
            or []
        ):
            _unpublish_and_delete(
                t,
                http,
                f"{base}/dataProducts/{product['id']}",
                product,
                f"Unified Catalog data product {product['name']!r}",
                _PRODUCT_CATEGORIES,
            )
        for term in (
            t.read(
                "Unified Catalog: list the run's terms",
                lambda: _named(http, creds, "terms", set(run.terms)),
            )
            or []
        ):
            _unpublish_and_delete(
                t,
                http,
                f"{base}/terms/{term['id']}",
                term,
                f"Unified Catalog term {term['name']!r}",
                _TERM_CATEGORIES,
            )
        for asset in assets:
            t.step(
                f"Unified Catalog: delete data asset {asset['id']} over {asset['source']['assetId']}",
                lambda asset=asset: http.delete(f"{base}/dataAssets/{asset['id']}", params=params),
            )

        # Then the Data Map: the entities, the glossary's terms, the glossary.
        if guids:
            t.step(
                "Data Map: delete the run's entities",
                lambda: http.delete(
                    f"{_data_map(creds)}/entity/bulk",
                    params=[("guid", guid) for guid in sorted(guids.values())],
                ),
            )
        glossary = _data_map_glossary(t, http, creds, run)
        if glossary is not None:
            terms = t.step(
                "Data Map: list the run's glossary terms",
                lambda: http.get(
                    f"{_data_map(creds)}/glossary/{glossary['guid']}/terms", params={"limit": 1000}
                ),
            )
            for term in [] if terms is None else terms.json() or []:
                t.step(
                    f"Data Map: delete glossary term {term.get('name')!r}",
                    lambda term=term: http.delete(
                        f"{_data_map(creds)}/glossary/term/{term['guid']}"
                    ),
                )
            t.step(
                f"Data Map: delete glossary {run.glossary}",
                lambda: http.delete(f"{_data_map(creds)}/glossary/{glossary['guid']}"),
            )

        # What is left: each of the run's objects, read again.
        left = _left(t, http, creds, run, sorted(guids.values()))
    return t.failures + left


def _left(t: _Teardown, http: httpx.Client, creds: dict[str, str], run: _Run, guids: list[str]):
    """Each object of the run's the account still holds, as a message."""
    left: list[str] = []
    for kind, names in (("dataProducts", {run.name}), ("terms", set(run.terms))):
        for item in (
            t.read(
                f"Unified Catalog: list {kind} after teardown",
                lambda kind=kind, names=names: _named(http, creds, kind, names),
            )
            or []
        ):
            left.append(f"left: Unified Catalog {kind} {item['name']!r} ({item['id']})")
    for asset in _data_assets(t, http, creds, guids):
        left.append(
            f"left: Unified Catalog data asset {asset['id']} over {asset['source']['assetId']}"
        )
    for guid in guids:
        response = t.step(
            f"Data Map: read entity {guid} after teardown",
            lambda guid=guid: http.get(
                f"{_data_map(creds)}/entity/guid/{guid}",
                params={"minExtInfo": "true", "ignoreRelationships": "true"},
            ),
            ok=(200, 404),
        )
        # Atlas's own state for a deleted entity is DELETED (EntityStatus); 404 is one removed.
        if response is not None and response.status_code == 200:
            if response.json()["entity"].get("status") != "DELETED":
                left.append(f"left: Data Map entity {guid}")
    for type_name, qn in _data_map_guids(t, http, creds, run):
        left.append(f"left: Data Map {type_name} {qn}")
    if _data_map_glossary(t, http, creds, run) is not None:
        left.append(f"left: Data Map glossary {run.glossary}")
    return left


async def _deprecate(client: httpx.AsyncClient, name: str) -> dict:
    """Deprecate the org's term ``name`` as a curator does, then publish to the same target; what
    that second publish reports."""
    terms = await client.get("/admin/glossary/terms")
    assert terms.status_code == 200, terms.text
    (term_id,) = [t["id"] for t in terms.json() if t["name"] == name]
    # Only an approved term is deprecated (glossary.term_not_approved); approve_terms approved it.
    marked = await client.post(
        f"/admin/glossary/terms/{term_id}/deprecate", json={"reason": "Superseded by party."}
    )
    assert marked.status_code == 200, marked.text
    republished = await client.post("/admin/metadata-export/publish")
    assert republished.status_code == 200, republished.text
    (outcome,) = republished.json()["targets"]
    return outcome


@pytest.fixture(scope="module")
async def published():
    creds = _credentials()
    run = _Run.new()
    result: dict | None = None
    try:
        email, oid = _a_tenant_user(creds)
        async with published_server(
            run.org,
            with_view=False,
            product=run.product,
            domain=DOMAIN,
            product_name=run.name,
            source=run.source,
            terms=run.terms,
        ) as client:
            await _hold_owner_role(client, run.org, email)
            await approve_terms(client, run.terms)
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
            # A curator deprecates a published term; the next publish moves the live Unified
            # Catalog term from PUBLISHED to EXPIRED.
            deprecated = await _deprecate(client, run.glossary_term)
        term, term_assets = _term(creds, run.class_term)
        expired, _ = _term(creds, run.glossary_term)
        found, assets, guids = _read_back(creds, run.name)
        yield run, result, found, assets, guids, oid, term, term_assets, deprecated, expired
    finally:
        problems = _tear_down(creds, run, result)
        assert not problems, "the run left the Purview account unclean (REQ-254):\n" + "\n".join(
            problems
        )


async def test_the_publish_reports_the_product_and_its_assets(published):
    result = published[1]
    assert result["ok"], refused(result)
    assert result["published"]["data_products"] == 1
    assert result["published"]["data_assets"] == 2


async def test_the_product_is_in_its_governance_domain_with_its_owner(published):
    found, oid = published[2], published[5]
    assert found is not None, "no Unified Catalog data product was found"
    assert found["contacts"]["owner"][0]["id"] == oid
    assert PRODUCT_FIELDS["purpose"] in found["description"]
    assert found["businessUse"] == PRODUCT_FIELDS["usage"]
    assert found["status"] == "Published"


async def test_each_member_is_a_data_asset_over_its_data_map_entity(published):
    assets, guids = published[3], published[4]
    assert len(assets) == 2 and len(set(guids)) == 2


async def test_the_class_term_is_in_its_domain_and_related_to_its_tables_asset(published):
    """REQ-1387/1389: the Class term lands in the Unified Catalog glossary, in its domain's
    governance domain, and is related to the data asset of the table it grounds, as Provisa's
    own relationship."""
    run, result, assets = published[0], published[1], published[3]
    term, term_assets = published[6], published[7]
    assert term is not None, "the Unified Catalog has no term for the class term"
    assert term["name"] == run.class_term and term["status"] == "Published"
    assert result["published"]["glossary_term"] == 2
    # The service lists a relationship as its entity and type alone: the one the Class term has
    # is a Related one to a data asset of the product's members, the orders table's.
    assert len(term_assets) == 1, term_assets
    related = term_assets[0]
    assert related["relationshipType"] == "Related", term_assets
    assert related["entityId"] in {a["entityId"] for a in assets}, (term_assets, assets)


async def test_a_deprecated_term_goes_from_published_to_expired(published):
    """REQ-2144/REQ-1387: a deprecated term is EXPIRED in the Unified Catalog. The term was
    PUBLISHED by the run's first publish, so the second moves a live term between the two."""
    deprecated, expired = published[8], published[9]
    assert deprecated["ok"], refused(deprecated)
    assert expired is not None
    # The service stores a status title-cased, as it reads PUBLISHED back as Published.
    assert expired["status"] == "Expired"
