# Copyright (c) 2026 Kenneth Stott
# Canary: 2f27b4de-2e83-4f17-bff8-b4e01f9db8cf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2118: the Purview target, against a stand-in Entra, Graph and Unified Catalog.

The Data Map publishes through the Atlas adapter (stood in here by the guids it returns); each
data product then becomes a Unified Catalog data product in the governance domain of its
domain's name, owned by its owner role's holders as Entra users, with its members' Data Map
entities as its data assets."""

# Requirements: REQ-2118, REQ-1387, REQ-1389, REQ-2141

from __future__ import annotations

import base64
import json
from dataclasses import replace
from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from provisa.api.metadata_export import purview
from provisa.api.metadata_export.atlas import AtlasExport
from provisa.api.metadata_export.builder import build_snapshot
from provisa.api.metadata_export.provider import PublishResult
from provisa.api.metadata_export.purview import (
    PurviewExport,
    catalog_status,
    product_id,
    update_frequency,
)
from provisa.api.metadata_export.registry import metadata_export
from provisa.core.models import (
    Column,
    DataProduct,
    Domain,
    MetadataExportTarget,
    ProvisaConfig,
    Source,
    SourceType,
    Table,
)

pytestmark = pytest.mark.unit

TENANT = "tenant-1"
CATALOG = f"https://{TENANT}-api.purview-service.microsoft.com/datagovernance/catalog"
# CatalogModelDataProductTypeEnum, 2026-03-20-preview.
PRODUCT_TYPES = {
    "Master",
    "Reference",
    "Analytical",
    "AI",
    "MasterDataAndReferenceData",
    "BusinessSystemOrApplication",
    "ModelTypes",
    "DashboardsOrReports",
    "Operational",
    "MLAITrainingDataSet",
    "MLAITestingDataSet",
    "TransactionalDataset",
    "AnalyticsModel",
    "SemanticModel",
}
DOMAIN_ID = "11111111-1111-1111-1111-111111111111"
PROVISA_OID = "oid-provisa"


def _token(scope: str) -> str:
    """An Entra access token as the catalog reads it: its claims name Provisa's principal."""
    claims = base64.urlsafe_b64encode(json.dumps({"oid": PROVISA_OID}).encode()).decode()
    return f"header.{claims.rstrip('=')}.{scope}"


def _target() -> MetadataExportTarget:
    return MetadataExportTarget(
        id="purview",
        provider="purview",
        endpoint="https://acct.purview.azure.com/catalog",
        auth_mode="entra",
        entra_tenant_id=TENANT,
        entra_client_id="client",
        entra_client_secret="secret",
        timeout_seconds=5,
    )


def _snapshot(*, owners=("owner@example.com",), domain="sales", status="active", glossary=None):
    config = ProvisaConfig(
        sources=[
            Source(
                id="pg",
                type=SourceType.postgresql,
                host="h",
                port=5432,
                database="d",
                username="u",
                password="p",
            )
        ],
        domains=[Domain(id=domain, description="Sales")],
        tables=[
            Table(
                source_id="pg",
                domain_id=domain,
                schema_name="public",
                table_name=name,
                product_id="sales_360",
                cache_ttl=ttl,
                columns=[Column(name="id", data_type="integer", visible_to=["admin"])],
            )
            for name, ttl in (("orders", 3600), ("customers", 86400))
        ],
        data_products=[
            DataProduct(
                id="sales_360",
                domain_id=domain,
                name="Sales 360",
                owner_role="data_owner",
                purpose="Every sale",
                usage="Join on customer_id",
                limitations="No refunds",
                status=status,
            )
        ],
        roles=[],
    )
    return build_snapshot(
        config,
        org_id="acme",
        dialect="postgres",
        contexts={},
        owner_emails={"data_owner": owners},
        glossary=glossary,
    )


class _Purview:
    """Stand-in Entra token endpoint, Graph users and Unified Catalog."""

    def __init__(self, *, domains: dict[str, str] | None = None, users: dict | None = None):
        self.domains = {"sales": DOMAIN_ID} if domains is None else domains
        self.users = {"owner@example.com": "oid-owner"} if users is None else users
        self.products: dict[str, dict] = {}
        self.assets: dict[str, str] = {}  # data map guid -> data asset id
        self.related: dict[str, set[str]] = {}
        self.calls: list[tuple[str, str]] = []
        self.terms: dict[str, dict] = {}
        # term id -> its relationships, as the catalog lists them
        self.term_relations: dict[str, list[dict]] = {}
        # term ids whose read answers 503
        self.unreadable: set[str] = set()

    def handle(self, request: httpx.Request) -> httpx.Response:
        url, method = request.url, request.method
        self.calls.append((method, url.path))
        if url.host == "login.microsoftonline.com":
            scope = dict(httpx.QueryParams(request.content.decode()))["scope"]
            return httpx.Response(200, json={"access_token": _token(scope)})
        if url.host == "graph.microsoft.com":
            assert request.headers["Authorization"].endswith("graph.microsoft.com/.default")
            email = url.path.rsplit("/", 1)[-1]
            if email not in self.users:
                return httpx.Response(404, json={"error": {"code": "Request_ResourceNotFound"}})
            return httpx.Response(200, json={"id": self.users[email]})
        assert request.headers["Authorization"].endswith("purview.azure.net/.default")
        body = json.loads(request.content) if request.content else {}
        assert url.params["api-version"] == purview.API_VERSION
        path = url.path.removeprefix("/datagovernance/catalog")
        if path == "/businessdomains":
            return httpx.Response(
                200, json={"value": [{"name": n, "id": i} for n, i in self.domains.items()]}
            )
        if path == "/dataAssets/query":
            wanted = body["sourceAssetIds"]
            found = [
                {"id": a, "source": {"assetId": g}} for g, a in self.assets.items() if g in wanted
            ]
            return httpx.Response(200, json={"value": found})
        if path == "/dataAssets":
            # As the live service does, whatever the published schema says (run 40102236534).
            missing = [k for k in ("name", "type") if k not in body]
            if missing:
                errors = {k: [f"Required property '{k}' not found in JSON."] for k in missing}
                return httpx.Response(400, json={"errors": errors})
            assert body["type"] == "General"
            if (body.get("source") or {}).get("type") != "DataMap":
                return httpx.Response(400, json={"error": {"code": "DataCatalogInvalidEntity"}})
            asset_id = f"asset-{body['source']['assetId']}"
            self.assets[body["source"]["assetId"]] = asset_id
            return httpx.Response(201, json={"id": asset_id})
        if (
            path.startswith("/dataProducts")
            and method in ("POST", "PUT")
            and "relationships" not in path
        ):
            # The service requires a data product's type (run 40105170193).
            if body.get("type") not in PRODUCT_TYPES:
                message = 'Property "type" is required.'
                return httpx.Response(
                    400, json={"error": {"code": "DataCatalogInvalidEntity", "message": message}}
                )
        if path == "/dataProducts" and method == "POST":
            self.products[body["id"]] = body
            return httpx.Response(201, json=body)
        if path == "/terms" and method == "GET":
            listed = [t for t in self.terms.values() if t["domain"] == url.params["domainId"]]
            return httpx.Response(200, json={"value": listed})
        if path == "/terms" and method == "POST":
            self.terms[body["id"]] = {**body, "systemData": {"createdBy": PROVISA_OID}}
            return httpx.Response(201, json=body)
        if path.startswith("/terms/") and path.endswith("/relationships"):
            tid = path.split("/")[2]
            relations = self.term_relations.setdefault(tid, [])
            category = url.params["entityType"]
            if method == "GET":
                return httpx.Response(
                    200, json={"value": [r for r in relations if r["category"] == category]}
                )
            if method == "POST":
                relations.append(
                    {**body, "category": category, "systemData": {"createdBy": PROVISA_OID}}
                )
                return httpx.Response(200, json=body)
            relations[:] = [
                r
                for r in relations
                if not (
                    r["category"] == category
                    and r["entityId"] == url.params["entityId"]
                    and r["relationshipType"] == url.params["relationshipType"]
                )
            ]
            return httpx.Response(204)
        if path.startswith("/terms/"):
            tid = path.split("/")[2]
            if method == "GET":
                if tid in self.unreadable:
                    return httpx.Response(503, json={"error": {"code": "ServiceUnavailable"}})
                if tid not in self.terms:
                    return httpx.Response(404, json={"error": {"code": "NotFound"}})
                return httpx.Response(200, json=self.terms[tid])
            self.terms[tid] = body
            return httpx.Response(200, json=body)
        if path.endswith("/relationships"):
            pid = path.split("/")[2]
            related = self.related.setdefault(pid, set())
            if method == "GET":
                return httpx.Response(200, json={"value": [{"entityId": e} for e in related]})
            if method == "POST":
                related.update(v["entityId"] for v in body.values())
                return httpx.Response(200, json={})
            related.discard(url.params["entityId"])
            return httpx.Response(204)
        pid = path.split("/")[2]
        if method == "GET":
            if pid not in self.products:
                return httpx.Response(404, json={"error": {"code": "NotFound"}})
            return httpx.Response(200, json=self.products[pid])
        if method == "PUT":
            self.products[pid] = body
            return httpx.Response(200, json=body)
        raise AssertionError(f"unexpected {method} {url}")


@pytest.fixture
def api(monkeypatch):
    fake = _Purview()
    real = httpx.AsyncClient
    monkeypatch.setattr(
        purview.httpx,
        "AsyncClient",
        lambda **kw: real(transport=httpx.MockTransport(fake.handle), **kw),
    )
    return fake


@pytest.fixture
def data_map(monkeypatch):
    """The Data Map publish: each table's guid, as the Atlas adapter returns it."""
    guids: dict[str, str] = {}

    async def _published(self, snapshot):
        result = PublishResult(provider_name=self.provider_name)
        for table in snapshot.tables:
            name = table.ref.parts[-1]
            if name in guids:
                result.bindings[table.semantic_uri] = (guids[name], "qn")
        return result

    monkeypatch.setattr(AtlasExport, "publish", _published)
    guids.update(orders="guid-orders", customers="guid-customers")
    return guids


def _export() -> PurviewExport:
    exporter = metadata_export(_target())
    assert isinstance(exporter, PurviewExport)
    return exporter


def test_a_purview_target_signs_in_with_entra():
    with pytest.raises(ValidationError, match="Microsoft Entra"):
        MetadataExportTarget(id="p", provider="purview", endpoint="https://x", auth_mode="bearer")


@pytest.mark.parametrize(
    ("status", "publish", "expected"),
    [
        ("active", False, "PUBLISHED"),
        ("deprecated", False, "EXPIRED"),
        ("retired", True, "EXPIRED"),
        ("proposed", True, "DRAFT"),
        (None, True, "PUBLISHED"),
        (None, False, "DRAFT"),
    ],
)
def test_the_status_is_the_catalogs(status: Any, publish: bool, expected: str):
    product = type("P", (), {"status": status, "publish": publish})()
    assert catalog_status(product) == expected  # type: ignore[arg-type]


def test_the_update_frequency_is_the_shortest_members_freshness():
    tables = [type("T", (), {"cache_ttl": t})() for t in (86400, 3600, None, -1)]
    assert update_frequency(tables) == "Hourly"  # type: ignore[arg-type]
    assert update_frequency([type("T", (), {"cache_ttl": -1})()]) is None  # type: ignore[list-item]


async def test_a_product_is_created_in_its_domain_with_owners_and_assets(api, data_map):
    snapshot = _snapshot()
    result = await _export().publish(snapshot)
    assert result.ok, result.errors
    assert result.published["data_products"] == 1 and result.published["data_assets"] == 2
    (product,) = snapshot.data_products
    body = api.products[product_id(product)]
    assert body["name"] == "Sales 360" and body["domain"] == DOMAIN_ID
    assert body["contacts"]["owner"] == [{"id": "oid-owner", "description": "owner@example.com"}]
    assert body["status"] == "PUBLISHED" and body["updateFrequency"] == "Hourly"
    assert body["businessUse"] == "Join on customer_id"
    assert "Every sale" in body["description"] and "Limitations: No refunds" in body["description"]
    assert set(api.assets) == {"guid-orders", "guid-customers"}
    assert api.related[product_id(product)] == {"asset-guid-orders", "asset-guid-customers"}


async def test_a_second_publish_updates_in_place_and_drops_a_former_member(api, data_map):
    await _export().publish(_snapshot())
    del data_map["customers"]  # no longer in the Data Map's result: no longer an asset
    result = await _export().publish(_snapshot())
    (product,) = _snapshot().data_products
    assert ("PUT", f"/datagovernance/catalog/dataProducts/{product_id(product)}") in api.calls
    assert api.related[product_id(product)] == {"asset-guid-orders"}
    assert [e.message for e in result.errors] == [
        "pg.public.customers is not in the Purview Data Map, so it cannot be an asset of sales_360"
    ]


async def test_a_product_with_no_governance_domain_of_its_name_is_reported(api, data_map):
    api.domains = {}
    result = await _export().publish(_snapshot())
    assert [e.message for e in result.errors] == ["Purview has no governance domain named 'sales'"]
    assert api.products == {}


async def test_an_owner_entra_does_not_know_is_reported(api, data_map):
    result = await _export().publish(_snapshot(owners=("owner@example.com", "gone@example.com")))
    assert [e.message for e in result.errors] == [
        "owner gone@example.com is not a user of the Entra tenant"
    ]
    (body,) = api.products.values()
    assert [o["id"] for o in body["contacts"]["owner"]] == ["oid-owner"]


# --- The Unified Catalog business glossary (REQ-1387, REQ-1389) -----------------------------


def _term(term_id: int, name: str, **extra: Any) -> dict:
    return {
        "id": term_id,
        "name": name,
        "definition": f"what {name} means",
        "is_abstract": False,
        "deprecated": False,
        "deprecation_asserted": False,
        "status_reason": None,
        "retire_on": None,
        **extra,
    }


def _glossary(*, edges=None, domains=None) -> dict:
    """order: a Class grounded in orders; party: a Concept above it; client: a synonym of it."""
    return {
        "terms": [
            _term(1, "order"),
            _term(2, "party", is_abstract=True),
            _term(3, "client", is_abstract=True),
        ],
        "refs": [],
        "table_refs": [
            {"term_id": 1, "source_id": "pg", "schema_name": "public", "table_name": "orders"}
        ],
        "edges": edges
        if edges is not None
        else [
            {"from_term_id": 1, "to_term_id": 2, "rel_type": "KIND_OF"},
            {"from_term_id": 3, "to_term_id": 1, "rel_type": "SYNONYM_OF"},
        ],
        "experts": [],
        "domains": domains
        if domains is not None
        else [{"term_id": t, "domain_id": "sales"} for t in (1, 2, 3)],
    }


def _ids(snapshot) -> dict[str, str]:
    return {t.name: purview.term_id(t) for t in snapshot.glossary_terms}


def _relations(api, tid: str, category: str) -> set[tuple[str, str]]:
    return {
        (r["entityId"], r["relationshipType"])
        for r in api.term_relations.get(tid, [])
        if r["category"] == category
    }


async def test_terms_land_in_their_domain_with_parent_status_and_relationships(api, data_map):
    snapshot = _snapshot(glossary=_glossary())
    # REQ-2144: a curator deprecated client; it is still in service, and EXPIRED in Purview.
    snapshot.glossary_terms = [
        replace(t, status="deprecated") if t.name == "client" else t
        for t in snapshot.glossary_terms
    ]
    result = await _export().publish(snapshot)
    assert result.ok, result.errors
    ids = _ids(snapshot)
    order, party, client = (api.terms[ids[n]] for n in ("order", "party", "client"))
    assert {t["domain"] for t in (order, party, client)} == {DOMAIN_ID}
    # KIND_OF is the hierarchy; the parent is created first.
    assert order["parentId"] == ids["party"] and "parentId" not in party
    assert (party["status"], client["status"]) == ("PUBLISHED", "EXPIRED")
    assert _relations(api, ids["client"], "TERM") == {(ids["order"], "Synonym")}
    # REQ-1389: the Class term is related to the data asset of the table it grounds.
    assert _relations(api, ids["order"], "DATAASSET") == {("asset-guid-orders", "Related")}
    assert _relations(api, ids["party"], "DATAASSET") == set()
    assert result.published["glossary_term"] == 3 and result.published["term_assignment"] == 1


async def test_a_republish_keeps_a_stewards_curation_and_adds_nothing(api, data_map):
    await _export().publish(_snapshot(glossary=_glossary()))
    ids = _ids(_snapshot(glossary=_glossary()))
    api.terms[ids["order"]]["acronyms"] = ["ORD"]
    before = {tid: list(r) for tid, r in api.term_relations.items()}
    result = await _export().publish(_snapshot(glossary=_glossary()))
    assert result.ok, result.errors
    assert api.terms[ids["order"]]["acronyms"] == ["ORD"]
    assert api.term_relations == before


async def test_a_term_whose_read_failed_is_refused_and_the_others_publish(api, data_map):
    """REQ-1389: written blind, the term would lose what a steward keeps on it; it is refused by
    name and the rest of the glossary publishes."""
    await _export().publish(_snapshot(glossary=_glossary()))
    ids = _ids(_snapshot(glossary=_glossary()))
    api.terms[ids["party"]]["acronyms"] = ["PTY"]
    api.unreadable.add(ids["party"])
    api.calls.clear()
    result = await _export().publish(_snapshot(glossary=_glossary()))
    refused = [e for e in result.errors if "not written" in e.message]
    assert [e.message.split(":")[0] for e in refused] == ["term 'party'"]
    assert "HTTP 503" in refused[0].message
    puts = {p.rsplit("/", 1)[-1] for m, p in api.calls if m == "PUT" and "/terms/" in p}
    assert ids["party"] not in puts
    assert {ids["order"], ids["client"]} <= puts
    assert api.terms[ids["party"]]["acronyms"] == ["PTY"]
    assert result.published["glossary_term"] == 2


async def test_a_stewards_relationships_stand_and_provisas_stale_ones_leave(api, data_map):
    """systemData.createdBy decides: a relationship another principal created is a person's.
    Provisa's own that the snapshot no longer carries is removed, and a term that left the
    snapshot loses Provisa's relationships but keeps its entry."""
    await _export().publish(_snapshot(glossary=_glossary()))
    ids = _ids(_snapshot(glossary=_glossary()))
    steward = {"createdBy": "oid-steward"}
    api.term_relations[ids["order"]].append(
        {
            "entityId": "asset-x",
            "relationshipType": "Related",
            "category": "DATAASSET",
            "systemData": steward,
        }
    )
    gone = {**_glossary(), "terms": _glossary()["terms"][:2], "edges": []}
    gone["domains"] = [d for d in gone["domains"] if d["term_id"] != 3]
    result = await _export().publish(_snapshot(glossary=gone))
    assert result.ok, result.errors
    assert ("asset-x", "Related") in _relations(api, ids["order"], "DATAASSET")
    assert _relations(api, ids["client"], "TERM") == set()
    assert ids["client"] in api.terms  # never deleted
    assert "parentId" not in api.terms[ids["order"]]  # the KIND_OF edge left


async def test_a_term_whose_domains_purview_lacks_is_reported(api, data_map):
    glossary = _glossary(
        domains=[
            {"term_id": 1, "domain_id": "sales"},
            {"term_id": 2, "domain_id": "ops"},
            {"term_id": 3, "domain_id": "sales"},
        ]
    )
    result = await _export().publish(_snapshot(glossary=glossary))
    assert [e.message for e in result.errors] == [
        "term 'party': Purview has no governance domain named 'ops'"
    ]
    assert "parentId" not in api.terms[_ids(_snapshot(glossary=glossary))["order"]]
