# Copyright (c) 2026 Kenneth Stott
# Canary: 126500e1-a5e7-4084-aef3-cf4bb6a16383
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2118: the Dataplex target, against a stand-in Dataplex API.

A data product becomes one Dataplex data product -- its fields, its owners (the owner role's
holders, by address) and its members as BigQuery assets: a BigQuery source's table in its own
project, a replica's export view in the engine's project. A member with no BigQuery object, and a
product whose owner role has no holder with an address, are reported by name."""

# Requirements: REQ-2118, REQ-1387, REQ-1389, REQ-2141

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from provisa.api.metadata_export import dataplex
from provisa.api.metadata_export.builder import build_snapshot
from provisa.api.metadata_export.dataplex import DataplexExport, member_resource, resource_id
from provisa.api.metadata_export.registry import metadata_export, registered_providers
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

PARENT = "projects/proj-x/locations/us"
PRODUCT = f"{PARENT}/dataProducts/sales-360"
EXPORT_VIEW = ("org_acme__export", "pg__public__orders")


def _target(**extra: Any) -> MetadataExportTarget:
    return MetadataExportTarget(
        id="dataplex",
        provider="dataplex",
        endpoint="https://dataplex.example",
        auth_mode="bearer",
        token="t0k",
        gcp_project="proj-x",
        gcp_location="us",
        timeout_seconds=5,
        **extra,
    )


def _table(source: str, name: str, **extra: Any) -> Table:
    return Table(
        source_id=source,
        domain_id="sales",
        schema_name="public" if source == "pg" else "warehouse",
        table_name=name,
        product_id="sales_360",
        columns=[Column(name="id", data_type="integer", visible_to=["admin"])],
        **extra,
    )


def _snapshot(*, owners: tuple[str, ...] = ("owner@example.com",), tables: list | None = None):
    config = ProvisaConfig(
        sources=[
            Source(id="bq", type=SourceType.bigquery, database="proj-src", description="BQ"),
            Source(
                id="pg",
                type=SourceType.postgresql,
                host="h",
                port=5432,
                database="d",
                username="u",
                password="p",
            ),
        ],
        domains=[Domain(id="sales", description="Sales")],
        tables=tables
        if tables is not None
        else [_table("bq", "sales"), _table("pg", "orders", cache_ttl=3600)],
        data_products=[
            DataProduct(
                id="sales_360",
                domain_id="sales",
                name="Sales 360",
                owner_role="data_owner",
                purpose="Every sale",
                limitations="No refunds",
                version="1.2.0",
                status="live",
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
    )


class _Dataplex:
    """A stand-in Dataplex API: data products and their assets, every change a finished
    long-running operation."""

    def __init__(self, products: dict | None = None, assets: dict | None = None) -> None:
        self.products: dict[str, dict] = dict(products or {})
        self.assets: dict[str, dict] = dict(assets or {})
        self.calls: list[tuple[str, str, dict, dict]] = []

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/v1/")
        params = dict(request.url.params)
        body = json.loads(request.content) if request.content else {}
        self.calls.append((request.method, path, params, body))
        assert request.headers["Authorization"] == "Bearer t0k"
        done = {"name": "operations/op-1", "done": True}
        if request.method == "GET" and path.endswith("/dataAssets"):
            return httpx.Response(200, json={"dataAssets": list(self.assets.values())})
        if request.method == "GET":
            if path not in self.products:
                return httpx.Response(404, json={"error": {"message": "not found"}})
            return httpx.Response(200, json=self.products[path])
        if request.method == "POST" and path.endswith("/dataProducts"):
            self.products[f"{path}/{params['dataProductId']}"] = body
            return httpx.Response(200, json=done)
        if request.method == "POST" and path.endswith("/dataAssets"):
            name = f"{path}/{params['dataAssetId']}"
            self.assets[name] = {**body, "name": name}
            return httpx.Response(200, json=done)
        if request.method == "PATCH":
            store = self.products if path in self.products else self.assets
            store[path] = {**store[path], **body}
            return httpx.Response(200, json=done)
        if request.method == "DELETE":
            self.assets.pop(path)
            return httpx.Response(200, json=done)
        raise AssertionError(f"unexpected {request.method} {path}")


@pytest.fixture
def api(monkeypatch):
    fake = _Dataplex()
    real = httpx.AsyncClient
    monkeypatch.setattr(
        dataplex.httpx,
        "AsyncClient",
        lambda **kw: real(transport=httpx.MockTransport(fake.handle), **kw),
    )
    monkeypatch.setattr(dataplex, "bigquery_engine_project", lambda: "proj-x")
    return fake


def _export() -> DataplexExport:
    exporter = metadata_export(_target())
    assert isinstance(exporter, DataplexExport)
    exporter.export_views = {("pg", "public", "orders"): EXPORT_VIEW}
    return exporter


def test_dataplex_is_resolvable_by_name_and_bigquery_dataplex_is_gone():
    assert "dataplex" in registered_providers()
    assert "bigquery_dataplex" not in registered_providers()


def test_a_dataplex_target_needs_its_project_and_location():
    with pytest.raises(ValidationError, match="GCP project and location"):
        MetadataExportTarget(id="d", provider="dataplex", endpoint="https://x", gcp_project="p")


def test_ids_are_rfc_1034():
    assert resource_id("sales_360") == "sales-360"
    assert resource_id("360_sales") == "p-360-sales"
    long = resource_id("x" * 80)
    assert len(long) == 63 and long.startswith("x")


async def test_a_new_product_is_created_with_its_fields_owners_and_assets(api):
    result = await _export().publish(_snapshot())
    assert result.ok, result.errors
    assert result.published == {"data_products": 1, "data_assets": 2}
    product = api.products[PRODUCT]
    assert product["displayName"] == "Sales 360"
    assert product["ownerEmails"] == ["owner@example.com"]
    assert product["accessApprovalConfig"] == {"approverEmails": ["owner@example.com"]}
    assert (
        "Every sale" in product["description"]
        and "Limitations: No refunds" in (product["description"])
    )
    assert product["labels"] == {
        "provisa-product": "sales_360",
        "provisa-version": "1_2_0",
        "provisa-status": "live",
    }
    by_resource = {a["resource"]: a for a in api.assets.values()}
    # A BigQuery source's table, in its own project; a replica's export view, in the engine's.
    assert set(by_resource) == {
        "//bigquery.googleapis.com/projects/proj-src/datasets/warehouse/tables/sales",
        "//bigquery.googleapis.com/projects/proj-x/datasets/org_acme__export/tables/"
        "pg__public__orders",
    }
    orders = by_resource[
        "//bigquery.googleapis.com/projects/proj-x/datasets/org_acme__export/tables/"
        "pg__public__orders"
    ]
    # The member's refresh cadence, from its freshness.
    assert orders["labels"]["provisa-freshness-seconds"] == "3600"


async def test_an_existing_product_is_updated_and_a_former_member_removed(api):
    stale = f"{PRODUCT}/dataAssets/old-table"
    api.products[PRODUCT] = {"displayName": "Old", "etag": "e1"}
    api.assets[stale] = {"name": stale, "resource": "//bigquery.googleapis.com/projects/p/x"}
    result = await _export().publish(_snapshot())
    assert result.ok, result.errors
    patch = next(c for c in api.calls if c[0] == "PATCH" and c[1] == PRODUCT)
    assert set(patch[2]["updateMask"].split(",")) == {
        "displayName",
        "description",
        "ownerEmails",
        "labels",
        "accessApprovalConfig",
    }
    assert patch[3]["etag"] == "e1"
    assert api.products[PRODUCT]["displayName"] == "Sales 360"
    assert stale not in api.assets and len(api.assets) == 2


async def test_a_member_with_no_bigquery_object_is_reported_by_name(api):
    snapshot = _snapshot(tables=[_table("bq", "sales"), _table("pg", "customers")])
    result = await _export().publish(snapshot)
    assert [e.message for e in result.errors] == [
        "pg.public.customers has no BigQuery table or view to publish as an asset of sales_360"
    ]
    assert result.published == {"data_products": 1, "data_assets": 1}


async def test_a_product_whose_owner_role_has_no_address_is_reported_and_not_sent(api):
    result = await _export().publish(_snapshot(owners=()))
    assert len(result.errors) == 1 and "has no holder with an email address" in (
        result.errors[0].message
    )
    assert api.calls == []


def test_a_view_member_has_no_bigquery_object():
    view = _table("pg", "totals", view_sql="SELECT 1 AS id")
    view = view.model_copy(update={"source_id": "__derived__", "schema_name": "views"})
    snapshot = _snapshot(tables=[_table("bq", "sales"), view])
    (product,) = snapshot.data_products
    members = {m.parts[-1]: m for m in product.members}
    kwargs = {
        "sources": {s.id: s for s in snapshot.sources},
        "tables": {t.ref.fqn(): t for t in snapshot.tables},
        "engine_project": "proj-x",
        "export_views": {},
    }
    assert member_resource(members["totals"], **kwargs) is None
    assert member_resource(members["sales"], **kwargs) == (
        "//bigquery.googleapis.com/projects/proj-src/datasets/warehouse/tables/sales"
    )


def test_the_snapshot_carries_each_products_owner_addresses():
    (product,) = _snapshot(owners=("a@example.com", "b@example.com")).data_products
    assert product.owner_emails == ("a@example.com", "b@example.com")


# --- The Dataplex business glossary (REQ-1387, REQ-1389, REQ-2141) ----------------------------

GLOSSARY = f"{PARENT}/glossaries/provisa-acme"
NUMBERED = "projects/123/locations/us"  # a term's catalog entry names the project by number


def _entry(term_id: str) -> str:
    return f"{NUMBERED}/entryGroups/@dataplex/entries/{NUMBERED}/glossaries/provisa-acme/terms/{term_id}"


SALES_ENTRY = f"{PARENT}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/proj-src/datasets/warehouse/tables/sales"


def _glossary(edges=None) -> dict:
    """sale: a Class on bq sales; sale id: a Property on its id column; deal: a Concept, a
    synonym of sale."""
    terms = [
        {
            "id": 1,
            "name": "sale",
            "definition": "A sale.",
            "is_abstract": False,
            "deprecated": False,
            "deprecation_asserted": False,
            "status_reason": None,
            "retire_on": None,
        },
        {
            "id": 2,
            "name": "sale id",
            "definition": "Names a sale.",
            "is_abstract": False,
            "deprecated": False,
            "deprecation_asserted": False,
            "status_reason": None,
            "retire_on": None,
        },
        {
            "id": 3,
            "name": "deal",
            "definition": "A sale, in the field.",
            "is_abstract": True,
            "deprecated": False,
            "deprecation_asserted": False,
            "status_reason": None,
            "retire_on": None,
        },
    ]
    return {
        "terms": terms,
        "refs": [
            {
                "term_id": 2,
                "column_name": "id",
                "source_id": "bq",
                "schema_name": "warehouse",
                "table_name": "sales",
            }
        ],
        "table_refs": [
            {"term_id": 1, "source_id": "bq", "schema_name": "warehouse", "table_name": "sales"},
        ],
        "edges": [{"from_term_id": 3, "to_term_id": 1, "rel_type": "SYNONYM_OF"}]
        if edges is None
        else edges,
        "experts": [],
        "domains": [],
    }


def _glossary_snapshot(glossary: dict):
    config_tables = [_table("bq", "sales")]
    built = build_snapshot(
        ProvisaConfig(
            sources=[
                Source(id="bq", type=SourceType.bigquery, database="proj-src", description="BQ")
            ],
            domains=[Domain(id="sales", description="Sales")],
            tables=config_tables,
            roles=[],
        ),
        org_id="acme",
        dialect="postgres",
        contexts={},
        glossary=glossary,
    )
    built.data_products = []
    return built


class _GlossaryApi:
    """A stand-in for Dataplex's glossary surface: glossaries (a long-running create),
    categories, terms, entry links and their lookup."""

    def __init__(self) -> None:
        self.resources: dict[str, dict] = {}
        self.links: dict[str, dict] = {}
        self.calls: list[tuple[str, str]] = []
        # The BigQuery entries Dataplex has catalogued.
        self.entries: set[str] = {SALES_ENTRY}

    def handle(self, request: httpx.Request) -> httpx.Response:
        if request.url.host == "cloudresourcemanager.googleapis.com":
            self.calls.append((request.method, request.url.path))
            assert request.url.path == "/v3/projects/proj-x", request.url.path
            return httpx.Response(200, json={"name": "projects/123", "projectId": "proj-x"})
        path = request.url.path.removeprefix("/v1/")
        params = dict(request.url.params)
        body = json.loads(request.content) if request.content else {}
        self.calls.append((request.method, path))
        if path.endswith(":lookupEntry"):
            # A term's catalog entry exists once the term does.
            terms = {
                _entry(n.rsplit("/", 1)[-1])
                for n in self.resources
                if n.startswith(f"{GLOSSARY}/terms/")
            }
            if params["entry"] in self.entries | terms:
                return httpx.Response(200, json={"name": params["entry"]})
            return httpx.Response(404, json={"error": {"message": "not found"}})
        if path.endswith(":lookupEntryLinks"):
            entry = params["entry"]
            found = [
                link
                for link in self.links.values()
                if any(r["name"] == entry for r in link["entryReferences"])
            ]
            return httpx.Response(200, json={"entryLinks": found})
        if request.method == "GET" and path == f"{GLOSSARY}/terms":
            terms = [r for n, r in self.resources.items() if n.startswith(f"{GLOSSARY}/terms/")]
            return httpx.Response(200, json={"terms": terms})
        if request.method == "GET":
            if path not in self.resources:
                return httpx.Response(404, json={"error": {"message": "not found"}})
            return httpx.Response(200, json=self.resources[path])
        if request.method == "POST" and path.endswith("/entryLinks"):
            name = f"{path}/{params['entry_link_id']}"
            self.links[name] = {**body, "name": name}
            return httpx.Response(200, json=self.links[name])
        if request.method == "POST":
            (id_param,) = params
            name = f"{path}/{params[id_param]}"
            self.resources[name] = {**body, "name": name}
            if path.endswith("/glossaries"):
                return httpx.Response(
                    200,
                    json={
                        "name": f"{PARENT}/operations/g",
                        "done": True,
                        "response": self.resources[name],
                    },
                )
            return httpx.Response(200, json=self.resources[name])
        if request.method == "PATCH":
            fields = params["updateMask"].split(",")
            self.resources[path] = {**self.resources[path], **{f: body[f] for f in fields}}
            return httpx.Response(200, json=self.resources[path])
        if request.method == "DELETE":
            self.links.pop(path)
            return httpx.Response(200, json={})
        raise AssertionError(f"unexpected {request.method} {path}")


@pytest.fixture
def glossary_api(monkeypatch):
    fake = _GlossaryApi()
    real = httpx.AsyncClient
    monkeypatch.setattr(
        dataplex.httpx,
        "AsyncClient",
        lambda **kw: real(transport=httpx.MockTransport(fake.handle), **kw),
    )
    monkeypatch.setattr(dataplex, "bigquery_engine_project", lambda: None)
    return fake


def _link_kinds(fake) -> set[tuple[str, tuple[tuple[str, str, str], ...]]]:
    return {
        (
            link["entryLinkType"].rsplit("/", 1)[-1],
            tuple((r["name"], r.get("path", ""), r["type"]) for r in link["entryReferences"]),
        )
        for link in fake.links.values()
    }


async def test_a_term_is_linked_once_its_catalog_entry_appears(glossary_api, monkeypatch):
    """Dataplex catalogs a new term's entry after the term, answering 403 "(or it may not exist)"
    until it does; the links wait for it rather than failing the glossary."""
    fake = glossary_api
    handle = fake.handle
    refused: list[str] = []

    def _late(request: httpx.Request) -> httpx.Response:
        entry = dict(request.url.params).get("entry", "")
        if "/glossaries/" in entry and entry not in refused:
            refused.append(entry)
            return httpx.Response(403, json={"error": {"message": "may not exist"}})
        return handle(request)

    fake.handle = _late
    real_sleep = asyncio.sleep
    monkeypatch.setattr(dataplex.asyncio, "sleep", lambda _s: real_sleep(0))
    result = await _export().publish(_glossary_snapshot(_glossary()))
    assert result.ok, result.errors
    assert refused  # each term entry was refused once before it appeared
    assert ("definition", ((SALES_ENTRY, "", "SOURCE"), (_entry("sale"), "", "TARGET"))) in (
        _link_kinds(fake)
    )


async def test_a_term_whose_entry_never_appears_is_reported_not_linked(glossary_api, monkeypatch):
    fake = glossary_api
    handle = fake.handle

    def _never(request: httpx.Request) -> httpx.Response:
        if "/glossaries/" in dict(request.url.params).get("entry", ""):
            return httpx.Response(403, json={"error": {"message": "may not exist"}})
        return handle(request)

    fake.handle = _never
    monkeypatch.setattr(DataplexExport, "entry_wait_seconds", 0)
    result = await _export().publish(_glossary_snapshot(_glossary()))
    messages = [e.message for e in result.errors]
    assert any("'sale' is not linked: Dataplex had no catalog entry" in m for m in messages)
    assert fake.links == {}


async def test_the_glossary_its_role_categories_terms_and_links_are_created(glossary_api):
    result = await _export().publish(_glossary_snapshot(_glossary()))
    assert result.ok, result.errors
    fake = glossary_api
    assert fake.resources[GLOSSARY]["displayName"] == "Provisa Glossary"
    assert {
        n.rsplit("/", 1)[-1]: r["displayName"]
        for n, r in fake.resources.items()
        if "/categories/" in n
    } == {
        "class": "Classes",
        "property": "Properties",
        "concept": "Concepts",
    }
    sale = fake.resources[f"{GLOSSARY}/terms/sale"]
    assert sale["parent"] == f"{GLOSSARY}/categories/class"
    assert sale["labels"] == {"provisa-role": "class"}
    assert fake.resources[f"{GLOSSARY}/terms/sale-id"]["labels"] == {"provisa-role": "property"}
    assert _link_kinds(fake) == {
        ("synonym", ((_entry("deal"), "", "UNSPECIFIED"), (_entry("sale"), "", "UNSPECIFIED"))),
        ("definition", ((SALES_ENTRY, "", "SOURCE"), (_entry("sale"), "", "TARGET"))),
        ("definition", ((SALES_ENTRY, "Schema.id", "SOURCE"), (_entry("sale-id"), "", "TARGET"))),
    }
    assert all(name.rsplit("/", 1)[-1].startswith("provisa-") for name in fake.links)
    # A definition link lives in the asset's entry group; a term-to-term link in @dataplex.
    groups = {name.split("/entryLinks/")[0].rsplit("/", 1)[-1] for name in fake.links}
    assert groups == {"@bigquery", "@dataplex"}
    assert result.published["term_assignment"] == 2 and result.published["glossary_term"] == 3


async def test_a_stewards_links_and_labels_stand_and_provisas_stale_links_leave(glossary_api):
    await _export().publish(_glossary_snapshot(_glossary()))
    fake = glossary_api
    steward_link = f"{NUMBERED}/entryGroups/@dataplex/entryLinks/my-link"
    fake.links[steward_link] = {
        "name": steward_link,
        "entryLinkType": f"{dataplex.LINK_TYPES}/related",
        "entryReferences": [
            {"name": _entry("deal"), "type": "UNSPECIFIED"},
            {"name": _entry("sale"), "type": "UNSPECIFIED"},
        ],
    }
    fake.resources[f"{GLOSSARY}/terms/sale"]["labels"]["team"] = "finance"
    result = await _export().publish(_glossary_snapshot(_glossary(edges=[])))
    assert result.ok, result.errors
    assert steward_link in fake.links
    kinds = {kind for kind, _ in _link_kinds(fake)}
    assert "synonym" not in kinds
    assert fake.resources[f"{GLOSSARY}/terms/sale"]["labels"] == {
        "team": "finance",
        "provisa-role": "class",
    }
    # Only Provisa's synonym link was deleted.
    deleted = [path for method, path in fake.calls if method == "DELETE"]
    assert len(deleted) == 1 and "/entryLinks/provisa-" in deleted[0]


async def test_a_table_dataplex_has_not_catalogued_yet_is_reported_by_name(
    glossary_api, monkeypatch
):
    """The definition link waits, bounded, for the table's BigQuery entry; when it never
    appears the table is reported by name and everything else still publishes."""
    glossary_api.entries.clear()
    monkeypatch.setattr(DataplexExport, "entry_wait_seconds", 0)
    result = await _export().publish(_glossary_snapshot(_glossary()))
    assert [e.message for e in result.errors] == [
        "bigquery.googleapis.com/projects/proj-src/datasets/warehouse/tables/sales is not linked "
        "to its term: Dataplex had no entry for it after 0 s"
    ]
    assert {kind for kind, _ in _link_kinds(glossary_api)} == {"synonym"}
    assert result.published["glossary_term"] == 3


async def test_a_term_whose_read_failed_is_refused_and_the_others_publish(glossary_api):
    """REQ-1389: written blind, the term would lose a steward's labels; it is refused by name
    and the rest of the glossary publishes."""
    fake = glossary_api
    await _export().publish(_glossary_snapshot(_glossary()))
    fake.resources[f"{GLOSSARY}/terms/sale"]["labels"]["team"] = "finance"
    handle = fake.handle

    def _unreadable_sale(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path.endswith(f"{GLOSSARY}/terms/sale"):
            return httpx.Response(503, json={"error": {"message": "unavailable"}})
        return handle(request)

    fake.handle = _unreadable_sale
    fake.calls.clear()
    result = await _export().publish(_glossary_snapshot(_glossary()))
    refused = [e for e in result.errors if "not written" in e.message]
    assert [e.message.split(":")[0] for e in refused] == ["term 'sale'"]
    assert "HTTP 503" in refused[0].message
    patched = {path for method, path in fake.calls if method == "PATCH"}
    assert f"{GLOSSARY}/terms/sale" not in patched
    assert f"{GLOSSARY}/terms/deal" in patched
    assert fake.resources[f"{GLOSSARY}/terms/sale"]["labels"]["team"] == "finance"
