# Copyright (c) 2026 Kenneth Stott
# Canary: 3b9e5a71-c204-4d8f-9e16-a0c7d2f58b43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2247: a CatalogReader lists its catalog's whole business glossary, every page read.

The catalog's HTTP API is mocked at the client: OpenMetadata pages by the ``paging.after``
cursor, DataHub by GraphQL scroll ids. Each term comes back with its name, its definition and its
broader terms (OpenMetadata's parent term, DataHub's IsA terms).
"""

# Requirements: REQ-2247

from __future__ import annotations

import httpx
import pytest

from provisa.api.catalog_import import (
    CatalogReader,
    CatalogTerm,
    GlossaryListingUnsupported,
    catalog_reader,
)
from provisa.api.metadata_export.datahub import DataHubExport
from provisa.api.metadata_export.openmetadata import OpenMetadataExport
from provisa.core.models import MetadataExportTarget


def _target(provider: str) -> MetadataExportTarget:
    return MetadataExportTarget(
        id="cat", provider=provider, endpoint="https://catalog.example", timeout_seconds=5
    )


def _response(method: str, url: str, body: dict) -> httpx.Response:
    return httpx.Response(200, json=body, request=httpx.Request(method, url))


OM_PAGES = {
    None: {
        "data": [
            {"id": "om-1", "name": "Customer", "description": "A buyer.", "parent": None},
            {
                "id": "om-2",
                "name": "Enterprise Customer",
                "description": "  A large buyer. ",
                "parent": {"id": "om-1", "name": "Customer", "type": "glossaryTerm"},
            },
        ],
        "paging": {"after": "cursor-2", "total": 3},
    },
    "cursor-2": {
        "data": [{"id": "om-3", "name": "Region", "description": ""}],
        "paging": {"total": 3},
    },
}


@pytest.mark.asyncio
async def test_openmetadata_lists_every_page_with_definition_and_parent(monkeypatch):
    calls: list[dict] = []

    async def _get(self, url, params=None, headers=None, **kwargs):
        calls.append({"url": str(url), "params": dict(params or {})})
        return _response("GET", str(url), OM_PAGES[(params or {}).get("after")])

    monkeypatch.setattr(httpx.AsyncClient, "get", _get)
    terms = await OpenMetadataExport(_target("openmetadata")).list_glossary()

    assert terms == [
        CatalogTerm("Customer", "A buyer.", (), "om-1"),
        CatalogTerm("Enterprise Customer", "A large buyer.", ("Customer",), "om-2"),
        CatalogTerm("Region", None, (), "om-3"),
    ]
    assert [c["url"] for c in calls] == ["https://catalog.example/api/v1/glossaryTerms"] * 2
    assert [c["params"].get("after") for c in calls] == [None, "cursor-2"]


def _dh_term(urn: str, name: str, definition: str, isa: list[tuple[str, str]], total=None):
    return {
        "entity": {
            "urn": urn,
            "properties": {"name": name, "definition": definition},
            "isA": {
                "total": len(isa) if total is None else total,
                "relationships": [
                    {"entity": {"urn": u, "properties": {"name": n}}} for u, n in isa
                ],
            },
        }
    }


DH_PAGES = {
    None: {
        "nextScrollId": "scroll-2",
        "searchResults": [
            _dh_term("urn:li:glossaryTerm:customer", "Customer", "A buyer.", []),
            _dh_term(
                "urn:li:glossaryTerm:ent",
                "Enterprise Customer",
                "A large buyer.",
                [("urn:li:glossaryTerm:customer", "Customer")],
            ),
        ],
    },
    "scroll-2": {
        "nextScrollId": None,
        "searchResults": [_dh_term("urn:li:glossaryTerm:region", "Region", "", [])],
    },
}


@pytest.mark.asyncio
async def test_datahub_scrolls_every_page_with_definition_and_isa(monkeypatch):
    calls: list[dict] = []

    async def _post(self, url, json=None, headers=None, **kwargs):
        calls.append({"url": str(url), "body": json})
        scroll = json["variables"]["input"].get("scrollId")
        return _response("POST", str(url), {"data": {"scrollAcrossEntities": DH_PAGES[scroll]}})

    monkeypatch.setattr(httpx.AsyncClient, "post", _post)
    terms = await DataHubExport(_target("datahub")).list_glossary()

    assert terms == [
        CatalogTerm("Customer", "A buyer.", (), "urn:li:glossaryTerm:customer"),
        CatalogTerm(
            "Enterprise Customer", "A large buyer.", ("Customer",), "urn:li:glossaryTerm:ent"
        ),
        CatalogTerm("Region", None, (), "urn:li:glossaryTerm:region"),
    ]
    assert {c["url"] for c in calls} == {"https://catalog.example/api/graphql"}
    inputs = [c["body"]["variables"]["input"] for c in calls]
    assert [i.get("scrollId") for i in inputs] == [None, "scroll-2"]
    assert all(i["types"] == ["GLOSSARY_TERM"] for i in inputs)


@pytest.mark.asyncio
async def test_datahub_graphql_error_is_raised_not_read_as_empty(monkeypatch):
    async def _post(self, url, json=None, headers=None, **kwargs):
        return _response("POST", str(url), {"errors": [{"message": "Unauthorized"}], "data": None})

    monkeypatch.setattr(httpx.AsyncClient, "post", _post)
    with pytest.raises(RuntimeError, match="Unauthorized"):
        await DataHubExport(_target("datahub")).list_glossary()


@pytest.mark.asyncio
async def test_datahub_term_with_more_isa_terms_than_read_is_refused(monkeypatch):
    page = {
        "nextScrollId": None,
        "searchResults": [
            _dh_term("urn:li:glossaryTerm:x", "X", "d", [("urn:li:glossaryTerm:y", "Y")], 5000)
        ],
    }

    async def _post(self, url, json=None, headers=None, **kwargs):
        return _response("POST", str(url), {"data": {"scrollAcrossEntities": page}})

    monkeypatch.setattr(httpx.AsyncClient, "post", _post)
    with pytest.raises(RuntimeError, match="5000 IsA terms"):
        await DataHubExport(_target("datahub")).list_glossary()


@pytest.mark.asyncio
async def test_openmetadata_http_failure_is_raised(monkeypatch):
    async def _get(self, url, params=None, headers=None, **kwargs):
        return httpx.Response(401, json={}, request=httpx.Request("GET", str(url)))

    monkeypatch.setattr(httpx.AsyncClient, "get", _get)
    with pytest.raises(httpx.HTTPStatusError):
        await OpenMetadataExport(_target("openmetadata")).list_glossary()


def test_a_provider_without_a_glossary_listing_refuses_by_name():
    with pytest.raises(GlossaryListingUnsupported, match="'atlas'"):
        catalog_reader(_target("atlas"))


@pytest.mark.parametrize(
    ("provider", "cls"), [("openmetadata", OpenMetadataExport), ("datahub", DataHubExport)]
)
def test_catalog_reader_resolves_the_reading_adapters(provider, cls):
    reader = catalog_reader(_target(provider))
    assert isinstance(reader, cls)
    assert isinstance(reader, CatalogReader)
