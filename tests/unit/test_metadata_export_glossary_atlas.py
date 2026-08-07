# Copyright (c) 2026 Kenneth Stott
# Canary: 9f2c47d1-8a3e-4b60-9e15-7c1d84f0a2b6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Native Atlas business-glossary publishing (REQ-1387).

Terms publish into the Provisa-OWNED glossary container only, created idempotently.
Ownership is the binding (REQ-1389): a stored guid updates a term in place through a rename,
glossary objects without a Provisa binding are never touched, term-to-entity assignments are
never written (steward-owned), and nothing is ever deleted. Transport is monkeypatched at
the httpx.AsyncClient method level, the same approach as the DataHub merge tests in
``test_metadata_export_vendors.py``.
"""

# Requirements: REQ-1387, REQ-1389

from __future__ import annotations

import httpx
import pytest

from provisa.api.metadata_export.atlas import (
    DEPRECATION_MARKER,
    GLOSSARY_NAME,
    AtlasExport,
    _glossary_qn,
    _glossary_uri,
)
from provisa.api.metadata_export.model import (
    AssetKind,
    AssetRef,
    GlossaryTermAsset,
    GlossaryTermEdge,
    MetadataSnapshot,
)
from provisa.core.models import MetadataExportConfig

ORG = "acme"
GLOSSARY_QN = _glossary_qn(ORG)
GLOSSARY_URI = _glossary_uri(ORG)
FOREIGN_GLOSSARY_GUID = "foreign-glossary-guid"
FOREIGN_TERM_GUID = "foreign-term-guid"


def _export() -> AtlasExport:
    return AtlasExport(
        MetadataExportConfig(
            enabled=True,
            provider="atlas",
            endpoint="http://atlas:21000",
            token="t",
            timeout_seconds=5,
        )
    )


def _term(term_id: int, name: str, **kwargs) -> GlossaryTermAsset:
    base = dict(
        term_id=term_id,
        name=name,
        definition=f"What {name} means.",
        is_abstract=True,
        deprecated=False,
        refs=(),
        experts=(),
        semantic_uri=f"provisa://{ORG}/terms/{name}",
    )
    base.update(kwargs)
    return GlossaryTermAsset(**base)


def _snapshot(terms, edges=()) -> MetadataSnapshot:
    return MetadataSnapshot(org_id=ORG, glossary_terms=list(terms), glossary_edges=list(edges))


class _FakeAtlas:
    """The glossary surface of one Atlas server: routes in, recorded writes out."""

    def __init__(self, glossaries=None, terms=None):
        self.glossaries = list(glossaries or [])
        self.terms = dict(terms or {})  # guid -> live term body
        self.requests: list[tuple[str, str]] = []
        self.glossary_creates: list[dict] = []
        self.term_creates: list[dict] = []
        self.term_puts: list[tuple[str, dict]] = []
        self._next: dict[str, int] = {}

    def _guid(self, prefix: str) -> str:
        self._next[prefix] = self._next.get(prefix, 0) + 1
        return f"{prefix}-{self._next[prefix]}"

    def install(self, monkeypatch) -> None:
        fake = self

        async def _get(self, url, headers=None, params=None):
            fake.requests.append(("GET", url))
            request = httpx.Request("GET", url)
            if url.endswith("/api/atlas/v2/glossary"):
                return httpx.Response(200, json=fake.glossaries, request=request)
            guid = url.rsplit("/", 1)[-1]
            if "/glossary/term/" in url and guid in fake.terms:
                return httpx.Response(200, json=fake.terms[guid], request=request)
            return httpx.Response(404, request=request)

        async def _post(self, url, json=None, headers=None):
            fake.requests.append(("POST", url))
            request = httpx.Request("POST", url)
            if url.endswith("/api/atlas/v2/glossary"):
                guid = fake._guid("glossary")
                fake.glossary_creates.append(json)
                fake.glossaries.append({**json, "guid": guid})
                return httpx.Response(200, json={**json, "guid": guid}, request=request)
            if url.endswith("/api/atlas/v2/glossary/term"):
                guid = fake._guid("term")
                fake.term_creates.append(json)
                fake.terms[guid] = {**json, "guid": guid}
                return httpx.Response(200, json={**json, "guid": guid}, request=request)
            return httpx.Response(404, request=request)

        async def _put(self, url, json=None, headers=None):
            fake.requests.append(("PUT", url))
            request = httpx.Request("PUT", url)
            guid = url.rsplit("/", 1)[-1]
            if "/glossary/term/" in url and guid in fake.terms:
                fake.term_puts.append((guid, json))
                fake.terms[guid] = dict(json)
                return httpx.Response(200, json=fake.terms[guid], request=request)
            return httpx.Response(404, request=request)

        async def _delete(self, url, headers=None):
            fake.requests.append(("DELETE", url))
            return httpx.Response(200, request=httpx.Request("DELETE", url))

        monkeypatch.setattr(httpx.AsyncClient, "get", _get)
        monkeypatch.setattr(httpx.AsyncClient, "post", _post)
        monkeypatch.setattr(httpx.AsyncClient, "put", _put)
        monkeypatch.setattr(httpx.AsyncClient, "delete", _delete)


@pytest.fixture
def fake(monkeypatch) -> _FakeAtlas:
    server = _FakeAtlas(
        glossaries=[
            # A glossary somebody else owns: listed by Atlas, never Provisa's to touch.
            {
                "guid": FOREIGN_GLOSSARY_GUID,
                "name": "Corporate Glossary",
                "qualifiedName": "corporate_glossary",
            }
        ],
        terms={
            FOREIGN_TERM_GUID: {
                "guid": FOREIGN_TERM_GUID,
                "name": "Churn",
                "qualifiedName": "Churn@corporate_glossary",
            }
        },
    )
    server.install(monkeypatch)
    return server


async def test_container_created_once_with_the_stable_per_org_qualified_name(fake):
    result = await _export().publish(_snapshot([_term(1, "Revenue")]))
    assert result.ok
    assert [g["qualifiedName"] for g in fake.glossary_creates] == [GLOSSARY_QN]
    assert fake.glossary_creates[0]["name"] == GLOSSARY_NAME
    assert result.published == {"glossary": 1, "glossary_term": 1}
    assert result.bindings[GLOSSARY_URI] == ("glossary-1", GLOSSARY_QN)


async def test_container_reused_from_the_listing_and_from_the_stored_binding(fake):
    first = await _export().publish(_snapshot([_term(1, "Revenue")]))
    # Listing path: a fresh exporter with no bindings matches the live container by its
    # stable qualifiedName instead of creating a second one.
    second = await _export().publish(_snapshot([_term(1, "Revenue")]))
    assert second.bindings[GLOSSARY_URI] == first.bindings[GLOSSARY_URI]
    # Stored-binding path: the guid is taken directly, without even listing.
    export = _export()
    export.stored_bindings = dict(second.bindings)
    fake.requests.clear()
    third = await export.publish(_snapshot([_term(1, "Revenue")]))
    assert third.bindings[GLOSSARY_URI] == first.bindings[GLOSSARY_URI]
    assert len(fake.glossary_creates) == 1
    assert ("GET", "http://atlas:21000/api/atlas/v2/glossary") not in fake.requests


async def test_term_create_carries_the_authored_fields_and_captures_the_binding(fake):
    term = _term(1, "Revenue", definition="Recognized income.")
    result = await _export().publish(_snapshot([term]))
    assert result.ok
    (created,) = fake.term_creates
    assert created["name"] == "Revenue"
    assert created["longDescription"] == "Recognized income."
    assert created["qualifiedName"] == f"Revenue@{GLOSSARY_QN}"
    assert created["anchor"] == {"glossaryGuid": "glossary-1"}
    assert result.bindings[term.semantic_uri] == ("term-1", f"Revenue@{GLOSSARY_QN}")


async def test_rename_updates_the_same_term_in_place_via_the_stored_binding(fake):
    fake.terms["term-live"] = {
        "guid": "term-live",
        "name": "Turnover",
        "qualifiedName": f"Turnover@{GLOSSARY_QN}",
        "longDescription": "Old definition.",
        # Steward-added enrichment Provisa never authors — the read-merge must keep it.
        "abbreviation": "REV",
    }
    export = _export()
    export.stored_bindings = {
        GLOSSARY_URI: ("glossary-live", GLOSSARY_QN),
        f"provisa://{ORG}/terms/Revenue": ("term-live", f"Turnover@{GLOSSARY_QN}"),
    }
    result = await export.publish(_snapshot([_term(1, "Revenue")]))
    assert result.ok
    assert fake.term_creates == []  # rename-safe: no duplicate under the new name
    ((guid, body),) = fake.term_puts
    assert guid == "term-live"
    assert body["name"] == "Revenue"
    assert body["qualifiedName"] == f"Revenue@{GLOSSARY_QN}"
    assert body["longDescription"] == "What Revenue means."
    assert body["abbreviation"] == "REV"  # steward field survives the update (REQ-1389)
    assert result.bindings[f"provisa://{ORG}/terms/Revenue"] == (
        "term-live",
        f"Revenue@{GLOSSARY_QN}",
    )


async def test_deprecation_is_the_short_description_prefix_and_only_the_prefix(fake):
    result = await _export().publish(_snapshot([_term(1, "Margin", deprecated=True)]))
    assert result.ok
    assert fake.term_creates[0]["shortDescription"] == DEPRECATION_MARKER
    # Un-deprecating strips OUR marker and keeps the steward's own text after it.
    fake.terms["term-live"] = {
        "guid": "term-live",
        "name": "Margin",
        "qualifiedName": f"Margin@{GLOSSARY_QN}",
        "shortDescription": f"{DEPRECATION_MARKER} steward note",
    }
    export = _export()
    export.stored_bindings = {
        GLOSSARY_URI: ("glossary-live", GLOSSARY_QN),
        f"provisa://{ORG}/terms/Margin": ("term-live", f"Margin@{GLOSSARY_QN}"),
    }
    result = await export.publish(_snapshot([_term(2, "Margin", deprecated=False)]))
    assert result.ok
    ((_, body),) = [(g, b) for g, b in fake.term_puts if g == "term-live"]
    assert body["shortDescription"] == "steward note"


async def test_glossary_objects_without_a_provisa_binding_are_never_touched(fake):
    result = await _export().publish(_snapshot([_term(1, "Revenue")]))
    assert result.ok
    for _, url in fake.requests:
        assert FOREIGN_GLOSSARY_GUID not in url
        assert FOREIGN_TERM_GUID not in url
    assert fake.terms[FOREIGN_TERM_GUID]["name"] == "Churn"


async def test_edges_map_onto_atlas_native_term_relations(fake):
    terms = [
        _term(1, "Revenue"),
        _term(2, "Income"),
        _term(3, "Turnover"),
        _term(4, "Margin"),
        _term(5, "Ledger"),
    ]
    edges = [
        GlossaryTermEdge(from_term_id=1, to_term_id=2, rel_type="KIND_OF"),
        GlossaryTermEdge(from_term_id=1, to_term_id=3, rel_type="SYNONYM_OF"),
        GlossaryTermEdge(from_term_id=1, to_term_id=4, rel_type="RELATED_TO"),
        GlossaryTermEdge(from_term_id=1, to_term_id=5, rel_type="PART_OF"),
    ]
    result = await _export().publish(_snapshot(terms, edges))
    assert result.ok
    by_uri = {u: g for u, (g, _) in result.bindings.items()}
    revenue = fake.terms[by_uri[f"provisa://{ORG}/terms/Revenue"]]
    assert revenue["isA"] == [{"termGuid": by_uri[f"provisa://{ORG}/terms/Income"]}]
    assert revenue["synonyms"] == [{"termGuid": by_uri[f"provisa://{ORG}/terms/Turnover"]}]
    # RELATED_TO and PART_OF both ride seeAlso — Atlas has no compositional term relation.
    assert revenue["seeAlso"] == [
        {"termGuid": by_uri[f"provisa://{ORG}/terms/Margin"]},
        {"termGuid": by_uri[f"provisa://{ORG}/terms/Ledger"]},
    ]


async def test_term_to_entity_assignments_are_never_written(fake):
    # REQ-1389: steward term assignments are HUMAN-OWNED. A term with published refs still
    # publishes as vocabulary only — no assignedEntities in any payload, no assignment route.
    term = _term(
        1,
        "Revenue",
        is_abstract=False,
        refs=(AssetRef(kind=AssetKind.COLUMN, parts=("wh", "public", "orders", "amount")),),
    )
    result = await _export().publish(_snapshot([term]))
    assert result.ok
    for body in fake.term_creates + [body for _, body in fake.term_puts]:
        assert "assignedEntities" not in body
    for _, url in fake.requests:
        assert "assignedEntities" not in url and "assignedTerm" not in url


async def test_a_term_leaving_the_snapshot_is_never_deleted(fake):
    fake.terms["term-live"] = {
        "guid": "term-live",
        "name": "Margin",
        "qualifiedName": f"Margin@{GLOSSARY_QN}",
    }
    export = _export()
    export.stored_bindings = {
        GLOSSARY_URI: ("glossary-live", GLOSSARY_QN),
        # A term a prior publish created, now gone from the snapshot.
        f"provisa://{ORG}/terms/Margin": ("term-live", f"Margin@{GLOSSARY_QN}"),
    }
    result = await export.publish(_snapshot([_term(1, "Revenue")]))
    assert result.ok
    assert [method for method, _ in fake.requests if method == "DELETE"] == []
    assert fake.terms["term-live"]["name"] == "Margin"  # untouched, bindings prune separately
