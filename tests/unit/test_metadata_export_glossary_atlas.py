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
glossary objects without a Provisa binding are never touched, Provisa writes and withdraws only
its own term-to-entity assignments (a steward's stand), and nothing is ever deleted. Transport is monkeypatched at
the httpx.AsyncClient method level, the same approach as the DataHub merge tests in
``test_metadata_export_vendors.py``.
"""

# Requirements: REQ-1387, REQ-1389

from __future__ import annotations

import httpx
import pytest

from provisa.api.metadata_export.atlas import (
    ASSIGNMENT_ATTRIBUTES,
    ASSIGNMENT_SOURCE,
    DEPRECATION_MARKER,
    SEMANTIC_ASSIGNMENT,
    GLOSSARY_NAME,
    AtlasExport,
    _glossary_qn,
    _glossary_uri,
)
from provisa.api.metadata_export.model import (
    AssetKind,
    AssetRef,
    ColumnAsset,
    GlossaryTermAsset,
    GlossaryTermEdge,
    MetadataSnapshot,
    SourceAsset,
    TableAsset,
)
from provisa.api.metadata_export.provider import PublishResult
from provisa.core.models import MetadataExportTarget

ORG = "acme"
GLOSSARY_QN = _glossary_qn(ORG)
GLOSSARY_URI = _glossary_uri(ORG)
FOREIGN_GLOSSARY_GUID = "foreign-glossary-guid"
FOREIGN_TERM_GUID = "foreign-term-guid"


def _export() -> AtlasExport:
    return AtlasExport(
        MetadataExportTarget(
            id="catalog",
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
        status="live",
        refs=(),
        experts=(),
        semantic_uri=f"provisa://{ORG}/terms/{name}",
    )
    base.update(kwargs)
    # REQ-2144: a deprecated term is one a curator deprecated -- its status.
    if base.pop("deprecated"):
        base["status"] = "deprecated"
    return GlossaryTermAsset(deprecated=False, **base)


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
        self.assigns: list[tuple[str, list]] = []
        self.dissociates: list[tuple[str, list]] = []
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
            if url.endswith("/assignedEntities"):
                fake.assigns.append((url.split("/terms/")[1].split("/")[0], json))
                return httpx.Response(204, request=request)
            return httpx.Response(404, request=request)

        async def _request(self, method, url, json=None, headers=None):
            fake.requests.append((method, url))
            fake.dissociates.append((url.split("/terms/")[1].split("/")[0], json))
            return httpx.Response(204, request=httpx.Request(method, url))

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
        monkeypatch.setattr(httpx.AsyncClient, "request", _request)


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


ORDERS = AssetRef(kind=AssetKind.TABLE, parts=("wh", "public", "orders"))
AMOUNT = AssetRef(kind=AssetKind.COLUMN, parts=("wh", "public", "orders", "amount"))
ORDERS_URI = f"provisa://{ORG}/sales/tables/orders"
AMOUNT_URI = f"{ORDERS_URI}#field:amount"


def _with_table(terms) -> MetadataSnapshot:
    return MetadataSnapshot(
        org_id=ORG,
        sources=[
            SourceAsset(
                ref=AssetRef(kind=AssetKind.SOURCE, parts=("wh",)),
                id="wh",
                source_type="postgresql",
                description="",
            )
        ],
        tables=[
            TableAsset(
                ref=ORDERS,
                name="orders",
                source_id="wh",
                domain_id=None,
                description="",
                columns=[
                    ColumnAsset(
                        ref=AMOUNT,
                        name="amount",
                        data_type="numeric",
                        description="",
                        semantic_uri=AMOUNT_URI,
                    )
                ],
                semantic_uri=ORDERS_URI,
            )
        ],
        glossary_terms=list(terms),
    )


def _published_entities(monkeypatch) -> None:
    """The entity bulk already ran: its guids are what the assignments address."""

    async def _entities(self, entities, snapshot):
        result = PublishResult(provider_name="atlas")
        result.bindings[ORDERS_URI] = ("orders-guid", "orders")
        result.bindings[AMOUNT_URI] = ("amount-guid", "amount")
        return result

    monkeypatch.setattr(AtlasExport, "_publish_entities", _entities)


def _provisa_assignment(entity_guid: str, relationship_guid: str) -> dict:
    return {
        "guid": entity_guid,
        "relationshipGuid": relationship_guid,
        "relationshipAttributes": {
            "typeName": SEMANTIC_ASSIGNMENT,
            "attributes": {"source": ASSIGNMENT_SOURCE},
        },
    }


async def test_a_class_term_is_assigned_to_its_table_and_a_property_to_its_column(
    fake, monkeypatch
):
    """REQ-1389 (amended): Provisa assigns its terms as semantic assignments whose source is
    Provisa, status DISCOVERED -- the Class to the table, the Property to the column."""
    _published_entities(monkeypatch)
    order = _term(1, "order", is_abstract=False, role="class", tables=(ORDERS,))
    amount = _term(2, "amount", is_abstract=False, role="property", refs=(AMOUNT,))
    result = await _export().publish(_with_table([order, amount, _term(3, "party")]))
    assert result.ok, result.errors
    by_term = {fake.terms[guid]["name"]: body for guid, body in fake.assigns}
    assert {name: [e["guid"] for e in body] for name, body in by_term.items()} == {
        "order": ["orders-guid"],
        "amount": ["amount-guid"],
    }
    (entry,) = by_term["order"]
    assert entry["relationshipAttributes"] == {
        "typeName": SEMANTIC_ASSIGNMENT,
        "attributes": ASSIGNMENT_ATTRIBUTES,
    }
    assert ASSIGNMENT_ATTRIBUTES["status"] == "DISCOVERED"
    assert fake.dissociates == []


async def test_human_assignments_stand_and_provisas_stale_ones_are_withdrawn(fake, monkeypatch):
    """An assignment from any other source is a person's: Provisa neither duplicates nor
    withdraws it. Its own assignment to an entity the term no longer grounds is withdrawn by
    relationship guid; a term that left the snapshot loses only Provisa's assignments."""
    _published_entities(monkeypatch)
    fake.terms["order-guid"] = {
        "guid": "order-guid",
        "name": "order",
        "assignedEntities": [
            {"guid": "orders-guid", "relationshipGuid": "rel-human"},  # a steward's
            _provisa_assignment("old-table-guid", "rel-stale"),
        ],
    }
    fake.terms["margin-guid"] = {
        "guid": "margin-guid",
        "name": "margin",
        "assignedEntities": [
            {"guid": "orders-guid", "relationshipGuid": "rel-human-2"},
            _provisa_assignment("amount-guid", "rel-margin"),
        ],
    }
    export = _export()
    export.stored_bindings = {
        GLOSSARY_URI: ("glossary-live", GLOSSARY_QN),
        f"provisa://{ORG}/terms/order": ("order-guid", f"order@{GLOSSARY_QN}"),
        f"provisa://{ORG}/terms/margin": ("margin-guid", f"margin@{GLOSSARY_QN}"),
    }
    order = _term(1, "order", is_abstract=False, role="class", tables=(ORDERS,))
    result = await export.publish(_with_table([order]))
    assert result.ok, result.errors
    assert fake.assigns == []
    assert sorted(fake.dissociates) == [
        ("margin-guid", [{"guid": "amount-guid", "relationshipGuid": "rel-margin"}]),
        ("order-guid", [{"guid": "old-table-guid", "relationshipGuid": "rel-stale"}]),
    ]


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


async def test_every_term_carries_its_role_and_a_stewards_attributes_stand(fake):
    """REQ-2141: the role rides the term's additionalAttributes under provisaRole, on create
    and on update; any other entry in the map is a steward's and stands."""
    fake.terms["term-live"] = {
        "guid": "term-live",
        "name": "order",
        "qualifiedName": f"order@{GLOSSARY_QN}",
        "additionalAttributes": {"owner_team": "finance"},
    }
    export = _export()
    export.stored_bindings = {
        GLOSSARY_URI: ("glossary-live", GLOSSARY_QN),
        f"provisa://{ORG}/terms/order": ("term-live", f"order@{GLOSSARY_QN}"),
    }
    order = _term(1, "order", is_abstract=False, role="class")
    result = await export.publish(_snapshot([order, _term(2, "party")]))
    assert result.ok, result.errors
    ((guid, updated),) = fake.term_puts[:1]
    assert guid == "term-live"
    assert updated["additionalAttributes"] == {"owner_team": "finance", "provisaRole": "class"}
    (created,) = fake.term_creates
    assert created["additionalAttributes"] == {"provisaRole": "concept"}
