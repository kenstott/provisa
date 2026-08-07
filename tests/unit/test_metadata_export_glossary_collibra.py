# Copyright (c) 2026 Kenneth Stott
# Canary: 4f8c2a91-6d3e-4b7f-9a05-c1e84d27b6f3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1387: native business-glossary publishing through Collibra's import job.

Collibra is the adapter where the safety argument is containment, not omission: the
import job's REPLACE semantics reach every mentioned asset and mentioned relation type,
so these tests hold the payload to the containment line — every glossary identifier and
every relation endpoint inside Provisa's own Glossary-type domain, no PATCH without a
stored binding, no DELETE ever, absence never published.
"""

# Requirements: REQ-1385, REQ-1387, REQ-1389

from __future__ import annotations

import json

import httpx
import pytest

from provisa.api.metadata_export.collibra import (
    ACTIVE_STATUS,
    DEFINITION_ATTRIBUTE,
    DEPRECATED_STATUS,
    GLOSSARY_TERM_TYPE,
    TERM_RELATION_TYPES,
    URI_ATTRIBUTE,
    CollibraExport,
    glossary_rows,
)
from provisa.api.metadata_export.model import (
    GlossaryTermAsset,
    GlossaryTermEdge,
    MetadataSnapshot,
)
from provisa.core.models import MetadataExportConfig

COMMUNITY = "Provisa"
GLOSSARY_DOMAIN = "Provisa Glossary"


def _term(term_id: int, name: str, **overrides) -> GlossaryTermAsset:
    fields = {
        "term_id": term_id,
        "name": name,
        "definition": None,
        "is_abstract": False,
        "deprecated": False,
        "semantic_uri": f"provisa://acme/terms/{name}",
    }
    fields.update(overrides)
    return GlossaryTermAsset(**fields)


@pytest.fixture
def snapshot() -> MetadataSnapshot:
    """Three published terms and one edge of each enum type, no physical assets —
    isolating the glossary rows from the table rows the vendor tests already cover."""
    return MetadataSnapshot(
        org_id="acme",
        glossary_terms=[
            _term(1, "customer", definition="A party that buys"),
            _term(3, "party", is_abstract=True),
            _term(4, "legacy customer", definition="Superseded", deprecated=True),
        ],
        glossary_edges=[
            GlossaryTermEdge(from_term_id=1, to_term_id=3, rel_type="KIND_OF"),
            GlossaryTermEdge(from_term_id=1, to_term_id=4, rel_type="RELATED_TO"),
            GlossaryTermEdge(from_term_id=4, to_term_id=1, rel_type="SYNONYM_OF"),
            GlossaryTermEdge(from_term_id=4, to_term_id=3, rel_type="PART_OF"),
        ],
    )


@pytest.fixture
def rows(snapshot):
    return glossary_rows(snapshot, COMMUNITY, GLOSSARY_DOMAIN)


# --- Row shape ------------------------------------------------------------------------------


def test_terms_are_business_term_assets_with_definition_and_uri(rows):
    by_name = {row["name"]: row for row in rows}
    customer = by_name["customer"]
    assert customer["resourceType"] == "Asset"
    assert customer["type"] == {"name": GLOSSARY_TERM_TYPE}
    assert customer["displayName"] == "customer"
    assert customer["attributes"][DEFINITION_ATTRIBUTE] == [{"value": "A party that buys"}]
    assert customer["attributes"][URI_ATTRIBUTE] == [{"value": "provisa://acme/terms/customer"}]
    # A term without a definition does not mention the Definition type at all — mentioning
    # it empty would still be a set-replacement of that type.
    assert DEFINITION_ATTRIBUTE not in by_name["party"]["attributes"]


def test_deprecation_rides_the_collibra_status(rows):
    by_name = {row["name"]: row for row in rows}
    assert by_name["legacy customer"]["status"] == {"name": DEPRECATED_STATUS}
    assert by_name["customer"]["status"] == {"name": ACTIVE_STATUS}
    assert by_name["party"]["status"] == {"name": ACTIVE_STATUS}


# --- Edge mapping ---------------------------------------------------------------------------


def test_each_enum_edge_maps_to_its_native_collibra_relation(rows):
    by_name = {row["name"]: row for row in rows}
    customer = by_name["customer"]["relations"]
    legacy = by_name["legacy customer"]["relations"]
    assert [t["name"] for t in customer[f"{TERM_RELATION_TYPES['KIND_OF']}:TARGET"]] == ["party"]
    assert [t["name"] for t in customer[f"{TERM_RELATION_TYPES['RELATED_TO']}:TARGET"]] == [
        "legacy customer"
    ]
    assert [t["name"] for t in legacy[f"{TERM_RELATION_TYPES['SYNONYM_OF']}:TARGET"]] == [
        "customer"
    ]
    assert [t["name"] for t in legacy[f"{TERM_RELATION_TYPES['PART_OF']}:TARGET"]] == ["party"]


def test_every_term_mentions_all_four_relation_types_and_no_others(rows):
    """The edge set anchored on a Provisa term is Provisa-authored, so each row carries the
    complete final set per type — empty included — making REPLACE the drift correction."""
    expected = {f"{relation}:TARGET" for relation in TERM_RELATION_TYPES.values()}
    for row in rows:
        assert set(row["relations"]) == expected
    party = next(row for row in rows if row["name"] == "party")
    assert all(targets == [] for targets in party["relations"].values())


# --- REPLACE scope containment --------------------------------------------------------------


def test_every_identifier_and_relation_endpoint_stays_inside_the_provisa_glossary_domain(rows):
    """The containment argument itself: REPLACE reaches only mentioned assets, and every
    asset the glossary payload mentions — row or endpoint — is a Provisa term in Provisa's
    own Glossary-type domain, so no steward glossary is ever in scope."""
    provisa_domain = {"name": GLOSSARY_DOMAIN, "community": {"name": COMMUNITY}}
    term_names = {"customer", "party", "legacy customer"}
    for row in rows:
        assert row["identifier"]["domain"] == provisa_domain
        assert row["identifier"]["name"] in term_names
        for targets in row["relations"].values():
            for target in targets:
                assert target["domain"] == provisa_domain
                assert target["name"] in term_names


# --- Transport ------------------------------------------------------------------------------


def _export() -> CollibraExport:
    return CollibraExport(
        MetadataExportConfig(enabled=True, provider="collibra", endpoint="https://collibra.example")
    )


def _fake_target(monkeypatch, *, community_exists: bool, domain_exists: bool) -> dict:
    """The Collibra surface the adapter touches, recording every call in order."""
    recorded = {"calls": [], "imports": []}

    async def _get(self, url, params=None, headers=None):
        recorded["calls"].append(("GET", url))
        request = httpx.Request("GET", url)
        if url.endswith("/rest/2.0/communities"):
            results = [{"id": "comm-1", "name": COMMUNITY}] if community_exists else []
        elif url.endswith("/rest/2.0/domains"):
            results = [{"id": "dom-1", "name": GLOSSARY_DOMAIN}] if domain_exists else []
        elif url.endswith("/rest/2.0/domainTypes"):
            results = [{"id": "type-glossary", "name": "Glossary"}]
        else:
            assert url.endswith("/rest/2.0/assets")
            results = [{"id": f"uuid-{params['name']}", "name": params["name"]}]
        return httpx.Response(200, json={"results": results}, request=request)

    async def _post(self, url, files=None, data=None, json=None, headers=None):
        recorded["calls"].append(("POST", url))
        request = httpx.Request("POST", url)
        if url.endswith("/rest/2.0/import/json-job"):
            recorded["imports"].append({"files": files, "data": data})
            return httpx.Response(200, request=request)
        if url.endswith("/rest/2.0/communities"):
            return httpx.Response(200, json={"id": "comm-1"}, request=request)
        assert url.endswith("/rest/2.0/domains")
        return httpx.Response(200, json={"id": "dom-1"}, request=request)

    async def _patch(self, url, json=None, headers=None):
        recorded["calls"].append(("PATCH", url, json))
        return httpx.Response(200, request=httpx.Request("PATCH", url))

    async def _delete(self, url, headers=None):
        recorded["calls"].append(("DELETE", url))
        return httpx.Response(200, request=httpx.Request("DELETE", url))

    monkeypatch.setattr(httpx.AsyncClient, "get", _get)
    monkeypatch.setattr(httpx.AsyncClient, "post", _post)
    monkeypatch.setattr(httpx.AsyncClient, "patch", _patch)
    monkeypatch.setattr(httpx.AsyncClient, "delete", _delete)
    return recorded


def _imported_rows(recorded: dict) -> list[dict]:
    assert len(recorded["imports"]) == 1
    return json.loads(recorded["imports"][0]["files"]["file"][1])


async def test_first_publish_creates_the_community_and_glossary_domain_once(snapshot, monkeypatch):
    recorded = _fake_target(monkeypatch, community_exists=False, domain_exists=False)
    result = await _export().publish(snapshot)
    assert result.ok
    posts = [url for method, *rest in recorded["calls"] if method == "POST" for url in rest[:1]]
    assert [url.rsplit("/", 1)[-1] for url in posts] == [
        "communities",
        "domains",
        "json-job",
    ]
    assert result.published == {"business term": 3}


async def test_repeat_publish_finds_the_domain_and_creates_nothing(snapshot, monkeypatch):
    recorded = _fake_target(monkeypatch, community_exists=True, domain_exists=True)
    result = await _export().publish(snapshot)
    assert result.ok
    posts = [call for call in recorded["calls"] if call[0] == "POST"]
    assert len(posts) == 1 and posts[0][1].endswith("/rest/2.0/import/json-job")


async def test_publish_captures_the_asset_uuid_binding_for_every_term(snapshot, monkeypatch):
    _fake_target(monkeypatch, community_exists=True, domain_exists=True)
    result = await _export().publish(snapshot)
    assert result.ok
    assert result.bindings == {
        "provisa://acme/terms/customer": ("uuid-customer", "customer"),
        "provisa://acme/terms/party": ("uuid-party", "party"),
        "provisa://acme/terms/legacy customer": ("uuid-legacy customer", "legacy customer"),
    }


async def test_rename_patches_the_live_asset_by_stored_uuid_before_the_import(
    snapshot, monkeypatch
):
    """REQ-1389: the import upserts by name, so a renamed term is re-addressed on the SAME
    asset first — otherwise the name-keyed import would mint a duplicate."""
    recorded = _fake_target(monkeypatch, community_exists=True, domain_exists=True)
    export = _export()
    export.stored_bindings = {"provisa://acme/terms/customer": ("uuid-old", "cust")}
    result = await export.publish(snapshot)
    assert result.ok
    patches = [call for call in recorded["calls"] if call[0] == "PATCH"]
    assert len(patches) == 1
    assert patches[0][1].endswith("/rest/2.0/assets/uuid-old")
    assert patches[0][2] == {"name": "customer", "displayName": "customer"}
    order = [call[0] for call in recorded["calls"]]
    assert order.index("PATCH") < order.index("POST")
    assert result.bindings["provisa://acme/terms/customer"] == ("uuid-customer", "customer")


async def test_no_asset_without_a_provisa_binding_is_ever_patched_or_deleted(snapshot, monkeypatch):
    recorded = _fake_target(monkeypatch, community_exists=True, domain_exists=True)
    result = await _export().publish(snapshot)
    assert result.ok
    assert [call for call in recorded["calls"] if call[0] in ("PATCH", "DELETE")] == []


async def test_a_term_absent_from_the_snapshot_is_unmentioned_never_deleted(snapshot, monkeypatch):
    """Steward glossaries and retired Provisa terms share one guarantee: absence from the
    payload. The import payload names nothing but the snapshot's own terms."""
    recorded = _fake_target(monkeypatch, community_exists=True, domain_exists=True)
    snapshot.glossary_terms = [t for t in snapshot.glossary_terms if t.name != "customer"]
    snapshot.glossary_edges = [
        e for e in snapshot.glossary_edges if 1 not in (e.from_term_id, e.to_term_id)
    ]
    result = await _export().publish(snapshot)
    assert result.ok
    assert [call for call in recorded["calls"] if call[0] == "DELETE"] == []
    payload = _imported_rows(recorded)
    assert {row["name"] for row in payload} == {"party", "legacy customer"}
    assert "customer" not in {
        target["name"]
        for row in payload
        for targets in row["relations"].values()
        for target in targets
    }


async def test_import_payload_never_names_an_asset_outside_the_provisa_glossary_domain(
    snapshot, monkeypatch
):
    recorded = _fake_target(monkeypatch, community_exists=False, domain_exists=False)
    result = await _export().publish(snapshot)
    assert result.ok
    provisa_domain = {"name": GLOSSARY_DOMAIN, "community": {"name": COMMUNITY}}
    for row in _imported_rows(recorded):
        assert row["identifier"]["domain"] == provisa_domain
        for targets in row["relations"].values():
            for target in targets:
                assert target["domain"] == provisa_domain
