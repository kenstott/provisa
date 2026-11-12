# Copyright (c) 2026 Kenneth Stott
# Canary: c58e2a90-4d17-4b3f-9a6c-0e7d1f84b2a5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2237: importing an ontology is an alignment. An upload proposes how each imported item
aligns with the terms Provisa derived from the model; a steward approves or rejects each; applying
makes every item a draft term carrying its definition and IRI (or the term it is the same entity
as), and writes only the approved alignments, so the imported meaning grounds through the model's
own columns. Nothing is approved for the steward, and an item aligned with nothing stays a
Concept."""

# Requirements: REQ-2237

from __future__ import annotations

import base64
import types
from typing import TYPE_CHECKING, Any, cast

import pytest
from sqlalchemy import insert

from provisa.api.admin import glossary_router
from provisa.api.admin import ontology_import_router as router
from provisa.api.errors import ApiError
from provisa.core.glossary import GROUNDED, NOT_GROUNDED
from provisa.core.models import Column, Table
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, sources

if TYPE_CHECKING:
    from fastapi import Request

FIBO = "https://spec.edmcouncil.org/fibo/ontology/"

ONTOLOGY = f"""
name: Banking
prefixes:
  fibo: {FIBO}
ontology:
- concept: Party
  type: EntityType
  iri: fibo:Party
  description: A person or organization that takes part in an agreement.
- concept: Customer
  type: EntityType
  extends: [ Party ]
  iri: fibo:Customer
  description: A party that receives goods or services.
  relationships:
  - name: email
    iri: fibo:hasEmail
    verbalizes: [ '{{Customer}} has {{String}}' ]
- concept: Ledger
  type: EntityType
  iri: fibo:Ledger
  description: A book of accounts.
""".encode()


def _request(body: dict | None = None) -> Request:
    async def _json() -> dict:
        return body or {}

    fake = types.SimpleNamespace(
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id="alice", roles=[], acting_roles=[]),
            active_org_id="default",
            role="steward",
        ),
        json=_json,
    )
    return cast("Request", fake)


def _upload(data: bytes, file_name: str = "banking.yaml", **extra: Any) -> dict:
    return {
        "file_name": file_name,
        "content_b64": base64.b64encode(data).decode(),
        "domains": ["d"],
        **extra,
    }


@pytest.fixture
def api(control_plane, monkeypatch):
    with control_plane.engine().begin() as c:
        c.exec_driver_sql('SET LOCAL search_path TO "org_default"')
        c.execute(insert(domains).values(id="d", description=""))
        c.execute(insert(sources).values(id="__derived__", type="postgresql"))
    db = control_plane.org()
    notified: list[str] = []

    async def _notify(org_id: str, reason: str) -> None:
        notified.append(reason)

    monkeypatch.setattr(router, "_pool", lambda: db)
    monkeypatch.setattr(router, "require_active_org_id", lambda request: "default")
    # Which right gates each endpoint, and how domains narrow curation, are the glossary's own
    # tests (test_glossary_rights.py, test_glossary_stewardship.py); here they must not block.
    monkeypatch.setattr(glossary_router, "_require_glossary_rw", lambda request: None)
    monkeypatch.setattr(glossary_router, "_authority", lambda request: None)
    monkeypatch.setattr(glossary_router, "_curate_authority", lambda request: None)
    monkeypatch.setattr(glossary_router, "_is_org_curator", lambda request: False)
    monkeypatch.setattr(glossary_router, "_notify", _notify)
    return types.SimpleNamespace(db=db, notified=notified)


async def _model(api, columns: list[str]) -> None:
    """The model: a customers table whose columns derive the terms the import aligns with."""
    async with api.db.acquire() as conn:
        await table_repo.upsert(
            conn,
            Table(
                source_id="__derived__",
                domain_id="d",
                schema_name="s",
                table_name="customers",
                columns=[Column(name=c, data_type="text", visible_to=[]) for c in columns],
                view_sql="SELECT 1",
            ),
        )


async def _terms(api) -> dict[str, dict]:
    async with api.db.acquire() as conn:
        return {t["name"]: t for t in await glossary_repo.list_terms(conn)}


def _item(review: dict, name: str) -> dict:
    return next(i for i in review["items"] if i["name"] == name)


def _alignment(item: dict, rel_type: str, basis: str) -> dict:
    return next(a for a in item["alignments"] if a["rel_type"] == rel_type and a["basis"] == basis)


async def test_an_import_proposes_alignments_and_approves_none_of_them(api):
    await _model(api, ["cust_id", "customer_email"])
    review = await router.import_ontology(_request(_upload(ONTOLOGY)))
    assert review["import"]["title"] == "Banking"
    assert review["import"]["format"] == "ossie"
    assert {i["name"] for i in review["items"]} == {"party", "customer", "customer email", "ledger"}
    terms = await _terms(api)

    customer = _item(review, "customer")
    assert customer["iri"] == f"{FIBO}Customer"
    assert customer["definition"] == "A party that receives goods or services."
    same = _alignment(customer, "SAME_AS", "name")
    assert same["term"]["term_id"] == terms["customer"]["id"]
    assert same["term"]["role"] == "property"
    # The model's customer email is described by the imported customer.
    part = _alignment(customer, "PART_OF", "qualifier")
    assert (part["term"]["name"], part["item_is_source"]) == ("customer email", False)
    # The file's own structure, between its items.
    kind = _alignment(customer, "KIND_OF", "ontology")
    assert kind["other_item"]["name"] == "party"
    assert _alignment(_item(review, "customer email"), "SAME_AS", "name")["term"]["name"] == (
        "customer email"
    )
    assert _item(review, "ledger")["alignments"] == []

    statuses = {a["status"] for i in review["items"] for a in i["alignments"]}
    assert statuses == {"proposed"}
    assert all(i["status"] == "pending" and i["term"] is None for i in review["items"])
    # Reading the file changed nothing in the glossary.
    assert set(await _terms(api)) == {"customer", "customer email"}


async def test_applying_makes_drafts_and_writes_only_what_was_approved_so_meaning_grounds(api):
    await _model(api, ["cust_id", "customer_email"])
    review = await router.import_ontology(_request(_upload(ONTOLOGY)))
    import_id = review["import"]["id"]
    customer = _item(review, "customer")
    email = _item(review, "customer email")

    # Unapproved, the import would make second terms named as the model's: refused, each named.
    with pytest.raises(ApiError) as caught:
        await router.apply_import(import_id, _request())
    assert caught.value.code == "ontology_import.apply_refused"
    assert "the glossary already holds a term 'customer'" in str(caught.value.detail)
    assert "the glossary already holds a term 'customer email'" in str(caught.value.detail)

    for a in (
        _alignment(customer, "SAME_AS", "name"),
        _alignment(email, "SAME_AS", "name"),
        _alignment(customer, "KIND_OF", "ontology"),
    ):
        assert (await router.approve_alignment(a["id"], _request()))["status"] == "approved"
    rejected = await router.reject_alignment(
        _alignment(customer, "PART_OF", "qualifier")["id"], _request()
    )
    assert rejected["status"] == "rejected"

    applied = await router.apply_import(import_id, _request())
    assert {c["name"] for c in applied["created"]} == {"party", "ledger"}
    assert {a["name"] for a in applied["aligned"]} == {"customer", "customer email"}
    assert len(applied["edges"]) == 1
    assert api.notified == ["ontology imported"]

    terms = await _terms(api)
    party, ledger, cust = terms["party"], terms["ledger"], terms["customer"]
    # Imported terms are drafts carrying the imported definition and IRI. None is approved.
    assert (party["status"], party["iri"]) == ("draft", f"{FIBO}Party")
    assert party["definition"] == "A person or organization that takes part in an agreement."
    assert party["domains"] == ["d"]
    assert not any(t["approved"] for t in terms.values())
    # The model's customer took the imported concept's IRI and, having none, its definition.
    assert cust["iri"] == f"{FIBO}Customer"
    assert cust["definition"] == "A party that receives goods or services."
    assert terms["customer email"]["iri"] == f"{FIBO}hasEmail"
    # Party grounds through the model's own customer column; Ledger aligned with nothing and
    # stays a Concept that reaches no data.
    assert (party["is_abstract"], party["grounding"]) == (True, GROUNDED)
    assert (ledger["is_abstract"], ledger["grounding"]) == (True, NOT_GROUNDED)
    async with api.db.acquire() as conn:
        edges = await glossary_repo.term_edges(conn)
    assert edges == [(cust["id"], party["id"], "KIND_OF")]

    after = await router.get_import(import_id, _request())
    assert after["import"]["applied_by"] == "alice"
    assert _item(after, "customer")["term"]["term_id"] == cust["id"]
    # The structure left unapproved is still proposed: a steward may approve it later.
    email_part = _alignment(_item(after, "customer email"), "PART_OF", "ontology")
    assert email_part["status"] == "proposed"


async def test_applying_again_writes_what_was_approved_since(api):
    await _model(api, ["cust_id", "customer_email"])
    review = await router.import_ontology(_request(_upload(ONTOLOGY)))
    import_id = review["import"]["id"]
    for name in ("customer", "customer email"):
        same = _alignment(_item(review, name), "SAME_AS", "name")
        await router.approve_alignment(same["id"], _request())
    await router.apply_import(import_id, _request())
    later = _alignment(_item(review, "customer email"), "PART_OF", "ontology")
    await router.approve_alignment(later["id"], _request())

    again = await router.apply_import(import_id, _request())
    assert (again["created"], again["aligned"]) == ([], [])
    terms = await _terms(api)
    assert again["edges"] == [
        {
            "alignment_id": later["id"],
            "from": terms["customer email"]["id"],
            "rel_type": "PART_OF",
            "to": terms["customer"]["id"],
        }
    ]


async def test_a_reimport_finds_each_term_by_its_iri_and_makes_none_twice(api):
    await _model(api, ["cust_id"])
    first = await router.import_ontology(_request(_upload(ONTOLOGY)))
    same = _alignment(_item(first, "customer"), "SAME_AS", "name")
    await router.approve_alignment(same["id"], _request())
    await router.apply_import(first["import"]["id"], _request())
    terms = await _terms(api)

    second = await router.import_ontology(_request(_upload(ONTOLOGY)))
    for name in ("party", "customer", "customer email", "ledger"):
        only = _item(second, name)["alignments"][0]
        assert (only["rel_type"], only["basis"]) == ("SAME_AS", "iri")
        assert only["term"]["term_id"] == terms[name]["id"]


async def test_an_excluded_item_makes_no_term_and_what_aligns_with_it_waits(api):
    await _model(api, ["cust_id", "customer_email"])
    review = await router.import_ontology(_request(_upload(ONTOLOGY)))
    import_id = review["import"]["id"]
    for name in ("customer", "customer email"):
        same = _alignment(_item(review, name), "SAME_AS", "name")
        await router.approve_alignment(same["id"], _request())
    kind = _alignment(_item(review, "customer"), "KIND_OF", "ontology")
    await router.approve_alignment(kind["id"], _request())
    party = _item(review, "party")
    edited = await router.edit_item(party["id"], _request({"excluded": True}))
    assert _item(edited, "party")["status"] == "excluded"

    applied = await router.apply_import(import_id, _request())
    assert {c["name"] for c in applied["created"]} == {"ledger"}
    assert applied["waiting"] == [
        {"alignment_id": kind["id"], "reason": "an end of it is excluded"}
    ]
    assert "party" not in await _terms(api)


async def test_a_renamed_item_comes_in_under_its_new_name(api):
    review = await router.import_ontology(_request(_upload(ONTOLOGY)))
    ledger = _item(review, "ledger")
    edited = await router.edit_item(
        ledger["id"], _request({"name": "General  Ledger", "definition": "The books."})
    )
    assert (_item(edited, "general ledger")["definition"]) == "The books."
    await router.apply_import(review["import"]["id"], _request())
    terms = await _terms(api)
    assert terms["general ledger"]["iri"] == f"{FIBO}Ledger"
    assert terms["general ledger"]["definition"] == "The books."


async def test_a_steward_adds_an_alignment_of_their_own_approved(api):
    await _model(api, ["client_tier"])
    review = await router.import_ontology(_request(_upload(ONTOLOGY)))
    terms = await _terms(api)
    ledger = _item(review, "ledger")
    added = await router.add_alignment(
        review["import"]["id"],
        _request(
            {
                "item_id": ledger["id"],
                "term_id": terms["client tier"]["id"],
                "rel_type": "PART_OF",
                "item_is_source": False,
            }
        ),
    )
    assert (added["status"], added["basis"], added["decided_by"]) == (
        "approved",
        "steward",
        "alice",
    )
    await router.apply_import(review["import"]["id"], _request())
    terms = await _terms(api)
    assert terms["ledger"]["grounding"] == GROUNDED
    async with api.db.acquire() as conn:
        assert await glossary_repo.term_edges(conn) == [
            (terms["client tier"]["id"], terms["ledger"]["id"], "PART_OF")
        ]


@pytest.mark.parametrize(
    "body, message",
    [
        ({"rel_type": "SAME_AS"}, "names the imported item"),
        ({"item_id": 0, "rel_type": "SAME_AS"}, "one term or one other item"),
        ({"item_id": 0, "term_id": 1, "rel_type": "LIKES"}, "rel_type is one of"),
        ({"item_id": 0, "other_item_id": 1, "rel_type": "SAME_AS"}, "never as another item"),
        ({"item_id": 0, "term_id": 1, "rel_type": "KIND_OF", "use_definition": True}, "only a"),
    ],
)
async def test_a_steward_alignment_that_says_nothing_coherent_is_refused(api, body, message):
    review = await router.import_ontology(_request(_upload(ONTOLOGY)))
    with pytest.raises(ApiError) as caught:
        await router.add_alignment(review["import"]["id"], _request(body))
    assert caught.value.code == "ontology_import.refused"
    assert message in str(caught.value.detail)


async def test_two_terms_approved_as_the_same_entity_refuse_the_apply(api):
    await _model(api, ["cust_id", "client_tier"])
    review = await router.import_ontology(_request(_upload(ONTOLOGY)))
    terms = await _terms(api)
    customer = _item(review, "customer")
    await router.approve_alignment(_alignment(customer, "SAME_AS", "name")["id"], _request())
    await router.add_alignment(
        review["import"]["id"],
        _request(
            {
                "item_id": customer["id"],
                "term_id": terms["client tier"]["id"],
                "rel_type": "SAME_AS",
            }
        ),
    )
    with pytest.raises(ApiError) as caught:
        await router.apply_import(review["import"]["id"], _request())
    assert caught.value.code == "ontology_import.apply_refused"
    assert "'customer' is approved as the same entity as 'customer', 'client tier'" in str(
        caught.value.detail
    )
    assert "party" not in await _terms(api)


async def test_use_definition_replaces_the_terms_own_definition(api):
    await _model(api, ["cust_id"])
    async with api.db.acquire() as conn:
        held = (await _terms(api))["customer"]["id"]
        await glossary_repo.set_definition(conn, held, "Someone we bill.")
    review = await router.import_ontology(_request(_upload(ONTOLOGY)))
    same = _alignment(_item(review, "customer"), "SAME_AS", "name")
    await router.approve_alignment(same["id"], _request({"use_definition": True}))
    await router.apply_import(review["import"]["id"], _request())
    assert (await _terms(api))["customer"]["definition"] == (
        "A party that receives goods or services."
    )


async def test_a_term_keeps_its_own_definition_unless_the_steward_chooses_the_items(api):
    await _model(api, ["cust_id"])
    async with api.db.acquire() as conn:
        held = (await _terms(api))["customer"]["id"]
        await glossary_repo.set_definition(conn, held, "Someone we bill.")
    review = await router.import_ontology(_request(_upload(ONTOLOGY)))
    same = _alignment(_item(review, "customer"), "SAME_AS", "name")
    await router.approve_alignment(same["id"], _request())
    await router.apply_import(review["import"]["id"], _request())
    customer = (await _terms(api))["customer"]
    assert (customer["definition"], customer["iri"]) == ("Someone we bill.", f"{FIBO}Customer")


async def test_a_derived_term_aligned_with_an_iri_is_curated_and_survives_its_column(api):
    await _model(api, ["cust_id", "customer_email"])
    review = await router.import_ontology(_request(_upload(ONTOLOGY)))
    email = _item(review, "customer email")
    assert email["definition"] is None  # the relationship has no description: only its IRI
    await router.approve_alignment(_alignment(email, "SAME_AS", "name")["id"], _request())
    await router.edit_item(_item(review, "customer")["id"], _request({"excluded": True}))
    await router.apply_import(review["import"]["id"], _request())

    await _model(api, ["cust_id"])
    kept = (await _terms(api))["customer email"]
    assert (kept["iri"], kept["deprecated"], kept["ref_count"]) == (f"{FIBO}hasEmail", True, 0)


async def test_an_rdf_upload_is_read_by_its_extension(api):
    turtle = b"""
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
<https://example.org/Customer> a owl:Class ; skos:definition "A party that buys."@en .
"""
    await _model(api, ["cust_id"])
    review = await router.import_ontology(_request(_upload(turtle, "mini.ttl")))
    assert review["import"]["format"] == "turtle"
    (item,) = review["items"]
    assert (item["name"], item["iri"], item["keys"]) == (
        "customer",
        "https://example.org/Customer",
        ["https://example.org/Customer"],
    )
    assert item["alignments"][0]["rel_type"] == "SAME_AS"


@pytest.mark.parametrize(
    "body, message",
    [
        ({"file_name": "x.yaml", "content_b64": "%%%", "domains": ["d"]}, "not base64"),
        (_upload(b"name: X\nontology: []\n"), "declares no class, property or concept"),
        (
            _upload(b'{"@context": "https://example.org/c.jsonld"}', "x.jsonld"),
            "remote JSON-LD context",
        ),
        (_upload(ONTOLOGY, extra="x"), "'extra' is not set here"),
    ],
)
async def test_a_file_that_cannot_be_imported_is_refused_and_nothing_is_kept(api, body, message):
    with pytest.raises(ApiError) as caught:
        await router.import_ontology(_request(body))
    assert caught.value.status_code == 400
    assert message in str(caught.value.detail)
    assert (await router.list_imports(_request()))["imports"] == []


async def test_imports_are_listed_with_their_counts_and_discarding_one_keeps_its_terms(api):
    review = await router.import_ontology(_request(_upload(ONTOLOGY)))
    await router.apply_import(review["import"]["id"], _request())
    (listed,) = (await router.list_imports(_request()))["imports"]
    assert listed["counts"]["items"] == {"applied": 4}
    # The file's structure, never approved, is still only proposed.
    assert listed["counts"]["alignments"] == {"proposed": 2}
    await router.discard_import(review["import"]["id"], _request())
    assert (await router.list_imports(_request()))["imports"] == []
    assert {"party", "customer", "customer email", "ledger"} <= set(await _terms(api))
    with pytest.raises(ApiError) as caught:
        await router.get_import(review["import"]["id"], _request())
    assert caught.value.status_code == 404


async def test_an_applied_item_is_the_glossarys_and_is_not_edited_here(api):
    review = await router.import_ontology(_request(_upload(ONTOLOGY)))
    await router.apply_import(review["import"]["id"], _request())
    with pytest.raises(ApiError) as caught:
        await router.edit_item(_item(review, "ledger")["id"], _request({"name": "books"}))
    assert "is in the glossary now" in str(caught.value.detail)


async def test_bulk_approval_decides_each_and_names_the_refused(api):
    await _model(api, ["cust_id"])
    review = await router.import_ontology(_request(_upload(ONTOLOGY)))
    ids = [a["id"] for i in review["items"] for a in i["alignments"]]
    decided = await router.bulk_approve(
        review["import"]["id"], _request({"alignment_ids": [*ids, 999]})
    )
    assert decided["approved"] == ids
    assert decided["refused"] == [{"id": 999, "message": "no import holds an alignment 999"}]


async def test_applying_an_import_embeds_the_terms_it_brought_into_service(api, monkeypatch):
    """REQ-2185 (TERMS ARE EMBEDDED AS THEY CHANGE): an applied import calls the one trigger."""
    from tests.unit.test_reference_library_terms_embedded import record_embeds

    embedded = record_embeds(monkeypatch)
    review = await router.import_ontology(_request(_upload(ONTOLOGY)))
    assert embedded == []
    await router.apply_import(review["import"]["id"], _request())
    assert embedded == ["default"]
