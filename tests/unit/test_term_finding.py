# Copyright (c) 2026 Kenneth Stott
# Canary: 0b7e5d93-4c2a-4f18-8a6e-3f9d21c7b5e4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2179: find terms in a document -- a reference library document scanned into candidate
terms (concepts only, mentions resolved, homonyms split, explicit or synthesized definitions with
their passages, conflicts side by side, matched against the glossary), each a draft in a review
queue the steward accepts, edits, merges or rejects; every model call held to the role's LLM
budget (REQ-1174). REQ-2185: a new document joins the library as proposed in the steward's
domains; one already there is not added again; a rescan shows matches and duplicates."""

# Requirements: REQ-2179, REQ-2185

from __future__ import annotations

import base64
import json
import re
import types
from typing import TYPE_CHECKING, Any, cast

import pytest
from sqlalchemy import insert

from provisa.api.admin import glossary_router
from provisa.api.admin import reference_library_router as library_api
from provisa.api.admin import term_finding_router as router
from provisa.api.errors import ApiError
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.schema_org import domains
from provisa.reference_library import library, store
from provisa.security.llm_budget import BudgetRefused
from provisa.term_finding import model, scan

if TYPE_CHECKING:
    from fastapi import Request

# -- the scan, without a model or a database ----------------------------------------------------

P = [
    scan.Passage(0, "Definitions", None, '"Customer" means any party that holds an account.'),
    scan.Passage(1, "Accounts", 2, "A customer account is opened after KYC checks."),
]


def test_a_mention_is_kept_only_as_a_concept_quoted_from_its_passage():
    left_out = scan.LeftOut()
    raw = json.dumps(
        [
            {
                "passage": 0,
                "name": "Customer",
                "category": "business concept",
                "sense": "a party with an account",
                "quote": "“Customer”  means any party that holds an account.",
                "explicit": True,
                "variants": [{"text": "CUST", "kind": "abbreviation"}, {"text": "x"}],
            },
            {
                "passage": 0,
                "name": "Jane Smith",
                "category": "individual",
                "sense": "a customer",
                "quote": "Customer",
                "explicit": False,
                "variants": [],
            },
            {
                "passage": 1,
                "name": "account",
                "category": "business concept",
                "sense": "s",
                "quote": "words the passage does not hold",
                "explicit": False,
                "variants": [],
            },
            {
                "passage": 9,
                "name": "kyc",
                "category": "process",
                "sense": "s",
                "quote": "KYC",
                "explicit": False,
                "variants": [],
            },
            {"name": "no passage"},
        ]
    )
    found = scan.read_mentions(f"```json\n{raw}\n```", P, 10, left_out)
    assert [(m.id, m.name, m.explicit) for m in found] == [(10, "customer", True)]
    assert found[0].variants == (scan.Variant("CUST", "abbreviation"),)
    assert left_out.as_dict() == {
        "not_a_concept": 1,
        "quote_not_in_passage": 1,
        "unknown_passage": 1,
        "malformed": 2,
        "unresolved": 0,
    }


def test_the_extraction_prompt_asks_for_concepts_and_never_named_individuals():
    prompt = scan.extraction_prompt("Bank policy", P)
    assert "Never a named individual" in prompt
    for category in scan.CATEGORIES:
        assert repr(category) in prompt
    assert "[P1] (section 'Accounts', page 2)" in prompt


def test_an_answer_that_is_not_json_fails_the_scan_by_name():
    with pytest.raises(scan.ScanFailed, match="passages 0-1 was not JSON"):
        scan.read_mentions("I found some terms", P, 0, scan.LeftOut())


def test_passages_are_read_in_batches_of_about_a_size():
    passages = [scan.Passage(i, None, None, "x" * 400) for i in range(10)]
    assert [len(b) for b in scan.batches(passages, limit=1000)] == [2, 2, 2, 2, 2]


def test_names_compare_without_case_article_or_plural():
    assert scan.normal("The Policies") == scan.normal("policy")
    assert scan.normal("Accounts") == "account"
    assert scan.normal("business") == "business"


def _term(*, id: int, name: str) -> dict:
    return {"id": id, "name": name}


def _candidate(name: str, *variants: str) -> scan.Candidate:
    return scan.Candidate(
        name=name,
        category="business concept",
        sense="",
        definition=None,
        definition_kind=scan.MISSING,
        passages=[],
        variants=[{"text": v, "kind": "alternative name", "keep": True} for v in variants],
        conflicts=[],
    )


def test_a_candidate_is_matched_against_terms_and_synonyms_as_a_merge():
    terms = [
        _term(id=1, name="client"),
        _term(id=2, name="customer"),
        _term(id=3, name="purchase order"),
    ]
    duplicate = _candidate("clients")
    by_synonym = _candidate("customers")
    by_variant = _candidate("po request", "purchase order")
    new = _candidate("ledger")
    scan.match([duplicate, by_synonym, by_variant, new], terms, [(2, 1)])
    assert (duplicate.match_term_id, duplicate.match_kind) == (1, scan.DUPLICATE)
    # "customer" is a synonym of "client": the candidate merges into the preferred term.
    assert (by_synonym.match_term_id, by_synonym.match_kind) == (1, scan.SYNONYM)
    assert by_synonym.matches == [{"term_id": 2, "name": "customer", "via": "customers"}]
    assert (by_variant.match_term_id, by_variant.match_kind) == (3, scan.SYNONYM)
    assert (new.match_term_id, new.matches) == (None, [])


def test_a_mention_the_resolution_leaves_out_is_counted():
    mentions = [
        scan.Mention(0, 0, "customer", "business concept", "s", "q", False, ()),
        scan.Mention(1, 1, "kyc", "process", "s", "q", False, ()),
    ]
    left_out = scan.LeftOut()
    groups = scan.read_groups(
        json.dumps([{"name": "customer", "mentions": [0, 0, 7]}, {"name": "dup", "mentions": [0]}]),
        mentions,
        left_out,
    )
    assert [(g.name, g.mentions) for g in groups] == [("customer", (0,))]
    assert left_out.unresolved == 1


# -- the model, held to the role's LLM budget ---------------------------------------------------


class _Guard:
    def __init__(self, *, refuse: bool = False, room: int | None = None, budget: bool = True):
        self.refuse = refuse
        self.room = room
        self.admitted = types.SimpleNamespace(budget=object() if budget else None)
        self.debited: list[tuple[Any, str]] = []

    async def admit(self, model_name: str) -> None:
        if self.refuse:
            raise BudgetRefused("role steward's LLM budget for the day is spent")

    def allowance(self, model_name: str) -> int | None:
        return self.room

    async def debit(self, usage: Any, model_name: str) -> None:
        self.debited.append((usage, model_name))


class _Client:
    model = "claude-test"

    def __init__(self, usage: Any):
        self.usage = usage
        self.asked: list[tuple[str, int]] = []

    async def complete_detailed(self, prompt: str, system: str, max_tokens: int) -> Any:
        self.asked.append((prompt, max_tokens))
        return types.SimpleNamespace(
            text="[]", vendor="anthropic", model="claude-test", usage=self.usage
        )


USAGE = types.SimpleNamespace(prompt_tokens=100, completion_tokens=20, prompt_tokens_details=None)


async def test_a_call_is_limited_to_the_roles_room_and_debited_with_what_it_used():
    guard, client = _Guard(room=500), _Client(USAGE)
    call = model.held(client, guard, None)
    assert await call("find terms", 4096) == "[]"
    assert client.asked[0][1] == 500
    ((usage, model_name),) = guard.debited
    assert (usage.input_tokens, usage.output_tokens, model_name) == (100, 20, "claude-test")


async def test_a_refused_budget_ends_the_scan_naming_the_window():
    call = model.held(_Client(USAGE), _Guard(refuse=True), None)
    with pytest.raises(scan.ScanFailed, match="budget for the day is spent"):
        await call("find terms", 4096)


async def test_a_call_with_no_reported_usage_is_refused_when_a_budget_holds_the_role():
    with pytest.raises(scan.ScanFailed, match="reported no usage"):
        await model.held(_Client(None), _Guard(), None)("p", 10)
    assert await model.held(_Client(None), _Guard(budget=False), None)("p", 10) == "[]"


# -- the API: scan, review queue, rescan ----------------------------------------------------------

SCHEMA = "org_default"

POLICY = b"""# Definitions

"Customer" means any party that holds an account with the Bank. KYC means know your customer checks.

# Accounts

A customer account is opened after KYC checks.

# Ledger

Each account in the general ledger records balances.

# Retention

Customer records are kept for seven years. Customer records are kept for five years.
"""


def _m(section: str, name: str, category: str, sense: str, quote: str, explicit: bool, *v) -> dict:
    return {
        "section": section,
        "name": name,
        "category": category,
        "sense": sense,
        "quote": quote,
        "explicit": explicit,
        "variants": [{"text": t, "kind": k} for t, k in v],
    }


MENTIONS = [
    _m(
        "Definitions",
        "customer",
        "business concept",
        "a party with an account",
        '"Customer" means any party that holds an account with the Bank.',
        True,
        ("Customers", "form"),
    ),
    _m(
        "Definitions",
        "know your customer check",
        "process",
        "identity checks",
        "KYC means know your customer checks.",
        True,
        ("KYC", "abbreviation"),
    ),
    _m("Definitions", "jane smith", "individual", "a customer", "Customer", False),
    _m(
        "Accounts",
        "account",
        "business concept",
        "a customer's relationship with the bank",
        "A customer account is opened after KYC checks.",
        False,
    ),
    _m(
        "Accounts",
        "customer",
        "business concept",
        "a party with an account",
        "A customer account is opened after KYC checks.",
        False,
    ),
    _m(
        "Ledger",
        "account",
        "business concept",
        "a ledger record",
        "Each account in the general ledger records balances.",
        False,
    ),
    _m("Ledger", "ledger", "business concept", "s", "the ledger holds everything", False),
    _m(
        "Retention",
        "record retention period",
        "policy",
        "how long records are kept",
        "Customer records are kept for seven years.",
        False,
    ),
    _m(
        "Retention",
        "record retention period",
        "policy",
        "how long records are kept",
        "Customer records are kept for five years.",
        False,
    ),
]
# The mentions kept, numbered in order: the individual and the misquote are left out.
GROUPS = [
    {
        "name": "customer",
        "sense": "a party",
        "mentions": [0, 3],
        "variants": [{"text": "client", "kind": "alternative name"}],
    },
    {"name": "know your customer check", "sense": "identity checks", "mentions": [1]},
    {
        "name": "account (customer relationship)",
        "sense": "a customer's relationship with the bank",
        "mentions": [2],
        "definition": "An arrangement a customer holds with the bank.",
        "uses": [2],
    },
    {
        "name": "account (ledger)",
        "sense": "a ledger record",
        "mentions": [4],
        "definition": "A record of balances in the general ledger.",
        "uses": [4],
    },
    {
        "name": "record retention period",
        "sense": "how long records are kept",
        "mentions": [5, 6],
        "conflicts": [[5, 6]],
        "definition": "How long customer records are kept.",
        "uses": [5, 6],
    },
]


async def fake_model(prompt: str, max_tokens: int) -> str:
    if "Group the mentions" in prompt:
        return json.dumps(GROUPS)
    ordinals = dict((s, int(n)) for n, s in re.findall(r"\[P(\d+)\] \(section '([^']+)'", prompt))
    return json.dumps(
        [
            {k: v for k, v in m.items() if k != "section"} | {"passage": ordinals[m["section"]]}
            for m in MENTIONS
            if m["section"] in ordinals
        ]
    )


async def _embed(texts: list[str]) -> list[list[float]]:
    return [[float(len(t)), 1.0] for t in texts]


EMBEDDER = library.Embedder(model="test-embed", embed=_embed)


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


@pytest.fixture
def api(control_plane, monkeypatch):
    with control_plane.engine().begin() as c:
        c.exec_driver_sql(f'SET LOCAL search_path TO "{SCHEMA}"')
        c.execute(insert(domains).values(id="d", description=""))
        c.execute(insert(domains).values(id="other", description=""))
    db = control_plane.org()
    launched: list[Any] = []
    notified: list[str] = []

    async def _notify(org_id: str, reason: str) -> None:
        notified.append(reason)

    async def _model(request):
        return fake_model

    async def _embedder():
        return EMBEDDER

    monkeypatch.setattr(router, "_pool", lambda: db)
    monkeypatch.setattr(router, "require_active_org_id", lambda request: "default")
    monkeypatch.setattr(router, "_launch", launched.append)
    monkeypatch.setattr(router, "_model", _model)
    monkeypatch.setattr(library_api, "_embedder", _embedder)
    # Which right gates each endpoint, and how domains narrow curation, are the glossary's own
    # tests (test_glossary_rights.py, test_glossary_stewardship.py); here they must not block.
    monkeypatch.setattr(glossary_router, "_require_glossary_rw", lambda request: None)
    monkeypatch.setattr(glossary_router, "_authority", lambda request: None)
    monkeypatch.setattr(glossary_router, "_curate_authority", lambda request: None)
    monkeypatch.setattr(glossary_router, "_is_org_curator", lambda request: False)
    monkeypatch.setattr(glossary_router, "_notify", _notify)
    return types.SimpleNamespace(db=db, launched=launched, notified=notified)


def _upload(data: bytes, **extra: Any) -> dict:
    return {"file_name": "policy.md", "content_b64": base64.b64encode(data).decode(), **extra}


async def _scanned(api, body: dict) -> dict:
    started = await router.start_scan(_request(body))
    assert started["scan"]["status"] == "running"
    await api.launched.pop()
    return await router.get_scan(started["scan"]["id"], _request())


def _by_name(found: dict) -> dict[str, dict]:
    return {d["name"]: d for d in found["drafts"]}


async def _names(api) -> dict[str, int]:
    async with api.db.acquire() as conn:
        return {t["name"]: t["id"] for t in await glossary_repo.list_terms(conn)}


async def _synonym_edges(api) -> set[tuple[str, str]]:
    names = {v: k for k, v in (await _names(api)).items()}
    async with api.db.acquire() as conn:
        edges = await glossary_repo.term_edges(conn)
    return {(names[f], names[t]) for f, t, rel in edges if rel == "SYNONYM_OF"}


async def test_a_new_document_joins_the_library_as_proposed_and_is_scanned_into_drafts(api):
    async with api.db.acquire() as conn:
        await glossary_repo.create_abstract_term(conn, "client", domains={"d"})
    found = await _scanned(api, _upload(POLICY, domains=["d"]))

    async with api.db.acquire() as conn:
        (document,) = await store.documents(conn)
    assert (document.status, document.domains, document.added_by) == ("proposed", ("d",), "alice")
    assert found["scan"]["status"] == "done"
    assert found["scan"]["summary"]["left_out"]["not_a_concept"] == 1
    assert found["scan"]["summary"]["left_out"]["quote_not_in_passage"] == 1

    drafts = _by_name(found)
    assert list(drafts) == [
        "customer",
        "know your customer check",
        "account (customer relationship)",
        "account (ledger)",
        "record retention period",
    ]
    # One concept, two mentions, its variant matching the glossary's "client": a merge.
    customer = drafts["customer"]
    assert (customer["proposal"], customer["match_kind"]) == ("merge", "synonym")
    assert customer["match"]["name"] == "client"
    assert customer["definition_kind"] == "explicit"
    assert customer["definition"].startswith('"Customer" means any party')
    assert [p["role"] for p in customer["passages"]] == ["definition", "supporting"]
    assert {v["text"]: v["keep"] for v in customer["variants"]} == {
        "client": True,
        "Customers": False,
    }
    first = customer["passages"][0]
    assert (first["section"], first["quote"]) == ("Definitions", customer["definition"])

    # One word, two meanings: two candidates, each scoped by its own passage.
    relationship = drafts["account (customer relationship)"]
    assert relationship["homonyms"] == ["account (ledger)"]
    assert [p["section"] for p in relationship["passages"]] == ["Accounts"]
    assert [p["section"] for p in drafts["account (ledger)"]["passages"]] == ["Ledger"]

    # No explicit definition: a synthesized one, every passage it drew on listed; explanations
    # that cannot both be true side by side.
    retention = drafts["record retention period"]
    assert retention["definition_kind"] == "synthesized"
    assert [p["role"] for p in retention["passages"]] == ["synthesis", "synthesis"]
    ((one, other),) = retention["conflicts"]
    assert {one["quote"], other["quote"]} == {
        "Customer records are kept for seven years.",
        "Customer records are kept for five years.",
    }


async def test_the_steward_edits_accepts_merges_rejects_and_bulk_accepts(api):
    async with api.db.acquire() as conn:
        client = await glossary_repo.create_abstract_term(conn, "client", domains={"d"})
    found = await _scanned(api, _upload(POLICY, domains=["d"]))
    drafts = _by_name(found)

    kyc = drafts["know your customer check"]
    edited = await router.edit_draft(
        kyc["id"], _request({"synonyms": ["KYC", "id check"], "definition": "Identity checks."})
    )
    assert edited["definition"] == "Identity checks."
    assert {v["text"]: v["keep"] for v in edited["variants"]} == {"KYC": True, "id check": True}
    accepted = await router.accept_draft(kyc["id"], _request())
    names = await _names(api)
    assert accepted["term_id"] == names["know your customer check"]
    async with api.db.acquire() as conn:
        term = await glossary_repo.get_term(conn, accepted["term_id"])
    assert term is not None
    assert (term["definition"], term["domains"]) == ("Identity checks.", ["d"])

    merged = await router.merge_draft(drafts["customer"]["id"], _request({}))
    assert merged["term_id"] == client
    assert await _synonym_edges(api) == {
        ("kyc", "know your customer check"),
        ("id check", "know your customer check"),
        ("customer", "client"),
    }
    assert "customers" not in await _names(api)

    assert (await router.reject_draft(drafts["account (ledger)"]["id"], _request()))[
        "status"
    ] == "rejected"

    bulk = await router.bulk_accept(
        found["scan"]["id"],
        _request(
            {
                "draft_ids": [
                    drafts["account (customer relationship)"]["id"],
                    drafts["record retention period"]["id"],
                    kyc["id"],
                ]
            }
        ),
    )
    assert [d["status"] for d in bulk["decided"]] == ["accepted", "accepted"]
    assert bulk["refused"] == [
        {
            "id": kyc["id"],
            "message": f"draft {kyc['id']} ('know your customer check') is already accepted",
        }
    ]
    after = await router.get_scan(found["scan"]["id"], _request())
    assert after["scan"]["counts"] == {"accepted": 3, "merged": 1, "rejected": 1}
    assert "ontology terms accepted from a document" in api.notified


async def test_a_rescan_shows_what_the_glossary_now_holds_as_matches_and_duplicates(api):
    first = await _scanned(api, _upload(POLICY, domains=["d"]))
    kyc = _by_name(first)["know your customer check"]
    await router.accept_draft(kyc["id"], _request())

    # The same content given again is the library's copy: it is not added a second time.
    again = await _scanned(api, _upload(POLICY, domains=["d"]))
    assert again["scan"]["document_id"] == first["scan"]["document_id"]
    async with api.db.acquire() as conn:
        assert len(await store.documents(conn)) == 1

    rescan = _by_name(again)
    assert (
        rescan["know your customer check"]["proposal"],
        rescan["know your customer check"]["match_kind"],
    ) == (
        "merge",
        "duplicate",
    )
    assert rescan["account (ledger)"]["proposal"] == "new"
    # The first scan's decisions stand: nothing is re-reviewed.
    assert (
        _by_name(await router.get_scan(first["scan"]["id"], _request()))[
            "know your customer check"
        ]["status"]
        == "accepted"
    )
    listed = await router.list_scans(_request(), document_id=first["scan"]["document_id"])
    assert [s["id"] for s in listed["scans"]] == [again["scan"]["id"], first["scan"]["id"]]


async def test_a_library_document_is_picked_and_its_removal_is_marked_on_the_scan(api):
    async with api.db.acquire() as conn:
        kept = await library.add(
            conn,
            library.file_content("policy.md", POLICY),
            source_kind=store.FILE,
            domains=["*"],
            status=store.KEPT,
            embedder=EMBEDDER,
            added_by="admin",
        )
    listed = await router.list_documents(_request())
    assert [(d["id"], d["status"]) for d in listed["documents"]] == [(kept, "kept")]
    found = await _scanned(api, {"document_id": kept, "domains": ["d"]})
    assert found["scan"]["document_removed"] is False
    async with api.db.acquire() as conn:
        await library.remove(conn, kept)
    after = await router.get_scan(found["scan"]["id"], _request())
    assert after["scan"]["document_removed"] is True
    assert after["scan"]["document_title"] == "Definitions"
    assert after["drafts"][0]["passages"][0]["section"] == "Definitions"


async def test_a_scan_that_cannot_finish_is_marked_failed_naming_why(api, monkeypatch):
    async def refused(request):
        async def call(prompt: str, max_tokens: int) -> str:
            raise scan.ScanFailed("role steward's LLM budget for the hour is spent")

        return call

    monkeypatch.setattr(router, "_model", refused)
    found = await _scanned(api, _upload(POLICY, domains=["d"]))
    assert found["scan"]["status"] == "failed"
    assert found["scan"]["error"] == "role steward's LLM budget for the hour is spent"
    assert found["drafts"] == []


async def test_a_scan_is_refused_by_name(api):
    async def refused(body: dict) -> ApiError:
        with pytest.raises(ApiError) as caught:
            await router.start_scan(_request(body))
        return caught.value

    both = await refused({**_upload(POLICY), "document_id": 1, "domains": ["d"]})
    assert both.code == "term_finding.one_document"
    missing = await refused({"document_id": 999, "domains": ["d"]})
    assert (missing.status_code, missing.code) == (404, "reference_library.not_found")
    bad_url = await refused({"url": "ftp://x/a", "domains": ["d"]})
    assert bad_url.code == "reference_library.invalid"
