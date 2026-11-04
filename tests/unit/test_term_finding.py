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

# Requirements: REQ-2179, REQ-2185, REQ-2202

from __future__ import annotations

import asyncio
import base64
import json
import re
import threading
import time
import types
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import insert, update

from provisa.api.admin import glossary_router
from provisa.api.admin import reference_library_router as library_api
from provisa.api.admin import term_finding_router as router
from provisa.api.errors import ApiError
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.request_thread import RequestThreadMiddleware
from provisa.core.request_context import current_org
from provisa.core.schema_org import domains
from provisa.core.schema_org import glossary_term_scans as scans_t
from provisa.llm import client as llm_client
from provisa.llm.client import ProvisaLLMClient
from provisa.reference_library import library, store
from provisa.security.llm_budget import BudgetRefused
from provisa.api.admin import term_finding_table as table
from provisa.term_finding import model, queue, scan

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
    def __init__(
        self,
        *,
        refuse: bool = False,
        refuse_after: int | None = None,
        room: int | None = None,
        budget: bool = True,
    ):
        self.refuse = refuse
        self.refuse_after = refuse_after
        self.admits = 0
        self.room = room
        self.admitted = types.SimpleNamespace(budget=object() if budget else None)
        self.debited: list[tuple[Any, str]] = []

    async def admit(self, model_name: str) -> None:
        if self.refuse or self.admits == self.refuse_after:
            raise BudgetRefused("role steward's LLM budget for the day is spent")
        self.admits += 1

    def allowance(self, model_name: str) -> int | None:
        return self.room

    async def debit(self, usage: Any, model_name: str) -> None:
        self.debited.append((usage, model_name))


class _Client:
    vendor = "anthropic"
    model = "claude-test"

    def __init__(self, usage: Any, *, cut_off: bool = False):
        self.usage = usage
        self.cut_off = cut_off
        self.asked: list[tuple[str, int]] = []
        self.reasoning_off: list[bool] = []

    async def complete_detailed(
        self, prompt: str, system: str, max_tokens: int, *, reasoning_off: bool
    ) -> Any:
        self.asked.append((prompt, max_tokens))
        self.reasoning_off.append(reasoning_off)
        return types.SimpleNamespace(
            text="[]",
            vendor="anthropic",
            model="claude-test",
            usage=self.usage,
            cut_off=self.cut_off,
        )


USAGE = types.SimpleNamespace(prompt_tokens=100, completion_tokens=20, prompt_tokens_details=None)


async def test_a_call_is_limited_to_the_roles_room_and_debited_with_what_it_used():
    guard, client = _Guard(room=500), _Client(USAGE)
    call = model.Held(client, guard, None)
    assert await call("find terms", 4096) == "[]"
    assert client.asked[0][1] == 500
    assert client.reasoning_off == [True]
    ((usage, model_name),) = guard.debited
    assert (usage.input_tokens, usage.output_tokens, model_name) == (100, 20, "claude-test")


async def test_a_refused_budget_ends_the_scan_naming_the_window():
    call = model.Held(_Client(USAGE), _Guard(refuse=True), None)
    with pytest.raises(scan.ScanFailed, match="budget for the day is spent"):
        await call("find terms", 4096)


async def test_a_call_with_no_reported_usage_is_refused_when_a_budget_holds_the_role():
    with pytest.raises(scan.ScanFailed, match="reported no usage"):
        await model.Held(_Client(None), _Guard(), None)("p", 10)
    assert await model.Held(_Client(None), _Guard(budget=False), None)("p", 10) == "[]"


async def test_a_cut_off_answer_is_counted_and_raised_naming_the_limit_it_stopped_at():
    guard = _Guard(room=500)
    with pytest.raises(scan.AnswerCutOff, match="anthropic:claude-test .* 500 output tokens"):
        await model.Held(_Client(USAGE, cut_off=True), guard, None)("find terms", 4096)
    assert len(guard.debited) == 1


# -- the model's reasoning ------------------------------------------------------------------------


class _Wire:
    """aisuite with the request recorded as the vendor would receive it: the provider each client
    is configured for, and every argument of every completion asked of it."""

    def __init__(self) -> None:
        self.configs: list[dict] = []
        self.sent: list[dict[str, Any]] = []
        self.chat = types.SimpleNamespace(completions=types.SimpleNamespace(create=self.create))

    def __call__(self, config: dict | None = None) -> _Wire:
        self.configs.append(config or {})
        return self

    def create(self, **request: Any) -> Any:
        self.sent.append(request)
        return types.SimpleNamespace(
            choices=[
                types.SimpleNamespace(
                    message=types.SimpleNamespace(content="[]"), finish_reason="stop"
                )
            ],
            usage=USAGE,
        )


#: The endpoint the "Demo bundles" build scans through (scripts/demo_bundle._ANSWERING_ENDPOINT).
OPENROUTER = {
    "id": "openrouter",
    "style": "openai",
    "base_url": "https://openrouter.ai/api/v1",
    "api_key_env": "OPENROUTER_API_KEY",
}


async def test_a_call_through_openrouter_turns_the_models_reasoning_off(monkeypatch):
    """Demo bundles 41930549595: anthropic/claude-haiku-5.5 on OpenRouter reasons by default
    (default_effort medium) and spent all of 4096, then 8192, output tokens before any text. The
    request carries OpenRouter's ``reasoning`` object with reasoning disabled."""
    wire = _Wire()
    monkeypatch.setattr("aisuite.Client", wire)
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-key")
    config = {
        "ai_endpoints": [OPENROUTER],
        "ai_models": {
            model.OPERATION: {"vendor": "openrouter", "model": "anthropic/claude-haiku-5.5"}
        },
    }
    client = ProvisaLLMClient(model.OPERATION, config=config)
    assert await model.Held(client, _Guard(budget=False), None)("find terms", 4096) == "[]"
    ((sent,),) = [wire.sent]
    assert sent["model"] == "openai:anthropic/claude-haiku-5.5"
    assert sent["max_tokens"] == 4096
    assert sent["extra_body"] == {"reasoning": {"enabled": False}}
    assert wire.configs == [{"openai": {"base_url": OPENROUTER["base_url"], "api_key": "test-key"}}]


async def test_a_call_to_anthropic_turns_extended_thinking_off(monkeypatch):
    wire = _Wire()
    monkeypatch.setattr("aisuite.Client", wire)
    config = {"ai_models": {model.OPERATION: {"vendor": "anthropic", "model": "claude-test"}}}
    client = ProvisaLLMClient(model.OPERATION, config=config, api_keys={"anthropic": "k"})
    await model.Held(client, _Guard(budget=False), None)("find terms", 4096)
    ((sent,),) = [wire.sent]
    assert sent["thinking"] == {"type": "disabled"}
    assert "extra_body" not in sent


async def test_a_vendor_whose_reasoning_cannot_be_turned_off_fails_the_call_naming_it(monkeypatch):
    """No silent fallback: a vendor Provisa has no documented way to turn reasoning off for is not
    asked as though it had none."""
    wire = _Wire()
    monkeypatch.setattr("aisuite.Client", wire)
    config = {"ai_models": {model.OPERATION: {"vendor": "groq", "model": "some-model"}}}
    client = ProvisaLLMClient(model.OPERATION, config=config, api_keys={"groq": "k"})
    with pytest.raises(scan.ScanFailed, match="groq:some-model failed: .*reasoning off"):
        await model.Held(client, _Guard(budget=False), None)("find terms", 4096)
    assert wire.sent == []


# -- an answer cut off, an answer that is not JSON ------------------------------------------------


PARTS = [scan.Passage(n, "Terms", n + 1, f"text {n}") for n in range(5)]


def _read(prompt: str) -> list[int]:
    return [int(n) for n in re.findall(r"^\[P(\d+)\]", prompt, re.MULTILINE)]


class _Reader:
    """A model that answers each passage with one concept, or cuts off / garbles when told to."""

    def __init__(
        self,
        *,
        cut: Callable[[list[int]], bool] = lambda read: False,
        cut_merge: bool = False,
        garbled: Callable[[int, str], bool] = lambda asked, prompt: False,
    ):
        self.cut = cut
        self.cut_merge = cut_merge
        self.garbled = garbled
        self.asked: list[str] = []

    @property
    def batches(self) -> list[list[int]]:
        return [_read(p) for p in self.asked if "Group the mentions" not in p]

    async def __call__(self, prompt: str, max_tokens: int) -> str:
        self.asked.append(prompt)
        merging = "Group the mentions" in prompt
        if self.cut_merge if merging else self.cut(_read(prompt)):
            raise scan.AnswerCutOff(
                f"fake:model stopped at its limit of {max_tokens} output tokens"
            )
        if self.garbled(len(self.asked), prompt):
            return '[\n  {"passage": 0, '
        if merging:
            ids = [int(n) for n in re.findall(r"^M(\d+) ", prompt, re.MULTILINE)]
            return json.dumps([{"name": "c", "sense": "s", "mentions": ids}])
        return json.dumps(
            [
                {
                    "passage": n,
                    "name": f"concept {n}",
                    "category": "role",
                    "sense": "s",
                    "quote": f"text {n}",
                    "explicit": True,
                    "variants": [],
                }
                for n in _read(prompt)
            ]
        )


class _Steps:
    def __init__(self, stop_at: int | None = None):
        self.heard: list[tuple[str, int, int]] = []
        self.stop_at = stop_at

    async def __call__(self, step: str, batch: int, batches: int) -> None:
        if len(self.heard) == self.stop_at:
            raise scan.ScanStopped
        self.heard.append((step, batch, batches))


async def test_a_cut_off_batch_is_read_in_halves_and_the_steps_count_the_extra_calls(monkeypatch):
    monkeypatch.setattr(scan, "batches", lambda p: [list(p[:4]), list(p[4:])])
    reader, steps = _Reader(cut=lambda read: len(read) > 2), _Steps()
    result = await scan.run("t", PARTS, reader, steps)
    assert reader.batches == [[0, 1, 2, 3], [0, 1], [2, 3], [4]]
    assert steps.heard == [
        ("reading", 1, 2),
        ("reading", 2, 4),
        ("reading", 3, 4),
        ("reading", 4, 4),
        ("merging", 0, 0),
    ]
    assert result.calls == len(reader.asked) == 5
    assert result.mentions == 5
    assert [p["ordinal"] for p in result.candidates[0].passages] == [0, 1, 2, 3, 4]


async def test_a_batch_cut_off_again_is_split_again():
    reader = _Reader(cut=lambda read: len(read) > 1)
    result = await scan.run("t", PARTS, reader, _Steps())
    assert reader.batches[:4] == [[0, 1, 2, 3, 4], [0, 1], [0], [1]]
    assert result.mentions == 5
    assert result.calls == 2 * len(PARTS) - 1 + 1


async def test_a_passage_cut_off_on_its_own_is_named():
    reader = _Reader(cut=lambda read: 2 in read)
    with pytest.raises(scan.ScanFailed) as failed:
        await scan.run("t", PARTS, reader, _Steps())
    assert str(failed.value) == (
        "the model's answer to passage 2 (section 'Terms', page 3) was cut off with that passage "
        "read alone: fake:model stopped at its limit of 4096 output tokens"
    )
    assert reader.batches == [[0, 1, 2, 3, 4], [0, 1], [2, 3, 4], [2]]


async def test_a_cut_off_resolution_fails_saying_so_and_is_asked_once():
    reader = _Reader(cut_merge=True)
    with pytest.raises(scan.ScanFailed) as failed:
        await scan.run("t", PARTS, reader, _Steps())
    assert str(failed.value) == (
        "the model's answer to the mentions' resolution was cut off: "
        "fake:model stopped at its limit of 8192 output tokens"
    )
    assert len(reader.asked) == 2


async def test_a_scan_stopped_between_the_halves_makes_no_further_call():
    reader = _Reader(cut=lambda read: len(read) > 2)
    with pytest.raises(scan.ScanStopped):
        await scan.run("t", PARTS, reader, _Steps(stop_at=1))
    assert len(reader.asked) == 1


@pytest.mark.parametrize(
    ("garbled", "heard"),
    [
        (
            lambda asked, prompt: asked == 1,
            [("reading", 1, 1), ("reading", 2, 2), ("merging", 0, 0)],
        ),
        (
            lambda asked, prompt: asked == 2,
            [("reading", 1, 1), ("merging", 0, 0), ("merging", 0, 0)],
        ),
    ],
    ids=["reading", "merging"],
)
async def test_an_answer_that_is_not_json_is_asked_for_once_more(garbled, heard):
    reader, steps = _Reader(garbled=garbled), _Steps()
    result = await scan.run("t", PARTS, reader, steps)
    assert steps.heard == heard
    assert result.calls == len(reader.asked) == 3
    assert result.mentions == 5
    assert len(result.candidates) == 1


async def test_an_answer_that_is_not_json_twice_is_named():
    reader = _Reader(garbled=lambda asked, prompt: True)
    with pytest.raises(scan.ScanFailed, match="asked twice") as failed:
        await scan.run("t", PARTS, reader, _Steps())
    assert "the model's answer to passages 0-4 was not JSON" in str(failed.value)
    assert len(reader.asked) == 2


async def test_a_resolution_that_is_not_json_twice_is_named():
    reader = _Reader(garbled=lambda asked, prompt: "Group the mentions" in prompt)
    with pytest.raises(scan.ScanFailed, match="asked twice") as failed:
        await scan.run("t", PARTS, reader, _Steps())
    assert "the model's answer to the mentions' resolution was not JSON" in str(failed.value)
    assert len(reader.asked) == 3


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


def fake_answer(prompt: str) -> str:
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


class _Model:
    """The model a scan asks, answering at once."""

    name = "fake:model"
    limited_to = None

    def __init__(self, answer: Callable[[str], str] = fake_answer):
        self.answer = answer

    async def __call__(self, prompt: str, max_tokens: int) -> str:
        return self.answer(prompt)


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
        return _Model()

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
    def spent(prompt: str) -> str:
        raise scan.ScanFailed("role steward's LLM budget for the hour is spent")

    async def refused(request):
        return _Model(spent)

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


# -- a scan finishes, or stops saying why ---------------------------------------------------------

REAL_LAUNCH = router._launch


def _served(api) -> TestClient:
    """The router behind the middleware every HTTP request passes (REQ-1882): the request runs on
    a loop checked out for it, and that loop stops when the request ends."""

    async def as_steward(scope, receive, send):
        scope.setdefault("state", {}).update(_request().state.__dict__)
        await served(scope, receive, send)

    served = FastAPI()
    served.include_router(router.router)
    return TestClient(RequestThreadMiddleware(as_steward))


def _settled(client: TestClient, scan_id: int) -> dict:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        found = client.get(f"/admin/glossary/term-finding/scans/{scan_id}").json()
        if found["scan"]["status"] != "running":
            return found
        time.sleep(0.05)
    raise AssertionError(f"scan {scan_id} is still running: {found['scan']}")


class _Vendor:
    """aisuite with the vendor's answers replaced: the rest of the call is the real client's."""

    def __init__(
        self,
        answer: Callable[[str], str | None] = fake_answer,
        finish: Callable[[str], str] = lambda prompt: "stop",
    ):
        self.asked = 0
        self.answer = answer
        self.finish = finish
        self.chat = types.SimpleNamespace(
            completions=types.SimpleNamespace(create=self.create),
        )

    def __call__(self, *args: Any) -> _Vendor:
        return self

    def create(self, *, model: str, messages: list[dict], max_tokens: int, thinking: dict) -> Any:
        self.asked += 1
        prompt = messages[-1]["content"]
        return types.SimpleNamespace(
            choices=[
                types.SimpleNamespace(
                    message=types.SimpleNamespace(content=self.answer(prompt)),
                    finish_reason=self.finish(prompt),
                )
            ],
            usage=USAGE,
        )


def _real_client(monkeypatch, vendor: _Vendor, guard: _Guard | None = None) -> Any:
    monkeypatch.setattr("aisuite.Client", vendor)
    config = {"ai_models": {model.OPERATION: {"vendor": "anthropic", "model": "fake-model"}}}
    client = ProvisaLLMClient(model.OPERATION, config=config, api_keys={"anthropic": "k"})
    held = guard or _Guard(budget=False)

    async def _model(request):
        return model.Held(client, held, None)

    monkeypatch.setattr(router, "_model", _model)
    return client


# -- a document too large to resolve in one answer ------------------------------------------------

#: A definitions section as long as 31 CFR 1010.100: one defined term per passage.
DEFINED = [
    scan.Passage(n, "1010.100", None, f"Term {n} means the regulated thing numbered {n}.")
    for n in range(180)
]
#: A group as the model writes it: a sense, a synthesized definition and its mentions.
SYNTHESIS = "A regulated thing the definitions section names, as its passages explain it. " * 3


def _definitions_answer(prompt: str) -> str:
    """A model's answer to a reading, a resolution or a joining of :data:`DEFINED`."""
    if "Group the mentions" in prompt:
        return json.dumps(
            [
                {
                    "name": name,
                    "sense": "a regulated thing",
                    "mentions": [int(n)],
                    "variants": [],
                    "conflicts": [],
                    "definition": SYNTHESIS,
                    "uses": [int(n)],
                }
                for n, name in re.findall(r"^M(\d+) \[[^]]+\] '([^']+)'", prompt, re.MULTILINE)
            ]
        )
    if "one concept" in prompt and "G0 " in prompt:
        return "[]"
    return json.dumps(
        [
            {
                "passage": n,
                "name": f"term {n}",
                "category": "business concept",
                "sense": "s",
                "quote": DEFINED[n].text,
                "explicit": False,
                "variants": [],
            }
            for n in _read(prompt)
        ]
    )


class _Spending(_Vendor):
    """A vendor whose answer costs a token for every :data:`model.CHARS_PER_TOKEN` characters,
    stopping at ``max_tokens`` with ``finish_reason='length'`` as a real one does."""

    chars_per_token = model.CHARS_PER_TOKEN

    def __init__(self) -> None:
        super().__init__(_definitions_answer)
        self.cut: list[int] = []

    def create(self, *, model: str, messages: list[dict], max_tokens: int, thinking: dict) -> Any:
        answered = super().create(
            model=model, messages=messages, max_tokens=max_tokens, thinking=thinking
        )
        choice = answered.choices[0]
        held = max_tokens * self.chars_per_token
        if len(choice.message.content) > held:
            self.cut.append(max_tokens)
            choice.message.content = choice.message.content[:held]
            choice.finish_reason = "length"
        return answered


async def test_a_document_whose_resolution_outgrows_one_answer_is_resolved_in_parts(monkeypatch):
    """Demo bundles 41882977548: 31 CFR 1010.100's mentions resolved in one call came back cut
    off at 8192 output tokens. Each resolution call is sized to fit its answer."""
    vendor = _Spending()
    monkeypatch.setattr("aisuite.Client", vendor)
    config = {"ai_models": {model.OPERATION: {"vendor": "anthropic", "model": "fake-model"}}}
    client = ProvisaLLMClient(model.OPERATION, config=config, api_keys={"anthropic": "k"})
    call = model.Held(client, _Guard(budget=False), None)
    result = await scan.run("31 CFR 1010.100", DEFINED, call, _Steps())
    assert scan.RESOLVE_MAX_TOKENS not in vendor.cut
    assert result.mentions == len(DEFINED)
    assert [c.name for c in result.candidates] == [f"term {n}" for n in range(len(DEFINED))]
    assert result.left_out.unresolved == 0
    assert result.calls == vendor.asked


def _mention(id: int, name: str, *variants: str) -> scan.Mention:
    return scan.Mention(
        id, id, name, "role", "s", "q", False, tuple(scan.Variant(v, "form") for v in variants)
    )


def test_mentions_named_alike_are_resolved_in_one_part():
    mentions = [
        _mention(0, "customer"),
        _mention(1, "kyc"),
        _mention(2, "the customers"),
        _mention(3, "know your customer", "KYC"),
        _mention(4, "ledger"),
    ]
    parts = scan.resolution_batches(mentions, limit=2)
    assert [[m.id for m in part] for part in parts] == [[0, 2], [1, 3], [4]]


def test_a_set_named_alike_larger_than_a_part_is_a_part_of_its_own():
    mentions = [_mention(0, "ledger"), *(_mention(n, "account") for n in range(1, 5))]
    parts = scan.resolution_batches(mentions, limit=2)
    assert [[m.id for m in part] for part in parts] == [[0], [1, 2, 3, 4]]


class _Parts(_Reader):
    """A model that resolves each mention into a group of its own and joins the groups the
    joining answer names."""

    def __init__(self, joins: list[list[int]], *, cut_join: bool = False):
        super().__init__()
        self.joins = joins
        self.cut_join = cut_join

    async def __call__(self, prompt: str, max_tokens: int) -> str:
        if "one concept" not in prompt or not prompt.count("\nG0 "):
            return await super().__call__(prompt, max_tokens)
        self.asked.append(prompt)
        if self.cut_join:
            raise scan.AnswerCutOff(
                f"fake:model stopped at its limit of {max_tokens} output tokens"
            )
        return json.dumps(self.joins)


async def test_concepts_named_differently_in_different_parts_are_joined(monkeypatch):
    monkeypatch.setattr(scan, "RESOLVE_BATCH_MENTIONS", 2)
    monkeypatch.setattr(
        scan, "resolution_batches", lambda m: [list(m[i : i + 2]) for i in range(0, len(m), 2)]
    )

    class _Each(_Parts):
        async def __call__(self, prompt: str, max_tokens: int) -> str:
            if "Group the mentions" in prompt:
                self.asked.append(prompt)
                return json.dumps(
                    [
                        {"name": f"concept {n}", "sense": "s", "mentions": [int(n)]}
                        for n in re.findall(r"^M(\d+) ", prompt, re.MULTILINE)
                    ]
                )
            return await super().__call__(prompt, max_tokens)

    reader, steps = _Each([[0, 3], [1, 4, 0]]), _Steps()
    result = await scan.run("t", PARTS, reader, steps)
    assert [(c.name, [p["ordinal"] for p in c.passages]) for c in result.candidates] == [
        ("concept 0", [0, 3]),
        ("concept 1", [1, 4]),
        ("concept 2", [2]),
    ]
    assert {"text": "concept 3", "kind": "alternative name", "keep": True} in (
        result.candidates[0].variants
    )
    assert steps.heard[-4:] == [("merging", 0, 0)] * 4
    assert result.calls == len(reader.asked) == 1 + 3 + 1


async def test_a_cut_off_part_or_joining_fails_saying_which(monkeypatch):
    monkeypatch.setattr(
        scan, "resolution_batches", lambda m: [list(m[i : i + 3]) for i in range(0, len(m), 3)]
    )
    with pytest.raises(scan.ScanFailed) as failed:
        await scan.run("t", PARTS, _Reader(cut_merge=True), _Steps())
    assert str(failed.value) == (
        "the model's answer to the mentions' resolution (part 1 of 2) was cut off: "
        "fake:model stopped at its limit of 8192 output tokens"
    )
    with pytest.raises(scan.ScanFailed) as failed:
        await scan.run("t", PARTS, _Parts([], cut_join=True), _Steps())
    assert str(failed.value) == (
        "the model's answer to the concepts' joining was cut off: "
        "fake:model stopped at its limit of 8192 output tokens"
    )


def test_a_joining_answer_keeps_each_group_in_its_first_set():
    groups = [
        scan.Group(f"g{n}", "s", (n,), (), (), "made up" if n == 2 else None, (n,))
        for n in range(4)
    ]
    left_out = scan.LeftOut()
    joined = scan.read_joins(json.dumps([[0, 2], [2, 3], "x", [9]]), groups, left_out)
    assert [(g.name, g.mentions, g.definition, g.uses) for g in joined] == [
        ("g0", (0, 2), "made up", (2,)),
        ("g1", (1,), None, (1,)),
        ("g3", (3,), None, (3,)),
    ]
    assert left_out.malformed == 1


def test_a_scan_outlives_the_request_and_ends_with_its_candidates(api, monkeypatch):
    """The real path but for the vendor: the request's loop stops when it has answered."""
    vendor = _Vendor()
    _real_client(monkeypatch, vendor)
    monkeypatch.setattr(router, "_launch", REAL_LAUNCH)
    with _served(api) as client:
        started = client.post(
            "/admin/glossary/term-finding/scans", json=_upload(POLICY, domains=["d"])
        ).json()
        assert started["scan"]["model"] == "anthropic:fake-model"
        found = _settled(client, started["scan"]["id"])
    assert found["scan"]["status"] == "done", found["scan"]
    assert len(found["drafts"]) == 5
    assert vendor.asked == found["scan"]["summary"]["calls"]
    assert found["scan"]["step"] == "merging"


async def _running(api, *, launch: str = "this", at: datetime | None = None) -> int:
    async with api.db.acquire() as conn:
        scan_id = await queue.start_scan(
            conn,
            document_id=None,
            document_title="t",
            document_source="s",
            domains=["d"],
            started_by="alice",
            model="fake:model",
            launch=launch,
        )
        if at is not None:
            await conn.execute_core(
                update(scans_t).where(scans_t.c.id == scan_id).values(progress_at=at)
            )
    return scan_id


async def _row(api, scan_id: int) -> queue.ScanRow:
    async with api.db.acquire() as conn:
        found = await queue.scan(conn, scan_id)
    assert found is not None
    return found


async def test_each_batch_and_the_merging_step_is_recorded_as_the_scan_goes(api, monkeypatch):
    monkeypatch.setattr(scan, "BATCH_CHARS", 1)
    seen: list[tuple] = []

    def answer(prompt: str) -> str:
        running = next(r for r in rows if r.id == scan_id)
        seen.append((running.step, running.batch, running.batches))
        return fake_answer(prompt)

    rows: list[queue.ScanRow] = []
    real = queue.record_progress

    async def recorded(conn, id, **values):
        moved = await real(conn, id, **values)
        rows[:] = [cast("queue.ScanRow", await queue.scan(conn, id))]
        return moved

    monkeypatch.setattr(queue, "record_progress", recorded)
    monkeypatch.setattr(scan, "batches", lambda p: [[x] for x in p])

    async with api.db.acquire() as conn:
        document = await library.add(
            conn,
            library.file_content("policy.md", POLICY),
            source_kind=store.FILE,
            domains=["*"],
            status=store.KEPT,
            embedder=EMBEDDER,
            added_by="admin",
        )
        passages = len(await store.passages(conn, document))
    scan_id = await _running(api)
    await router.run_scan(api.db, scan_id, document, "t", cast("Any", _Model(answer)))
    assert seen == [
        *(("reading", n, passages) for n in range(1, passages + 1)),
        ("merging", 0, 0),
    ]


async def test_a_stalled_model_fails_the_scan_naming_the_model(api, monkeypatch):
    released = threading.Event()

    def stalls(prompt: str) -> str:
        released.wait(30)
        return "[]"

    _real_client(monkeypatch, _Vendor(stalls))
    monkeypatch.setattr(llm_client, "CALL_TIMEOUT_S", 0.05)
    found = await _scanned(api, _upload(POLICY, domains=["d"]))
    released.set()
    assert found["scan"]["status"] == "failed"
    assert found["scan"]["error"] == "anthropic:fake-model gave no answer within 0.05 seconds"


async def test_an_answer_that_is_garbage_fails_the_scan_saying_so(api, monkeypatch):
    _real_client(monkeypatch, _Vendor(lambda prompt: "I found some terms for you!"))
    found = await _scanned(api, _upload(POLICY, domains=["d"]))
    assert found["scan"]["status"] == "failed"
    assert "was not JSON" in found["scan"]["error"]


def _cut_off_when_more_than_one(prompt: str) -> str:
    return "length" if len(_read(prompt)) > 1 else "stop"


async def test_a_dense_batch_is_read_in_halves_by_the_real_client_and_the_scan_ends(
    api, monkeypatch
):
    def answer(prompt: str) -> str:
        whole = fake_answer(prompt)
        return whole[: len(whole) // 2] if len(_read(prompt)) > 1 else whole

    vendor, guard = _Vendor(answer, _cut_off_when_more_than_one), _Guard()
    _real_client(monkeypatch, vendor, guard)
    found = await _scanned(api, _upload(POLICY, domains=["d"]))
    assert found["scan"]["status"] == "done", found["scan"]
    assert len(found["drafts"]) == 5
    assert found["scan"]["summary"]["calls"] == vendor.asked > 2
    assert guard.admits == len(guard.debited) == vendor.asked
    assert found["scan"]["batch"] == found["scan"]["batches"] == 0


async def test_a_model_that_spends_its_limit_before_any_text_is_read_in_halves(api, monkeypatch):
    def answer(prompt: str) -> str | None:
        return None if len(_read(prompt)) > 1 else fake_answer(prompt)

    vendor, guard = _Vendor(answer, _cut_off_when_more_than_one), _Guard()
    _real_client(monkeypatch, vendor, guard)
    found = await _scanned(api, _upload(POLICY, domains=["d"]))
    assert found["scan"]["status"] == "done", found["scan"]
    assert guard.admits == len(guard.debited) == vendor.asked


async def test_a_passage_cut_off_alone_fails_the_real_scan_naming_it_and_the_budget(
    api, monkeypatch
):
    vendor = _Vendor(lambda prompt: "[{", lambda prompt: "length")
    _real_client(monkeypatch, vendor, _Guard(room=500))
    found = await _scanned(api, _upload(POLICY, domains=["d"]))
    assert found["scan"]["status"] == "failed"
    error = found["scan"]["error"]
    assert re.fullmatch(
        r"the model's answer to passage \d+ \(.*\) was cut off with that passage read alone: "
        r"anthropic:fake-model stopped at its limit of 500 output tokens "
        r"\(the role's LLM budget held a call to 500 output tokens\)",
        error,
    ), error


async def test_the_budget_refuses_an_extra_call_like_any_other(api, monkeypatch):
    vendor = _Vendor(fake_answer, _cut_off_when_more_than_one)
    _real_client(monkeypatch, vendor, _Guard(refuse_after=1))
    found = await _scanned(api, _upload(POLICY, domains=["d"]))
    assert found["scan"]["status"] == "failed"
    assert found["scan"]["error"] == "role steward's LLM budget for the day is spent"
    assert vendor.asked == 1


async def test_an_extra_call_that_stalls_is_given_up_on_like_any_other(api, monkeypatch):
    released = threading.Event()

    def stalls_after_the_first(prompt: str) -> str:
        if len(_read(prompt)) > 1 and vendor.asked == 1:
            return "[{"
        released.wait(30)
        return "[]"

    vendor = _Vendor(stalls_after_the_first, _cut_off_when_more_than_one)
    _real_client(monkeypatch, vendor)
    monkeypatch.setattr(llm_client, "CALL_TIMEOUT_S", 0.05)
    found = await _scanned(api, _upload(POLICY, domains=["d"]))
    released.set()
    assert found["scan"]["status"] == "failed"
    assert found["scan"]["error"] == "anthropic:fake-model gave no answer within 0.05 seconds"
    assert vendor.asked == 2


async def test_a_key_the_vendor_refuses_fails_the_scan_saying_where_to_set_it(api, monkeypatch):
    def refused(prompt: str) -> str:
        error = RuntimeError("Error code: 401")
        error.status_code = 401  # type: ignore[attr-defined]
        raise error

    _real_client(monkeypatch, _Vendor(refused))
    found = await _scanned(api, _upload(POLICY, domains=["d"]))
    assert found["scan"]["error"] == (
        "the vendor refused the API key for anthropic:fake-model; "
        "set a working one under Admin, AI Models"
    )


@pytest.mark.parametrize(
    ("config", "keys", "said"),
    [
        ({}, {}, "no AI model is set for finding terms"),
        ({model.OPERATION: ""}, {}, "no AI model is set for finding terms"),
        ({model.OPERATION: {"vendor": "anthropic"}}, {}, "no AI model is set for finding terms"),
        (
            {model.OPERATION: {"vendor": "anthropic", "model": "m"}},
            {},
            "anthropic:m cannot run: anthropic has no API key. Set one under Admin, AI Models",
        ),
    ],
)
async def test_a_model_that_cannot_run_is_refused_saying_where_to_set_it(
    monkeypatch, config, keys, said
):
    async def resolved(db):
        return {"ai_models": config}

    async def org_keys(db):
        return keys

    monkeypatch.setattr("provisa.core.org_settings.resolve_org_config", resolved)
    monkeypatch.setattr("provisa.core.org_secrets.read_org_api_keys", org_keys)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(model.ModelRefused, match=re.escape(said)) as refused:
        await model.for_role(types.SimpleNamespace(model_db=None, roles={}), "steward")
    assert "Admin, AI Models" in str(refused.value)


async def test_a_scan_that_cannot_run_is_refused_and_adds_no_document(api, monkeypatch):
    async def refused(request):
        raise model.ModelRefused("no AI model is set")

    monkeypatch.setattr(router, "_model", refused)
    with pytest.raises(ApiError) as caught:
        await router.start_scan(_request(_upload(POLICY, domains=["d"])))
    assert (caught.value.status_code, caught.value.code) == (409, "term_finding.no_model")
    assert api.launched == []
    async with api.db.acquire() as conn:
        assert await store.documents(conn) == []
        assert await queue.scans(conn) == []


def test_a_key_is_looked_for_where_the_call_would_look(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ENDPOINT_KEY", raising=False)

    def client(vendor: str, keys: dict[str, str] | None = None, **extra: Any) -> ProvisaLLMClient:
        return ProvisaLLMClient(
            model.OPERATION,
            config={
                "ai_models": {model.OPERATION: {"vendor": vendor, "model": "m", **extra}},
                "ai_endpoints": [
                    {
                        "id": "mine",
                        "style": "openai",
                        "base_url": "http://x",
                        "api_key_env": "ENDPOINT_KEY",
                    },
                    {"id": "open", "style": "openai", "base_url": "http://x"},
                ],
            },
            api_keys=keys,
        )

    assert client("anthropic").key_problem() == "anthropic has no API key"
    assert client("anthropic", {"anthropic": "k"}).key_problem() is None
    assert client("mine").key_problem() == "mine has no API key"
    assert client("open").key_problem() is None
    assert client("ollama").key_problem() is None
    fallback = {"vendor": "ollama", "model": "m"}
    assert client("anthropic", fallback=fallback).key_problem() is None
    fallback = {"vendor": "openai", "model": "m"}
    assert client("anthropic", fallback=fallback).key_problem() == "anthropic has no API key"
    monkeypatch.setenv("ANTHROPIC_API_KEY", "k")
    assert client("anthropic").key_problem() is None


def test_a_stalled_call_is_tried_on_the_fallback(monkeypatch):
    calls: list[str] = []

    def respond(self, vendor, model_name, prompt, system, max_tokens):
        calls.append(vendor)
        if vendor == "anthropic":
            threading.Event().wait(5)
        return llm_client.Completion("answer", vendor, model_name, None, False)

    monkeypatch.setattr(ProvisaLLMClient, "_respond", respond)
    monkeypatch.setattr(llm_client, "CALL_TIMEOUT_S", 0.05)
    fallback = {"vendor": "openai", "model": "n"}
    config = {"ai_models": {"x": {"vendor": "anthropic", "model": "m", "fallback": fallback}}}
    assert ProvisaLLMClient("x", config=config).complete_sync("p") == "answer"
    assert calls == ["anthropic", "openai"]
    with pytest.raises(llm_client.LLMTimeoutError, match="anthropic:m gave no answer"):
        ProvisaLLMClient(
            "x", config={"ai_models": {"x": {"vendor": "anthropic", "model": "m"}}}
        ).complete_sync("p")


def test_a_call_runs_in_the_callers_context(monkeypatch):
    def respond(self, vendor, model_name, prompt, system, max_tokens):
        return llm_client.Completion(str(current_org.get()), vendor, model_name, None, False)

    monkeypatch.setattr(ProvisaLLMClient, "_respond", respond)
    token = current_org.set("acme")
    try:
        config = {"ai_models": {"x": {"vendor": "anthropic", "model": "m"}}}
        assert ProvisaLLMClient("x", config=config).complete_sync("p") == "acme"
    finally:
        current_org.reset(token)


async def test_a_budget_that_refuses_is_shown_on_the_scan_and_no_call_is_made(api, monkeypatch):
    client = _Client(USAGE)

    async def refusing(request):
        return model.Held(client, _Guard(refuse=True), None)

    monkeypatch.setattr(router, "_model", refusing)
    found = await _scanned(api, _upload(POLICY, domains=["d"]))
    assert found["scan"]["status"] == "failed"
    assert found["scan"]["error"] == "role steward's LLM budget for the day is spent"
    assert client.asked == []


async def test_a_budget_that_holds_a_call_short_is_shown(api, monkeypatch):
    held = model.Held(_Client(USAGE), _Guard(room=500), None)
    assert await held("p", 4096) == "[]"
    assert held.limited_to == 500

    class Limited(_Model):
        limited_to = 500

    async def limited(request):
        return Limited()

    monkeypatch.setattr(router, "_model", limited)
    found = await _scanned(api, _upload(POLICY, domains=["d"]))
    assert (found["scan"]["status"], found["scan"]["budget_limit"]) == ("done", 500)

    async def garbage(request):
        return Limited(lambda prompt: "cut sho")

    monkeypatch.setattr(router, "_model", garbage)
    found = await _scanned(api, _upload(POLICY, domains=["d"]))
    assert "held a call to 500 output tokens" in found["scan"]["error"]


async def test_scans_a_restart_left_running_are_failed_saying_so(api):
    old = await _running(api, launch="earlier")
    this = await _running(api, launch="this")
    await router.give_up_on_left_behind(api.db, "this")
    assert ((await _row(api, old)).status, (await _row(api, old)).error) == (
        "failed",
        router.RESTARTED,
    )
    assert (await _row(api, old)).finished_at is not None
    assert (await _row(api, this)).status == "running"


async def test_a_scan_that_predates_launches_is_failed_too(api):
    scan_id = await _running(api)
    async with api.db.acquire() as conn:
        await conn.execute_core(
            update(scans_t).where(scans_t.c.id == scan_id).values(launch=None, progress_at=None)
        )
    await router.give_up_on_left_behind(api.db, "this")
    assert (await _row(api, scan_id)).error == router.RESTARTED


async def test_a_scan_with_no_progress_past_the_limit_is_failed_saying_so(api):
    now = datetime.now(UTC)
    stalled = await _running(api, at=now - router.STALLED_AFTER - timedelta(seconds=1))
    moving = await _running(api, at=now - router.STALLED_AFTER + timedelta(seconds=30))
    await router.give_up_on_left_behind(api.db, "this")
    assert ((await _row(api, stalled)).status, (await _row(api, stalled)).error) == (
        "failed",
        router.STALLED,
    )
    assert (await _row(api, moving)).status == "running"


async def test_the_watch_looks_at_startup_in_every_org_runtime_and_outlives_a_failed_pass(
    api, monkeypatch
):
    scan_id = await _running(api, launch="earlier")
    passes = 0
    real = router.give_up_on_left_behind

    async def give_up(pool, launch):
        nonlocal passes
        passes += 1
        if passes == 1:
            raise RuntimeError("a pass that failed")
        await real(pool, launch)

    monkeypatch.setattr(router, "give_up_on_left_behind", give_up)
    monkeypatch.setattr(router, "KEEP_EVERY_S", 0)
    state = types.SimpleNamespace(
        model_db=api.db,
        org_registry=types.SimpleNamespace(
            runtimes=lambda: [types.SimpleNamespace(org_id="default", env="prod")]
        ),
    )
    watch = asyncio.ensure_future(router.keep(state))
    try:
        for _ in range(200):
            if (await _row(api, scan_id)).status == "failed":
                break
            await asyncio.sleep(0.01)
    finally:
        watch.cancel()
    assert (await _row(api, scan_id)).error == router.RESTARTED


async def test_shutting_down_fails_a_running_scan_saying_so(api):
    scan_id = await _running(api)
    entered = asyncio.Event()

    class Stuck(_Model):
        async def __call__(self, prompt: str, max_tokens: int) -> str:
            entered.set()
            await asyncio.Event().wait()
            return ""

    async with api.db.acquire() as conn:
        document = await library.add(
            conn,
            library.file_content("policy.md", POLICY),
            source_kind=store.FILE,
            domains=["*"],
            status=store.KEPT,
            embedder=EMBEDDER,
            added_by="admin",
        )
    work = asyncio.ensure_future(
        router.run_scan(api.db, scan_id, document, "t", cast("Any", Stuck()))
    )
    await entered.wait()
    work.cancel()
    with pytest.raises(asyncio.CancelledError):
        await work
    assert (await _row(api, scan_id)).error == router.STOPPED


def test_a_person_cancels_a_running_scan_and_it_queues_nothing(api, monkeypatch):
    asked, release = threading.Event(), threading.Event()

    def waits(prompt: str) -> str:
        asked.set()
        release.wait(20)
        return fake_answer(prompt)

    vendor = _Vendor(waits)
    _real_client(monkeypatch, vendor)
    monkeypatch.setattr(router, "_launch", REAL_LAUNCH)
    with _served(api) as client:
        started = client.post(
            "/admin/glossary/term-finding/scans", json=_upload(POLICY, domains=["d"])
        ).json()
        scan_id = started["scan"]["id"]
        assert asked.wait(20)
        cancelled = client.post(f"/admin/glossary/term-finding/scans/{scan_id}/cancel")
        assert cancelled.status_code == 200
        assert cancelled.json()["scan"]["error"] == "cancelled by alice"
        release.set()
        found = _settled(client, scan_id)
        assert (found["scan"]["status"], found["scan"]["error"]) == ("failed", "cancelled by alice")
        assert found["drafts"] == []
        time.sleep(0.2)
        assert vendor.asked == 1
        again = client.post(f"/admin/glossary/term-finding/scans/{scan_id}/cancel")
        assert again.status_code == 409


async def test_an_ended_scan_keeps_its_first_ending(api):
    scan_id = await _running(api)
    async with api.db.acquire() as conn:
        assert await queue.fail_scan(conn, scan_id, "cancelled by alice")
        assert not await queue.fail_scan(conn, scan_id, "later")
        assert not await queue.finish_scan(conn, scan_id, [_candidate("x")], {})
        assert not await queue.record_progress(
            conn, scan_id, step="merging", batch=0, batches=0, budget_limit=None
        )
        assert await queue.drafts(conn, scan_id) == []
    assert (await _row(api, scan_id)).error == "cancelled by alice"


# -- REQ-2202: a registered table scanned; a scan only ---------------------------------------

TABLE_SOURCE = {
    "domain": "d",
    "table": "policy_sections",
    "id_column": "section_id",
    "text_column": "body",
}
TABLE_ROWS = [
    ("POL-1", '"Customer" means any party that holds an account.'),
    ("POL-2", None),
    ("POL-4", "A customer account is opened after KYC checks."),
]


def _row_answer(prompt: str) -> str:
    """Each row read names the concept "customer", quoting the row; the mentions resolve into it."""
    if "Group the mentions" in prompt:
        ids = [int(i) for i in re.findall(r"^M(\d+) ", prompt, re.M)]
        return json.dumps([{"name": "customer", "sense": "a party", "mentions": ids}])
    rows = re.findall(r"\[P(\d+)\] \(row '[^']+'\)\n(.+)", prompt)
    return json.dumps(
        [
            {
                "passage": int(n),
                "name": "customer",
                "category": "business concept",
                "sense": "a party",
                "quote": text,
                "explicit": text.startswith('"Customer" means'),
                "variants": [],
            }
            for n, text in rows
        ]
    )


class _TableModel(_Model):
    def __init__(self, answer=_row_answer, refuse: str | None = None):
        super().__init__(answer)
        self.refuse = refuse
        self.reckoned: list[tuple[list[str], str]] = []

    async def require_room(self, prompts: list[str], what: str) -> None:
        self.reckoned.append((prompts, what))
        if self.refuse:
            raise model.OverBudget(self.refuse)


@pytest.fixture
def tables(api, monkeypatch):
    read: list[tuple[Any, str]] = []

    async def _read(state: Any, source: table.TableSource, role_id: str) -> list[scan.Passage]:
        read.append((source, role_id))
        return table.passages(TABLE_ROWS)

    held = _TableModel()

    async def _model(request):
        return held

    monkeypatch.setattr(table, "read", _read)
    monkeypatch.setattr(router, "_model", _model)
    return types.SimpleNamespace(read=read, model=held)


async def test_a_table_is_scanned_its_candidates_citing_the_table_and_row_ids(api, tables):
    started = await router.start_scan(_request({"table": TABLE_SOURCE, "domains": ["d"]}))
    assert started["scan"]["status"] == "running"
    assert started["scan"]["source_table"] == TABLE_SOURCE
    assert "document" not in started
    await api.launched.pop()

    ((source, role_id),) = tables.read
    assert (source.name, role_id) == ("d.policy_sections", "steward")
    ((prompts, what),) = tables.model.reckoned
    assert what == "d.policy_sections"
    assert prompts == scan.reading_prompts(
        "d.policy_sections", table.passages(TABLE_ROWS), scan.TABLE
    )

    found = await router.get_scan(started["scan"]["id"], _request())
    assert found["scan"]["status"] == "done"
    assert found["scan"]["document_title"] == "d.policy_sections"
    assert found["scan"]["document_removed"] is False
    (draft,) = found["drafts"]
    assert draft["name"] == "customer"
    assert [(p["row"], p["role"]) for p in draft["passages"]] == [
        ("POL-1", "definition"),
        ("POL-4", "supporting"),
    ]
    # A scan only: the reference library holds nothing new.
    async with api.db.acquire() as conn:
        assert await store.documents(conn) == []
        (row,) = await queue.scans(conn)
    assert (row.document_id, row.source_table) == (None, TABLE_SOURCE)


async def test_a_table_scan_the_budget_has_no_room_for_is_refused_before_it_starts(api, tables):
    tables.model.refuse = "role 'steward' has 100 tokens left of its LLM budget for the day"
    with pytest.raises(ApiError) as caught:
        await router.start_scan(_request({"table": TABLE_SOURCE, "domains": ["d"]}))
    assert (caught.value.status_code, caught.value.code) == (409, "term_finding.over_budget")
    assert "100 tokens left" in str(caught.value.detail)
    assert api.launched == []
    async with api.db.acquire() as conn:
        assert await queue.scans(conn) == []


async def test_a_table_with_no_text_to_read_is_refused_before_it_starts(api, tables, monkeypatch):
    async def _empty(state: Any, source: table.TableSource, role_id: str) -> list[scan.Passage]:
        return []

    monkeypatch.setattr(table, "read", _empty)
    with pytest.raises(ApiError) as caught:
        await router.start_scan(_request({"table": TABLE_SOURCE, "domains": ["d"]}))
    assert caught.value.code == "term_finding.no_rows"
    assert api.launched == []


async def test_a_scan_reads_one_source_a_table_or_a_document(api, tables):
    with pytest.raises(ApiError) as caught:
        await router.start_scan(
            _request({"table": TABLE_SOURCE, "document_id": 1, "domains": ["d"]})
        )
    assert caught.value.code == "term_finding.one_document"
    assert tables.read == []


TERMS_SOURCE = {
    "domain": "d",
    "table": "data_dictionary",
    "term_column": "term",
    "definition_column": "meaning",
}
TERMS_ROWS = [
    ("Client", "A party that holds an account with the Bank."),
    ("Churn rate", "The share of customers who leave in a period."),
]


@pytest.fixture
def terms_table(api, monkeypatch):
    read: list[tuple[Any, str]] = []

    async def _rows(state: Any, source: Any, role_id: str) -> list[tuple]:
        read.append((source, role_id))
        return TERMS_ROWS

    async def _no_model(request):
        raise AssertionError("a table of terms asks no model")

    monkeypatch.setattr(table, "rows", _rows)
    monkeypatch.setattr(router, "_model", _no_model)
    return types.SimpleNamespace(read=read)


async def test_a_table_of_terms_is_queued_at_once_matched_against_the_glossary(api, terms_table):
    async with api.db.acquire() as conn:
        client = await glossary_repo.create_abstract_term(conn, "client", domains={"d"})
    started = await router.start_scan(_request({"table": TERMS_SOURCE, "domains": ["d"]}))
    assert started["scan"]["status"] == "done"
    assert started["scan"]["source_table"] == TERMS_SOURCE
    assert api.launched == []
    ((source, role_id),) = terms_table.read
    assert (source.name, role_id) == ("d.data_dictionary", "steward")

    found = await router.get_scan(started["scan"]["id"], _request())
    drafts = _by_name(found)
    assert list(drafts) == ["Client", "Churn rate"]
    assert (drafts["Client"]["proposal"], drafts["Client"]["match_kind"]) == ("merge", "duplicate")
    assert drafts["Client"]["match"]["term_id"] == client
    churn = drafts["Churn rate"]
    assert (churn["proposal"], churn["definition_kind"]) == ("new", "explicit")
    assert churn["definition"] == "The share of customers who leave in a period."
    assert [(p["row"], p["role"]) for p in churn["passages"]] == [("Churn rate", "definition")]
    async with api.db.acquire() as conn:
        assert await store.documents(conn) == []


async def test_a_table_of_terms_with_no_term_is_refused(api, terms_table, monkeypatch):
    async def _blank(state: Any, source: Any, role_id: str) -> list[tuple]:
        return [(None, "A definition with no term.")]

    monkeypatch.setattr(table, "rows", _blank)
    with pytest.raises(ApiError) as caught:
        await router.start_scan(_request({"table": TERMS_SOURCE, "domains": ["d"]}))
    assert caught.value.code == "term_finding.no_rows"
    async with api.db.acquire() as conn:
        assert await queue.scans(conn) == []
