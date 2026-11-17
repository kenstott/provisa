# Copyright (c) 2026 Kenneth Stott
# Canary: 6b1f0d2e-3c4a-4e8b-9a71-2f5d8c0e4b19
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Relationship discovery done while the caller waits (POST /admin/discover/relationships) is a
run, as the dialog's is (REQ-413): it runs only the kinds asked for, judges semantics by the
assigned model's own key whatever its vendor, keeps every proposal it read, and says what each
kind found. On a real model store (SQLite), the model faked at its client boundary."""

# Requirements: REQ-167, REQ-413, REQ-1789

from __future__ import annotations

import json
from typing import Any

import pytest
from pydantic import ValidationError
from sqlalchemy import insert, select

from provisa.api.admin import discovery
from provisa.api.errors import ApiError
from provisa.core import schema_org as so
from provisa.core.database import Database
from provisa.discovery.analyzer import RelationshipCandidate
from provisa.llm.client import Completion
from tests.unit.test_discovery_run_api import (
    _REQUEST,
    _World,
    _candidates,
    _nothing_published,  # noqa: F401 -- the autouse fixture the store's tables rely on
    _store,
    _suggestion,
    _table,
)

_OPENAI = {"ai_models": {"relationship_inference": {"vendor": "openai", "model": "gpt-test"}}}
_ANTHROPIC = {
    "ai_models": {"relationship_inference": {"vendor": "anthropic", "model": "claude-test"}}
}


async def _sales(db: Database) -> None:
    """orders and customers, both of the sales domain."""
    await _table(
        db, 1, "sales", "orders", [("id", "integer", True), ("customer_id", "integer", False)]
    )
    await _table(db, 2, "sales", "customers", [("id", "integer", True), ("name", "text", False)])


class _Discovery:
    """The route's surroundings: a store holding orders and customers, the model's answer and
    whether the vendor said it stopped at its limit, and how often foreign keys were read."""

    def __init__(self, monkeypatch, db: Database, *, config: dict, api_keys: dict[str, str]):
        self.db = db
        self.world = _World(monkeypatch, db, config=config, api_keys=api_keys)
        self.answer: str | Exception = "[]"
        self.cut_off = False
        self.fk_calls = 0
        world = self.world

        def respond(client, vendor, model, prompt, system, max_tokens, *args, **kwargs):
            world.model_calls.append((vendor, model, max_tokens))
            if isinstance(self.answer, Exception):
                raise self.answer
            answer, self.answer = self.answer, "[]"  # a split request's halves propose none
            return Completion(answer, vendor, model, None, self.cut_off)

        async def fk(source_pools, source_types, conn, table_ids):
            self.fk_calls += 1
            return [c for c in world.fk_found if c.source_table_id in table_ids]

        monkeypatch.setattr("provisa.llm.client.ProvisaLLMClient._respond", respond)
        monkeypatch.setattr("provisa.discovery.run.collect_fk_for_tables", fk)

    async def run(self, *kinds: str, scope: str = "cross-domain", **over: Any) -> dict:
        body = discovery.DiscoverRequest.model_validate(
            {"scope": scope, "kinds": list(kinds), **over}
        )
        return await discovery.trigger_discovery(_REQUEST, body)

    async def runs(self) -> int:
        async with self.db.acquire() as conn:
            return len((await conn.execute_core(select(so.discovery_runs.c.id))).fetchall())


async def _discovery(monkeypatch, tmp_path, *, config: dict, api_keys: dict) -> _Discovery:
    db = await _store(f"sqlite+pysqlite:///{tmp_path / 'store.db'}")
    await _sales(db)
    return _Discovery(monkeypatch, db, config=config, api_keys=api_keys)


def _fk() -> RelationshipCandidate:
    return RelationshipCandidate(1, "customer_id", 2, "id", "many-to-one", 1.0, "Foreign key")


async def _listed(run: dict) -> list[dict]:
    return (await discovery.list_proposals(_REQUEST, run["id"]))["proposals"]


# --- a run, stored as the dialog's ------------------------------------------------------------


async def test_the_route_is_a_run_whose_proposals_are_stored(monkeypatch, tmp_path):
    d = await _discovery(monkeypatch, tmp_path, config=_OPENAI, api_keys={"openai": "k"})
    d.answer = json.dumps(
        [_suggestion((1, "customer_id"), (2, "id")), _suggestion((1, "id"), (2, "id"), 0.3)]
    )
    found = await d.run("semantics", scope="table", table_id=1)
    run = found["run"]
    assert run["status"] == "done"
    assert run["scope"]["source_domains"] == ["sales"]
    assert found["candidates_found"] == 1 and len(found["stored_ids"]) == 1
    # Every proposal read is kept with the run, the one under the threshold with why.
    assert [(r["source_column"], r["reason"]) for r in await _listed(run)] == [
        ("customer_id", None),
        ("id", "below_threshold"),
    ]
    assert [c["id"] for c in await _candidates(d.db)] == found["stored_ids"]


async def test_a_table_not_registered_is_refused(monkeypatch, tmp_path):
    d = await _discovery(monkeypatch, tmp_path, config=_OPENAI, api_keys={"openai": "k"})
    with pytest.raises(ApiError) as refused:
        await d.run("foreign_keys", scope="table", table_id=99)
    assert (refused.value.status_code, refused.value.code) == (404, "discovery.table_not_found")
    assert await d.runs() == 0


# --- kinds honoured -----------------------------------------------------------------------------


async def test_foreign_keys_only_makes_no_model_call(monkeypatch, tmp_path):
    d = await _discovery(monkeypatch, tmp_path, config=_OPENAI, api_keys={"openai": "k"})
    d.world.fk_found = [_fk()]
    found = await d.run("foreign_keys")
    assert d.world.model_calls == []
    assert d.fk_calls == 1
    assert found["foreign_keys"] == {"found": 1, "suggested": 1}
    assert "semantics" not in found


async def test_foreign_keys_only_runs_with_no_model_configured(monkeypatch, tmp_path):
    d = await _discovery(monkeypatch, tmp_path, config=_OPENAI, api_keys={})
    found = await d.run("foreign_keys")
    assert found["foreign_keys"] == {"found": 0, "suggested": 0}
    assert d.world.model_calls == []


async def test_semantics_only_collects_no_foreign_keys(monkeypatch, tmp_path):
    d = await _discovery(monkeypatch, tmp_path, config=_OPENAI, api_keys={"openai": "k"})
    d.answer = json.dumps([_suggestion((1, "customer_id"), (2, "id"))])
    found = await d.run("semantics")
    assert d.fk_calls == 0
    assert "foreign_keys" not in found
    assert found["semantics"]["suggested"] == 1
    assert found["semantics"]["empty_reason"] is None
    assert [c["suggested_name"] for c in await _candidates(d.db)] == ["purchase-history"]


def test_a_run_names_at_least_one_kind_and_a_known_scope():
    with pytest.raises(ValidationError):
        discovery.DiscoverRequest.model_validate({"scope": "cross-domain", "kinds": []})
    with pytest.raises(ValidationError):
        discovery.DiscoverRequest.model_validate({"scope": "cross-domain"})
    with pytest.raises(ValidationError):
        discovery.DiscoverRequest.model_validate({"scope": "cross-domain", "kinds": ["guesses"]})
    with pytest.raises(ValidationError):
        discovery.DiscoverRequest.model_validate({"scope": "galaxy", "kinds": ["foreign_keys"]})


# --- the assigned model, whatever its vendor ----------------------------------------------------


async def test_a_non_anthropic_assigned_model_with_its_key_runs(monkeypatch, tmp_path):
    d = await _discovery(monkeypatch, tmp_path, config=_OPENAI, api_keys={"openai": "k"})
    d.answer = json.dumps([_suggestion((1, "customer_id"), (2, "id"))])
    found = await d.run("foreign_keys", "semantics")
    assert [call[:2] for call in d.world.model_calls] == [("openai", "gpt-test")]
    assert found["semantics"]["suggested"] == 1


async def test_an_anthropic_key_does_not_make_another_vendors_model_runnable(monkeypatch, tmp_path):
    d = await _discovery(monkeypatch, tmp_path, config=_OPENAI, api_keys={"anthropic": "k"})
    kinds = await discovery.discovery_kinds(_REQUEST)
    assert kinds["semantics"] == {
        "available": False,
        "model": "openai:gpt-test",
        "reason": "no_key",
    }
    with pytest.raises(ApiError) as refused:
        await d.run("semantics")
    assert refused.value.code == "discovery.semantics_unavailable"
    assert d.world.model_calls == []


async def test_the_assigned_models_key_from_the_environment_counts(monkeypatch, tmp_path):
    d = await _discovery(monkeypatch, tmp_path, config=_OPENAI, api_keys={})
    monkeypatch.setenv("OPENAI_API_KEY", "k")
    kinds = await discovery.discovery_kinds(_REQUEST)
    assert kinds["semantics"]["available"] is True
    await d.run("semantics")
    assert [call[:2] for call in d.world.model_calls] == [("openai", "gpt-test")]


async def test_no_model_configured_reports_semantics_unavailable(monkeypatch, tmp_path):
    d = await _discovery(monkeypatch, tmp_path, config=_ANTHROPIC, api_keys={})
    kinds = await discovery.discovery_kinds(_REQUEST)
    assert kinds == {
        "foreign_keys": {"available": True},
        "semantics": {"available": False, "model": "anthropic:claude-test", "reason": "no_key"},
    }
    with pytest.raises(ApiError) as refused:
        await d.run("foreign_keys", "semantics")
    assert refused.value.status_code == 409
    assert refused.value.params["model"] == "anthropic:claude-test"
    # Refused before anything ran: no run, no foreign keys read, nothing stored.
    assert d.fk_calls == 0
    assert await d.runs() == 0
    assert await _candidates(d.db) == []


# --- why semantics found none -------------------------------------------------------------------


async def test_an_unreadable_answer_is_reported(monkeypatch, tmp_path):
    d = await _discovery(monkeypatch, tmp_path, config=_OPENAI, api_keys={"openai": "k"})
    d.answer = '[{"source_table_id": 1, oops'
    found = await d.run("semantics")
    assert found["semantics"]["suggested"] == 0
    assert found["semantics"]["empty_reason"] == "unreadable"


async def test_all_below_the_confidence_threshold_is_reported_with_the_count(monkeypatch, tmp_path):
    d = await _discovery(monkeypatch, tmp_path, config=_OPENAI, api_keys={"openai": "k"})
    d.answer = json.dumps(
        [
            _suggestion((1, "customer_id"), (2, "id"), 0.3),
            _suggestion((1, "id"), (2, "id"), 0.5),
        ]
    )
    found = await d.run("semantics")
    sem = found["semantics"]
    assert sem["suggested"] == 0
    assert sem["empty_reason"] == "below_threshold"
    assert sem["below_threshold"] == 2
    assert sem["proposed"] == 2
    assert abs(sem["threshold"] - 0.7) <= 1e-14 * 0.7
    # Kept with the run all the same, for a reviewer to tick.
    assert {r["reason"] for r in await _listed(found["run"])} == {"below_threshold"}


async def test_all_already_rejected_is_reported(monkeypatch, tmp_path):
    d = await _discovery(monkeypatch, tmp_path, config=_OPENAI, api_keys={"openai": "k"})
    async with d.db.acquire() as conn:
        await conn.execute_core(
            insert(so.relationship_candidates).values(
                source_table_id=1,
                source_column="customer_id",
                target_table_id=2,
                target_column="id",
                cardinality="many-to-one",
                confidence=0.9,
                status="rejected",
                scope="earlier",
            )
        )
    d.answer = json.dumps([_suggestion((1, "customer_id"), (2, "id"))])
    found = await d.run("semantics")
    assert found["semantics"]["suggested"] == 0
    assert found["semantics"]["empty_reason"] == "filtered"
    assert found["run"]["report"]["removed"]["rejected"] == 1
    assert found["stored_ids"] == []


async def test_all_already_present_is_reported(monkeypatch, tmp_path):
    d = await _discovery(monkeypatch, tmp_path, config=_OPENAI, api_keys={"openai": "k"})
    async with d.db.acquire() as conn:
        await conn.execute_core(
            insert(so.relationships).values(
                id="orders-customer",
                source_table_id=1,
                source_column="customer_id",
                target_table_id=2,
                target_column="id",
                cardinality="many-to-one",
            )
        )
    d.answer = json.dumps([_suggestion((1, "customer_id"), (2, "id"))])
    found = await d.run("semantics")
    assert found["semantics"]["empty_reason"] == "filtered"
    assert found["run"]["report"]["removed"]["existing"] == 1


async def test_a_suggestion_the_foreign_keys_found_is_already_present(monkeypatch, tmp_path):
    d = await _discovery(monkeypatch, tmp_path, config=_OPENAI, api_keys={"openai": "k"})
    d.world.fk_found = [_fk()]
    d.answer = json.dumps([_suggestion((1, "customer_id"), (2, "id"))])
    found = await d.run("foreign_keys", "semantics")
    assert found["foreign_keys"] == {"found": 1, "suggested": 1}
    assert found["run"]["report"]["removed"]["matches_foreign_key"] == 1
    assert found["semantics"]["suggested"] == 0
    assert len(found["stored_ids"]) == 1


async def test_an_empty_list_from_the_model_is_reported(monkeypatch, tmp_path):
    d = await _discovery(monkeypatch, tmp_path, config=_OPENAI, api_keys={"openai": "k"})
    found = await d.run("semantics")
    assert found["semantics"]["empty_reason"] == "none_proposed"


async def test_suggestions_naming_columns_that_do_not_exist_are_counted(monkeypatch, tmp_path):
    d = await _discovery(monkeypatch, tmp_path, config=_OPENAI, api_keys={"openai": "k"})
    d.answer = json.dumps([_suggestion((1, "no_such_column"), (2, "id"))])
    found = await d.run("semantics")
    assert found["semantics"]["suggested"] == 0
    assert found["semantics"]["invalid"] == 1
    assert [r["reason"] for r in await _listed(found["run"])] == ["unknown_column"]


# --- a model failure surfaces -------------------------------------------------------------------


@pytest.mark.parametrize(
    "answer, said",
    [
        (RuntimeError("401 invalid x-api-key"), "401 invalid x-api-key"),
        (TimeoutError("openai:gpt-test gave no answer within 300 seconds"), "no answer within"),
    ],
    ids=["bad-key", "timeout"],
)
async def test_a_model_failure_is_reported_with_its_message(monkeypatch, tmp_path, answer, said):
    d = await _discovery(monkeypatch, tmp_path, config=_OPENAI, api_keys={"openai": "k"})
    d.answer = answer
    found = await d.run("foreign_keys", "semantics")
    semantics = found["semantics"]
    assert semantics["empty_reason"] == "requests_failed"
    assert [said in f["message"] for f in semantics["failed"]] == [True]
    assert found["stored_ids"] == []


@pytest.mark.parametrize(
    "answer", ["", "I can't help with that request."], ids=["empty", "refusal"]
)
async def test_an_answer_that_is_no_list_is_reported_unreadable(monkeypatch, tmp_path, answer):
    d = await _discovery(monkeypatch, tmp_path, config=_OPENAI, api_keys={"openai": "k"})
    d.answer = answer
    found = await d.run("semantics")
    assert found["semantics"]["empty_reason"] == "unreadable"
    assert found["stored_ids"] == []


# --- an answer cut off at the model's token limit -----------------------------------------------
#
# The shape of the answer the default demo's cross-domain run got from claude-haiku-4-5 (captured
# 2026-11-14 on an isolated instance): a ```json fence, a pretty-printed list, and the answer stopped
# at max_tokens in the middle of an element, so neither the list nor the fence ever closed.


def _pretty(element: dict) -> str:
    return "  " + json.dumps(element, indent=2).replace("\n", "\n  ")


_CUT_OFF_ANSWER = (
    "```json\n[\n"
    + _pretty(_suggestion((1, "customer_id"), (2, "id")))
    + ',\n  {\n    "source_table_id": 2,\n    "source_column": "name",\n'
    '    "target_table_id": 1,\n    "target_column": "id",\n    "cardin'
)


async def test_a_cut_off_answer_keeps_its_complete_suggestions(monkeypatch, tmp_path):
    d = await _discovery(monkeypatch, tmp_path, config=_ANTHROPIC, api_keys={"anthropic": "k"})
    d.answer = _CUT_OFF_ANSWER
    d.cut_off = True
    found = await d.run("semantics")
    assert found["semantics"]["proposed"] == 1
    assert found["semantics"]["suggested"] == 1
    assert [(c["source_column"], c["target_column"]) for c in await _candidates(d.db)] == [
        ("customer_id", "id")
    ]


async def test_an_unclosed_list_the_model_did_not_cut_off_is_unreadable(monkeypatch, tmp_path):
    # Only an answer the vendor says stopped at its limit is read element by element; any other
    # broken list is reported as unreadable, never partly kept.
    d = await _discovery(monkeypatch, tmp_path, config=_ANTHROPIC, api_keys={"anthropic": "k"})
    d.answer = _CUT_OFF_ANSWER
    found = await d.run("semantics")
    assert found["semantics"]["empty_reason"] == "unreadable"
    assert found["semantics"]["cut_short"] == 0
    assert found["stored_ids"] == []


async def test_a_complete_answer_is_not_cut_short(monkeypatch, tmp_path):
    d = await _discovery(monkeypatch, tmp_path, config=_ANTHROPIC, api_keys={"anthropic": "k"})
    d.answer = (
        "```json\n" + json.dumps([_suggestion((1, "customer_id"), (2, "id"))], indent=2) + "\n```"
    )
    found = await d.run("semantics")
    assert found["semantics"]["suggested"] == 1
    assert found["semantics"]["cut_short"] == 0


async def test_a_bracket_in_the_text_after_the_list_is_not_taken_for_its_end(monkeypatch, tmp_path):
    d = await _discovery(monkeypatch, tmp_path, config=_ANTHROPIC, api_keys={"anthropic": "k"})
    d.answer = (
        "Here are the relationships:\n"
        + json.dumps([_suggestion((1, "customer_id"), (2, "id"))])
        + "\n\nThe ids follow the schema [see the table list] above."
    )
    found = await d.run("semantics")
    assert found["semantics"]["suggested"] == 1
    assert found["semantics"]["empty_reason"] is None
