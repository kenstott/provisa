# Copyright (c) 2026 Kenneth Stott
# Canary: 6b1f0d2e-3c4a-4e8b-9a71-2f5d8c0e4b19
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Relationship discovery runs only the kinds asked for, judges semantics by the assigned model's
own key whatever its vendor, and says what each kind found (REQ-413)."""

# Requirements: REQ-413

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pydantic import ValidationError

from provisa.api.admin import discovery
from provisa.api.errors import ApiError
from provisa.discovery.analyzer import RelationshipCandidate
from provisa.discovery.collector import DiscoveryInput, TableMeta

#: The request the router reads only for its capability check, which these tests stand in for.
_REQUEST = cast(Any, object())

_KEY_ENVS = ("ANTHROPIC_API_KEY", "OPENAI_API_KEY", "MISTRAL_API_KEY")


def _table(table_id: int, name: str, columns: list[str]) -> TableMeta:
    return TableMeta(
        table_id=table_id,
        source_id="src1",
        domain_id="d1",
        schema_name="public",
        table_name=name,
        columns=[{"name": c, "type": "integer"} for c in columns],
        sample_values=[],
    )


def _suggestion(confidence: float = 0.9, src_col: str = "customer_id") -> dict:
    return {
        "source_table_id": 1,
        "source_column": src_col,
        "target_table_id": 2,
        "target_column": "id",
        "cardinality": "many-to-one",
        "confidence": confidence,
        "reasoning": "orders name their customer",
        "suggested_name": "purchase-history",
    }


class _Conn:
    def __init__(self, rejected: list[dict]):
        self._rejected = rejected

    async def fetch(self, sql: str, *args: Any) -> list[dict]:
        assert "relationship_candidates" in sql and "rejected" in sql, sql
        return self._rejected


class _Pool:
    def __init__(self, conn: _Conn):
        self._conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self._conn


class _World:
    """The discovery router's surroundings, with what each part was asked recorded."""

    def __init__(self, monkeypatch, *, config: dict, api_keys: dict[str, str]):
        self.model_calls: list[tuple[str, str]] = []
        self.fk_calls = 0
        self.metadata_calls = 0
        self.stored: list[RelationshipCandidate] = []
        self.answer: str | Exception = "[]"
        self.rejected: list[dict] = []
        self.existing: list[dict] = []
        self.fk_found: list[RelationshipCandidate] = []

        for env in _KEY_ENVS:
            monkeypatch.delenv(env, raising=False)
        monkeypatch.setattr(discovery, "require_capability_request", lambda request, cap: None)
        monkeypatch.setattr(
            discovery,
            "state",
            SimpleNamespace(
                model_db=_Pool(_Conn(self.rejected)),
                federation_engine=object(),
                source_pools={},
                source_types={},
            ),
        )

        async def fk(*args, **kwargs):
            self.fk_calls += 1
            return list(self.fk_found)

        async def metadata(*args, **kwargs):
            self.metadata_calls += 1
            return DiscoveryInput(
                tables=[
                    _table(1, "orders", ["id", "customer_id"]),
                    _table(2, "customers", ["id", "name"]),
                ],
                existing_relationships=list(self.existing),
                rejected_pairs=[],
            )

        async def store(conn, candidates, scope):
            self.stored.extend(candidates)
            return list(range(1, len(candidates) + 1))

        async def org_config(pool):
            return config

        async def org_keys(pool):
            return dict(api_keys)

        async def no_jev(pool):
            return None

        def complete(client, vendor, model, prompt, system, max_tokens):
            self.model_calls.append((vendor, model))
            if isinstance(self.answer, Exception):
                raise self.answer
            return self.answer

        monkeypatch.setattr(discovery, "collect_fk_candidates", fk)
        monkeypatch.setattr(discovery, "collect_metadata", metadata)
        monkeypatch.setattr(discovery.candidates_repo, "store_candidates", store)
        monkeypatch.setattr("provisa.core.org_settings.resolve_org_config", org_config)
        monkeypatch.setattr("provisa.core.org_secrets.read_org_api_keys", org_keys)
        monkeypatch.setattr("provisa.core.org_secrets.resolve_jev_api_key", no_jev)
        monkeypatch.setattr("provisa.llm.client.ProvisaLLMClient._complete_sync", complete)

    async def run(self, *kinds: str) -> dict:
        body = discovery.DiscoverRequest.model_validate(
            {"scope": "cross-domain", "kinds": list(kinds)}
        )
        return await discovery.trigger_discovery(_REQUEST, body)


_OPENAI = {"ai_models": {"relationship_inference": {"vendor": "openai", "model": "gpt-test"}}}
_ANTHROPIC = {
    "ai_models": {"relationship_inference": {"vendor": "anthropic", "model": "claude-test"}}
}


def _fk() -> RelationshipCandidate:
    return RelationshipCandidate(1, "customer_id", 2, "id", "many_to_one", 1.0, "Foreign key")


# --- kinds honoured -----------------------------------------------------------------------------


async def test_foreign_keys_only_makes_no_model_call(monkeypatch):
    world = _World(monkeypatch, config=_OPENAI, api_keys={"openai": "k"})
    world.fk_found = [_fk()]
    found = await world.run("foreign_keys")
    assert world.model_calls == []
    assert world.metadata_calls == 0
    assert world.fk_calls == 1
    assert found["foreign_keys"] == {"found": 1}
    assert "semantics" not in found


async def test_foreign_keys_only_runs_with_no_model_configured(monkeypatch):
    world = _World(monkeypatch, config=_OPENAI, api_keys={})
    found = await world.run("foreign_keys")
    assert found["foreign_keys"] == {"found": 0}
    assert world.model_calls == []


async def test_semantics_only_collects_no_foreign_keys(monkeypatch):
    world = _World(monkeypatch, config=_OPENAI, api_keys={"openai": "k"})
    world.answer = json.dumps([_suggestion()])
    found = await world.run("semantics")
    assert world.fk_calls == 0
    assert "foreign_keys" not in found
    assert found["semantics"]["suggested"] == 1
    assert found["semantics"]["empty_reason"] is None
    assert [c.suggested_name for c in world.stored] == ["purchase-history"]


def test_a_run_names_at_least_one_kind():
    with pytest.raises(ValidationError):
        discovery.DiscoverRequest.model_validate({"scope": "cross-domain", "kinds": []})
    with pytest.raises(ValidationError):
        discovery.DiscoverRequest.model_validate({"scope": "cross-domain"})
    with pytest.raises(ValidationError):
        discovery.DiscoverRequest.model_validate({"scope": "cross-domain", "kinds": ["guesses"]})


# --- the assigned model, whatever its vendor ----------------------------------------------------


async def test_a_non_anthropic_assigned_model_with_its_key_runs(monkeypatch):
    world = _World(monkeypatch, config=_OPENAI, api_keys={"openai": "k"})
    world.answer = json.dumps([_suggestion()])
    found = await world.run("foreign_keys", "semantics")
    assert world.model_calls == [("openai", "gpt-test")]
    assert found["semantics"]["suggested"] == 1


async def test_an_anthropic_key_does_not_make_another_vendors_model_runnable(monkeypatch):
    world = _World(monkeypatch, config=_OPENAI, api_keys={"anthropic": "k"})
    kinds = await discovery.discovery_kinds(_REQUEST)
    assert kinds["semantics"] == {
        "available": False,
        "model": "openai:gpt-test",
        "reason": "no_key",
    }
    with pytest.raises(ApiError) as refused:
        await world.run("semantics")
    assert refused.value.code == "discovery.semantics_unavailable"
    assert world.model_calls == []


async def test_the_assigned_models_key_from_the_environment_counts(monkeypatch):
    world = _World(monkeypatch, config=_OPENAI, api_keys={})
    monkeypatch.setenv("OPENAI_API_KEY", "k")
    kinds = await discovery.discovery_kinds(_REQUEST)
    assert kinds["semantics"]["available"] is True
    world.answer = "[]"
    await world.run("semantics")
    assert world.model_calls == [("openai", "gpt-test")]


async def test_no_model_configured_reports_semantics_unavailable(monkeypatch):
    world = _World(monkeypatch, config=_ANTHROPIC, api_keys={})
    kinds = await discovery.discovery_kinds(_REQUEST)
    assert kinds == {
        "foreign_keys": {"available": True},
        "semantics": {"available": False, "model": "anthropic:claude-test", "reason": "no_key"},
    }
    with pytest.raises(ApiError) as refused:
        await world.run("foreign_keys", "semantics")
    assert refused.value.status_code == 409
    assert refused.value.params["model"] == "anthropic:claude-test"
    # Refused before anything ran.
    assert world.fk_calls == 0
    assert world.stored == []


# --- why semantics found none -------------------------------------------------------------------


async def test_an_unreadable_answer_is_reported(monkeypatch):
    world = _World(monkeypatch, config=_OPENAI, api_keys={"openai": "k"})
    world.answer = '[{"source_table_id": 1, oops'
    found = await world.run("semantics")
    assert found["semantics"]["suggested"] == 0
    assert found["semantics"]["empty_reason"] == "unreadable"


async def test_all_below_the_confidence_threshold_is_reported_with_the_count(monkeypatch):
    world = _World(monkeypatch, config=_OPENAI, api_keys={"openai": "k"})
    world.answer = json.dumps([_suggestion(0.3), _suggestion(0.5, src_col="id")])
    found = await world.run("semantics")
    sem = found["semantics"]
    assert sem["suggested"] == 0
    assert sem["empty_reason"] == "filtered"
    assert sem["below_threshold"] == 2
    assert sem["proposed"] == 2
    assert abs(sem["threshold"] - 0.7) <= 1e-14 * 0.7


async def test_all_already_rejected_is_reported(monkeypatch):
    world = _World(monkeypatch, config=_OPENAI, api_keys={"openai": "k"})
    world.rejected.append(
        {
            "source_table_id": 1,
            "source_column": "customer_id",
            "target_table_id": 2,
            "target_column": "id",
        }
    )
    world.answer = json.dumps([_suggestion()])
    found = await world.run("semantics")
    assert found["semantics"]["suggested"] == 0
    assert found["semantics"]["empty_reason"] == "filtered"
    assert found["semantics"]["already_known"] == 1
    assert world.stored == []


async def test_all_already_present_is_reported(monkeypatch):
    world = _World(monkeypatch, config=_OPENAI, api_keys={"openai": "k"})
    world.existing.append(
        {
            "source_table_id": 1,
            "source_column": "customer_id",
            "target_table_id": 2,
            "target_column": "id",
            "cardinality": "many-to-one",
        }
    )
    world.answer = json.dumps([_suggestion()])
    found = await world.run("semantics")
    assert found["semantics"]["empty_reason"] == "filtered"
    assert found["semantics"]["already_known"] == 1


async def test_a_suggestion_the_foreign_keys_found_is_already_present(monkeypatch):
    world = _World(monkeypatch, config=_OPENAI, api_keys={"openai": "k"})
    world.fk_found = [_fk()]
    world.answer = json.dumps([_suggestion()])
    found = await world.run("foreign_keys", "semantics")
    assert found["foreign_keys"] == {"found": 1}
    assert found["semantics"]["already_known"] == 1
    assert found["semantics"]["suggested"] == 0
    assert len(world.stored) == 1


async def test_an_empty_list_from_the_model_is_reported(monkeypatch):
    world = _World(monkeypatch, config=_OPENAI, api_keys={"openai": "k"})
    world.answer = "[]"
    found = await world.run("semantics")
    assert found["semantics"]["empty_reason"] == "none_proposed"


async def test_suggestions_naming_columns_that_do_not_exist_are_counted(monkeypatch):
    world = _World(monkeypatch, config=_OPENAI, api_keys={"openai": "k"})
    world.answer = json.dumps([_suggestion(src_col="no_such_column")])
    found = await world.run("semantics")
    assert found["semantics"]["empty_reason"] == "filtered"
    assert found["semantics"]["invalid"] == 1


# --- a model failure surfaces -------------------------------------------------------------------


@pytest.mark.parametrize(
    "answer, said",
    [
        (RuntimeError("401 invalid x-api-key"), "401 invalid x-api-key"),
        (TimeoutError("openai:gpt-test gave no answer within 300 seconds"), "no answer within"),
        ("", "empty answer"),
        ("I can't help with that request.", "I can't help with that request."),
    ],
    ids=["bad-key", "timeout", "empty", "refusal"],
)
async def test_a_model_failure_is_returned_as_an_error(monkeypatch, answer, said):
    world = _World(monkeypatch, config=_OPENAI, api_keys={"openai": "k"})
    world.answer = answer
    with pytest.raises(ApiError) as failed:
        await world.run("foreign_keys", "semantics")
    assert failed.value.status_code == 502
    assert failed.value.code == "discovery.model_failed"
    assert failed.value.params["model"] == "openai:gpt-test"
    assert said in failed.value.params["error"]
    assert said in str(failed.value.detail)
    assert world.stored == []
