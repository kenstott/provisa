# Copyright (c) 2026 Kenneth Stott
# Canary: 5b8e2d71-9c3a-4f06-8e1b-d47a20c6f9e3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An administrative-trail entry belongs to the org the act is about (REQ-2087, REQ-1303): it is
stored in that org's trail and redacted with that org's rules, whichever org the actor is bound to
-- a platform admin acting on org B writes org B's entry under org B's rules."""

# Requirements: REQ-2087, REQ-1303

from __future__ import annotations

import ast
import asyncio
import inspect
from pathlib import Path

import pytest

from provisa.core import org_membership
from provisa.core.model_change import ModelPlane
from provisa.core.request_context import reset_current_org, set_current_org
from provisa.security import redaction

ROOT = Path(__file__).resolve().parents[2]
A_SECRET = "alpha-only-token-1234"
B_SECRET = "bravo-only-token-5678"


@pytest.fixture(autouse=True)
def _fresh_rules():
    redaction.reset()
    yield
    redaction.reset()


def _org(org_id: str, rules: list[dict]) -> None:
    def resolve(reference: str) -> str:
        raise KeyError(reference)

    redaction.set_org(org_id, redaction.parse_rules(rules, "org"), {}, resolve)


class _Store:
    """An org's model store: the rows it was given, and the org whose model it holds."""

    def __init__(self, org_id: str) -> None:
        self.model = ModelPlane(org_id=org_id, env="prod")
        self.written: list[dict] = []

    def acquire(self):
        store = self

        class _Conn:
            async def execute_core(self, statement):
                store.written.append(statement.compile().params)

        class _Acquire:
            async def __aenter__(self):
                return _Conn()

            async def __aexit__(self, *exc):
                return False

        return _Acquire()


def test_a_platform_admin_acting_on_org_b_writes_org_bs_entry_under_org_bs_rules():
    _org("alpha", [{"name": "a", "pattern": A_SECRET}])
    _org("bravo", [{"name": "b", "pattern": B_SECRET}])
    b_store = _Store("bravo")
    # The platform admin's request is bound to their own org, not the org they act on.
    token = set_current_org("alpha")
    try:
        asyncio.run(
            org_membership.record_admin_action(
                b_store,
                org_id="bravo",
                action="grant_org_admin",
                actor_id="platform-admin",
                subject_id="u1",
                detail={"b": B_SECRET, "a": A_SECRET},
            )
        )
    finally:
        reset_current_org(token)
    assert len(b_store.written) == 1
    # Org B's rule redacts its text; org A's rule does not reach org B's entry.
    assert b_store.written[0]["detail"] == {"b": "[REDACTED:b]", "a": A_SECRET}


def test_an_entry_is_refused_by_a_store_of_another_org():
    a_store = _Store("alpha")
    with pytest.raises(org_membership.AdminTrailOfAnotherOrg) as refused:
        asyncio.run(
            org_membership.record_admin_action(
                a_store,
                org_id="bravo",
                action="x",
                actor_id="a",
                subject_id="s",
                detail={},
            )
        )
    assert "bravo" in str(refused.value) and "alpha" in str(refused.value)
    assert a_store.written == []


def test_the_org_is_a_required_argument_with_no_default():
    param = inspect.signature(org_membership.record_admin_action).parameters["org_id"]
    assert param.kind is inspect.Parameter.KEYWORD_ONLY
    assert param.default is inspect.Parameter.empty


def _calls_without_org(path: Path) -> list[int]:
    tree = ast.parse(path.read_text(), filename=str(path))
    missing = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        fn = node.func
        name = fn.attr if isinstance(fn, ast.Attribute) else getattr(fn, "id", None)
        if name != "record_admin_action":
            continue
        if not any(kw.arg == "org_id" for kw in node.keywords):
            missing.append(node.lineno)
    return missing


def test_every_caller_names_the_org_the_entry_is_about():
    callers = 0
    missing: list[str] = []
    for path in sorted((ROOT / "provisa").rglob("*.py")):
        text = path.read_text()
        if "record_admin_action(" not in text:
            continue
        lines = _calls_without_org(path)
        callers += text.count("record_admin_action(") - ("def record_admin_action(" in text)
        missing += [f"{path.relative_to(ROOT)}:{line}" for line in lines]
    assert callers >= 28, f"the walk found only {callers} calls; it no longer sees the callers"
    assert missing == [], f"record_admin_action called without org_id at: {missing}"


def test_the_walk_catches_a_caller_without_the_org(tmp_path):
    bad = tmp_path / "bad.py"
    bad.write_text(
        "async def f(db):\n"
        "    await record_admin_action(db, action='x', actor_id='a', subject_id='s', detail={})\n"
        "    await org_membership.record_admin_action(\n"
        "        db, org_id='o', action='x', actor_id='a', subject_id='s', detail={}\n"
        "    )\n"
    )
    assert _calls_without_org(bad) == [2]
