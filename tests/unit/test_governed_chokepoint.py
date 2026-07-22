# Copyright (c) 2026 Kenneth Stott
# Canary: 2a7f9c14-6d3b-4e81-9f02-7c5a1e8b3d6f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SECURITY: the single governed-execution chokepoint (one pipeline).

The platform's core safety claim is that EVERY byte of data returned to any client
passed through the one governance pipeline — RLS, column masking, column visibility,
write ACL. If a code path could route around it, an attacker (or an accidental
"second pipeline") could egress ungoverned data: unmasked columns, other tenants'
rows. This module makes that claim ENFORCED, two complementary ways:

  1. Runtime (round-trip stamp) — the TOP of the pipeline mints an unforgeable
     capability stamp for every plan; the terminal (_execute_plan) refuses to run
     any plan it did not itself stamp. You can only ask the pipeline whether an
     output came from it; nothing else can mint or forge a stamp. A hand-crafted /
     side-door / replayed-un-minted plan is rejected before a single row is read.

  2. Static (parallel-pipeline ratchet) — a "second pipeline" is characterised by
     CO-LOCATING routing (decide_route) + governance (apply_governance) + execution.
     Exactly one module may do that (the real pipeline); the GraphQL path is a known,
     documented exception pending its own collapse. A NEW module that co-locates the
     signature (a resurrected _compile_govern_execute) fails this test by name.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from provisa.executor.result import QueryResult
from provisa.pgwire import _pipeline
from provisa.pgwire._pipeline import _Plan, _mint_stamp, require_governed_plan, stamp_is_valid
from provisa.transpiler.router import Route

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PROVISA = _REPO_ROOT / "provisa"


# --------------------------------------------------------------------------- #
# 1. Runtime: the round-trip stamp — an un-minted plan cannot execute.
# --------------------------------------------------------------------------- #
def test_stamp_mint_is_valid_and_forgery_is_not():
    minted = _mint_stamp()
    assert stamp_is_valid(minted) is True
    # A caller cannot forge or guess a stamp — the token space is 256-bit random and
    # membership is known only to the pipeline.
    assert stamp_is_valid("f" * 64) is False
    assert stamp_is_valid("") is False
    assert stamp_is_valid(None) is False


async def test_execute_plan_refuses_unstamped_plan():
    """An attacker/side-door that assembles a plan and calls the terminal directly is
    rejected — governed data cannot egress around the pipeline."""
    plan = _Plan(route=Route.ENGINE, sql="SELECT 1", source_id="pg", dialect="trino",
                 physical_sql="SELECT 1")  # no stamp — not produced by the pipeline top
    with pytest.raises(PermissionError, match="ungoverned plan"):
        await _pipeline._execute_plan(plan, state=object())


async def test_execute_plan_refuses_forged_stamp():
    plan = _Plan(route=Route.ENGINE, sql="SELECT 1", source_id="pg", dialect="trino",
                 physical_sql="SELECT 1", stamp="f" * 64)
    with pytest.raises(PermissionError, match="ungoverned plan"):
        await _pipeline._execute_plan(plan, state=object())


async def test_execute_plan_accepts_pipeline_minted_plan():
    """The round-trip: a plan carrying a stamp the pipeline itself minted passes the gate
    and executes. Proves the stamp is not merely a tripwire — the legitimate path works."""

    class _FakeEngine:
        async def execute_engine(self, sql, params=None, session_hints=None):
            return QueryResult(rows=[(1,)], column_names=["n"])

    class _FakeState:
        federation_engine = _FakeEngine()

    plan = _Plan(route=Route.ENGINE, sql="SELECT 1", source_id="pg", dialect="trino",
                 physical_sql="SELECT 1", stamp=_mint_stamp())
    result = await _pipeline._execute_plan(plan, state=_FakeState())
    assert result.rows == [(1,)]


# --------------------------------------------------------------------------- #
# 2. Static: no NEW parallel pipeline may be introduced.
# --------------------------------------------------------------------------- #
# A parallel pipeline is a module that CALLS both decide_route (routing) and
# apply_governance (governance) — i.e. re-assembles the govern+route step outside the
# one pipeline. Internal subsystems (mv refresh, discovery, cache) execute engine SQL
# but never route+govern, so they are not flagged. This is a RATCHET: extend the
# allowlist only by collapsing the entry onto the one pipeline, never by adding to it.
_ALLOWED_ROUTE_AND_GOVERN = {
    "provisa/pgwire/_pipeline.py",  # THE one governed pipeline
    # KNOWN DEBT: the GraphQL endpoint assembles its own govern+route+execute. It is a
    # parallel pipeline slated for the same collapse _compile_govern_execute got. Do NOT
    # add new entries here — collapse them onto _govern_and_route/_execute_plan instead.
    "provisa/api/data/endpoint.py",
}


def _calls_symbol(tree: ast.AST, name: str) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            fn = node.func
            if isinstance(fn, ast.Name) and fn.id == name:
                return True
            if isinstance(fn, ast.Attribute) and fn.attr == name:
                return True
    return False


def test_direct_engine_execution_verifies_the_stamp():
    """require_governed_plan is the shared last-moment gate: it rejects an un-minted plan and admits a
    pipeline-minted one. _execute_plan is not the only terminal — the Arrow/streaming sinks call this
    directly before executing plan SQL, so no terminal can run an ungoverned plan (REQ-1176)."""
    unstamped = _Plan(route=Route.ENGINE, sql="x", source_id="pg", dialect="trino", physical_sql="x")
    with pytest.raises(PermissionError, match="ungoverned plan"):
        require_governed_plan(unstamped)
    unstamped.stamp = "f" * 64
    with pytest.raises(PermissionError, match="ungoverned plan"):
        require_governed_plan(unstamped)
    unstamped.stamp = _mint_stamp()
    require_governed_plan(unstamped)  # a pipeline-minted plan is admitted


# Arrow/stream engine methods execute a governed PLAN's SQL directly (they never carry raw system SQL —
# unlike execute_engine/execute_native, which internal subsystems use). So EVERY module that calls them
# MUST verify the stamp at the last moment (require_governed_plan) or route through _execute_plan. A new
# sink that runs plan SQL on the Arrow/stream terminal without verifying is exactly the hole this closes.
_ARROW_STREAM_METHODS = ("execute_engine_arrow", "execute_engine_stream")


def test_arrow_stream_sinks_verify_the_stamp():
    offenders = []
    for path in _PROVISA.rglob("*.py"):
        rel = path.relative_to(_REPO_ROOT).as_posix()
        if rel == "provisa/pgwire/_pipeline.py":
            continue
        text = path.read_text()
        if any(f".{m}(" in text for m in _ARROW_STREAM_METHODS):
            if "require_governed_plan" not in text and "_execute_plan" not in text:
                offenders.append(rel)
    assert not offenders, (
        "These modules execute a plan's SQL on the Arrow/stream engine terminal without verifying the "
        f"governed-provenance stamp (require_governed_plan) at the last moment: {offenders}. An "
        "un-minted/side-door plan would run ungoverned. Add require_governed_plan(plan) immediately "
        "before the engine call."
    )


def test_no_new_parallel_governed_pipeline():
    offenders = []
    for path in _PROVISA.rglob("*.py"):
        rel = path.relative_to(_REPO_ROOT).as_posix()
        tree = ast.parse(path.read_text())
        if _calls_symbol(tree, "decide_route") and _calls_symbol(tree, "apply_governance"):
            if rel not in _ALLOWED_ROUTE_AND_GOVERN:
                offenders.append(rel)
    assert not offenders, (
        "New parallel governed pipeline(s) detected — these modules co-locate routing "
        "(decide_route) and governance (apply_governance) instead of routing through the one "
        f"pipeline (_govern_and_route / _execute_plan): {offenders}. Collapse them onto the "
        "chokepoint; do not add them to the allowlist."
    )
