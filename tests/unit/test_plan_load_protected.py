# Copyright (c) 2026 Kenneth Stott
# Canary: 9e5f4c90-0d44-4118-9f05-6a6b2b4f1cb7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1141: load_protected in the PLAN stage — forces MATERIALIZED, reads prep only on first load."""

from __future__ import annotations

from provisa.core.models import Source, SourceType
from provisa.federation.engine import build_trino_engine
from provisa.federation.plan import PrepStep, Route, Strategy, build_execution_plan


def _src(sid: str, type_: SourceType = SourceType.postgresql, **kw) -> Source:
    return Source(id=sid, type=type_, host="h", port=1, database="d", username="u", **kw)


def _always_stale(_sid: str) -> bool:
    return True


def test_load_protected_forces_materialized_removes_direct_route():
    # A single reachable PG source would route DIRECT (VIRTUAL); load_protected forces MATERIALIZED
    # so it goes to the engine store instead — the live route is removed (REQ-1141).
    plan = build_execution_plan(
        [_src("pg")],
        build_trino_engine(),
        _always_stale,
        load_protected_of=lambda _sid: True,
        materialization_backend="postgresql",
    )
    assert plan.route is Route.ENGINE
    assert plan.prep == [PrepStep("pg", Strategy.MATERIALIZED)]


def test_load_protected_resident_source_never_preps_on_read():
    # Even though the staleness oracle says stale, a resident load-protected source is NOT prepped
    # on the query path — the scheduler owns refresh (REQ-1141).
    plan = build_execution_plan(
        [_src("pg")],
        build_trino_engine(),
        _always_stale,
        load_protected_of=lambda _sid: True,
        resident_of=lambda _sid: True,
        materialization_backend="postgresql",
    )
    assert plan.prep == []  # zero query-path pull of a resident protected source


def test_load_protected_first_load_preps_when_not_resident():
    plan = build_execution_plan(
        [_src("pg")],
        build_trino_engine(),
        lambda _sid: False,  # not stale by the generic oracle — irrelevant for load_protected
        load_protected_of=lambda _sid: True,
        resident_of=lambda _sid: False,
        materialization_backend="postgresql",
    )
    assert plan.prep == [PrepStep("pg", Strategy.MATERIALIZED)]  # first load happens on read


def test_non_protected_source_unaffected():
    plan = build_execution_plan([_src("pg")], build_trino_engine(), _always_stale)
    assert plan.route is Route.DIRECT and plan.prep == []
