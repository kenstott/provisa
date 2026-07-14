# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-860: Source-level freshness gate — strategy mapping, decision, and plan integration."""

from __future__ import annotations

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation.engine import build_trino_engine
from provisa.federation.plan import build_execution_plan
from provisa.freshness.predicate import Probe, Ttl, TtlThenProbe
from provisa.freshness.source_gate import (
    gate_source,
    source_strategy,
    source_subject,
)


def _src(sid: str, type_: SourceType, **kw) -> Source:
    return Source(id=sid, type=type_, host="h", port=1, database="d", username="u", **kw)


# ---- source_strategy mapping ------------------------------------------------


def test_strategy_ttl():
    s = _src("s", SourceType.openapi, base_url="http://x", change_signal="ttl", cache_ttl=30)
    strat = source_strategy(s)
    assert strat == Ttl(30.0)


def test_strategy_probe():
    s = _src("s", SourceType.openapi, base_url="http://x", change_signal="probe")
    assert source_strategy(s) == Probe()


def test_strategy_ttl_probe():
    s = _src("s", SourceType.openapi, base_url="http://x", change_signal="ttl_probe", cache_ttl=60)
    assert source_strategy(s) == TtlThenProbe(60.0)


def test_strategy_ttl_without_cache_ttl_raises():
    s = _src("s", SourceType.openapi, base_url="http://x", change_signal="ttl", cache_ttl=None)
    with pytest.raises(ValueError, match="requires cache_ttl"):
        source_strategy(s)


def test_strategy_ttl_probe_without_cache_ttl_raises():
    s = _src(
        "s", SourceType.openapi, base_url="http://x", change_signal="ttl_probe", cache_ttl=None
    )
    with pytest.raises(ValueError, match="requires cache_ttl"):
        source_strategy(s)


def test_strategy_push_signal_raises():
    s = _src("s", SourceType.postgresql, change_signal="debezium")
    with pytest.raises(ValueError, match="no read-time freshness predicate"):
        source_strategy(s)


# ---- gate_source decision ---------------------------------------------------


def test_gate_requires_opt_in():
    s = _src("s", SourceType.openapi, base_url="http://x", change_signal="ttl", cache_ttl=30)
    # freshness_gate defaults False
    with pytest.raises(ValueError, match="freshness_gate is False"):
        gate_source(s, source_subject(refreshed_at=100.0), now=110.0)


def test_gate_ttl_fresh_within_window():
    s = _src(
        "s",
        SourceType.openapi,
        base_url="http://x",
        change_signal="ttl",
        cache_ttl=30,
        freshness_gate=True,
    )
    d = gate_source(s, source_subject(refreshed_at=100.0), now=110.0)
    assert d.is_fresh


def test_gate_ttl_stale_past_window():
    s = _src(
        "s",
        SourceType.openapi,
        base_url="http://x",
        change_signal="ttl",
        cache_ttl=30,
        freshness_gate=True,
    )
    d = gate_source(s, source_subject(refreshed_at=100.0), now=200.0)
    assert not d.is_fresh


def test_gate_probe_unchanged_is_fresh():
    s = _src(
        "s",
        SourceType.openapi,
        base_url="http://x",
        change_signal="probe",
        freshness_gate=True,
    )
    subject = source_subject(refreshed_at=1.0, token="v1", baseline="v1")
    assert gate_source(s, subject, now=9999.0).is_fresh


def test_gate_probe_changed_is_stale():
    s = _src(
        "s",
        SourceType.openapi,
        base_url="http://x",
        change_signal="probe",
        freshness_gate=True,
    )
    subject = source_subject(refreshed_at=1.0, token="v2", baseline="v1")
    assert not gate_source(s, subject, now=9999.0).is_fresh


# ---- plan integration: gate feeds the residency prep phase ------------------


def test_stale_gated_source_triggers_prep():
    api = _src(
        "api",
        SourceType.openapi,
        base_url="http://x",
        change_signal="ttl",
        cache_ttl=30,
        freshness_gate=True,
    )
    # subject: landed at t=100, now=200 → past TTL → stale → must be prepped (refresh path).
    plan = build_execution_plan(
        [api],
        build_trino_engine(),
        lambda _sid: False,  # generic oracle says fresh — the gate overrides it
        freshness_subject_of=lambda _sid: source_subject(refreshed_at=100.0),
        now=200.0,
    )
    assert [p.source_id for p in plan.prep] == ["api"]


def test_fresh_gated_source_no_prep():
    api = _src(
        "api",
        SourceType.openapi,
        base_url="http://x",
        change_signal="ttl",
        cache_ttl=30,
        freshness_gate=True,
    )
    plan = build_execution_plan(
        [api],
        build_trino_engine(),
        lambda _sid: True,  # generic oracle says stale — the gate overrides it to fresh
        freshness_subject_of=lambda _sid: source_subject(refreshed_at=100.0),
        now=110.0,
    )
    assert plan.prep == []


def test_ungated_source_uses_generic_oracle():
    # freshness_gate off → is_stale drives prep even when a subject provider is present.
    api = _src("api", SourceType.openapi, base_url="http://x")
    plan = build_execution_plan(
        [api],
        build_trino_engine(),
        lambda _sid: True,
        freshness_subject_of=lambda _sid: source_subject(refreshed_at=100.0),
        now=110.0,
    )
    assert [p.source_id for p in plan.prep] == ["api"]
