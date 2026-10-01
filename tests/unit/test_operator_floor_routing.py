# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7e9d21-6c4a-4f18-9e02-8a5d1c7f4b60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The operator's settings are the FLOOR of every read (REQ-030, amended 2026-09-30).

A source the operator marked ``load_protected`` or ``prefer_materialized`` is read from the
platform's landed copy — never pulled live by a query. The one routing decision enforces it: with no
hint such a source routes to the engine (which serves the landed copy), and a request hint that would
read it live (``route=direct``) is rejected with an error naming the operator setting. A hint that
moves AWAY from the source (``route=federated``) is allowed; writes are unaffected (REQ-031).
"""

# Requirements: REQ-030, REQ-826, REQ-1141

from __future__ import annotations

import pytest

from provisa.transpiler.router import OperatorFloorViolation, Route, decide_route

_TYPES = {
    "pg-main": "postgresql",
    "pg-other": "postgresql",
    "pets-api": "openapi",
}
_DIALECTS = {"pg-main": "postgres", "pg-other": "postgres", "pets-api": ""}
_SAME_DB = {"pg-main": "db:5432/app", "pg-other": "db:5432/app"}


def test_a_load_protected_source_never_routes_direct():
    decision = decide_route(
        {"pg-main"}, _TYPES, _DIALECTS, operator_floor={"pg-main": "load_protected"}
    )
    assert decision.route == Route.ENGINE
    assert "load_protected" in decision.reason


def test_a_prefer_materialized_source_never_routes_direct():
    decision = decide_route(
        {"pg-main"}, _TYPES, _DIALECTS, operator_floor={"pg-main": "prefer_materialized"}
    )
    assert decision.route == Route.ENGINE
    assert "prefer_materialized" in decision.reason


@pytest.mark.parametrize("setting", ["load_protected", "prefer_materialized"])
def test_a_direct_hint_below_the_floor_is_rejected_naming_the_setting(setting):
    with pytest.raises(OperatorFloorViolation) as exc:
        decide_route(
            {"pg-main"},
            _TYPES,
            _DIALECTS,
            steward_hint="direct",
            operator_floor={"pg-main": setting},
        )
    assert setting in str(exc.value)
    assert "pg-main" in str(exc.value)


def test_the_violation_is_a_permission_error_every_transport_already_maps():
    assert issubclass(OperatorFloorViolation, PermissionError)


def test_a_federated_hint_on_a_floored_source_is_allowed():
    decision = decide_route(
        {"pg-main"},
        _TYPES,
        _DIALECTS,
        steward_hint="federated",
        operator_floor={"pg-main": "load_protected"},
    )
    assert decision.route == Route.ENGINE


def test_colocated_sources_with_one_floored_never_route_direct():
    decision = decide_route(
        {"pg-main", "pg-other"},
        _TYPES,
        _DIALECTS,
        source_dsns=_SAME_DB,
        operator_floor={"pg-other": "load_protected"},
    )
    assert decision.route == Route.ENGINE
    assert "pg-other" in decision.reason


def test_a_floored_api_source_is_not_called_live():
    decision = decide_route(
        {"pets-api"}, _TYPES, _DIALECTS, operator_floor={"pets-api": "load_protected"}
    )
    assert decision.route == Route.ENGINE


def test_writes_to_a_floored_source_still_route_direct():
    decision = decide_route(
        {"pg-main"},
        _TYPES,
        _DIALECTS,
        is_mutation=True,
        operator_floor={"pg-main": "load_protected"},
    )
    assert decision.route == Route.DIRECT


def test_an_unfloored_source_is_unchanged():
    assert decide_route({"pg-main"}, _TYPES, _DIALECTS, operator_floor={}).route == Route.DIRECT
    hinted = decide_route({"pg-main"}, _TYPES, _DIALECTS, steward_hint="direct", operator_floor={})
    assert hinted.route == Route.DIRECT


def test_the_floor_is_required_so_no_caller_can_silently_skip_it():
    with pytest.raises(TypeError):
        decide_route({"pg-main"}, _TYPES, _DIALECTS)  # type: ignore[call-arg]


def test_a_direct_hint_never_sends_sql_to_a_neo4j_source():
    """neo4j has a registered driver but is a VIRTUAL source (issue #119): its reads go through
    the engine and its row_materialize cache (REQ-1865), never raw SQL text to the source — a
    ``route=direct`` hint included."""
    decision = decide_route(
        {"graph"},
        {"graph": "neo4j"},
        {"graph": ""},
        steward_hint="direct",
        operator_floor={},
    )
    assert decision.route == Route.ENGINE


def test_prefer_materialized_alone_is_a_floor():
    """prefer_materialized blocks live reads on its own — no cache_ttl, window or probe needed
    (REQ-826 amended 2026-09-30); its refresh timing follows the normal rules (REQ-1907)."""
    from types import SimpleNamespace

    from provisa.core.operator_floor import floor_setting

    bare = SimpleNamespace(
        load_protected=False,
        prefer_materialized=True,
        cache_ttl=None,
        off_peak_window=None,
        change_signal="ttl",
    )
    assert floor_setting(bare) == "prefer_materialized"
    assert floor_setting(SimpleNamespace(load_protected=False, prefer_materialized=False)) is None
