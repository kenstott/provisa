# Copyright (c) 2026 Kenneth Stott
# Canary: 6556d70e-832c-45f4-a66d-eaa8988e1a2e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2155: data quality has its own rights. Reading a table's constraint decisions and check
candidates needs dq_read; deciding, removing, exporting and creating checks needs dq_rw. Neither
needs table_registration any more, so a data quality steward manages checks without registering
tables."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.admin import profiler_checks_router as router


class _Gated(Exception):
    pass


@pytest.fixture
def asked(monkeypatch):
    seen: list[str] = []

    def _gate(_request, capability):
        seen.append(capability)
        raise _Gated

    def _read(_request):
        seen.append("dq_read")
        raise _Gated

    monkeypatch.setattr(router, "require_capability_request", _gate)
    monkeypatch.setattr(router, "require_dq_read_request", _read)
    return seen


_REQUEST = SimpleNamespace(state=SimpleNamespace())


async def _first_gate(call) -> None:
    with pytest.raises(_Gated):
        await call


@pytest.mark.parametrize(
    "call",
    [
        lambda: router.list_constraints(_REQUEST, 1),
        lambda: router.check_candidates(_REQUEST, 1),
    ],
)
async def test_reading_needs_dq_read(asked, call):
    await _first_gate(call())
    assert asked == ["dq_read"]


@pytest.mark.parametrize(
    "call",
    [
        lambda: router.decide_constraint(_REQUEST, 1, None),
        lambda: router.forget_constraint(_REQUEST, 1, "c"),
        lambda: router.export_constraint(_REQUEST, 1, "c", None),
        lambda: router.create_drift_check(_REQUEST, 1, None),
        lambda: router.create_expectation_check(_REQUEST, 1, None),
    ],
)
async def test_managing_checks_needs_dq_rw(asked, call):
    await _first_gate(call())
    assert asked == ["dq_rw"]


# -- the read gate: dq_read, which dq_rw implies (REQ-2155) ------------------------------------


def _caller(*caps: str):
    from provisa.auth.models import AuthIdentity

    identity = AuthIdentity(user_id="u", email=None, display_name=None, roles=["r"], raw_claims={})
    identity.acting_roles = ["r"]
    return SimpleNamespace(state=SimpleNamespace(identity=identity)), {
        "r": {"capabilities": list(caps), "domain_access": ["*"]}
    }


@pytest.mark.parametrize("caps", [("dq_read",), ("dq_rw",), ("dq_read", "dq_rw")])
def test_a_dq_reader_or_manager_passes_the_read_gate(monkeypatch, caps):
    from provisa.api import app
    from provisa.api.admin.capabilities import require_dq_read_request

    request, roles = _caller(*caps)
    monkeypatch.setattr(app.state, "roles", roles)
    require_dq_read_request(request)


def test_a_caller_with_neither_is_refused_naming_dq_read(monkeypatch):
    from provisa.api import app
    from provisa.api.admin.capabilities import require_dq_read_request
    from provisa.api.errors import ApiError

    request, roles = _caller("table_registration")
    monkeypatch.setattr(app.state, "roles", roles)
    with pytest.raises(ApiError, match="dq_read"):
        require_dq_read_request(request)


@pytest.fixture
def read_asked(monkeypatch):
    from provisa.api.admin import profiler_router

    seen: list[str] = []

    def _gate(_request):
        seen.append("dq_read")
        raise _Gated

    def _other(_request, capability):
        seen.append(capability)
        raise _Gated

    for module in (router, profiler_router):
        monkeypatch.setattr(module, "require_dq_read_request", _gate)
        monkeypatch.setattr(module, "require_capability_request", _other)
    return seen, profiler_router


async def test_reading_profile_runs_needs_dq_read_not_table_registration(read_asked):
    seen, profiler = read_asked
    for call in (
        lambda: profiler.list_profilers(_REQUEST),
        lambda: profiler.list_profile_runs(_REQUEST, 1),
        lambda: profiler.get_profile_run(_REQUEST, 1, "r", None),
        lambda: profiler.get_measure_history(_REQUEST, 1, "r", "rowCount", None),
    ):
        seen.clear()
        await _first_gate(call())
        assert seen == ["dq_read"]
