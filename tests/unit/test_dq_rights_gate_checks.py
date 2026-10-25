# Copyright (c) 2026 Kenneth Stott
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

    monkeypatch.setattr(router, "require_capability_request", _gate)
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
