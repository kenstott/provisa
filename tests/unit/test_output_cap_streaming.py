# Copyright (c) 2026 Kenneth Stott
# Canary: 2d8f5b19-7c40-4e63-a1b7-9e5c3a0d6f28
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1044: the output-row tier ceiling applies to every terminal that streams a plan.

The ceiling is the commercial plugin's (``commerce.enforce_output_cap``); it is applied where every
terminal takes the plan's permits (``acquire_plan_permits``), so a streamed result past the ceiling
ends with the tier error — never a short result — and its record says 402."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.executor.result import StreamingQueryResult
from provisa.federation.live_concurrency import acquire_plan_permits


class _TierCapExceeded(Exception):
    status_code = 402


class _Plugin:
    """The plugin's contract: re-wrap the stream and abort at row N+1."""

    def __init__(self, ceiling: int) -> None:
        self.ceiling = ceiling

    def enforce_output_cap(self, result, caps, plan):
        ceiling = self.ceiling

        def _batches():
            seen = 0
            for batch in result.batches():
                seen += len(batch)
                if seen > ceiling:
                    raise _TierCapExceeded(f"{plan}: more than {ceiling} rows")
                yield batch

        return StreamingQueryResult(_batches(), column_names=list(result.column_names))


def _plan(**over):
    base = dict(live_caps=(), live_caps_org=None, tier_caps=object(), tier_plan="trial")
    base.update(over)
    return SimpleNamespace(**base)


def _stream(n_batches: int, size: int = 2) -> StreamingQueryResult:
    return StreamingQueryResult(
        ([(i, j) for j in range(size)] for i in range(n_batches)), column_names=["a", "b"]
    )


@pytest.fixture
def ceiling_three(monkeypatch):
    monkeypatch.setattr("provisa.core.commerce.load", lambda: _Plugin(3))


def test_a_stream_past_the_ceiling_ends_with_the_tier_error(ceiling_three):
    stream = acquire_plan_permits(SimpleNamespace(), _plan()).wrap_stream(_stream(3))
    batches = stream.batches()
    assert next(batches) == [(0, 0), (0, 1)]
    with pytest.raises(_TierCapExceeded, match="more than 3 rows"):
        list(batches)


def test_batches_guarded_for_an_arrow_terminal_end_the_same_way(ceiling_three):
    guarded = acquire_plan_permits(SimpleNamespace(), _plan()).guard(iter([[1, 2], [3, 4], [5, 6]]))
    with pytest.raises(_TierCapExceeded):
        list(guarded)


def test_a_plan_without_tier_ceilings_streams_as_it_is(ceiling_three):
    stream = _stream(3)
    assert (
        acquire_plan_permits(SimpleNamespace(), _plan(tier_caps=None)).wrap_stream(stream) is stream
    )


def test_a_stream_under_the_ceiling_is_whole(ceiling_three):
    stream = acquire_plan_permits(SimpleNamespace(), _plan()).wrap_stream(_stream(1))
    assert list(stream.batches()) == [[(0, 0), (0, 1)]]


def test_the_drains_record_says_402(ceiling_three, monkeypatch):
    from provisa.pgwire import _pipeline

    completed: list[int] = []
    monkeypatch.setattr(
        "provisa.audit.pipeline.complete_audit_record",
        lambda record, started, status, rows: completed.append(status),
    )
    plan = SimpleNamespace(audit_deferred=object(), audit=None)
    stream = acquire_plan_permits(SimpleNamespace(), _plan()).wrap_stream(_stream(3))
    drain = _pipeline._AuditedDrain(plan, stream.batches(), len)
    with pytest.raises(_TierCapExceeded):
        list(drain)
    assert completed == [402]
