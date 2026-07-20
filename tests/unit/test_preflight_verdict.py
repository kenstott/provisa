# Copyright (c) 2026 Kenneth Stott
# Canary: 5e1a9c34-7b2f-4d08-9a16-8c0d3e7f2b45
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1165: the preflight verdict vocabulary — coercion + the run_preflight runtime."""

from __future__ import annotations

import pytest

from provisa.mv.preflight import (
    CONTINUE,
    Decision,
    PreflightContractError,
    Verdict,
    run_preflight,
    to_verdict,
)


@pytest.mark.parametrize(
    "raw,expected",
    [
        (None, Decision.CONTINUE),
        (True, Decision.CONTINUE),
        (False, Decision.ABORT),
        ("continue", Decision.CONTINUE),
        ("abort", Decision.ABORT),
        ("quarantine", Decision.QUARANTINE),
        (Verdict(Decision.QUARANTINE, "hold"), Decision.QUARANTINE),
        (CONTINUE, Decision.CONTINUE),
    ],
)
def test_to_verdict_coercions(raw, expected) -> None:
    assert to_verdict(raw).decision is expected


@pytest.mark.parametrize("bad", [[], {"a": 1}, [{"row": 1}], 3, 1.5])
def test_to_verdict_rejects_non_verdict_returns(bad) -> None:
    # REQ-1165: a preflight CHECK returns a verdict, never rows — a list/dict/number is a contract error.
    with pytest.raises(PreflightContractError):
        to_verdict(bad)


def test_to_verdict_unknown_string_is_error() -> None:
    with pytest.raises(PreflightContractError):
        to_verdict("proceed")


@pytest.mark.asyncio
async def test_run_preflight_none_is_continue() -> None:
    assert (await run_preflight(None, [{"a": 1}], None)).is_continue


@pytest.mark.asyncio
async def test_run_preflight_sync_and_async_hooks() -> None:
    def sync_hook(rows, ctx):
        return Verdict(Decision.ABORT, "sync")

    async def async_hook(rows, ctx):
        return Verdict(Decision.QUARANTINE, "async")

    assert (await run_preflight(sync_hook, [], None)).decision is Decision.ABORT
    assert (await run_preflight(async_hook, [], None)).decision is Decision.QUARANTINE


@pytest.mark.asyncio
async def test_run_preflight_short_circuits_lazy_rows() -> None:
    # A quantified check over a lazy generator stops at the first decisive row (constant memory).
    consumed = []

    def gen():
        for i in [1, -1, 2, 3]:
            consumed.append(i)
            yield {"v": i}

    def hook(rows, ctx):
        if any(r["v"] < 0 for r in rows):
            return Verdict(Decision.ABORT, "neg")
        return CONTINUE

    v = await run_preflight(hook, gen(), None)
    assert v.decision is Decision.ABORT
    assert consumed == [1, -1]  # never pulled rows 2 and 3
