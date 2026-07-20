# Copyright (c) 2026 Kenneth Stott
# Canary: 9a2c6e08-3f47-4b19-8d52-1e0b4a7c9f63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1165 / REQ-940: Arrow record-batch framing + streaming preflight evaluation."""

from __future__ import annotations

import pytest

from provisa.mv.preflight import CONTINUE, Decision, Verdict
from provisa.processors.arrow import (
    arrow_decode,
    arrow_encode,
    record_batches,
    rows_of,
    stream_preflight,
)


class _Ctx:
    def ok(self):
        return CONTINUE

    def abort(self, reason=None):
        return Verdict(Decision.ABORT, reason)

    def quarantine(self, reason=None):
        return Verdict(Decision.QUARANTINE, reason)

    def warn(self, _r):
        pass


def test_record_batches_respect_batch_size() -> None:
    rows = [{"i": n} for n in range(10)]
    batches = list(record_batches(rows, batch_size=3))
    assert [b.num_rows for b in batches] == [3, 3, 3, 1]
    assert list(rows_of(batches)) == rows


def test_record_batches_empty_yields_nothing() -> None:
    assert list(record_batches([])) == []


def test_arrow_ipc_roundtrip() -> None:
    rows = [{"qty": n, "k": str(n)} for n in range(5)]
    blob = arrow_encode(rows, batch_size=2)
    assert isinstance(blob, bytes) and blob
    assert list(arrow_decode(blob)) == rows


def test_arrow_encode_empty_is_empty_bytes() -> None:
    assert arrow_encode([]) == b""
    assert list(arrow_decode(b"")) == []


@pytest.mark.asyncio
async def test_stream_preflight_short_circuits() -> None:
    # Batches decode lazily; a violating row in the second batch aborts without touching the third.
    decoded_batches = {"n": 0}

    def counting_batches():
        for chunk in ([{"v": 1}], [{"v": -9}], [{"v": 2}]):
            decoded_batches["n"] += 1
            yield from record_batches(chunk, batch_size=1)

    def hook(rows, ctx):
        if any(r["v"] < 0 for r in rows):
            return ctx.abort("neg")
        return ctx.ok()

    v = await stream_preflight(hook, counting_batches(), _Ctx())
    assert v.decision is Decision.ABORT
    assert decoded_batches["n"] == 2  # third batch never produced


@pytest.mark.asyncio
async def test_stream_preflight_none_is_continue() -> None:
    v = await stream_preflight(None, record_batches([{"v": 1}]), _Ctx())
    assert v.is_continue
