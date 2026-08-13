# Copyright (c) 2026 Kenneth Stott
# Canary: 9d3b7f21-5a6c-4e08-b2d1-7c4a0e93f5b6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1428: no OTel signal's backlog may starve another's compaction.

The signals were compacted in the fixed order logs, metrics, traces. On the SaaS node one run
spent minutes draining logs and metrics and was killed — by a restart or the node's idle-stop —
before traces ever got a turn, so trace rows stopped landing in Iceberg entirely and the ops
`queries` report read an empty table while 15k trace files sat in the object store.
"""

# Requirements: REQ-1428

from __future__ import annotations

import asyncio
import datetime as dt
import threading
import types

import pytest

from provisa.scheduler import jobs


@pytest.fixture
def compaction_state(monkeypatch):
    """An AppState stub with S3 credentials present, so compaction is not gated off."""
    monkeypatch.setenv("PROVISA_OTEL_S3_ACCESS_KEY", "ak")
    monkeypatch.setenv("PROVISA_OTEL_S3_SECRET_KEY", "sk")
    monkeypatch.setenv("PROVISA_OTEL_BUCKET", "provisa-otel")
    monkeypatch.delenv("OTEL_COMPACT_DATE", raising=False)
    state = types.SimpleNamespace(
        otel_s3_endpoint="https://object-store.invalid",
        otel_compact_file_chunk=50,
        otel_compact_max_files_per_run=500,
        federation_engine=object(),
    )
    monkeypatch.setattr("provisa.api.app.state", state, raising=False)
    return state


@pytest.mark.asyncio
async def test_a_slow_signal_cannot_hold_the_run_hostage(compaction_state, monkeypatch):
    """traces must start while logs is still draining — the starvation this job suffered."""
    traces_started = threading.Event()
    logs_released = threading.Event()
    seen: list[str] = []

    def fake_compact(signal, *args):
        seen.append(signal)
        if signal == "logs":
            # Only a concurrent run can set this while logs is still inside its compaction.
            assert traces_started.wait(timeout=10), "traces never started while logs was draining"
            logs_released.set()
        elif signal == "traces":
            traces_started.set()

    monkeypatch.setattr(jobs, "_compact_signal", fake_compact)

    await asyncio.wait_for(jobs.compact_otel_signals(), timeout=15)

    assert sorted(seen) == ["logs", "metrics", "traces"]
    assert logs_released.is_set()


@pytest.mark.asyncio
async def test_every_signal_is_compacted_with_its_own_budget(compaction_state, monkeypatch):
    calls: dict[str, tuple] = {}

    def fake_compact(signal, target, endpoint, ak, sk, bucket, file_chunk, max_files, engine):
        calls[signal] = (bucket, file_chunk, max_files, engine)

    monkeypatch.setattr(jobs, "_compact_signal", fake_compact)

    await jobs.compact_otel_signals()

    assert set(calls) == {"logs", "metrics", "traces"}
    for signal in calls:
        assert calls[signal] == (
            "provisa-otel",
            50,
            500,
            compaction_state.federation_engine,
        ), f"{signal} did not get the configured per-signal budget"


@pytest.mark.asyncio
async def test_a_failing_signal_does_not_suppress_the_others(compaction_state, monkeypatch):
    """A raise in one signal surfaces, but only after the other two have run."""
    seen: list[str] = []

    def fake_compact(signal, *args):
        seen.append(signal)
        if signal == "metrics":
            raise RuntimeError("iceberg insert rejected")

    monkeypatch.setattr(jobs, "_compact_signal", fake_compact)

    with pytest.raises(RuntimeError, match="iceberg insert rejected"):
        await jobs.compact_otel_signals()

    assert sorted(seen) == ["logs", "metrics", "traces"]


@pytest.mark.asyncio
async def test_a_chunk_fetches_its_objects_concurrently(monkeypatch):
    """Serial per-file round trips capped a run far below its budget, so ingest outran it."""
    import pyarrow as pa

    in_flight = 0
    peak = 0
    lock = threading.Lock()

    class _Body:
        def read(self):
            return b""

    def fake_get_object(Bucket, Key):
        nonlocal in_flight, peak
        with lock:
            in_flight += 1
            peak = max(peak, in_flight)
        try:
            barrier.wait(timeout=10)
        finally:
            with lock:
                in_flight -= 1
        return {"Body": _Body()}

    keys = [f"traces/provisa/year=2026/month=08/day=11/hour=19/f{i}.parquet" for i in range(8)]

    class _S3:
        def get_paginator(self, _op):
            return types.SimpleNamespace(
                paginate=lambda **kw: [{"Contents": [{"Key": k} for k in keys]}]
            )

        get_object = staticmethod(fake_get_object)

        def delete_objects(self, **kw):
            return {}

    barrier = threading.Barrier(len(keys), timeout=10)
    monkeypatch.setattr(jobs, "_OTEL_FETCH_WORKERS", len(keys))
    monkeypatch.setattr("boto3.client", lambda *a, **k: _S3())
    # traces.timestamp arrives from otlp2parquet as an arrow instant, not an epoch integer
    # (REQ-1435) — an int64 here is refused by _instants_from_epoch_ints and the chunk never
    # reaches the insert this asserts on.
    monkeypatch.setattr(
        "pyarrow.parquet.read_table",
        lambda _buf: pa.table(
            {
                "timestamp": pa.array(
                    [dt.datetime(2026, 8, 11, 19, 0, 0)], type=pa.timestamp("us")
                ),
                "span_name": ["s"],
            }
        ),
    )
    inserted: list[int] = []
    monkeypatch.setattr(
        jobs, "_insert_otel_iceberg", lambda engine, sig, tbl, d: inserted.append(len(tbl))
    )

    # A barrier of the full chunk width: it can only be cleared if every fetch is running at once.
    jobs._compact_signal(
        "traces", None, "https://object-store.invalid", "ak", "sk", "b", len(keys), 500, object()
    )

    assert peak == len(keys), f"only {peak} fetches overlapped — the chunk is still serial"
    assert inserted == [len(keys)]
