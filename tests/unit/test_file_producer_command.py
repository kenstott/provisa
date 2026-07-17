# Copyright (c) 2026 Kenneth Stott
# Canary: 1954ebce-8350-4173-9f79-15cadbe2659f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-861: on-stale producer command for file-based sources.

A file source may carry a ``producer_command`` (argv) that refreshes the file in place. It runs
at the on-stale gate point (the loader is invoked only after REQ-860 reports stale) BEFORE the
file is read, never modifying ``path`` or defining an MV; a non-zero exit fails loud.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.core.models import Source, SourceType
from provisa.events.source_loader import SourceRowLoader
from provisa.executor.result import QueryResult
from provisa.freshness.producer import (
    ProducerCommandError,
    has_producer,
    run_producer,
)
from provisa.freshness.source_gate import gate_source, source_subject


class _Engine:
    def __init__(self, result):
        self._result = result
        self.sql: str | None = None

    async def execute_engine(self, sql, *a, **k):
        self.sql = sql
        return self._result


def _file_src(cmd, stype="csv"):
    return SimpleNamespace(id="prices", type=SimpleNamespace(value=stype), producer_command=cmd)


def _tbl():
    return SimpleNamespace(schema_name="main", table_name="prices")


def _ok_run(calls):
    def _run(cmd, **kw):
        calls.append((cmd, kw))
        return SimpleNamespace(returncode=0, stderr="")

    return _run


# ---- has_producer -----------------------------------------------------------------------------


def test_has_producer_true_for_file_with_command():
    assert has_producer(_file_src(["fetch.sh"])) is True


def test_has_producer_false_when_no_command():
    assert has_producer(_file_src(None)) is False
    assert has_producer(_file_src([])) is False


def test_has_producer_false_for_non_file_type():
    assert has_producer(_file_src(["fetch.sh"], stype="postgresql")) is False


# ---- run_producer -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_producer_invokes_subprocess_no_shell(monkeypatch):
    calls: list = []
    monkeypatch.setattr("provisa.freshness.producer.subprocess.run", _ok_run(calls))
    await run_producer(_file_src(["python", "fetch.py", "--out", "prices.csv"]))
    assert len(calls) == 1
    cmd, kw = calls[0]
    assert cmd == ["python", "fetch.py", "--out", "prices.csv"]
    assert kw["shell"] is False


@pytest.mark.asyncio
async def test_run_producer_nonzero_exit_fails_loud(monkeypatch):
    def _run(cmd, **kw):
        return SimpleNamespace(returncode=2, stderr="boom")

    monkeypatch.setattr("provisa.freshness.producer.subprocess.run", _run)
    with pytest.raises(ProducerCommandError, match="exited 2"):
        await run_producer(_file_src(["fetch.sh"]))


@pytest.mark.asyncio
async def test_run_producer_without_command_raises():
    with pytest.raises(ValueError, match="no producer_command"):
        await run_producer(_file_src(None))


# ---- loader wiring (the file-read path) -------------------------------------------------------


@pytest.mark.asyncio
async def test_stale_load_runs_producer_then_reads(monkeypatch):
    calls: list = []
    monkeypatch.setattr("provisa.freshness.producer.subprocess.run", _ok_run(calls))
    engine = _Engine(QueryResult(rows=[(1,)], column_names=["id"], column_types=None))
    rows = await SourceRowLoader(engine).load(_file_src(["fetch.sh"]), _tbl())
    assert calls, "producer command must run before the read"
    assert engine.sql == 'SELECT * FROM "prices"."main"."prices"'  # file read AFTER producer
    assert rows == [{"id": 1}]


@pytest.mark.asyncio
async def test_no_producer_declared_plain_read(monkeypatch):
    calls: list = []
    monkeypatch.setattr("provisa.freshness.producer.subprocess.run", _ok_run(calls))
    engine = _Engine(QueryResult(rows=[(1,)], column_names=["id"], column_types=None))
    await SourceRowLoader(engine).load(_file_src(None), _tbl())
    assert calls == []  # no command → never spawns a subprocess
    assert engine.sql == 'SELECT * FROM "prices"."main"."prices"'


@pytest.mark.asyncio
async def test_producer_failure_aborts_before_read(monkeypatch):
    def _run(cmd, **kw):
        return SimpleNamespace(returncode=1, stderr="no upstream")

    monkeypatch.setattr("provisa.freshness.producer.subprocess.run", _run)
    engine = _Engine(QueryResult(rows=[(1,)], column_names=["id"], column_types=None))
    with pytest.raises(ProducerCommandError):
        await SourceRowLoader(engine).load(_file_src(["fetch.sh"]), _tbl())
    assert engine.sql is None  # stale file never read after producer failure


# ---- gate integration: fresh source → loader (and producer) never reached ---------------------


def _gated_source():
    return Source(
        id="prices",
        type=SourceType.csv,
        path="prices.csv",
        freshness_gate=True,
        change_signal="ttl",
        cache_ttl=60,
        producer_command=["fetch.sh"],
    )


async def _read_when_stale(source, subject, now, engine, table):
    """Mirror plan._needs_refresh: a fresh gate skips prep, so the loader is never invoked."""
    if source.freshness_gate and gate_source(source, subject, now).is_fresh:
        return
    await SourceRowLoader(engine).load(source, table)


def test_source_model_carries_producer_command():
    assert _gated_source().producer_command == ["fetch.sh"]


@pytest.mark.asyncio
async def test_fresh_gate_skips_producer(monkeypatch):
    calls: list = []
    monkeypatch.setattr("provisa.freshness.producer.subprocess.run", _ok_run(calls))
    engine = _Engine(QueryResult(rows=[(1,)], column_names=["id"], column_types=None))
    src = _gated_source()
    fresh_subject = source_subject(refreshed_at=1000.0)  # just landed
    await _read_when_stale(src, fresh_subject, now=1001.0, engine=engine, table=_tbl())
    assert calls == []  # gate fresh → loader/producer never reached
    assert engine.sql is None


@pytest.mark.asyncio
async def test_stale_gate_runs_producer(monkeypatch):
    calls: list = []
    monkeypatch.setattr("provisa.freshness.producer.subprocess.run", _ok_run(calls))
    engine = _Engine(QueryResult(rows=[(1,)], column_names=["id"], column_types=None))
    src = _gated_source()
    stale_subject = source_subject(refreshed_at=1000.0)  # landed long ago
    await _read_when_stale(src, stale_subject, now=9000.0, engine=engine, table=_tbl())
    assert len(calls) == 1  # gate stale → producer ran before the read
    assert engine.sql == 'SELECT * FROM "prices"."main"."prices"'
