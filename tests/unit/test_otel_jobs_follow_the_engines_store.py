# Copyright (c) 2026 Kenneth Stott
# Canary: 8bb1c957-c0a9-4a17-802e-b106eac1abd9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The OTel maintenance jobs run only on an engine whose telemetry is the Iceberg ``otel`` catalog.

``reclaim_otel_storage`` runs Trino's Iceberg procedures (``ALTER TABLE otel.signals.<signal>
EXECUTE expire_snapshots / remove_orphan_files``), and ``compact_otel_signals`` writes the
collector's Parquet into those same Iceberg tables. Only an engine that declares the ``otel``
catalog (``EngineBackend.has_otel_catalog``: Trino) has them. A native engine (DuckDB) keeps its
telemetry in the dedicated ops store, which has no snapshots or orphan files and prunes its own
rows (REQ-1910); there both jobs have nothing to do. On DuckDB the reclaim statements failed every
tick with "Parser Error: syntax error at or near EXECUTE"."""

# Requirements: REQ-302, REQ-303

from __future__ import annotations

import types

import pytest

from provisa.federation.engine import build_engine
from provisa.scheduler import jobs


class _Runtime:
    """The bound engine runtime: its declared capability, and every statement sent to it."""

    def __init__(self, kind: str) -> None:
        self.has_otel_catalog = build_engine(kind).backend.has_otel_catalog
        self.sent: list[str] = []

    def execute_engine_sync(self, sql, params=None, *, session_hints=None, authorization=None):
        self.sent.append(sql)


def _state(monkeypatch, runtime: _Runtime) -> None:
    monkeypatch.setenv("PROVISA_OTEL_S3_ACCESS_KEY", "ak")
    monkeypatch.setenv("PROVISA_OTEL_S3_SECRET_KEY", "sk")
    monkeypatch.delenv("OTEL_COMPACT_DATE", raising=False)
    state = types.SimpleNamespace(
        otel_snapshot_retention_hours=1,
        otel_s3_endpoint="https://object-store.invalid",
        otel_compact_file_chunk=50,
        otel_compact_max_files_per_run=500,
        # The deployment's shared engine, which the jobs name and run as the deployment org.
        org_id="default",
        shared_federation_engine=runtime,
    )
    monkeypatch.setattr("provisa.api.app.state", state, raising=False)


@pytest.mark.asyncio
async def test_reclaim_sends_no_iceberg_procedure_to_duckdb(monkeypatch):
    runtime = _Runtime("duckdb")
    _state(monkeypatch, runtime)
    await jobs.reclaim_otel_storage()
    assert runtime.sent == []


@pytest.mark.asyncio
async def test_reclaim_runs_the_iceberg_procedures_on_trino(monkeypatch):
    runtime = _Runtime("trino")
    _state(monkeypatch, runtime)
    await jobs.reclaim_otel_storage()
    assert len(runtime.sent) == 6 and all("EXECUTE" in sql for sql in runtime.sent)


@pytest.mark.asyncio
async def test_compaction_compacts_nothing_into_duckdb(monkeypatch):
    runtime = _Runtime("duckdb")
    _state(monkeypatch, runtime)
    compacted: list[str] = []
    monkeypatch.setattr(jobs, "_compact_signal", lambda signal, *a: compacted.append(signal))
    await jobs.compact_otel_signals()
    assert compacted == [] and runtime.sent == []


@pytest.mark.asyncio
async def test_compaction_compacts_every_signal_on_trino(monkeypatch):
    runtime = _Runtime("trino")
    _state(monkeypatch, runtime)
    compacted: list[str] = []
    monkeypatch.setattr(jobs, "_compact_signal", lambda signal, *a: compacted.append(signal))
    await jobs.compact_otel_signals()
    assert sorted(compacted) == ["logs", "metrics", "traces"]
