# Copyright (c) 2026 Kenneth Stott
# Canary: 6a2e9c14-7b3f-4d58-91a0-c4f8e2d7b365
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a Data Profiler block-samples an Iceberg table on Trino (REQ-1934).

An Iceberg table of 40 data files, read in place by the Trino engine through the shared Hive
metastore. Trino's TABLESAMPLE SYSTEM drops whole splits before reading them, so the profile reads
only the sampled files. The sampling unit is a file: a small percentage of 40 files can come back
far under its target, so the run reads again at four times the percentage until the sample holds at
least half its target, and records every attempt.

What Trino read is measured by Trino itself: the physical input rows of each sampled profile
statement (``/v1/query``).
"""

# Requirements: REQ-1934

from __future__ import annotations

import json
import os
import tempfile
import uuid
from pathlib import Path

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration, pytest.mark.requires_hive]

_ROLE = "org_admin"
_FILES = 40
_ROWS_PER_FILE = 25_000
_ROWS = _FILES * _ROWS_PER_FILE
_BUDGET = 60_000  # cells: 3 profiled columns x 1M rows -> a 2% sample, under one file's share
_TARGET = _BUDGET / (3 * _ROWS)


def _make_iceberg_table(root: Path) -> str:
    """An Iceberg table written as ``_FILES`` appends, one data file each."""
    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog

    cat = SqlCatalog("d", uri=f"sqlite:///{root}/cat.db", warehouse=f"file://{root}")
    cat.create_namespace("db")
    table = None
    for f in range(_FILES):
        ids = list(range(f * _ROWS_PER_FILE + 1, (f + 1) * _ROWS_PER_FILE + 1))
        data = pa.table(
            {
                "id": pa.array(ids, pa.int64()),
                "region": pa.array([f"r{i % 7}" for i in ids]),
                "amount": pa.array([float(i % 101) for i in ids], pa.float64()),
            }
        )
        if table is None:
            table = cat.create_table("db.orders", schema=data.schema)
        table.append(data)
    return f"{root}/db/orders"


def _config(path: str) -> dict:
    return {
        "auth": {"provider": "none"},
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "lake", "description": "Lake"}],
        # org_admin is the reserved administrative role (REQ-1349): every org has it.
        "roles": [],
        "sources": [
            {"id": "lake_orders", "type": "iceberg", "path": path},
            {
                "id": "profiler",
                "type": "data_profiler",
                "mapping": {
                    "cron": "0 3 * * *",
                    "sample_above_cells": _BUDGET,
                    "low_cardinality_max": 100,
                },
            },
        ],
        "tables": [
            {
                "source_id": "lake_orders",
                "schema": "main",
                "table": "lake_orders",
                "domain_id": "lake",
                "profiler_source_id": "profiler",
                "columns": [
                    {"name": n, "data_type": t, "visible_to": [_ROLE]}
                    for n, t in (("id", "bigint"), ("region", "varchar"), ("amount", "double"))
                ],
            }
        ],
    }


def _call(srv, method: str, path: str) -> object:
    response = httpx.request(
        method,
        f"{srv.base_url}{path}",
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 120,
    )
    assert response.status_code == 200, response.text
    return response.json()


def _trino_sampled_reads(marker: str) -> list[int]:
    """The raw input rows of each finished Trino query that sampled the profiled table, oldest
    first."""
    base = f"http://{os.environ.get('TRINO_HOST', 'localhost')}:{os.environ['TRINO_PORT']}"
    headers = {"X-Trino-User": "provisa-itest"}
    listed = httpx.get(f"{base}/v1/query", headers=headers, timeout=30).json()
    sampled = sorted(
        (
            q
            for q in listed
            if "TABLESAMPLE" in q["query"].upper()
            and marker in q["query"]
            and q["state"] == "FINISHED"
        ),
        key=lambda q: q["queryStats"]["createTime"],
    )
    reads = []
    for q in sampled:
        info = httpx.get(f"{base}/v1/query/{q['queryId']}", headers=headers, timeout=30).json()
        stats = info["queryStats"]
        # Rows the query's table scans read from storage.
        assert "physicalInputPositions" in stats, sorted(stats)
        reads.append(int(stats["physicalInputPositions"]))
    return reads


@pytest.fixture(scope="module")
def run():
    from tests.integration.isolated_server import IsolatedServer

    lake = Path(os.environ["PROVISA_E2E_FILE_LAKE_HOST"]) / f"profiler-{uuid.uuid4().hex[:8]}"
    lake.mkdir(parents=True)
    path = _make_iceberg_table(lake)
    with tempfile.TemporaryDirectory() as workdir:
        cfg = Path(workdir) / "config.yaml"
        cfg.write_text(yaml.safe_dump(_config(path)))
        srv = IsolatedServer(
            "profiler_iceberg_sample",
            engine="trino",
            config=str(cfg),
            env={
                "PROVISA_ENGINE_LAKEHOUSE_METASTORE_HOST": "hive-metastore",
                "PROVISA_ENGINE_LAKEHOUSE_METASTORE_PORT": "9083",
            },
        )
        try:
            srv.start()
            [member] = _call(srv, "GET", "/admin/profilers/profiler/catalog")  # type: ignore[misc]
            _call(srv, "POST", f"/admin/tables/{member['memberId']}/profile-runs")
            runs = _call(srv, "GET", f"/admin/tables/{member['memberId']}/profile-runs")
            yield runs[0]  # type: ignore[index]
        finally:
            srv.stop_process()


def test_an_iceberg_table_on_trino_is_block_sampled_by_whole_files(run):
    assert (run["status"], run["sample_method"], run["row_count"]) == (
        "succeeded",
        "block",
        _ROWS,
    ), run
    assert run["target_fraction"] == pytest.approx(_TARGET)
    attempts = json.loads(run["sample_attempts"])
    # Every attempt but the last came back under half its target and was read again at four times
    # the percentage; the last holds at least half the target and is what was profiled.
    assert attempts[0]["percent"] == pytest.approx(_TARGET * 100)
    for before, after in zip(attempts, attempts[1:]):
        assert before["rows"] * 2 < _TARGET * _ROWS
        assert after["percent"] == pytest.approx(min(100.0, before["percent"] * 4))
    assert attempts[-1]["rows"] == run["profiled_rows"]
    assert run["profiled_rows"] * 2 >= _TARGET * _ROWS
    assert run["sample_fraction"] == pytest.approx(run["profiled_rows"] / _ROWS)
    # Whole files: every sampled read is a multiple of a file's rows.
    assert all(a["rows"] % _ROWS_PER_FILE == 0 for a in attempts), attempts


def test_trino_read_only_the_sampled_files(run):
    reads = _trino_sampled_reads('"main"."lake_orders"')
    attempts = json.loads(run["sample_attempts"])
    assert len(reads) >= len(attempts), (reads, attempts)
    # The last sampled statements Trino ran are this run's attempts: each read exactly the rows of
    # the files it sampled -- not the table.
    assert reads[-len(attempts) :] == [a["rows"] for a in attempts]
    assert reads[-1] < _ROWS
