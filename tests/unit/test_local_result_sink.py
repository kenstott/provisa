# Copyright (c) 2026 Kenneth Stott
# Canary: 20f5fc19-cc74-4ced-9f76-b12f40499029
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The local result sink (REQ-1191, REQ-1192, REQ-1193, REQ-1195): a large result written to a file
in the org's folder -- by the engine where it writes the format, by Provisa from the rows
otherwise -- served at /data/redirect-file/<name> to the user who asked for it alone, and deleted
once past redirect.ttl."""

# Requirements: REQ-1191, REQ-1192, REQ-1193, REQ-1195

from __future__ import annotations

import csv
import json
import os
import time
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import provisa.core.settings_catalog  # noqa: F401 -- declares the settings read here
from provisa.audit.context import audit_identity_scope
from provisa.core.request_context import current_org
from provisa.executor import local_results
from provisa.executor.local_results import schedule_removal as _real_schedule_removal
from provisa.executor.result import QueryResult
from provisa.federation import result_sink

ROWS = [(1, "ada", 9.5), (2, "ben", None), (3, "cy", 1.25)]
#: The base URI of the HTTP request a result is written for (the TestClient's own host).
BASE = "http://testserver"
COLUMNS = ["id", "name", "score"]


@pytest.fixture
def org_folder(tmp_path, monkeypatch) -> Path:
    """The bound org's folder, in a data directory of this test's own."""
    from provisa.core import org_files

    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path))
    folder = org_files.folder(current_org.get() or "default")
    folder.mkdir(parents=True)
    return folder


class _RowsEngine:
    """An engine that writes no result file itself: Provisa writes it from the rows."""

    def __init__(self) -> None:
        self.ran: list[tuple[str, list | None]] = []

    def writes_result_file(self, fmt: str) -> bool:
        return False

    def execute_engine_sync(self, sql: str, params: list | None = None) -> QueryResult:
        self.ran.append((sql, params))
        return QueryResult(rows=list(ROWS), column_names=list(COLUMNS))


class _FileEngine:
    """An engine that writes the result file itself (as DuckDB and Postgres do)."""

    def __init__(self) -> None:
        self.written: list[tuple] = []

    def writes_result_file(self, fmt: str) -> bool:
        return fmt == "csv"

    def write_result_file(self, sql: str, fmt: str, params: list | None, path: str) -> int:
        self.written.append((sql, fmt, params, path))
        Path(path).write_text("id\n1\n2\n")
        return 2


@pytest.fixture(autouse=True)
def _no_timers(monkeypatch):
    """A result's removal timer is recorded, not started."""
    scheduled: list[tuple[str, int]] = []
    monkeypatch.setattr(
        local_results, "schedule_removal", lambda path, ttl: scheduled.append((path.name, ttl))
    )
    return scheduled


def _write(engine, fmt: str, *, user: str = "ada@example.com", ttl: int = 600) -> dict:
    with audit_identity_scope(user, "http"), local_results.bound_request_base(BASE + "/"):
        return local_results.write(
            SimpleNamespace(federation_engine=engine), "SELECT 1", fmt, [7], ttl
        )


def _path(handle: dict, folder: Path) -> Path:
    served_at = BASE + local_results.SERVE_PATH
    assert handle["redirect_url"].startswith(served_at)
    return folder / "results" / handle["redirect_url"][len(served_at) :]


# -- writing ------------------------------------------------------------------------------------


def test_an_engine_that_writes_the_format_writes_the_file(org_folder, _no_timers):
    engine = _FileEngine()
    handle = _write(engine, "csv", ttl=900)
    path = _path(handle, org_folder)
    assert engine.written == [("SELECT 1", "csv", [7], str(path))]
    assert path.read_text() == "id\n1\n2\n"
    assert handle == {
        "sink": "local",
        "redirect_url": f"{BASE}/data/redirect-file/{path.name}",
        "row_count": 2,
        "expires_in": 900,
        "content_type": "text/csv",
    }
    assert _no_timers == [(path.name, 900)]


@pytest.mark.parametrize("fmt", ["parquet", "csv", "arrow", "json", "ndjson", "orc"])
def test_provisa_writes_the_file_from_the_rows_otherwise(org_folder, fmt):
    engine = _RowsEngine()
    handle = _write(engine, fmt)
    path = _path(handle, org_folder)
    assert engine.ran == [("SELECT 1", [7])]
    assert handle["row_count"] == 3
    assert handle["content_type"] == local_results.FILE_FORMATS[fmt][0]
    assert path.suffix == local_results.FILE_FORMATS[fmt][1]
    if fmt == "parquet":
        assert pq.read_table(path).to_pylist()[1] == {"id": 2, "name": "ben", "score": None}
    elif fmt == "csv":
        assert next(csv.reader(path.read_text().splitlines())) == COLUMNS
    elif fmt == "json":
        assert json.loads(path.read_text())[0] == {"id": 1, "name": "ada", "score": 9.5}
    elif fmt == "ndjson":
        assert [json.loads(line)["name"] for line in path.read_text().splitlines()] == [
            "ada",
            "ben",
            "cy",
        ]
    elif fmt == "orc":
        import pyarrow.orc as orc

        assert orc.read_table(str(path)).num_rows == 3
    else:
        import pyarrow as pa

        assert pa.ipc.open_stream(path.read_bytes()).read_all().num_rows == 3


def test_a_format_no_file_is_written_in_is_refused_by_name(org_folder):
    with pytest.raises(local_results.ResultFormatNotWritable, match="'xlsx'"):
        _write(_RowsEngine(), "xlsx")


def test_a_result_with_no_user_to_serve_it_to_is_not_written(org_folder):
    with pytest.raises(local_results.ResultOwnerUnknown):
        local_results.write(
            SimpleNamespace(federation_engine=_RowsEngine()), "SELECT 1", "csv", None, 60
        )
    assert not (org_folder / "results").exists() or not list((org_folder / "results").iterdir())


def test_a_write_that_fails_leaves_no_file(org_folder):
    class _Failing(_FileEngine):
        def write_result_file(self, sql, fmt, params, path):
            Path(path).write_text("partial")
            raise RuntimeError("engine died")

    with pytest.raises(RuntimeError, match="engine died"):
        _write(_Failing(), "csv")
    assert list((org_folder / "results").iterdir()) == []


def test_duckdb_writes_each_file_format_itself(tmp_path):
    """REQ-1195: DuckDB's own COPY ... TO writes the file, with the statement's bound values."""
    import duckdb

    con = duckdb.connect()
    select = "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) t(id, name) WHERE id >= ?"
    for fmt in sorted(result_sink.DUCKDB_FILE_FORMATS):
        path = tmp_path / f"r.{fmt}"
        written = con.execute(result_sink.duckdb_copy_file(select, str(path), fmt), [2]).fetchone()
        assert written == (2,), fmt
        assert path.stat().st_size > 0
    assert (tmp_path / "r.csv").read_text().splitlines() == ["id,name", "2,b", "3,c"]
    assert json.loads((tmp_path / "r.json").read_text()) == [
        {"id": 2, "name": "b"},
        {"id": 3, "name": "c"},
    ]
    assert pq.read_table(tmp_path / "r.parquet").num_rows == 2


def test_postgres_writes_the_csv_file_itself(embedded_postgres, tmp_path):
    """REQ-1195: Postgres's COPY ... TO STDOUT, streamed into the file on Provisa's host, with the
    statement's $N values bound into it."""
    import psycopg2.pool

    from provisa.federation.pg_runtime import PgFederationRuntime

    pool = psycopg2.pool.ThreadedConnectionPool(1, 2, embedded_postgres.dsn())
    runtime = SimpleNamespace(_read_pool=pool, refresh_live_relations=lambda sql: None)
    path = tmp_path / "r.csv"
    try:
        written = PgFederationRuntime.write_result_csv(
            runtime,  # type: ignore[arg-type]
            "SELECT g AS id, 'it''s ' || g AS note FROM generate_series(1, 5) g "
            "WHERE g > $1 AND 'a%' LIKE 'a%'",
            [2],
            str(path),
        )
    finally:
        pool.closeall()
    assert written == 3
    assert list(csv.reader(path.read_text().splitlines())) == [
        ["id", "note"],
        ["3", "it's 3"],
        ["4", "it's 4"],
        ["5", "it's 5"],
    ]


# -- serving: the owner alone (REQ-1192) --------------------------------------------------------


def test_the_owner_opens_the_result_and_another_user_is_refused(org_folder):
    handle = _write(_RowsEngine(), "csv", user="ada@example.com")
    name = _path(handle, org_folder).name
    path, content_type = local_results.open_for(name, "ada@example.com", 600)
    assert path == org_folder / "results" / name
    assert content_type == "text/csv"
    with pytest.raises(local_results.NotResultOwner):
        local_results.open_for(name, "ben@example.com", 600)


@pytest.mark.parametrize(
    "name", ["../secrets.json", "x.csv", "0" * 32 + ".exe", "", "0" * 32 + ".csv.owner"]
)
def test_a_name_that_is_not_a_result_is_not_found(org_folder, name):
    with pytest.raises(local_results.ResultFileNotFound):
        local_results.open_for(name, "ada@example.com", 600)


def test_a_result_past_its_ttl_is_not_found(org_folder):
    handle = _write(_RowsEngine(), "csv")
    path = _path(handle, org_folder)
    old = time.time() - 120
    os.utime(path, (old, old))
    with pytest.raises(local_results.ResultFileNotFound):
        local_results.open_for(path.name, "ada@example.com", 60)


def _client(identity_user: str | None) -> TestClient:
    from provisa.api.data.redirect_file import router

    app = FastAPI()

    @app.middleware("http")
    async def _identity(request, call_next):
        if identity_user is not None:
            request.state.identity = SimpleNamespace(user_id=identity_user)
        return await call_next(request)

    app.include_router(router)
    return TestClient(app)


def test_the_endpoint_serves_the_owner_and_refuses_another_user(org_folder):
    handle = _write(_RowsEngine(), "parquet", user="ada@example.com")
    url = handle["redirect_url"]

    got = _client("ada@example.com").get(url)
    assert got.status_code == 200, got.text
    assert got.headers["content-type"] == "application/vnd.apache.parquet"
    path = _path(handle, org_folder)
    assert got.content == path.read_bytes()

    refused = _client("ben@example.com").get(url)
    assert refused.status_code == 403
    assert _client(None).get(url).status_code == 401
    missing = _client("ada@example.com").get("/data/redirect-file/" + "0" * 32 + ".csv")
    assert missing.status_code == 404


# -- reaping (REQ-1193) -------------------------------------------------------------------------


def test_results_past_the_ttl_are_deleted_with_their_records(org_folder):
    engine = _RowsEngine()
    old = _path(_write(engine, "csv"), org_folder)
    young = _path(_write(engine, "csv"), org_folder)
    stale = time.time() - 3600
    os.utime(old, (stale, stale))
    results = org_folder / "results"
    orphan = results / ("f" * 32 + ".csv.owner")
    orphan.write_text("{}")
    other = results / "notes.txt"
    other.write_text("kept")

    assert local_results.reap_expired(results, 600) == 1
    assert not old.exists() and not (results / (old.name + ".owner")).exists()
    assert young.exists() and (results / (young.name + ".owner")).exists()
    assert not orphan.exists()
    assert other.exists()


def test_each_new_result_sweeps_the_expired_ones(org_folder):
    old = _path(_write(_RowsEngine(), "csv"), org_folder)
    stale = time.time() - 3600
    os.utime(old, (stale, stale))
    _write(_RowsEngine(), "csv", ttl=600)
    assert not old.exists()


def test_the_timer_removes_the_result_at_its_ttl(org_folder, monkeypatch):
    """REQ-1193: as the object store's results are, a result is removed by the background timer
    once its ttl has passed."""
    import asyncio

    from provisa.core import connection_loop

    scheduled: list[tuple[float, object, str]] = []
    monkeypatch.setattr(
        connection_loop,
        "spawn_after",
        lambda delay, coro, *, name: scheduled.append((delay, coro, name)),
    )
    path = org_folder / "results"
    path.mkdir(exist_ok=True)
    result = path / ("a" * 32 + ".csv")
    result.write_text("x")
    (path / (result.name + ".owner")).write_text("{}")

    _real_schedule_removal(result, 45)
    assert [(d, n) for d, _, n in scheduled] == [(45, f"result-file-cleanup:{result.name}")]
    asyncio.run(scheduled[0][1])  # type: ignore[arg-type]
    assert not result.exists() and not (path / (result.name + ".owner")).exists()
