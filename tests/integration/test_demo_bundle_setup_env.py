# Copyright (c) 2026 Kenneth Stott
# Canary: dd062c06-3ecd-4d4b-8f98-54508a1c2840
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The bundle build's setup server runs on an environment of the build's own (scripts/demo_bundle.py),
and its views' history is exported from where it kept them.

The build gives its servers an environment of their own. Without HOME, DuckDB cannot place its
per-user extension directory: the server failed to start ("To set the TimeZone setting, the icu
extension needs to be loaded. But it could not be autoloaded."), on a demo whose views keep time
(the reproduce demo's bitemporal report). The build now names where DuckDB finds its extensions
(_duckdb_env), and this sets the reproduce demo up with HOME unset, its trading book in an
embedded Postgres of the test's own, to show the server starts and its setup runs.

Nor is a server given this process's own environment: the stores' addresses only. Given the whole
of it, the setup server took a test session's PROVISA_MATERIALIZE_URL for its own and kept its
views in the session's PostgreSQL, and the export then found no store (CI run 39239941042). Here
this process holds such a URL, unreachable, and the views are exported from the server's own
DuckDB store, with the records of the refreshes that built them."""

# Requirements: REQ-2016, REQ-2049

from __future__ import annotations

import importlib.util
import shutil
from pathlib import Path

import psycopg
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_REPO = Path(__file__).resolve().parents[2]


def _builder():
    spec = importlib.util.spec_from_file_location(
        "demo_bundle", _REPO / "scripts" / "demo_bundle.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _stores(builder, demo: Path, port: int):
    """The build's own Stores for the demo, its trading book at the embedded Postgres instead of
    its compose service (never started here)."""
    stores = builder.Stores(demo)
    book = {"REPRODUCE_PG_HOST": "127.0.0.1", "REPRODUCE_PG_PORT": str(port)}
    stores.addresses.update(book)
    stores.env.update(book)
    return stores


@pytest.fixture
def book():
    from tests.embedded_pg import start_local_postgres

    pg = start_local_postgres()
    try:
        with psycopg.connect(pg.dsn("postgres"), autocommit=True) as conn:
            conn.execute("CREATE DATABASE reproduce")
        db = _REPO / "demo" / "named" / "reproduce" / "db"
        with psycopg.connect(pg.dsn("reproduce"), autocommit=True) as conn:
            conn.execute((db / "init.sql").read_text())
            conn.execute((db / "reader.sql").read_text())
        yield pg
    finally:
        pg.stop()


def test_the_setup_server_runs_on_the_builds_own_environment(tmp_path, monkeypatch, book):
    import json

    demo = tmp_path / "named" / "reproduce"
    shutil.copytree(_REPO / "demo" / "named" / "reproduce", demo)
    monkeypatch.delenv("HOME", raising=False)
    monkeypatch.setenv("PROVISA_MATERIALIZE_URL", "postgresql://nobody:x@127.0.0.1:1/nowhere")
    builder = _builder()
    stores = _stores(builder, demo, book.port)
    # Raises SystemExit naming an early exit (the server did not start), a timeout, or a store
    # not where the views are exported from.
    builder._set_up(tmp_path, demo, stores, 300, None)
    builder._export_views(tmp_path, demo, stores)
    record = json.loads((demo / "seeded" / "views.json").read_text())
    assert record["tables"] == ["mv_view_exposure"]
    assert record["refreshes"] == 2, record
