# Copyright (c) 2026 Kenneth Stott
# Canary: dd062c06-3ecd-4d4b-8f98-54508a1c2840
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The bundle build's setup server starts where the build has no HOME (scripts/demo_bundle.py).

The build gives its servers an environment of their own. Without HOME, DuckDB cannot place its
per-user extension directory: the server failed to start ("To set the TimeZone setting, the icu
extension needs to be loaded. But it could not be autoloaded."), on a demo whose views keep time
(the reproduce demo's bitemporal report). The build now names where DuckDB finds its extensions
(_duckdb_env), and this sets the reproduce demo up with HOME unset, its trading book in an
embedded Postgres of the test's own, to show the server starts and its setup runs."""

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


class _Book:
    """The demo's stores as the build names them: its trading book, here an embedded Postgres."""

    def __init__(self, port: int) -> None:
        self.env = {"REPRODUCE_PG_HOST": "127.0.0.1", "REPRODUCE_PG_PORT": str(port)}


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


def test_the_setup_server_starts_and_sets_up_with_home_absent(tmp_path, monkeypatch, book):
    demo = tmp_path / "named" / "reproduce"
    shutil.copytree(_REPO / "demo" / "named" / "reproduce", demo)
    monkeypatch.delenv("HOME", raising=False)
    # Raises SystemExit naming an early exit (the server did not start) or a timeout.
    _builder()._set_up(tmp_path, demo, _Book(book.port), 300)
