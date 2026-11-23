# Copyright (c) 2026 Kenneth Stott
# Canary: 5b7e2c91-0d4a-4f3e-9a68-1c2f7d3e8b40
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Each demo sandbox owns its embedded DuckDB store (REQ-2006, REQ-1992): two sandboxes never open
one file, and a write into one runs while the other's is held (REQ-1901: one operation at a time
per store file, under that file's own sentinel lock)."""

# Requirements: REQ-2006, REQ-1992, REQ-1901

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import make_url

from provisa.core import org_files
from provisa.federation import materialize_broker
from provisa.federation.engine import embedded_store_dsn

_SANDBOXES = ("sbx0123456789ab", "sbxba9876543210")


@pytest.fixture
def stores(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path))
    for org in _SANDBOXES:
        asyncio.run(org_files.prepare(org))
    return [make_url(embedded_store_dsn(org)).database for org in _SANDBOXES]


def test_two_sandboxes_never_open_one_store_file(stores, tmp_path):
    first, second = stores
    assert first != second
    assert first == str(tmp_path / "orgs" / _SANDBOXES[0] / "materialize.duckdb")
    assert second == str(tmp_path / "orgs" / _SANDBOXES[1] / "materialize.duckdb")


def test_a_write_into_one_sandbox_runs_while_the_others_store_is_held(stores):
    first, second = stores
    materialize_broker.get_broker(second).execute("CREATE TABLE mat_store.main.held (x INTEGER)")

    def _write_first_while_second_is_held(con) -> None:
        con.execute("SELECT count(*) FROM mat_store.main.held").fetchone()
        materialize_broker.get_broker(first).execute("CREATE TABLE mat_store.main.own (x INTEGER)")

    materialize_broker._with_store(second, _write_first_while_second_is_held, read_only=True)
    assert materialize_broker.get_broker(first).tables("main") == ["own"]
    assert materialize_broker.get_broker(second).tables("main") == ["held"]
