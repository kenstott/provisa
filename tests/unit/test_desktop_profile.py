# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-910: capabilities preset -> self-contained desktop launch environment."""

from __future__ import annotations

import pytest

from provisa.core.desktop_profile import load_profile


def test_demo_preset_is_self_contained_and_duckdb(tmp_path):
    p = load_profile("demo", data_dir=tmp_path)
    # DuckDB engine, in-memory cache -> no Trino, no Redis server, no Docker
    assert p.env["PROVISA_ENGINE"] == "duckdb"
    assert p.env["PROVISA_REDIS_EMBEDDED"] == "1"
    # sqlite control plane via the ASYNC aiosqlite driver -> no initdb, truly instant
    assert p.control_plane_store == "sqlite"
    assert p.env["PLATFORM_DATABASE_URL"].startswith("sqlite+aiosqlite:///")
    assert p.env["TENANT_DATABASE_URL"].startswith("sqlite+aiosqlite:///")
    assert "iceberg" in p.sources  # reached via the DuckDB engine (native everywhere)
    assert any("no external services" in n for n in p.notes)


def test_demo_ephemeral_uses_in_memory_sqlite():
    p = load_profile("demo", ephemeral=True)
    assert p.env["PLATFORM_DATABASE_URL"] == "sqlite+aiosqlite:///:memory:"


def test_unknown_preset_rejected():
    with pytest.raises(ValueError, match="unknown preset"):
        load_profile("nope")
