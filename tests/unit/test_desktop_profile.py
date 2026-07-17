# Copyright (c) 2026 Kenneth Stott
# Canary: e80f5c38-3233-46d9-96c2-ca7857ec02a4
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


def test_native_preset_is_self_contained_with_no_demo_sources(tmp_path):
    # The default desktop install: same self-contained DuckDB/sqlite runtime as demo, but no seeded data.
    p = load_profile("native", data_dir=tmp_path)
    assert p.env["PROVISA_ENGINE"] == "duckdb"
    assert p.env["PROVISA_REDIS_EMBEDDED"] == "1"
    assert p.control_plane_store == "sqlite"
    assert p.env["PLATFORM_DATABASE_URL"].startswith("sqlite+aiosqlite:///")
    assert p.sources == []  # empty model — nothing seeded
    assert "PROVISA_ENGINE_URL" not in p.env
    assert "TRINO_HOST" not in p.env
    assert "OTEL_EXPORTER_OTLP_ENDPOINT" not in p.env


def test_native_with_trino_docker_engine_override(tmp_path):
    # Wizard picks the Trino-on-Docker engine: PROVISA_ENGINE=trino + endpoint, control plane stays native.
    p = load_profile(
        "native", data_dir=tmp_path, engine="trino", trino_endpoint=("localhost", 8080)
    )
    assert p.env["PROVISA_ENGINE"] == "trino"
    assert p.env["TRINO_HOST"] == "localhost"
    assert p.env["TRINO_PORT"] == "8080"
    assert p.env["PLATFORM_DATABASE_URL"].startswith(
        "sqlite+aiosqlite:///"
    )  # still native control plane


def test_native_with_external_engine_and_store(tmp_path):
    p = load_profile(
        "native",
        data_dir=tmp_path,
        engine="sqlalchemy",
        engine_url="postgresql+psycopg://u:p@host:5432/db",
        materialize_url="snowflake://u:p@acct/db/schema?warehouse=wh",
    )
    assert p.env["PROVISA_ENGINE"] == "sqlalchemy"
    assert p.env["PROVISA_ENGINE_URL"] == "postgresql+psycopg://u:p@host:5432/db"
    assert p.env["PROVISA_MATERIALIZE_URL"] == "snowflake://u:p@acct/db/schema?warehouse=wh"


def test_native_with_observability_collector(tmp_path):
    p = load_profile("native", data_dir=tmp_path, otlp_endpoint="http://collector:4317")
    assert p.env["OTEL_EXPORTER_OTLP_ENDPOINT"] == "http://collector:4317"
    # the "leave OTLP unset" note must not appear once a collector is wired
    assert not any("leave OTEL_EXPORTER_OTLP_ENDPOINT unset" in n for n in p.notes)


def test_unknown_engine_override_rejected(tmp_path):
    with pytest.raises(ValueError, match="unknown federation_engine"):
        load_profile("native", data_dir=tmp_path, engine="nope")
