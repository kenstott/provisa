# Copyright (c) 2026 Kenneth Stott
# Canary: e80f5c38-3233-46d9-96c2-ca7857ec02a4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-910: capabilities preset -> self-contained desktop launch environment."""

from __future__ import annotations

import pytest

from provisa.core.desktop_profile import load_profile

#: What a started embedded plane looks like, without paying initdb in a unit test.
_FAKE_SOCKET = ("/tmp/fake-pgsock", 55432, "datadir-password")


@pytest.fixture(autouse=True)
def _embedded_pg(monkeypatch):
    """Stand in for the bundled PostgreSQL every self-contained preset now starts (REQ-1535).

    The resolver's contract is that it starts the plane and publishes the socket it was GIVEN BACK,
    so the double records the data directory it was called with and returns a socket the profile
    could not have guessed. Booting a real cluster per test is what the integration probe does.
    """
    import provisa.core.control_plane_pg as cp

    calls: list[str] = []
    monkeypatch.setattr(cp, "start", lambda datadir, **kw: (calls.append(datadir), _FAKE_SOCKET)[1])
    return calls


def _pg_url() -> str:
    host, port, password = _FAKE_SOCKET
    return f"postgresql+psycopg://provisa:{password}@/provisa?host={host}&port={port}"


def test_demo_preset_is_self_contained_and_duckdb(tmp_path):
    p = load_profile("demo", data_dir=tmp_path)
    # DuckDB engine, in-memory cache -> no Trino, no Redis server, no Docker
    assert p.env["PROVISA_ENGINE"] == "duckdb"
    assert p.env["PROVISA_REDIS_EMBEDDED"] == "1"
    # REQ-1535: the bundled PostgreSQL, so the demo can show environments (a schema each, REQ-1488)
    assert p.control_plane_store == "embedded_pg"
    assert p.env["PLATFORM_DATABASE_URL"] == _pg_url()
    assert p.env["TENANT_DATABASE_URL"] == _pg_url()
    assert "iceberg" in p.sources  # reached via the DuckDB engine (native everywhere)
    assert any("no external services" in n for n in p.notes)


def test_an_ephemeral_profile_runs_a_throwaway_embedded_postgresql(
    tmp_path, _embedded_pg, monkeypatch
):
    """REQ-2014: there is no in-memory PostgreSQL, so an ephemeral profile runs the embedded one in
    a throwaway data directory -- never the install's -- stopped and removed when the process ends."""
    discarded: list = []
    monkeypatch.setattr(
        "provisa.core.desktop_profile.atexit.register", lambda fn, *a: discarded.append((fn, a))
    )
    p = load_profile("demo", ephemeral=True, data_dir=tmp_path)
    assert p.env["PLATFORM_DATABASE_URL"] == p.env["TENANT_DATABASE_URL"] == _pg_url()
    (started,) = _embedded_pg
    assert not started.startswith(str(tmp_path)) and started.endswith("control-pg")
    assert [(fn.__name__, str(a[0])) for fn, a in discarded] == [
        ("_discard_control_plane", started)
    ]


def test_a_control_plane_store_other_than_postgresql_is_refused_by_name():
    """REQ-2014: the control plane is PostgreSQL only; sqlite is no longer offered."""
    with pytest.raises(ValueError, match="unknown control_plane_store 'sqlite'"):
        load_profile("demo", control_plane="sqlite")


def test_linux_arm64_has_no_embedded_postgresql_and_says_so(tmp_path, monkeypatch):
    """REQ-2014: pgserver ships no wheel for Linux arm64, so the embedded plane is refused there,
    naming the external PostgreSQL to use instead."""
    monkeypatch.setattr("platform.system", lambda: "Linux")
    monkeypatch.setattr("platform.machine", lambda: "aarch64")
    with pytest.raises(ValueError, match="Linux arm64 has no embedded PostgreSQL"):
        load_profile("native", data_dir=tmp_path)


def test_unknown_preset_rejected():
    with pytest.raises(ValueError, match="unknown preset"):
        load_profile("nope")


def test_native_preset_is_self_contained_with_no_demo_sources(tmp_path, _embedded_pg):
    # The default desktop install: same self-contained DuckDB + embedded PostgreSQL runtime as demo,
    # but no seeded data. The plane lives under the install's own data dir, never the demo's.
    p = load_profile("native", data_dir=tmp_path)
    assert p.env["PROVISA_ENGINE"] == "duckdb"
    assert p.env["PROVISA_REDIS_EMBEDDED"] == "1"
    assert p.control_plane_store == "embedded_pg"
    assert p.env["PLATFORM_DATABASE_URL"] == _pg_url()
    assert _embedded_pg == [str(tmp_path / "control-pg")]
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
    assert p.env["PLATFORM_DATABASE_URL"] == _pg_url()  # still the native control plane


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
