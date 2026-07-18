# Copyright (c) 2026 Kenneth Stott
# Canary: 6474479c-f5b0-46ae-a2cd-c2404d09e616
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Translate a capabilities.yaml preset into a concrete launch environment (REQ-910).

config/capabilities.yaml declares INTENT (roles -> option ids + prereqs). This module is the one place
that maps a preset to the runtime toggles that make the desktop server start SELF-CONTAINED — no the engine,
Redis, MinIO, or Docker. The 'demo' preset yields a few-seconds, offline start:

  federation_engine: duckdb   -> PROVISA_ENGINE=duckdb            (federation_engine.py:build_engine)
  cache: in_memory            -> PROVISA_REDIS_EMBEDDED=1         (fakeredis, REQ-829; app.py:1303)
  observability: off          -> leave OTLP endpoint unset       (otel_setup: no-op when empty)
  external_services: []       -> nothing to boot                 (the engine/MinIO paths gated off)

The runtime's discover()/probe still enforces functional truth on top of this declared intent.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

# REQ-1127: the pip wheel embeds config/ under provisa/_config/ so the embedded tier (REQ-1126)
# is self-contained. Prefer the packaged copy; fall back to the repo-root config/ in the dev tree.
_PACKAGED_CAPABILITIES = Path(__file__).resolve().parents[1] / "_config" / "capabilities.yaml"
_REPO_CAPABILITIES = Path(__file__).resolve().parents[2] / "config" / "capabilities.yaml"
_CAPABILITIES = (
    _PACKAGED_CAPABILITIES if _PACKAGED_CAPABILITIES.exists() else _REPO_CAPABILITIES
)

# capability engine id -> PROVISA_ENGINE registry key (engine.py _ENGINE_BUILDERS: engine/pg/duckdb/sqlalchemy)
_ENGINE_KEY = {
    "duckdb": "duckdb",
    "pg_duckdb": "pg",  # build_pg_engine; discover() activates the pg_duckdb connectors where present
    "postgres_fdw": "pg",
    "trino": "trino",
    "sqlalchemy": "sqlalchemy",
}

# REQ-889: the container tier adds Trino + observability strictly as COMPUTE, never as the metadata
# home. The metadata/config/roles store is the EMBEDDED single source of truth in every tier and MUST
# NOT be relocated onto a compute-only tier addition. A preset that names one of these as its
# control_plane_store is an invariant violation — fail loud (never silently accept a compute engine as
# the metadata home).
_COMPUTE_ONLY_TIER_ADDITIONS = frozenset(
    {"trino", "trino-byo", "clickhouse", "observability", "otel", "otlp"}
)


@dataclass(frozen=True)
class LaunchProfile:
    """A resolved launch spec derived from a capabilities preset."""

    name: str
    env: dict[str, str]  # process-env additions that select the self-contained runtime
    control_plane_store: str  # where tenant/catalog state lives (embedded_pg | sqlite | ...)
    sources: list[str]
    notes: list[str] = field(default_factory=list)


def _default_data_dir(preset: str) -> Path:
    return Path.home() / ".provisa" / preset


def _apply_materialize(
    materialize_url: str | None,
    spec: dict,
    env: dict[str, str],
    notes: list[str],
    *,
    preset: str,
    data_dir: Path | str | None,
    ephemeral: bool,
) -> None:
    """Resolve the tenant materialization store env (REQ-989). An explicit external DSN wins; otherwise
    the preset's embedded store maps to an in-process DuckDB/SQLite store — never the platform tenant
    DB — so the zero-config stack lands locally."""
    if materialize_url:
        env["PROVISA_MATERIALIZE_URL"] = materialize_url
        notes.append(f"external materialization store: {materialize_url.split('://', 1)[0]}://…")
        return
    mat_store = spec.get("materialization_store")
    if mat_store not in ("duckdb_file", "sqlite"):
        return
    scheme = "duckdb" if mat_store == "duckdb_file" else "sqlite+aiosqlite"
    if ephemeral:
        env["PROVISA_MATERIALIZE_URL"] = f"{scheme}:///:memory:"
        notes.append(f"materialization store: in-memory {mat_store} (ephemeral)")
        return
    mdd = Path(data_dir) if data_dir else _default_data_dir(preset)
    fname = "materialize.duckdb" if mat_store == "duckdb_file" else "materialize.db"
    env["PROVISA_MATERIALIZE_URL"] = f"{scheme}:///{mdd / fname}"
    notes.append(f"materialization store: {mat_store} under {mdd} (embedded)")


def _apply_control_plane(
    cp_store: str,
    env: dict[str, str],
    notes: list[str],
    *,
    preset: str,
    data_dir: Path | str | None,
    ephemeral: bool,
) -> None:
    """Resolve the metadata-home (control-plane store) env for the tier. REQ-889: the metadata home is
    the embedded single source of truth in every tier and is NEVER a compute-only tier addition —
    fail loud if a preset names one (Trino/observability are added strictly as compute)."""
    if cp_store in _COMPUTE_ONLY_TIER_ADDITIONS:
        raise ValueError(
            f"REQ-889: control_plane_store {cp_store!r} is a compute-only tier addition; the "
            "metadata/config/roles store must stay on the embedded home (sqlite/embedded_pg) or an "
            "explicit RDB — Trino/observability are added strictly as compute, never the metadata home"
        )
    if cp_store != "sqlite":
        return
    # SQLAlchemy control plane on sqlite — no initdb, no pgserver, no Python-version gate.
    if ephemeral:
        env["PLATFORM_DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
        env["TENANT_DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
        notes.append("control plane: in-memory sqlite (ephemeral, fastest)")
        return
    dd = Path(data_dir) if data_dir else _default_data_dir(preset)
    env["PLATFORM_DATABASE_URL"] = f"sqlite+aiosqlite:///{dd / 'platform.db'}"
    env["TENANT_DATABASE_URL"] = f"sqlite+aiosqlite:///{dd / 'tenant.db'}"
    notes.append(f"control plane: sqlite files under {dd} (instant, persistent)")


def load_profile(
    preset: str = "demo",
    *,
    data_dir: Path | str | None = None,
    capabilities_path: Path | None = None,
    ephemeral: bool = False,
    engine: str | None = None,
    engine_url: str | None = None,
    materialize_url: str | None = None,
    trino_endpoint: tuple[str, int] | None = None,
    otlp_endpoint: str | None = None,
) -> LaunchProfile:
    """Resolve a capabilities preset into the launch environment for a self-contained desktop start.

    A ``sqlite`` control_plane_store yields a TRULY INSTANT control plane (no initdb) via the SQLAlchemy
    ``Database`` abstraction (REQ-837, tested). ``ephemeral=True`` uses in-memory sqlite (zero on-disk
    state, fastest possible) instead of files under ``data_dir``.

    Wizard-time overrides layer the operator's chosen deployment on top of the preset's self-contained
    base (REQ-910 desktop tiers). Each override maps to the SAME env var the runtime already resolves:

      engine          override the federation engine id (e.g. 'trino' for the Docker engine, or an
                      external 'sqlalchemy'/'pg'/'clickhouse') -> PROVISA_ENGINE (engine.py:build_engine)
      engine_url      external engine DSN -> PROVISA_ENGINE_URL (engine.py:configured_engine_url)
      materialize_url external materialization store DSN -> PROVISA_MATERIALIZE_URL
                      (engine.py:configured_materialize_url)
      trino_endpoint  (host, port) of an external/Docker Trino -> TRINO_HOST/TRINO_PORT
                      (engine.py:configured_engine_endpoint)
      otlp_endpoint   Provisa's own observability is ALWAYS on (self-telemetry, viewable in Admin).
                      This override only REDIRECTS the OTLP export to external obs infra —
                      OTEL_EXPORTER_OTLP_ENDPOINT (otel_setup). Points at either the bundled demo
                      collector/prometheus/grafana Docker stack or the operator's real collector; left
                      unset when no external obs integration is chosen.
    """
    caps = yaml.safe_load((capabilities_path or _CAPABILITIES).read_text())
    presets = caps.get("presets", {})
    if preset not in presets:
        raise ValueError(f"unknown preset {preset!r}; defined: {sorted(presets)}")
    spec = presets[preset]

    engine_id = engine or spec["federation_engine"]
    if engine_id not in _ENGINE_KEY:
        raise ValueError(f"unknown federation_engine {engine_id!r}; known: {sorted(_ENGINE_KEY)}")
    env: dict[str, str] = {"PROVISA_ENGINE": _ENGINE_KEY[engine_id]}

    if spec.get("cache") == "in_memory":
        env["PROVISA_REDIS_EMBEDDED"] = "1"  # fakeredis — no Redis server, no Docker

    notes: list[str] = []
    if engine_url:
        env["PROVISA_ENGINE_URL"] = engine_url
        notes.append(f"external federation engine: {engine_url.split('://', 1)[0]}://…")
    _apply_materialize(
        materialize_url, spec, env, notes, preset=preset, data_dir=data_dir, ephemeral=ephemeral
    )
    if trino_endpoint:
        host, port = trino_endpoint
        env["TRINO_HOST"] = host
        env["TRINO_PORT"] = str(port)
        notes.append(f"Trino engine endpoint: {host}:{port}")
    if otlp_endpoint:
        env["OTEL_EXPORTER_OTLP_ENDPOINT"] = otlp_endpoint
        notes.append(f"obs export redirected to external collector: {otlp_endpoint}")

    cp_store = spec.get("control_plane_store", "embedded_pg")
    _apply_control_plane(
        cp_store, env, notes, preset=preset, data_dir=data_dir, ephemeral=ephemeral
    )
    if spec.get("observability", "off") == "off" and not otlp_endpoint:
        notes.append(
            "observability off: leave OTEL_EXPORTER_OTLP_ENDPOINT unset (config endpoint '')"
        )
    if not spec.get("external_services"):
        notes.append("no external services: the engine/MinIO/Docker paths are not started")
    target = spec.get("startup_target_seconds")
    if target:
        notes.append(f"startup target: {target}s")

    return LaunchProfile(
        name=preset,
        env=env,
        control_plane_store=spec.get("control_plane_store", "embedded_pg"),
        sources=list(spec.get("sources", [])),
        notes=notes,
    )
