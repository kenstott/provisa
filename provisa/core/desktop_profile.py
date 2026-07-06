# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Translate a capabilities.yaml preset into a concrete launch environment (REQ-910).

config/capabilities.yaml declares INTENT (roles -> option ids + prereqs). This module is the one place
that maps a preset to the runtime toggles that make the desktop server start SELF-CONTAINED — no Trino,
Redis, MinIO, or Docker. The 'demo' preset yields a few-seconds, offline start:

  federation_engine: duckdb   -> PROVISA_ENGINE=duckdb            (federation_engine.py:build_engine)
  cache: in_memory            -> PROVISA_REDIS_EMBEDDED=1         (fakeredis, REQ-829; app.py:1303)
  observability: off          -> leave OTLP endpoint unset       (otel_setup: no-op when empty)
  external_services: []       -> nothing to boot                 (Trino/MinIO paths gated off)

The runtime's discover()/probe still enforces functional truth on top of this declared intent.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

_CAPABILITIES = Path(__file__).resolve().parents[2] / "config" / "capabilities.yaml"

# capability engine id -> PROVISA_ENGINE registry key (engine.py _ENGINE_BUILDERS: trino/pg/duckdb/sqlalchemy)
_ENGINE_KEY = {
    "duckdb": "duckdb",
    "pg_duckdb": "pg",  # build_pg_engine; discover() activates the pg_duckdb connectors where present
    "postgres_fdw": "pg",
    "trino": "trino",
    "sqlalchemy": "sqlalchemy",
}


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


def load_profile(
    preset: str = "demo",
    *,
    data_dir: Path | str | None = None,
    capabilities_path: Path | None = None,
    ephemeral: bool = False,
) -> LaunchProfile:
    """Resolve a capabilities preset into the launch environment for a self-contained desktop start.

    A ``sqlite`` control_plane_store yields a TRULY INSTANT control plane (no initdb) via the SQLAlchemy
    ``Database`` abstraction (REQ-837, tested). ``ephemeral=True`` uses in-memory sqlite (zero on-disk
    state, fastest possible) instead of files under ``data_dir``.
    """
    caps = yaml.safe_load((capabilities_path or _CAPABILITIES).read_text())
    presets = caps.get("presets", {})
    if preset not in presets:
        raise ValueError(f"unknown preset {preset!r}; defined: {sorted(presets)}")
    spec = presets[preset]

    engine_id = spec["federation_engine"]
    if engine_id not in _ENGINE_KEY:
        raise ValueError(f"preset {preset!r} names unknown federation_engine {engine_id!r}")
    env: dict[str, str] = {"PROVISA_ENGINE": _ENGINE_KEY[engine_id]}

    if spec.get("cache") == "in_memory":
        env["PROVISA_REDIS_EMBEDDED"] = "1"  # fakeredis — no Redis server, no Docker

    notes: list[str] = []
    cp_store = spec.get("control_plane_store", "embedded_pg")
    if cp_store == "sqlite":
        # SQLAlchemy control plane on sqlite — no initdb, no pgserver, no Python-version gate.
        if ephemeral:
            env["PLATFORM_DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
            env["TENANT_DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
            notes.append("control plane: in-memory sqlite (ephemeral, fastest)")
        else:
            dd = Path(data_dir) if data_dir else _default_data_dir(preset)
            env["PLATFORM_DATABASE_URL"] = f"sqlite+aiosqlite:///{dd / 'platform.db'}"
            env["TENANT_DATABASE_URL"] = f"sqlite+aiosqlite:///{dd / 'tenant.db'}"
            notes.append(f"control plane: sqlite files under {dd} (instant, persistent)")
    if spec.get("observability", "off") == "off":
        notes.append("observability off: leave OTEL_EXPORTER_OTLP_ENDPOINT unset (config endpoint '')")
    if not spec.get("external_services"):
        notes.append("no external services: Trino/MinIO/Docker paths are not started")
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
