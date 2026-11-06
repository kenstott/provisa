# Copyright (c) 2026 Kenneth Stott
# Canary: 30035524-316a-4035-bd3a-061141187519
#
# This source code is licensed under the Business Source License 1.1
"""The DuckDB extensions an air-gapped install bundles (scripts/ci/mirror_duckdb_extensions.py)
are every one the DuckDB engine's connectors declare, the core scanners, and what a file
source's location needs (REQ-1997, REQ-1998). The list once came from a module the connectors
had moved out of, and an install got postgres and sqlite alone."""

from __future__ import annotations

import importlib.util
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "ci" / "mirror_duckdb_extensions.py"


def _mirror():
    spec = importlib.util.spec_from_file_location("mirror_duckdb_extensions", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_every_extension_the_duckdb_engine_loads_is_mirrored():
    from provisa.federation.engine import build_duckdb_engine

    listed = _mirror()._extension_list()
    declared = {
        c.extension: bool(getattr(c, "install_from_community", False))
        for c in build_duckdb_engine().connectors.values()
        if getattr(c, "extension", None)
    }
    for ext, community in declared.items():
        assert listed[ext] == community, ext
    assert {"motherduck", "snowflake", "mssql", "iceberg"} <= set(listed)


def test_the_core_scanners_and_file_location_extensions_are_mirrored_from_core():
    listed = _mirror()._extension_list()
    for ext in ("postgres", "sqlite", "httpfs", "azure"):
        assert listed[ext] is False, ext  # DuckDB's core repository


def test_crypto_is_mirrored_from_query_farms_repository_and_required():
    # DuckDB 2.0's repositories do not carry crypto; Query.Farm documents its own
    # (https://query.farm/duckdb_extension_crypto.html). The maintainer's decision is that it ships.
    mirror = _mirror()
    from provisa.federation.duckdb_extensions import EXTENSION_REPOSITORIES

    assert EXTENSION_REPOSITORIES["crypto"] == (
        "query_farm",
        "https://duckdb-extensions.query.farm",
    )
    assert mirror._sources()["crypto"] == "query_farm"
    assert "crypto" in mirror._REQUIRED
    assert mirror._sources()["postgres"] == "core"
