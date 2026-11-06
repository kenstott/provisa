# Copyright (c) 2026 Kenneth Stott
# Canary: 30035524-316a-4035-bd3a-061141187519
#
# This source code is licensed under the Business Source License 1.1
"""The DuckDB extensions an air-gapped install bundles in provisa-duckdb-ext
(scripts/ci/mirror_duckdb_extensions.py) are every community extension the DuckDB engine's
connectors declare, and crypto from Query.Farm's repository. The list once came from a module the
connectors had moved out of, and an install got postgres and sqlite alone. Since DuckDB 2.0 the
core extensions come from the third-party per-extension wheels (REQ-1142 amendment, TEMPORARY
EXTENSION SOURCES FOR DUCKDB 2.0), so this mirror no longer carries them."""

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


def test_every_community_extension_the_duckdb_engine_loads_is_mirrored():
    from provisa.federation.engine import build_duckdb_engine

    listed = _mirror()._extension_list()
    community = {
        c.extension
        for c in build_duckdb_engine().connectors.values()
        if getattr(c, "extension", None) and getattr(c, "install_from_community", False)
    }
    assert listed == sorted(community)
    assert {"snowflake", "mssql", "bigquery", "mongo", "firebird"} <= set(listed)


def test_the_core_extensions_are_not_mirrored():
    # They come from the third-party wheels (CORE_EXTENSION_WHEELS), by real name and alias alike.
    from provisa.federation.duckdb_extensions import CORE_EXTENSION_WHEELS

    sources = _mirror()._sources()
    for ext in (*CORE_EXTENSION_WHEELS, "postgres", "sqlite"):
        assert ext not in sources, ext


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
    assert mirror._REQUIRED == {"crypto"}
    assert set(mirror._sources().values()) == {"community", "query_farm"}


def test_the_wheel_fill_order_names_crypto_and_every_mirrored_community_extension():
    # Derived from the engine's connectors, so a community extension a connector adds is never
    # missed by the fill order (packaging/duckdb-ext/collect.py PRIORITY).
    import sys

    spec = importlib.util.spec_from_file_location(
        "duckdb_ext_collect_mirror", _SCRIPT.parents[2] / "packaging/duckdb-ext/collect.py"
    )
    assert spec is not None and spec.loader is not None
    collect = importlib.util.module_from_spec(spec)
    sys.modules["duckdb_ext_collect_mirror"] = collect
    spec.loader.exec_module(collect)

    assert set(collect.PRIORITY) == set(_mirror()._sources())
    assert {"gsheets", "airport"} <= set(collect.PRIORITY)
