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
    assert "crypto" in mirror._REQUIRED
    assert set(mirror._sources().values()) == {"community", "query_farm"}


def test_airport_is_mirrored_from_query_farms_repository_and_required():
    # DuckDB 2.0's community airport refuses to load ("The airport extension for DuckDB 2.0 and
    # later is distributed from the Query.Farm extension repository"); the maintainer's decision
    # (2026-11-07) is that provisa-duckdb-ext ships Query.Farm's build.
    mirror = _mirror()
    from provisa.federation.duckdb_extensions import EXTENSION_REPOSITORIES

    assert EXTENSION_REPOSITORIES["airport"] == (
        "query_farm",
        "https://duckdb-extensions.query.farm",
    )
    assert mirror._sources()["airport"] == "query_farm"
    assert mirror._REQUIRED == {"crypto", "airport"}


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


def test_every_mirrored_extension_keeps_the_info_duckdb_trusts_it_by(tmp_path):
    # DuckDB 2.0 trusts a community extension's signature only with the .info that names the
    # community repository beside it: provisa-duckdb-ext 0.2.0 shipped mssql, mongo, snowflake,
    # bigquery and firebird without it, and DuckDB 2.0 refused to load every one (signature
    # "missing or invalid"); the same binary with DuckDB's own .info loads.
    mirror = _mirror()
    src_dir = tmp_path / "install" / "v2.0.0" / "osx_arm64"
    src_dir.mkdir(parents=True)
    (src_dir / "mssql.duckdb_extension").write_bytes(b"BIN")
    (src_dir / "mssql.duckdb_extension.info").write_bytes(b"INFO")
    dst = tmp_path / "out" / "v2.0.0" / "osx_arm64"
    mirror._copy_extension(src_dir / "mssql.duckdb_extension", dst)  # noqa: SLF001
    assert (dst / "mssql.duckdb_extension").read_bytes() == b"BIN"
    assert (dst / "mssql.duckdb_extension.info").read_bytes() == b"INFO"


def test_an_installed_extension_without_its_info_is_not_mirrored(tmp_path):
    import pytest

    mirror = _mirror()
    (tmp_path / "x.duckdb_extension").write_bytes(b"BIN")
    with pytest.raises(FileNotFoundError, match="x.duckdb_extension.info"):
        mirror._copy_extension(tmp_path / "x.duckdb_extension", tmp_path / "out")  # noqa: SLF001


def test_a_named_repositorys_trust_anchor_is_mirrored_beside_its_extensions(tmp_path):
    # DuckDB 2.0 loads from a user-provided repository (crypto FROM query_farm) only where it
    # knows the repository and the keys it signs with: the file CREATE EXTENSION REPOSITORY writes
    # in extension_repository_directory, which an air-gapped install must be given.
    mirror = _mirror()
    repos = tmp_path / "repos"
    repos.mkdir()
    (repos / "query_farm.duckdb_extension.repo.json").write_text('{"name": "query_farm"}')
    dst = tmp_path / "out" / "v2.0.0" / "osx_arm64" / "repositories" / "query_farm"
    mirror._copy_trust(repos, "query_farm", dst)  # noqa: SLF001
    assert (dst / "query_farm.duckdb_extension.repo.json").read_text() == '{"name": "query_farm"}'


def test_a_repository_without_its_trust_anchor_is_not_mirrored(tmp_path):
    import pytest

    mirror = _mirror()
    with pytest.raises(FileNotFoundError, match="query_farm.duckdb_extension.repo.json"):
        mirror._copy_trust(tmp_path, "query_farm", tmp_path / "out")  # noqa: SLF001
