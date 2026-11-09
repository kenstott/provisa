# Copyright (c) 2026 Kenneth Stott
# Canary: d717b13c-e102-44cb-960f-f3b7e77e6f03
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Staging the DuckDB extensions from the wheels the package index serves (REQ-294).

stage_wheels lays the running platform's blobs out of the fetched wheels WITHOUT touching
extensions.duckdb.org: the core extensions from the duckdb-extension-* projects, every other one from
the version parts of provisa-duckdb-ext. It fails loud (BundledExtensionsMissing), naming each, when a
wheel is absent or carries nothing for this exact DuckDB version+platform. The wheels are built here
so the test is hermetic: no package index is reached.
"""

from __future__ import annotations

import inspect
import json
import types
import zipfile
from pathlib import Path

import duckdb
import pytest

from provisa.federation import duckdb_extensions, stage_duckdb_extensions
from provisa.federation.duckdb_extensions import (
    CORE_EXTENSION_WHEELS,
    EXT_PACKAGE_PARTS,
    EXT_PACKAGE_RELEASE,
    MANIFEST,
    REPOSITORY_ANCHORS,
    BundledExtensionsMissing,
    extension_config,
    is_staged,
    stage_wheels,
    wheel_requirements,
)

QF = "repositories/query_farm"


def _version_platform() -> tuple[str, str]:
    con = duckdb.connect()
    try:
        return (
            con.execute("SELECT version()").fetchone()[0],
            con.execute("PRAGMA platform").fetchone()[0],
        )
    finally:
        con.close()


def _core_wheels(where: Path, version: str, skip=()) -> list[Path]:
    """The third-party wheels: <package>/extensions/<version>/<name>.duckdb_extension, no platform
    dir, the wheel being the platform's own."""
    where.mkdir(parents=True, exist_ok=True)
    wheels: list[Path] = []
    for extension, package in CORE_EXTENSION_WHEELS.items():
        if extension in skip:
            continue
        path = where / f"{package}-2.0.0-py3-none-any.whl"
        with zipfile.ZipFile(path, "w") as wheel:
            wheel.writestr(f"{package}/__init__.py", "")
            wheel.writestr(
                f"{package}/extensions/{version}/{extension}.duckdb_extension",
                f"CORE-{extension}",
            )
        wheels.append(path)
    return wheels


def _part_wheel(where: Path, ver: str, plat: str, part: int, files: dict[str, str]) -> Path:
    where.mkdir(parents=True, exist_ok=True)
    path = where / f"provisa_duckdb_ext-{EXT_PACKAGE_RELEASE}.{part}-py3-none-any.whl"
    with zipfile.ZipFile(path, "w") as wheel:
        for rel, body in files.items():
            wheel.writestr(f"provisa_duckdb_ext/_ext/{ver}/{plat}/{rel}", body)
    return path


def _required_mirrored() -> dict[str, str]:
    """What every staging needs from the parts: crypto and airport, in their repository's layout."""
    return {
        f"{QF}/crypto.duckdb_extension": "CRYPTO",
        f"{QF}/crypto.duckdb_extension.info": "INFO",
        f"{QF}/airport.duckdb_extension": "AIRPORT",
        f"{QF}/airport.duckdb_extension.info": "INFO",
        f"{QF}/query_farm.duckdb_extension.repo.json": '{"name": "query_farm"}',
    }


def _full(tmp_path: Path, ver: str, plat: str, **part1: str) -> list[Path]:
    wheels = tmp_path / "wheels"
    return [
        *_core_wheels(wheels, ver),
        _part_wheel(wheels, ver, plat, 1, {**_required_mirrored(), **part1}),
    ]


def test_the_core_extensions_are_the_third_party_wheels():
    assert CORE_EXTENSION_WHEELS == {
        "postgres_scanner": "duckdb_extension_postgres_scanner",
        "httpfs": "duckdb_extension_httpfs",
        "sqlite_scanner": "duckdb_extension_sqlite_scanner",
        "iceberg": "duckdb_extension_iceberg",
        "azure": "duckdb_extension_azure",
        "delta": "duckdb_extension_delta",
        "avro": "duckdb_extension_avro",  # iceberg loads it
        "vss": "duckdb_extension_vss",
        "inet": "duckdb_extension_inet",
    }


def test_the_requirements_pin_each_project_and_every_version_part():
    wanted = wheel_requirements("2.0.0")
    assert "duckdb-extension-httpfs==2.0.0" in wanted
    parts = [r for r in wanted if r.startswith("provisa-duckdb-ext==")]
    assert parts == [
        f"provisa-duckdb-ext=={EXT_PACKAGE_RELEASE}.{p}" for p in range(1, EXT_PACKAGE_PARTS + 1)
    ]
    assert len(wanted) == len(CORE_EXTENSION_WHEELS) + EXT_PACKAGE_PARTS


def test_stage_takes_core_from_the_third_party_wheels_and_the_rest_from_the_parts(tmp_path):
    ver, plat = _version_platform()
    wheels = tmp_path / "wheels"
    found = [
        *_core_wheels(wheels, ver),
        _part_wheel(wheels, ver, plat, 1, _required_mirrored()),
        _part_wheel(
            wheels,
            ver,
            plat,
            2,
            {"mssql.duckdb_extension": "MSSQL", "mssql.duckdb_extension.info": "INFO"},
        ),
    ]
    out = stage_wheels(found, tmp_path / "staged")
    staged = out / ver / plat
    for extension in CORE_EXTENSION_WHEELS:
        assert (staged / f"{extension}.duckdb_extension").read_text() == f"CORE-{extension}"
    assert (staged / "mssql.duckdb_extension").read_text() == "MSSQL"


def test_stage_writes_the_manifest_of_what_it_staged(tmp_path):
    ver, plat = _version_platform()
    out = stage_wheels(_full(tmp_path, ver, plat), tmp_path / "staged")
    manifest = json.loads((out / ver / plat / MANIFEST).read_text())
    assert manifest["duckdb"] == ver and manifest["platform"] == plat
    assert "crypto" in manifest["extensions"] and "httpfs" in manifest["extensions"]


def test_a_wheel_of_a_third_party_project_is_read_never_imported(tmp_path):
    # Its package is read from the zip: code in its __init__ never executes.
    ver, plat = _version_platform()
    wheels = _full(tmp_path, ver, plat)
    trap = next(w for w in wheels if "httpfs" in w.name)
    with zipfile.ZipFile(trap, "a") as wheel:
        wheel.writestr("duckdb_extension_httpfs/evil.py", "raise RuntimeError('imported')")
    stage_wheels(wheels, tmp_path / "staged")


def test_a_missing_third_party_wheel_is_refused_by_name(tmp_path):
    ver, plat = _version_platform()
    wheels = tmp_path / "wheels"
    found = [
        *_core_wheels(wheels, ver, skip=("delta",)),
        _part_wheel(wheels, ver, plat, 1, _required_mirrored()),
    ]
    with pytest.raises(
        BundledExtensionsMissing, match="delta: duckdb-extension-delta has no build"
    ):
        stage_wheels(found, tmp_path / "staged")


def test_a_third_party_wheel_of_another_duckdb_is_refused_by_name(tmp_path):
    ver, plat = _version_platform()
    wheels = tmp_path / "wheels"
    found = [
        *_core_wheels(wheels, "v0.0.1"),
        _part_wheel(wheels, ver, plat, 1, _required_mirrored()),
    ]
    with pytest.raises(BundledExtensionsMissing, match="iceberg: duckdb-extension-iceberg has no"):
        stage_wheels(found, tmp_path / "staged")


def test_parts_missing_a_required_extension_are_refused_by_name(tmp_path):
    ver, plat = _version_platform()
    wheels = tmp_path / "wheels"
    with pytest.raises(BundledExtensionsMissing, match="crypto: not in any provisa-duckdb-ext"):
        stage_wheels(
            [*_core_wheels(wheels, ver), _part_wheel(wheels, ver, plat, 1, {})],
            tmp_path / "staged",
        )


def test_stage_copies_a_named_repositorys_extension_with_its_info(tmp_path):
    # Query.Farm's on DuckDB 2.0: DuckDB keeps it under repositories/query_farm/ and loads it by
    # the .info beside it, so both are staged in that layout.
    ver, plat = _version_platform()
    out = stage_wheels(_full(tmp_path, ver, plat), tmp_path / "staged")
    staged = out / ver / plat / "repositories" / "query_farm"
    assert (staged / "crypto.duckdb_extension").read_text() == "CRYPTO"
    assert (staged / "crypto.duckdb_extension.info").read_text() == "INFO"


def test_a_bundled_extension_without_its_info_is_refused_by_name(tmp_path):
    # DuckDB 2.0 does not trust a community extension's signature without the .info naming its
    # repository (provisa-duckdb-ext 0.2.0 shipped them so, and none loaded): staging one would
    # only move the refusal to the first query.
    ver, plat = _version_platform()
    with pytest.raises(BundledExtensionsMissing, match=r"mongo: no \.info"):
        stage_wheels(
            _full(tmp_path, ver, plat, **{"mongo.duckdb_extension": "MONGO"}), tmp_path / "staged"
        )


def test_staging_gathers_the_trust_anchors_duckdb_reads_repositories_from(tmp_path):
    ver, plat = _version_platform()
    out = stage_wheels(_full(tmp_path, ver, plat), tmp_path / "staged")
    anchor = out / ver / plat / REPOSITORY_ANCHORS / "query_farm.duckdb_extension.repo.json"
    assert anchor.read_text() == '{"name": "query_farm"}'


def test_a_repositorys_extension_without_its_trust_anchor_is_refused_by_name(tmp_path):
    ver, plat = _version_platform()
    wheels = tmp_path / "wheels"
    files = _required_mirrored()
    del files[f"{QF}/query_farm.duckdb_extension.repo.json"]
    with pytest.raises(BundledExtensionsMissing, match="query_farm: no trust anchor"):
        stage_wheels(
            [*_core_wheels(wheels, ver), _part_wheel(wheels, ver, plat, 1, files)],
            tmp_path / "staged",
        )


# ---- the stage step ------------------------------------------------------------------------------


def test_a_directory_already_filled_fetches_nothing(tmp_path):
    ver, plat = _version_platform()
    target = tmp_path / "staged"
    stage_wheels(_full(tmp_path, ver, plat), target)
    assert is_staged(target)

    def never(_requirements, _dest):
        raise AssertionError("a filled directory fetched")

    assert stage_duckdb_extensions.stage(target, fetch=never) is False


def test_an_empty_directory_fetches_the_pinned_requirements_once(tmp_path):
    ver, plat = _version_platform()
    asked: list[list[str]] = []

    def fetch(requirements, dest):
        asked.append(list(requirements))
        _full(dest / "x", ver, plat)
        for wheel in (dest / "x" / "wheels").iterdir():
            wheel.rename(dest / wheel.name)

    target = tmp_path / "staged"
    assert stage_duckdb_extensions.stage(target, fetch=fetch) is True
    assert asked == [wheel_requirements()]
    assert is_staged(target)
    assert [p.name for p in target.iterdir() if p.name.startswith(".wheels.")] == []


def test_pip_download_is_the_configured_index_and_binary_only(monkeypatch, tmp_path):
    ran: list[list[str]] = []
    monkeypatch.setattr(
        stage_duckdb_extensions.subprocess,
        "run",
        lambda command, check: ran.append(command) or types.SimpleNamespace(returncode=0),
    )
    stage_duckdb_extensions.pip_download(["x==1"], tmp_path)
    assert ran[0][1:5] == ["-m", "pip", "download", "--no-deps"]
    assert "--only-binary=:all:" in ran[0] and ran[0][-1] == "x==1"
    # No --index-url: pip's own configuration names where (an enterprise Artifactory).
    assert not any("index" in arg for arg in ran[0])


def test_a_failed_pip_download_names_the_requirements_and_the_fix(monkeypatch, tmp_path):
    def run(command, check):
        raise stage_duckdb_extensions.subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(stage_duckdb_extensions.subprocess, "run", run)
    with pytest.raises(BundledExtensionsMissing, match="x==1") as refused:
        stage_duckdb_extensions.pip_download(["x==1"], tmp_path)
    assert "PROVISA_DUCKDB_EXT_DIR" in str(refused.value)


# ---- loading -------------------------------------------------------------------------------------


def test_load_staged_crypto_unstaged_is_false_never_an_error(tmp_path, monkeypatch):
    # Nothing staged: LOAD crypto FROM query_farm fails, and the caller refuses by name.
    monkeypatch.setenv("PROVISA_DUCKDB_EXT_DIR", str(tmp_path / "empty"))
    con = duckdb.connect()
    try:
        assert duckdb_extensions.load_staged(con, "crypto") is False
    finally:
        con.close()


class _Con:
    def __init__(self):
        self.statements: list[str] = []

    def execute(self, statement, params=None):
        if statement.startswith("SELECT count(*) FROM duckdb_extension_repositories()"):
            assert params == ["query_farm"]
            return types.SimpleNamespace(fetchall=lambda: [(1,)])  # the repository is known
        self.statements.append(statement)
        raise duckdb.IOException("IO Error: not found")


def test_load_staged_loads_crypto_from_its_repository_only(monkeypatch):
    # DuckDB 2.0 loads crypto from Query.Farm's staged copy alone: no plain LOAD is tried after it.
    monkeypatch.delenv("PROVISA_DUCKDB_EXT_DIR", raising=False)
    monkeypatch.setattr(duckdb_extensions, "extension_directory", lambda: None)
    con = _Con()
    assert duckdb_extensions.load_staged(con, "crypto") is False  # type: ignore[arg-type]
    assert con.statements == ["LOAD crypto FROM query_farm"]


def test_load_staged_crypto_where_query_farm_is_not_a_known_repository_is_false(tmp_path):
    # A fresh machine knows no user-provided repository (DuckDB 2.0 keeps them, with their
    # signing keys, in extension_repository_directory): LOAD crypto FROM query_farm is then a
    # binder error, and the call is refused by name like any extension not staged.
    con = duckdb.connect(
        config={
            "extension_directory": str(tmp_path / "ext"),
            "extension_repository_directory": str(tmp_path / "no-repositories"),
        }
    )
    try:
        known = {
            r[0]
            for r in con.execute(
                "SELECT repository_name FROM duckdb_extension_repositories()"
            ).fetchall()
        }
        assert "query_farm" not in known
        assert duckdb_extensions.load_staged(con, "crypto") is False
    finally:
        con.close()


def test_a_staged_directory_is_connected_to_with_its_trust_anchors(tmp_path, monkeypatch):
    ver, plat = _version_platform()
    monkeypatch.setenv("PROVISA_DUCKDB_EXT_DIR", str(tmp_path))
    assert extension_config() == {
        "extension_directory": str(tmp_path),
        "extension_repository_directory": str(tmp_path / ver / plat / REPOSITORY_ANCHORS),
        "autoinstall_known_extensions": False,
        "autoload_known_extensions": False,
    }
    # REQ-294: with no directory named, nothing is installed on the way to DuckDB's own either.
    monkeypatch.delenv("PROVISA_DUCKDB_EXT_DIR")
    assert extension_config() == {
        "autoinstall_known_extensions": False,
        "autoload_known_extensions": False,
    }


class _LoadingCon:
    def __init__(self):
        self.statements: list[str] = []

    def execute(self, statement):
        self.statements.append(statement)


def test_load_extension_loads_airport_from_query_farms_repository_and_installs_nothing():
    # DuckDB 2.0's community airport throws at LOAD; Query.Farm's staged copy loads only with
    # LOAD airport FROM query_farm (a plain LOAD looks for the community copy).
    con = _LoadingCon()
    duckdb_extensions.load_extension(con, "airport")  # type: ignore[arg-type]
    assert con.statements == ["LOAD airport FROM query_farm"]


def test_load_extension_loads_a_community_extension_and_installs_nothing():
    con = _LoadingCon()
    duckdb_extensions.load_extension(con, "mssql")  # type: ignore[arg-type]
    assert con.statements == ["LOAD mssql"]


# REQ-294 supersedes REQ-1142's THE SERVER TIER STAGES AT START: at runtime DuckDB never installs.


def test_no_process_installs_the_extension_set_at_start():
    assert not hasattr(duckdb_extensions, "install_extension_set")
    assert not hasattr(duckdb_extensions, "install_and_load")
    from provisa.api import app as app_module

    assert not hasattr(app_module, "install_server_extensions")
    assert "install_server_extensions" not in inspect.getsource(app_module.lifespan)


def test_with_no_staged_directory_nothing_is_known_until_an_extension_loads(monkeypatch):
    monkeypatch.delenv("PROVISA_DUCKDB_EXT_DIR", raising=False)
    assert duckdb_extensions.staged_extensions() is None


def test_a_staged_directory_gates_attach_on_its_manifest(tmp_path, monkeypatch):
    from provisa.federation.engine import build_duckdb_engine

    ver, plat = _version_platform()
    target = tmp_path / "staged"
    stage_wheels(
        _full(
            tmp_path,
            ver,
            plat,
            **{"mssql.duckdb_extension": "M", "mssql.duckdb_extension.info": "I"},
        ),
        target,
    )
    monkeypatch.setenv("PROVISA_DUCKDB_EXT_DIR", str(target))
    engine = build_duckdb_engine()
    assert "postgresql" in engine.connectors  # a core wheel's extension was staged
    assert "google_sheets" not in engine.connectors
    assert "gsheets" in (engine.unreachable_reason("google_sheets") or "")
