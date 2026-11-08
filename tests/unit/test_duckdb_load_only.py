# Copyright (c) 2026 Kenneth Stott
# Canary: 7f2d4a91-3c6e-4b18-a5d2-8e1f0c9b6a37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""REQ-294: at runtime DuckDB only loads extensions, and an install holds every one it can load."""

from __future__ import annotations

import ast
import importlib.util
import os
import re
import shutil
import subprocess
import sys
import textwrap
import types
from pathlib import Path

import duckdb
import pytest

from provisa.core import engine_memory
from provisa.federation.connector_duckdb import DuckDBFirebirdConnector
from provisa.federation.duckdb_extensions import (
    BundledExtensionsMissing,
    load_extension,
    missing_extensions,
    required_extensions,
    verify_bundle,
)
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

_REPO = Path(__file__).resolve().parents[2]
_RUNTIME = _REPO / "provisa"
_INSTALL = re.compile(r"^\s*(FORCE\s+)?INSTALL\b|UPDATE\s+EXTENSIONS\b")
_AUTO_ON = re.compile(
    r"""auto(install|load)_known_extensions['"]?\s*[:=,]\s*(True\b|['"]?true)""", re.IGNORECASE
)
_ALL_BLOBS = {"postgres": "postgres_scanner", "sqlite": "sqlite_scanner"}


def _runtime_sources() -> list[Path]:
    return sorted(p for p in _RUNTIME.rglob("*.py") if "_ui" not in p.parts)


def _docstrings(tree: ast.Module) -> set[int]:
    held: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            first = node.body[0] if node.body else None
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
                held.add(id(first.value))
    return held


def test_no_runtime_code_issues_an_install():
    offenders: list[str] = []
    for path in _runtime_sources():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        documents = _docstrings(tree)
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and id(node) not in documents
                and _INSTALL.search(node.value)
            ):
                offenders.append(f"{path.relative_to(_REPO)}:{node.lineno}: {node.value[:60]!r}")
    assert offenders == []


def test_no_runtime_code_turns_auto_install_on():
    offenders = [
        str(p.relative_to(_REPO))
        for p in _runtime_sources()
        if _AUTO_ON.search(p.read_text(encoding="utf-8"))
    ]
    assert offenders == []


def test_every_connection_is_opened_through_the_load_only_entry_point():
    # duckdb.connect anywhere else would open a connection with auto-install on, DuckDB's default.
    offenders: list[str] = []
    for path in _runtime_sources():
        if path == _RUNTIME / "core" / "engine_memory.py":
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "connect"
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "duckdb"
            ):
                offenders.append(f"{path.relative_to(_REPO)}:{node.lineno}")
    assert offenders == []


def _setting(con: duckdb.DuckDBPyConnection, name: str):
    return con.execute("SELECT current_setting(?)", [name]).fetchall()[0][0]


def test_a_connection_has_auto_install_and_auto_load_off():
    con = engine_memory.connect()
    assert _setting(con, "autoinstall_known_extensions") is False
    assert _setting(con, "autoload_known_extensions") is False


def test_a_caller_cannot_turn_auto_install_back_on():
    con = engine_memory.connect(
        extra={"autoinstall_known_extensions": True, "autoload_known_extensions": True}
    )
    assert _setting(con, "autoinstall_known_extensions") is False
    assert _setting(con, "autoload_known_extensions") is False


def test_the_federation_runtime_has_auto_install_off_and_stays_on_utc():
    rt = DuckDBFederationRuntime()
    assert _setting(rt._con, "autoinstall_known_extensions") is False
    assert _setting(rt._con, "autoload_known_extensions") is False
    # REQ-2050: set once the connection is open, and reaching the cursors queries run on.
    assert _setting(rt._con.cursor(), "TimeZone") == "UTC"


def test_a_missing_extension_is_refused_naming_it_and_the_fix(tmp_path, monkeypatch):
    monkeypatch.setenv(engine_memory.EXT_DIR_ENV, str(tmp_path))
    con = engine_memory.connect()
    with pytest.raises(engine_memory.ExtensionNotStaged) as refused:
        load_extension(con, "firebird")
    message = str(refused.value)
    assert "firebird" in message
    assert "provisa[embedded]" in message and "stage_duckdb_extensions.py" in message


@pytest.mark.asyncio
async def test_the_probe_only_loads_and_names_the_fix_when_absent():
    ran: list[str] = []

    async def fetch(sql: str):
        ran.append(sql)
        raise duckdb.IOException("not found")

    result = await DuckDBFirebirdConnector().probe(fetch)
    assert ran == ["LOAD firebird"]
    assert result.available is False
    assert "stage_duckdb_extensions.py" in (result.remediation or "")


# ---- no network, through the real code path -----------------------------------------------------

_OFFLINE_CHILD = textwrap.dedent(
    """
    import asyncio, socket
    # The block is the operating system's, so it holds for DuckDB's own sockets too.
    try:
        socket.create_connection(("1.1.1.1", 443), timeout=5)
    except OSError:
        print("NETWORK BLOCKED")
    else:
        raise SystemExit("the network is reachable: this test proves nothing")
    from provisa.federation.connector_duckdb import DuckDBFirebirdConnector
    from provisa.federation.duckdb_extensions import _fetch, connect, load_extension
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime
    rt = DuckDBFederationRuntime()
    load_extension(rt._con, "firebird")
    n = rt._con.execute(
        "SELECT count(*) FROM duckdb_extensions() WHERE loaded AND extension_name = 'firebird'"
    ).fetchall()[0][0]
    assert n == 1
    result = asyncio.run(DuckDBFirebirdConnector().probe(_fetch(connect())))
    assert result.available, result
    print("FIREBIRD LOADED")
    """
)


def _without_network() -> list[str]:
    """The command prefix that denies a process all network access, by the operating system."""
    if sys.platform == "darwin":
        return ["sandbox-exec", "-p", "(version 1)(allow default)(deny network*)"]
    for prefix in (["unshare", "-rn"], ["sudo", "-n", "unshare", "-n"]):
        if shutil.which(prefix[0]):
            if subprocess.run([*prefix, "true"], capture_output=True).returncode == 0:
                return prefix
    pytest.fail("this machine offers no way to run a process without network (unshare, sudo)")


def _firebird_blob() -> Path:
    """firebird as a connected machine staged it (scripts/stage_duckdb_extensions.py)."""
    con = duckdb.connect()
    try:
        version = con.execute("SELECT version()").fetchall()[0][0]
        platform = con.execute("PRAGMA platform").fetchall()[0][0]
        home = con.execute("SELECT current_setting('home_directory')").fetchall()[0][0]
    finally:
        con.close()
    root = Path(home or Path.home()) / ".duckdb" / "extensions" / version / platform
    blob = root / "firebird.duckdb_extension"
    assert blob.is_file(), f"run scripts/stage_duckdb_extensions.py first: {blob} is missing"
    return blob


def test_firebird_loads_from_a_staged_directory_with_the_network_blocked(tmp_path):
    blob = _firebird_blob()
    staged = tmp_path / "ext"
    (staged / blob.parent.parent.name / blob.parent.name).mkdir(parents=True)
    shutil.copy2(blob, staged / blob.parent.parent.name / blob.parent.name / blob.name)
    done = subprocess.run(
        [*_without_network(), sys.executable, "-c", _OFFLINE_CHILD],
        capture_output=True,
        text=True,
        cwd=_REPO,
        env={**os.environ, engine_memory.EXT_DIR_ENV: str(staged)},
        timeout=120,
    )
    assert done.returncode == 0, done.stdout + done.stderr
    assert "NETWORK BLOCKED" in done.stdout
    assert "FIREBIRD LOADED" in done.stdout


# ---- the install-time check ---------------------------------------------------------------------


def _version_platform() -> tuple[str, str]:
    con = duckdb.connect()
    try:
        return (
            con.execute("SELECT version()").fetchall()[0][0],
            con.execute("PRAGMA platform").fetchall()[0][0],
        )
    finally:
        con.close()


def _populate(root: Path, names: list[str]) -> None:
    ver, plat = _version_platform()
    (root / ver / plat).mkdir(parents=True, exist_ok=True)
    for name in names:
        (root / ver / plat / f"{_ALL_BLOBS.get(name, name)}.duckdb_extension").write_bytes(b"x")


def test_every_connector_extension_is_required():
    from provisa.federation.engine import build_duckdb_engine

    required = required_extensions()
    declared = {c.extension for c in build_duckdb_engine().connectors.values() if c.extension}
    assert declared <= set(required)
    assert {"firebird", "gsheets", "airport", "avro", "vss", "crypto"} <= set(required)


def test_a_full_directory_has_nothing_missing(tmp_path):
    _populate(tmp_path, list(required_extensions()))
    assert missing_extensions(tmp_path, *_version_platform()) == []


def test_the_check_names_each_absent_extension(tmp_path):
    names = [n for n in required_extensions() if n not in ("firebird", "gsheets")]
    _populate(tmp_path, names)
    assert missing_extensions(tmp_path, *_version_platform()) == ["firebird", "gsheets"]


def test_verify_bundle_refuses_an_install_lacking_extensions(tmp_path, monkeypatch):
    # As provisa-duckdb-ext 0.1.1 does: postgres and sqlite, nothing else.
    _populate(tmp_path, ["postgres", "sqlite"])
    monkeypatch.setenv(engine_memory.EXT_DIR_ENV, str(tmp_path))
    with pytest.raises(BundledExtensionsMissing) as refused:
        verify_bundle()
    assert "firebird" in str(refused.value) and "airport" in str(refused.value)


def test_every_extension_of_the_bundle_is_assigned_to_a_distribution():
    spec = importlib.util.spec_from_file_location(
        "collect", _REPO / "packaging" / "duckdb-ext" / "collect.py"
    )
    assert spec is not None and spec.loader is not None
    collect: types.ModuleType = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(collect)
    assigned = {name for names in collect.PARTS.values() for name in names}
    assert {_ALL_BLOBS.get(n, n) for n in required_extensions()} <= assigned
    assert "motherduck_impl" in assigned  # brought in by LOAD motherduck
