# Copyright (c) 2026 Kenneth Stott
# Canary: e3b19f82-e39f-4f4a-a3dc-96e2a56e8e4c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Load + probe the DuckDB extensions that back federation connectors (REQ-899, REQ-904, REQ-294).

Each DuckDB connector that needs an extension declares it (``Connector.extension``). The binary is put in
DuckDB's extension directory at build or packaging time, never at runtime: the wheels of provisa[embedded],
a Provisa installer, the container image, or ``scripts/stage_duckdb_extensions.py`` on a source checkout.
At runtime an extension is only LOADed (:func:`load_extension`), and one that isn't staged is refused by
name. ``probe`` is LOAD-ONLY too: it verifies the extension loads and registers its scanner/attach symbol;
it never opens a live connection to a source. That answers "is this connector's engine capability
installed?", which is distinct from "can we reach a live source?" (a per-source concern, not a probe).

Set ``PROVISA_DUCKDB_EXT_DIR`` to load from a bundled directory instead of the default per-user directory;
every connection (``engine_memory.connect``) reads the same variable.

``python -m provisa.federation.duckdb_extensions`` is the install-time check: it exits non-zero, naming
what is missing, unless this install holds every extension for its DuckDB version and platform.
"""

from __future__ import annotations

import importlib
import os
import shutil
import sys
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import duckdb

from provisa.core import engine_memory

from provisa.federation.connector import Connector
from provisa.federation.engine import build_duckdb_engine

#: The distributions that together carry every extension: one wheel of every platform's extensions
#: passes PyPI's 100 MB per-file limit, so they are split (packaging/duckdb-ext/collect.py, PARTS).
_BUNDLE_PACKAGES = ("provisa_duckdb_ext", "provisa_duckdb_ext_warehouse", "provisa_duckdb_ext_saas")

#: What a connection needs that no connector declares, name -> from the community registry. postgres and
#: sqlite back the base ATTACH connectors; httpfs and azure a file source's location (REQ-1997); vss the
#: catalog index; avro is what iceberg loads when it loads; crypto backs sha224/384/512 (REQ-2171); inet
#: backs PostgreSQL's inet and cidr types and functions.
_UNDECLARED_EXTENSIONS: dict[str, bool] = {
    "postgres": False,
    "sqlite": False,
    "httpfs": False,
    "azure": False,
    "vss": False,
    "avro": False,
    "inet": False,
    "crypto": True,
}

#: The file an extension is on disk under, where its name is an alias of the real one.
_BLOB_NAMES = {"postgres": "postgres_scanner", "sqlite": "sqlite_scanner"}

#: Extensions whose LOAD itself calls their vendor's service, whatever is staged (motherduck asks
#: api.motherduck.com which build to use): an install holds their files, but only a connected machine
#: can load them.
LOAD_NEEDS_SERVICE = frozenset({"motherduck"})


def extension_directory() -> str | None:
    """The bundled DuckDB extension directory, or None to use DuckDB's default per-user directory."""
    return os.environ.get(engine_memory.EXT_DIR_ENV) or None


class BundledExtensionsMissing(RuntimeError):
    """The provisa-duckdb-ext package is installed but lacks blobs for this DuckDB version/platform.

    This is a packaging defect (the wheel was built against a different DuckDB build), NOT a runtime
    condition to paper over: an air-gapped/enterprise install must get every extension through PyPI, so
    we fail loud with a precise remediation rather than silently reaching extensions.duckdb.org.
    """


def _version_and_platform() -> tuple[str, str]:
    """DuckDB's on-disk names for its version and platform (``v1.5.5``, ``osx_arm64``)."""
    con = engine_memory.connect()
    try:
        return (
            engine_memory.first_row(con.execute("SELECT version()"))[0],
            engine_memory.first_row(con.execute("PRAGMA platform"))[0],
        )
    finally:
        con.close()


def stage_bundled_extensions(target: str | Path) -> Path:
    """Copy the running platform's DuckDB extension blobs from the ``provisa-duckdb-ext`` PyPI package
    into ``target`` (idempotent) and return it — the offline, firewall-safe alternative to DuckDB's
    network install. Point ``PROVISA_DUCKDB_EXT_DIR`` at the returned dir so every ``LOAD`` resolves
    locally and ``extensions.duckdb.org`` is never contacted.

    Raises ``ModuleNotFoundError`` when the package isn't installed (a source checkout, which stages
    with ``scripts/stage_duckdb_extensions.py``), and ``BundledExtensionsMissing`` when the package is present but has no blobs for this exact DuckDB
    version + platform.
    """
    importlib.import_module(_BUNDLE_PACKAGES[0])  # ModuleNotFoundError propagates by design
    raw_version, platform = _version_and_platform()
    blobs: list[Path] = []
    for package in _BUNDLE_PACKAGES:
        try:
            ext_root = importlib.import_module(package).ext_root
        except ModuleNotFoundError:
            continue  # an older bundle without this part: verify_bundle names what that leaves out
        blobs += sorted((ext_root() / raw_version / platform).glob("*.duckdb_extension"))
    if not blobs:
        raise BundledExtensionsMissing(
            f"provisa-duckdb-ext has no DuckDB extensions for {platform} @ {raw_version}; "
            f"rebuild the package against duckdb {raw_version}."
        )

    dst = Path(target) / raw_version / platform
    dst.mkdir(parents=True, exist_ok=True)
    for blob in blobs:
        out = dst / blob.name
        if not out.exists():
            shutil.copy2(blob, out)
    return Path(target)


def connect() -> duckdb.DuckDBPyConnection:
    """A DuckDB connection honoring ``PROVISA_DUCKDB_EXT_DIR`` — the shared entry point for any
    caller that needs a bare connection able to load bundled extensions (probing here,
    the VSS catalog index in ``duckdb_runtime.build_vss_index_connection``)."""
    return engine_memory.connect()


# Settings an extension is loaded with, whatever loads it. SET GLOBAL: a plain SET stays on the one
# connection that ran it and never reaches the cursors (con.cursor()) queries run on.
_EXTENSION_SETTINGS: dict[str, tuple[str, ...]] = {
    # #241 (upstream duckdb/duckdb-postgres#564, open): with filter pushdown on, the Top-N dynamic
    # filter of `ORDER BY <col> … NULLS FIRST LIMIT n` reaches Postgres as `WHERE <col> IS NULL`,
    # so a sorted page of a Postgres-attached relation holds only its NULL-key rows (every DESC sort
    # the UI sends). Off, every row and every order is correct; WHERE predicates are then applied
    # in DuckDB instead of in Postgres. Re-enable once a postgres_scanner build fixes #564.
    "postgres": ("SET GLOBAL pg_experimental_filter_pushdown = false",),
}


def _snowflake_driver() -> dict[str, str]:
    """REQ-899: the snowflake extension reads Snowflake through Arrow ADBC and searches only its own
    locations for the driver, never the adbc-driver-snowflake package Provisa ships, so it is
    named to the extension -- read from the process environment when the extension connects."""
    import adbc_driver_snowflake

    return {"SNOWFLAKE_ADBC_DRIVER_PATH": adbc_driver_snowflake._driver_path()}  # pyright: ignore[reportPrivateUsage]


#: The process environment an extension is loaded with, by extension.
_EXTENSION_ENV: dict[str, Callable[[], dict[str, str]]] = {"snowflake": _snowflake_driver}


def load_extension(con: duckdb.DuckDBPyConnection, extension: str) -> None:
    """LOAD ``extension`` on ``con``, or refuse naming it and the fix — the one place that spells an
    extension's bring-up, with the environment (``_EXTENSION_ENV``) and settings
    (``_EXTENSION_SETTINGS``) it is always loaded with. It is never installed here (REQ-294)."""
    if extension in _EXTENSION_ENV:
        os.environ.update(_EXTENSION_ENV[extension]())
    engine_memory.load(con, extension)
    for statement in _EXTENSION_SETTINGS.get(extension, ()):
        con.execute(statement)


def load_staged(con: duckdb.DuckDBPyConnection, extension: str) -> bool:
    """LOAD ``extension`` from the extension directory (REQ-2171). False where it is not staged: the
    caller refuses by name."""
    directory = extension_directory()
    if directory:
        con.execute(f"SET extension_directory = '{directory}'")
    try:
        con.execute(f"LOAD {extension}")
    except duckdb.IOException:
        return False
    return True


def _fetch(con: duckdb.DuckDBPyConnection):
    """An async-shaped ``fetch(sql) -> list[dict]`` adapter over a DuckDB connection (REQ-904 probe API).

    DuckDB is synchronous; the connector probe signature is async, so this wraps ``execute`` in a
    coroutine that returns dict rows (empty for statements like LOAD that yield no result set).
    """

    async def fetch(sql: str) -> list[dict]:
        cur = con.execute(sql)
        if cur.description is None:
            return []
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    return fetch


@dataclass(frozen=True)
class ExtensionProbe:  # REQ-899
    key: str
    source_type: str
    extension: str
    available: bool
    reason: str
    remediation: str | None = None


def duckdb_extension_connectors() -> list[Connector]:
    """The DuckDB engine's connectors that depend on a loadable extension (REQ-899)."""
    engine = build_duckdb_engine()
    return [c for c in engine.connectors.values() if c.extension is not None]


def required_extensions() -> dict[str, bool]:
    """name -> from the community registry: every extension a connection of this version may load, the
    one list that packaging, staging and the install-time check all read."""
    required = {
        c.extension or "": bool(c.install_from_community) for c in duckdb_extension_connectors()
    }
    return {**_UNDECLARED_EXTENSIONS, **required}


def missing_extensions(directory: str | Path, raw_version: str, platform: str) -> list[str]:
    """The required extensions with no file in ``directory`` for this version and platform."""
    src = Path(directory) / raw_version / platform
    return sorted(
        name
        for name in required_extensions()
        if not (src / f"{_BLOB_NAMES.get(name, name)}.duckdb_extension").is_file()
    )


def verify_bundle() -> None:
    """REQ-294, the install-time check: this install's own extensions (those staged by the wheels of
    provisa[embedded], or under ``PROVISA_DUCKDB_EXT_DIR``) are every required one, and each loads
    with nothing fetched. Raises ``BundledExtensionsMissing`` naming what isn't."""
    given = extension_directory()
    with tempfile.TemporaryDirectory() as staged:
        directory = given or str(stage_bundled_extensions(staged))
        raw_version, platform = _version_and_platform()
        missing = missing_extensions(directory, raw_version, platform)
        if missing:
            raise BundledExtensionsMissing(
                f"this install lacks DuckDB extensions for {platform} @ {raw_version}: "
                f"{', '.join(missing)}. Install provisa-duckdb-ext for duckdb {raw_version}."
            )
        failed: list[str] = []
        for name in sorted(required_extensions()):
            if name in LOAD_NEEDS_SERVICE:
                continue
            con = engine_memory.connect(extra={"extension_directory": directory})
            try:
                engine_memory.load(con, name)
            except engine_memory.ExtensionNotStaged as e:
                failed.append(f"{name}: {e}")
            finally:
                con.close()
        if failed:
            raise BundledExtensionsMissing("; ".join(failed))
    print(f"{len(required_extensions())} DuckDB extensions present for {platform} @ {raw_version}")


async def probe_extensions() -> list[ExtensionProbe]:
    """Probe (LOAD + symbol) each extension-backed DuckDB connector.

    A fresh connection per connector isolates a failed load from the others. No live source is opened.
    """
    probes: list[ExtensionProbe] = []
    for connector in duckdb_extension_connectors():
        con = connect()
        try:
            result = await connector.probe(_fetch(con))
        finally:
            con.close()
        probes.append(
            ExtensionProbe(
                key=connector.key or connector.source_type,
                source_type=connector.source_type,
                extension=connector.extension or "",
                available=result.available,
                reason=result.reason,
                remediation=result.remediation,
            )
        )
    return probes


if __name__ == "__main__":
    try:
        verify_bundle()
    except (BundledExtensionsMissing, ModuleNotFoundError) as refused:
        sys.exit(f"DuckDB extension check failed: {refused}")
