# Copyright (c) 2026 Kenneth Stott
# Canary: e3b19f82-e39f-4f4a-a3dc-96e2a56e8e4c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Stage + probe the DuckDB community extensions that back federation connectors (REQ-899, REQ-904).

Each DuckDB connector that needs an extension declares it (``Connector.extension``). Before the engine
can attach such a source, the extension binary must be present in DuckDB's extension directory
(``INSTALL ... FROM community`` — the STAGE step, run once at startup / packaging so it is reachable
offline) and must actually load (``LOAD`` — the PROBE step). ``probe`` is deliberately LOAD-ONLY: it
verifies the extension loads and registers its scanner/attach symbol; it never opens a live connection
to a source. That answers "is this connector's engine capability installed and reachable?", which is
distinct from "can we reach a live source?" (live reachability is a per-source concern, not a probe).

Set ``PROVISA_DUCKDB_EXT_DIR`` to stage into (and load from) a bundled directory instead of the default
per-user cache; the federation runtime reads the same variable so what is staged here is reachable there.
"""

from __future__ import annotations

import functools
import importlib.util
import json
import logging
import os
import shutil
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import duckdb

from provisa.core import engine_memory

from provisa.federation.connector import Connector
from provisa.federation.engine import build_duckdb_engine, duckdb_engine_connectors

_log = logging.getLogger(__name__)

_EXT_DIR_ENV = "PROVISA_DUCKDB_EXT_DIR"

#: Extensions DuckDB 2.0's own repositories do not carry, by name: (the named repository they are
#: published in, its prefix as the publisher documents it). provisa-duckdb-ext mirrors them; DuckDB
#: keeps one under ``<version>/<platform>/repositories/<repository>/`` with its ``.info`` and loads
#: it with ``LOAD <name> FROM <repository>``, offline -- the prefix is used by the mirror alone.
EXTENSION_REPOSITORIES: dict[str, tuple[str, str]] = {
    # https://query.farm/duckdb_extension_crypto.html (DuckDB 2.0) and
    # https://query.farm/blog/duckdb-2-query-farm-repository/
    "crypto": ("query_farm", "https://duckdb-extensions.query.farm"),
    # DuckDB 2.0's community airport refuses to load ("The airport extension for DuckDB 2.0 and
    # later is distributed from the Query.Farm extension repository");
    # https://query.farm/products/extensions/airport/ (maintainer decision 2026-11-07).
    "airport": ("query_farm", "https://duckdb-extensions.query.farm"),
}


#: The manifest stage_bundled_extensions writes beside what it staged, per DuckDB version and
#: platform: the extensions staged there (REQ-1142, ATTACHABLE MEANS STAGED).
MANIFEST = "staged.json"

#: Where staging gathers the named repositories' trust anchors, under ``<version>/<platform>/``:
#: the extension_repository_directory a connection to the staged directory is given.
REPOSITORY_ANCHORS = "extension_repositories"

#: The name a connector gives an extension whose file is named otherwise (DuckDB's own aliases).
_SHORT_NAMES = {"postgres_scanner": "postgres", "sqlite_scanner": "sqlite"}

#: The source types whose rows are landed through a scratch DuckDB read with the same extension
#: their connector attaches with (events/source_loader.make_firebird_loader, make_airport_loader):
#: where it is not staged they cannot land either.
LANDS_THROUGH_ITS_EXTENSION = frozenset({"firebird", "airport"})


@functools.cache
def running_version_platform() -> tuple[str, str]:
    """The running DuckDB's version (``v2.0.0``) and platform (``osx_arm64``), as its extension
    directory names them."""
    con = engine_memory.connect()
    try:
        (version,) = con.execute("SELECT version()").fetchall()[0]
        (platform,) = con.execute("PRAGMA platform").fetchall()[0]
    finally:
        con.close()
    return str(version), str(platform)


def extension_directory() -> str | None:
    """The bundled DuckDB extension directory, or None to use DuckDB's default per-user cache."""
    return os.environ.get(_EXT_DIR_ENV) or None


#: The core extensions installed from the third-party per-extension, per-platform PyPI wheels, by the
#: extension's file name -> the importable package of its wheel. TEMPORARY (REQ-1142 amendment,
#: "TEMPORARY EXTENSION SOURCES FOR DUCKDB 2.0", 2026-11-06): duckdb-extension-<name>==<duckdb version>
#: from https://github.com/santosh-d3vpl3x/duckdb_extensions (MIT), until Provisa's own per-extension
#: PyPI projects exist. Each wheel holds the official, signed binary at
#: ``<package>/extensions/<raw_duckdb_version>/<name>.duckdb_extension`` (its ``__init__.py`` is
#: empty: the package is located, never imported), and DuckDB loads it with its signature check on.
CORE_EXTENSION_WHEELS: dict[str, str] = {
    "postgres_scanner": "duckdb_extension_postgres_scanner",
    "httpfs": "duckdb_extension_httpfs",
    "sqlite_scanner": "duckdb_extension_sqlite_scanner",
    "iceberg": "duckdb_extension_iceberg",
    "azure": "duckdb_extension_azure",
    "delta": "duckdb_extension_delta",
    # iceberg's init loads avro ("The iceberg extension requires the avro extension"): staged
    # beside it from the same source (lead's ruling 2026-11-06, under the maintainer's decision).
    "avro": "duckdb_extension_avro",
}


class BundledExtensionsMissing(RuntimeError):
    """The embedded extra is installed but an extension package lacks blobs for this DuckDB
    version/platform, or one of its packages is absent.

    This is a packaging defect (a wheel built against a different DuckDB build, or a partial install),
    NOT a runtime condition to paper over: an air-gapped/enterprise install must get every extension
    through PyPI, so we fail loud with a precise remediation rather than silently reaching
    extensions.duckdb.org.
    """


def _core_wheel_blob(package: str, extension: str, raw_version: str) -> Path | None:
    """Where ``package``'s wheel keeps ``extension`` for DuckDB ``raw_version``, or None when the
    package is not installed. ``find_spec`` of a top-level package locates it without running it."""
    spec = importlib.util.find_spec(package)
    if spec is None or not spec.submodule_search_locations:
        return None
    (root,) = spec.submodule_search_locations
    return Path(root) / "extensions" / raw_version / f"{extension}.duckdb_extension"


def _copy_missing(files: list[Path], src: Path, dst: Path) -> None:
    for blob in files:
        out = dst / blob.relative_to(src)
        if not out.exists():
            out.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(blob, out)


def stage_bundled_extensions(target: str | Path) -> Path:
    """Copy the running platform's DuckDB extension blobs from the PyPI packages of the embedded extra
    into ``target/<version>/<platform>/`` (idempotent) and return ``target``: the core extensions
    (CORE_EXTENSION_WHEELS) from their third-party per-extension wheels, every other one from
    ``provisa-duckdb-ext``. Each package is one wheel per platform, so the installer's own wheel
    choice brings this platform's binaries -- the offline, firewall-safe alternative to DuckDB's
    network ``INSTALL``. Point ``PROVISA_DUCKDB_EXT_DIR`` at the returned dir so every ``LOAD``
    resolves locally, signature-checked, and ``extensions.duckdb.org`` is never contacted.

    Raises ``ModuleNotFoundError`` when ``provisa-duckdb-ext`` isn't installed (a dev checkout without
    the embedded extra; the caller decides), and ``BundledExtensionsMissing`` naming every package
    that is absent or has no blobs for this exact DuckDB version + platform.
    """
    from provisa_duckdb_ext import ext_root  # type: ignore[import-not-found]  # ModuleNotFoundError propagates by design

    raw_version, platform = running_version_platform()

    src = ext_root() / raw_version / platform
    # Every file below the platform dir: a named repository's extension (EXTENSION_REPOSITORIES)
    # sits under repositories/<repository>/ beside the .info DuckDB loads it by.
    files = sorted(p for p in src.rglob("*") if p.is_file()) if src.is_dir() else []
    problems: list[str] = []
    if not any(p.suffix == ".duckdb_extension" for p in files):
        problems.append(f"provisa-duckdb-ext has none (looked in {src})")
    # DuckDB 2.0 trusts an extension's signature by the repository its .info names, so one shipped
    # without it does not load (provisa-duckdb-ext 0.2.0's community extensions).
    problems += [
        f"{p.name.removesuffix('.duckdb_extension')}: no .info beside it in provisa-duckdb-ext, "
        "so DuckDB does not trust its signature"
        for p in files
        if p.suffix == ".duckdb_extension" and not p.with_name(f"{p.name}.info").is_file()
    ]
    # A named repository's extensions load only where DuckDB knows the repository and its keys.
    anchors = _repository_anchors(src, files)
    problems += [
        f"{repo}: no trust anchor ({repo}.duckdb_extension.repo.json) beside its extensions in "
        "provisa-duckdb-ext, so DuckDB does not know the repository"
        for repo, anchor in anchors.items()
        if anchor is None
    ]
    core: list[Path] = []
    for extension, package in CORE_EXTENSION_WHEELS.items():
        wheel = package.replace("_", "-")
        blob = _core_wheel_blob(package, extension, raw_version)
        if blob is None:
            problems.append(f"{extension}: {wheel} is not installed")
        elif not blob.is_file():
            problems.append(f"{extension}: {wheel} has none (looked for {blob})")
        else:
            core.append(blob)
    if problems:
        raise BundledExtensionsMissing(
            f"DuckDB extensions missing for {platform} @ {raw_version}: "
            + "; ".join(problems)
            + f". Install provisa[embedded] for this platform, built against duckdb {raw_version}."
        )

    dst = Path(target) / raw_version / platform
    _copy_missing(files, src, dst)
    for blob in core:
        _copy_missing([blob], blob.parent, dst)
    for anchor in anchors.values():
        if anchor is not None:
            _copy_missing([anchor], anchor.parent, dst / REPOSITORY_ANCHORS)
    write_manifest(dst, raw_version, platform)
    return Path(target)


def _repository_anchors(src: Path, files: list[Path]) -> dict[str, Path | None]:
    """Each named repository the package carries extensions of (``repositories/<repo>/``), with
    its trust anchor beside them, or None where it has none."""
    out: dict[str, Path | None] = {}
    for p in files:
        rel = p.relative_to(src).parts
        if p.suffix == ".duckdb_extension" and len(rel) == 3 and rel[0] == "repositories":
            anchor = p.parent / f"{rel[1]}.duckdb_extension.repo.json"
            out[rel[1]] = anchor if anchor.is_file() else None
    return out


def extension_config() -> dict[str, str | bool]:
    """The configuration a DuckDB connection is opened with to load from the staged extension
    directory: the directory, the named repositories' trust anchors staged in it (DuckDB 2.0 reads
    them only at startup), and no network autoinstall. Empty where nothing is staged: DuckDB's own
    directory and repositories (a server or a dev checkout, REQ-1142)."""
    directory = extension_directory()
    if directory is None:
        return {}
    raw_version, platform = running_version_platform()
    return {
        "extension_directory": directory,
        "extension_repository_directory": str(
            Path(directory) / raw_version / platform / REPOSITORY_ANCHORS
        ),
        "autoinstall_known_extensions": False,
    }


def write_manifest(dst: Path, raw_version: str, platform: str) -> None:
    """Record what ``dst`` holds: every extension staged there, a named repository's included,
    by its file name (REQ-1142, ATTACHABLE MEANS STAGED: the engine reads this, not the connectors
    it declares)."""
    names = sorted(
        {p.name.removesuffix(".duckdb_extension") for p in dst.rglob("*.duckdb_extension")}
    )
    manifest = {"duckdb": raw_version, "platform": platform, "extensions": names}
    (dst / MANIFEST).write_text(json.dumps(manifest, indent=1) + "\n")


class StagedManifestMissing(RuntimeError):
    """An extension directory is set but holds no manifest for the running DuckDB and platform:
    it was not staged by this Provisa for this DuckDB, so what it can attach is unknown."""


#: What this server process installed from DuckDB's repositories when it started; None until it
#: has (REQ-1142, THE SERVER TIER STAGES AT START).
_installed_at_start: frozenset[str] | None = None


def staged_extensions() -> frozenset[str] | None:
    """The extensions a connector may count on for the running DuckDB version and platform: those
    in the staged directory's manifest (REQ-1142, ATTACHABLE MEANS STAGED), each by its file name
    and by the name a connector gives it (postgres for postgres_scanner), or with no staged
    directory those the server installed when it started (THE SERVER TIER STAGES AT START). None
    where neither has happened: a process that is not a server, DuckDB installing on first use."""
    directory = extension_directory()
    if directory is None:
        return _installed_at_start
    raw_version, platform = running_version_platform()
    path = Path(directory) / raw_version / platform / MANIFEST
    if not path.is_file():
        raise StagedManifestMissing(
            f"{_EXT_DIR_ENV}={directory} has no {MANIFEST} for DuckDB {raw_version} on {platform} "
            f"(looked for {path}); stage it with stage_bundled_extensions (provisa[embedded])."
        )
    names = frozenset(json.loads(path.read_text())["extensions"])
    return names | {_SHORT_NAMES[n] for n in names if n in _SHORT_NAMES}


def _needs(connector: Connector) -> tuple[str, ...]:
    """The extensions ``connector``'s way of reaching needs: each it names in ``staged_needs``,
    else its ``extension``. Empty for a connector needing none (DuckDB's own csv, parquet and
    database files)."""
    needs = getattr(connector, "staged_needs", None)
    if needs is None:
        needs = (connector.extension,) if connector.extension else ()
    return tuple(needs)


def unstaged_needs(connector: Connector, staged: frozenset[str]) -> tuple[str, ...]:
    """The extensions ``connector`` needs that are not in ``staged``."""
    return tuple(n for n in _needs(connector) if n not in staged)


#: The reader that lands a source type's replicas where the engine cannot attach it, decided and
#: not yet built: a source of the type names it while the deployment lacks it. Empty: MotherDuck
#: and Google Sheets have theirs (REQ-1998, MOTHERDUCK LANDS THROUGH ITS POSTGRES ENDPOINT;
#: events/motherduck_reader.py, events/sheets_reader.py). An extension that lags a future DuckDB
#: release gets its own reader decision, named here until its reader exists.
PENDING_READERS: dict[str, str] = {}


def unstaged_reason(source_type: str, missing: tuple[str, ...]) -> str:
    """What is missing for a ``source_type`` source that no reader in this deployment reads: the
    extensions its only reader needs are not staged for this DuckDB and platform, and the reader
    that would land it instead, where one is decided (PENDING_READERS)."""
    raw_version, platform = running_version_platform()
    which = " and ".join(missing)
    unavailable = (
        f"DuckDB's {which} extension{'s' if len(missing) > 1 else ''} "
        f"{'are' if len(missing) > 1 else 'is'} not available for DuckDB {raw_version} on {platform}"
    )
    if source_type in PENDING_READERS:
        return (
            f"needs {PENDING_READERS[source_type]} to land it, which this deployment does not "
            f"have yet: {unavailable}"
        )
    return (
        f"no reader of {source_type!r} sources in this deployment: {unavailable}, and Provisa "
        "has no other reader to land it"
    )


def connect() -> duckdb.DuckDBPyConnection:
    """A DuckDB connection honoring ``PROVISA_DUCKDB_EXT_DIR`` — the shared entry point for any
    caller that needs a bare connection able to load bundled extensions (probing here,
    the VSS catalog index in ``duckdb_runtime.build_vss_index_connection``)."""
    config: dict[str, str | bool | int | float | list[str]] = dict(extension_config())
    return engine_memory.connect(extra=config)


# Settings an extension is loaded with, whatever loads it. SET GLOBAL: a plain SET stays on the one
# connection that ran it and never reaches the cursors (con.cursor()) queries run on.
#
# None today. #241 turned the postgres extension's filter pushdown off on DuckDB 1.5 (upstream
# duckdb/duckdb-postgres#564: the Top-N dynamic filter of `ORDER BY <col> ... NULLS FIRST LIMIT n`
# reached Postgres as `WHERE <col> IS NULL`, so a sorted page held only its NULL-key rows).
# postgres_scanner 2.0.0 answers that page correctly with pushdown on (measured 2026-11-06: 20,000
# rows, 2,000 NULL keys, DESC NULLS FIRST LIMIT 2005 -- 1.5.5 with pushdown on answered 2,000 rows,
# 2.0.0 answers all 2,005), and renamed the setting pg_enable_filter_pushdown, so filters reach
# Postgres again at the extension's default.
_EXTENSION_SETTINGS: dict[str, tuple[str, ...]] = {}


def _snowflake_driver() -> dict[str, str]:
    """REQ-899: the snowflake extension reads Snowflake through Arrow ADBC and searches only its own
    locations for the driver, never the adbc-driver-snowflake package Provisa ships, so it is
    named to the extension -- read from the process environment when the extension connects."""
    import adbc_driver_snowflake

    return {"SNOWFLAKE_ADBC_DRIVER_PATH": adbc_driver_snowflake._driver_path()}  # pyright: ignore[reportPrivateUsage]


#: The process environment an extension is loaded with, by extension.
_EXTENSION_ENV: dict[str, Callable[[], dict[str, str]]] = {"snowflake": _snowflake_driver}


def install_statements(extension: str, *, from_community: bool) -> list[str]:
    """The statements that INSTALL ``extension``: from its named repository for an extension of
    EXTENSION_REPOSITORIES (created where the connection does not know it), else from community or
    the core registry."""
    if extension in EXTENSION_REPOSITORIES:
        repository, prefix = EXTENSION_REPOSITORIES[extension]
        return [
            "SET allow_extension_repositories = 'allowed'",
            f"CREATE EXTENSION REPOSITORY IF NOT EXISTS {repository} WITH PREFIX '{prefix}'",
            f"INSTALL {extension} FROM {repository}",
        ]
    return [f"INSTALL {extension} FROM community" if from_community else f"INSTALL {extension}"]


def bring_up_statements(extension: str, *, from_community: bool) -> list[str]:
    """The INSTALL and LOAD statements that bring ``extension`` up."""
    load = f"LOAD {extension}"
    if extension in EXTENSION_REPOSITORIES:
        load += f" FROM {EXTENSION_REPOSITORIES[extension][0]}"
    return [*install_statements(extension, from_community=from_community), load]


def install_extension_set() -> frozenset[str] | None:
    """REQ-1142 (Amended 2026-11-06, THE SERVER TIER STAGES AT START): INSTALL every extension the
    DuckDB engine's connectors need, each on a connection of its own, and remember what installed:
    the engine counts a connector as attachable only when its extensions did. An extension that
    does not install (no build for the running DuckDB) is named in the log and left out, so the
    source shows it when added. None where a staged directory holds the set: nothing to install."""
    global _installed_at_start
    if extension_directory() is not None:
        return None
    wanted: dict[str, bool] = {}
    for connector in duckdb_engine_connectors():
        for need in _needs(connector):
            community = bool(connector.install_from_community) and need == connector.extension
            wanted[need] = wanted.get(need, False) or community
    installed: set[str] = set()
    for extension, from_community in sorted(wanted.items()):
        con = connect()
        try:
            for statement in install_statements(extension, from_community=from_community):
                con.execute(statement)
        except duckdb.Error as error:
            _log.warning("DuckDB extension %s did not install: %s", extension, error)
        else:
            installed.add(extension)
        finally:
            con.close()
    _installed_at_start = frozenset(installed)
    return _installed_at_start


def install_and_load(
    con: duckdb.DuckDBPyConnection, extension: str, *, from_community: bool = True
) -> None:
    """INSTALL then LOAD ``extension`` on ``con`` — the one place that spells the two-step DuckDB
    extension bring-up, so callers never write a bare ``INSTALL``/``LOAD`` string themselves —
    with the environment (``_EXTENSION_ENV``) and settings (``_EXTENSION_SETTINGS``) the extension
    is always loaded with.

    An extension of EXTENSION_REPOSITORIES comes from its named repository whatever
    ``from_community`` says: DuckDB 2.0 keeps it under ``repositories/<repository>/`` and finds it
    only with ``LOAD <name> FROM <repository>``. The repository is created where the connection
    does not know it (a server or a dev checkout); a staged directory's trust anchor already names
    it."""
    if extension in _EXTENSION_ENV:
        os.environ.update(_EXTENSION_ENV[extension]())
    for statement in bring_up_statements(extension, from_community=from_community):
        con.execute(statement)
    for statement in _EXTENSION_SETTINGS.get(extension, ()):
        con.execute(statement)


def load_staged(con: duckdb.DuckDBPyConnection, extension: str) -> bool:
    """LOAD ``extension`` from the extension directory, never INSTALLing it, so the connection does
    not reach the network (REQ-2171). False where it is not staged: the caller refuses by name.

    An extension of EXTENSION_REPOSITORIES loads from its repository's staged copy alone, and only
    where this DuckDB knows the repository: DuckDB 2.0 keeps a user-provided repository, with the
    keys it trusts its extensions' signatures by, in its extension_repository_directory, and one
    it does not know cannot be loaded from (a machine that never staged it)."""
    directory = extension_directory()
    if directory:
        con.execute(f"SET extension_directory = '{directory}'")
    statement = f"LOAD {extension}"
    if extension in EXTENSION_REPOSITORIES:
        repository = EXTENSION_REPOSITORIES[extension][0]
        known = con.execute(
            "SELECT count(*) FROM duckdb_extension_repositories() WHERE repository_name = ?",
            [repository],
        ).fetchall()[0][0]
        if not known:
            return False
        statement += f" FROM {repository}"
    try:
        con.execute(statement)
    except duckdb.IOException:
        return False
    return True


def _fetch(con: duckdb.DuckDBPyConnection):
    """An async-shaped ``fetch(sql) -> list[dict]`` adapter over a DuckDB connection (REQ-904 probe API).

    DuckDB is synchronous; the connector probe signature is async, so this wraps ``execute`` in a
    coroutine that returns dict rows (empty for statements like INSTALL/LOAD that yield no result set).
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


async def stage_and_probe() -> list[ExtensionProbe]:
    """Stage (INSTALL) then probe (LOAD + symbol) each extension-backed DuckDB connector.

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
