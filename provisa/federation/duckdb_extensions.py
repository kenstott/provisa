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

Each DuckDB connector that needs an extension declares it (``Connector.extension``). The binary is put
in the extension directory once, at install or first launch, by the stage command
(``python -m provisa.runtime_deps.stage_assets``), from the machine's configured package index: the
core extensions from the ``duckdb-extension-*`` projects on PyPI, the rest from the version parts of
``provisa-duckdb-ext``. At runtime an extension is only LOADed (:func:`load_extension`), and one that
isn't staged is refused by name. ``probe`` is LOAD-ONLY too: it verifies the extension loads and
registers its scanner/attach symbol; it never opens a live connection to a source.

Set ``PROVISA_DUCKDB_EXT_DIR`` to load from a staged directory instead of DuckDB's per-user directory;
every connection reads the same variable. ``python -m provisa.federation.duckdb_extensions`` is the
install-time check: it exits non-zero, naming what is missing, unless the directory holds every
required extension for this DuckDB version and platform.
"""

from __future__ import annotations

import functools
import json
import logging
import os
import sys
import zipfile
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

import duckdb

from provisa.core import engine_memory
from provisa.core.engine_memory import motherduck_extension_limit

from provisa.federation.connector import Connector
from provisa.federation.engine import build_duckdb_engine

_log = logging.getLogger(__name__)

_EXT_DIR_ENV = engine_memory.EXT_DIR_ENV

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


#: The core extensions fetched from the existing per-extension, per-platform PyPI wheels, by the
#: extension's file name -> the importable package of its wheel (REQ-294, ASSETS COME FROM PACKAGE
#: REPOSITORIES): duckdb-extension-<name>==<duckdb version> from
#: https://github.com/santosh-d3vpl3x/duckdb_extensions (MIT). Each wheel holds the official, signed
#: binary at ``<package>/extensions/<raw_duckdb_version>/<name>.duckdb_extension``, and DuckDB loads
#: it with its signature check on.
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
    # What a connection needs that no connector declares: the catalog index, PostgreSQL's inet type.
    "vss": "duckdb_extension_vss",
    "inet": "duckdb_extension_inet",
}

#: The release of ``provisa-duckdb-ext`` this Provisa stages, and how many version parts it is split
#: across (REQ-294): ``<release>.<part>`` for part 1..N, one project, each part one wheel per
#: platform under PyPI's per-file limit. packaging/duckdb-ext/collect.py's PARTS is the assignment;
#: tests/unit/test_duckdb_ext_collect.py holds the two equal.
EXT_PACKAGE = "provisa-duckdb-ext"
EXT_PACKAGE_RELEASE = "0.3.0"
EXT_PACKAGE_PARTS = 2

#: The extensions a staged directory must hold on every platform: the core wheels' and the two
#: the mirror refuses to release without. Other community extensions depend on what their publisher
#: built for this DuckDB and platform, so they are recorded by the manifest, not required.
_REQUIRED_MIRRORED = ("crypto", "airport")


class BundledExtensionsMissing(RuntimeError):
    """A package fetched for this DuckDB version/platform lacks an extension it must carry, or the
    staged directory does not hold every required one.

    This is a packaging defect (a wheel built against a different DuckDB build, or a partial fetch),
    NOT a runtime condition to paper over: we fail loud with a precise remediation rather than
    silently reaching extensions.duckdb.org.
    """


def wheel_requirements(duckdb_version: str | None = None) -> list[str]:
    """The pinned requirements the stage command fetches for this DuckDB: each core extension's own
    project, and every version part of ``provisa-duckdb-ext``."""
    version = duckdb_version or duckdb.__version__
    return [
        *(f"{package.replace('_', '-')}=={version}" for package in CORE_EXTENSION_WHEELS.values()),
        *(f"{EXT_PACKAGE}=={EXT_PACKAGE_RELEASE}.{p}" for p in range(1, EXT_PACKAGE_PARTS + 1)),
    ]


def _write(wheel: zipfile.ZipFile, member: str, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(wheel.read(member))


def _stage_core_wheel(
    wheel: zipfile.ZipFile, package: str, extension: str, raw_version: str, dst: Path
) -> bool:
    """Lay the core extension of ``wheel`` under ``dst``; False where the wheel does not hold it."""
    member = f"{package}/extensions/{raw_version}/{extension}.duckdb_extension"
    if member not in wheel.namelist():
        return False
    _write(wheel, member, dst / f"{extension}.duckdb_extension")
    return True


def _stage_part_wheel(
    wheel: zipfile.ZipFile, raw_version: str, platform: str, dst: Path
) -> list[str]:
    """Lay a ``provisa-duckdb-ext`` part's files under ``dst``; return what is wrong with it. A
    named repository's extension (EXTENSION_REPOSITORIES) sits under repositories/<repository>/
    beside the .info DuckDB loads it by, and its trust anchor is gathered for the connection."""
    prefix = f"provisa_duckdb_ext/_ext/{raw_version}/{platform}/"
    members = [m for m in wheel.namelist() if m.startswith(prefix) and not m.endswith("/")]
    names = set(members)
    problems: list[str] = []
    for member in members:
        rel = PurePosixPath(member.removeprefix(prefix))
        if rel.name.endswith(".duckdb_extension") and f"{member}.info" not in names:
            # DuckDB 2.0 trusts an extension's signature by the repository its .info names.
            problems.append(
                f"{rel.name.removesuffix('.duckdb_extension')}: no .info beside it in "
                f"{EXT_PACKAGE}, so DuckDB does not trust its signature"
            )
        if (
            rel.name.endswith(".duckdb_extension")
            and rel.parts[0] == "repositories"
            and f"{prefix}{rel.parent}/{rel.parts[1]}.duckdb_extension.repo.json" not in names
        ):
            problems.append(
                f"{rel.parts[1]}: no trust anchor ({rel.parts[1]}.duckdb_extension.repo.json) "
                f"beside its extensions in {EXT_PACKAGE}, so DuckDB does not know the repository"
            )
        _write(wheel, member, dst.joinpath(*rel.parts))
        if rel.name.endswith(".duckdb_extension.repo.json"):
            _write(wheel, member, dst / REPOSITORY_ANCHORS / rel.name)
    return sorted(set(problems))


def stage_wheels(wheels: Iterable[Path], target: str | Path) -> Path:
    """Lay the running platform's extensions out of the fetched ``wheels`` under
    ``target/<version>/<platform>/`` and record them, or raise ``BundledExtensionsMissing`` naming
    every one absent or malformed. A core wheel's project name says which it is; every other wheel
    is a ``provisa-duckdb-ext`` part. Return ``target``."""
    raw_version, platform = running_version_platform()
    dst = Path(target) / raw_version / platform
    by_project = {p.replace("_", "-"): (e, p) for e, p in CORE_EXTENSION_WHEELS.items()}
    problems: list[str] = []
    staged: set[str] = set()
    for path in sorted(wheels):
        project = path.name.split("-", 1)[0].replace("_", "-")
        with zipfile.ZipFile(path) as wheel:
            if project in by_project:
                extension, package = by_project[project]
                if _stage_core_wheel(wheel, package, extension, raw_version, dst):
                    staged.add(extension)
            else:
                problems += _stage_part_wheel(wheel, raw_version, platform, dst)
    problems += [
        f"{extension}: {package.replace('_', '-')} has no build for {raw_version}"
        for extension, package in CORE_EXTENSION_WHEELS.items()
        if extension not in staged
    ]
    problems += [
        f"{name}: not in any {EXT_PACKAGE} {EXT_PACKAGE_RELEASE} part for {platform}"
        for name in _REQUIRED_MIRRORED
        if not any(dst.rglob(f"{name}.duckdb_extension"))
    ]
    if problems:
        raise BundledExtensionsMissing(
            f"DuckDB extensions missing for {platform} @ {raw_version}: " + "; ".join(problems)
        )
    write_manifest(dst, raw_version, platform)
    return Path(target)


def required_extensions() -> tuple[str, ...]:
    """Every extension a staged directory must hold, by file name: the one list staging and the
    install-time check read."""
    return tuple(sorted({*CORE_EXTENSION_WHEELS, *_REQUIRED_MIRRORED}))


def missing_extensions(directory: str | Path) -> list[str]:
    """The required extensions with no file in ``directory`` for the running DuckDB and platform."""
    raw_version, platform = running_version_platform()
    src = Path(directory) / raw_version / platform
    return [n for n in required_extensions() if not any(src.rglob(f"{n}.duckdb_extension"))]


def is_staged(directory: str | Path) -> bool:
    """Whether ``directory`` is already filled for this DuckDB and platform: a manifest and every
    required extension. Such a directory is used without fetching anything (REQ-294)."""
    raw_version, platform = running_version_platform()
    manifest = Path(directory) / raw_version / platform / MANIFEST
    return manifest.is_file() and not missing_extensions(directory)


def extension_config() -> dict[str, str | bool | int | float | list[str]]:
    """The configuration a DuckDB connection is opened with to load from the staged extension
    directory: the directory, the named repositories' trust anchors staged in it (DuckDB 2.0 reads
    them only at startup), and no network autoinstall anywhere (REQ-294). Where no directory is
    named, DuckDB's per-user one is read, which a source checkout fills once."""
    directory = extension_directory()
    settings = dict(engine_memory.load_only_config(directory))
    if directory is None:
        return settings
    raw_version, platform = running_version_platform()
    settings["extension_repository_directory"] = str(
        Path(directory) / raw_version / platform / REPOSITORY_ANCHORS
    )
    return settings


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


def staged_extensions() -> frozenset[str] | None:
    """The extensions a connector may count on for the running DuckDB version and platform: those
    in the staged directory's manifest (REQ-1142, ATTACHABLE MEANS STAGED), each by its file name
    and by the name a connector gives it (postgres for postgres_scanner). None where no directory
    is named (a source checkout on DuckDB's per-user directory): what it holds is learned when an
    extension loads, and one that is not there is refused by name."""
    directory = extension_directory()
    if directory is None:
        return None
    raw_version, platform = running_version_platform()
    path = Path(directory) / raw_version / platform / MANIFEST
    if not path.is_file():
        raise StagedManifestMissing(
            f"{_EXT_DIR_ENV}={directory} has no {MANIFEST} for DuckDB {raw_version} on {platform} "
            f"(looked for {path}); {engine_memory.STAGE_FIX}."
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
#: not yet built (REQ-1998, MOTHERDUCK LANDS THROUGH ITS POSTGRES ENDPOINT, maintainer 2026-11-06:
#: per-type readers; J866). A source of the type names it while the deployment lacks it.
PENDING_READERS: dict[str, str] = {
    "motherduck": "MotherDuck's Postgres-endpoint reader",
    "google_sheets": "the Google Sheets API reader",
}


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


def motherduck_reader_missing() -> str | None:
    """What is missing to read a MotherDuck source in this deployment, or None where the engine's
    DuckDB loads MotherDuck's extension (REQ-1998). Elsewhere a MotherDuck source is a replica
    landed through MotherDuck's Postgres endpoint (REQ-1998, MOTHERDUCK LANDS THROUGH ITS POSTGRES
    ENDPOINT), whose reader this deployment does not have yet: the source names it."""
    limit = motherduck_extension_limit()
    if limit is None:
        return None
    return (
        f"needs {PENDING_READERS['motherduck']} to land it, which this deployment does not have "
        f"yet: {limit}"
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


def load_statement(extension: str) -> str:
    """The statement that loads ``extension``: DuckDB 2.0 keeps a named repository's extension
    under ``repositories/<repository>/`` and finds it only with ``LOAD <name> FROM <repository>``."""
    if extension in EXTENSION_REPOSITORIES:
        return f"LOAD {extension} FROM {EXTENSION_REPOSITORIES[extension][0]}"
    return f"LOAD {extension}"


def load_extension(con: duckdb.DuckDBPyConnection, extension: str) -> None:
    """LOAD ``extension`` on ``con``, or refuse naming it and the fix — the one place that spells an
    extension's bring-up, with the environment (``_EXTENSION_ENV``) and settings
    (``_EXTENSION_SETTINGS``) it is always loaded with. It is never installed here (REQ-294)."""
    if extension in _EXTENSION_ENV:
        os.environ.update(_EXTENSION_ENV[extension]())
    engine_memory.load(con, extension, load_statement(extension))
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


def verify_staged_extensions(directory: str | Path | None = None) -> int:
    """REQ-294, the install-time check: ``directory`` (default the staged one) holds every required
    extension for this DuckDB and platform, and each loads with nothing fetched. Return how many;
    raise ``BundledExtensionsMissing`` naming what isn't."""
    named = directory if directory is not None else extension_directory()
    if named is None:
        raise BundledExtensionsMissing(f"{_EXT_DIR_ENV} is not set: {engine_memory.STAGE_FIX}")
    raw_version, platform = running_version_platform()
    missing = missing_extensions(named)
    if missing:
        raise BundledExtensionsMissing(
            f"{named} lacks DuckDB extensions for {platform} @ {raw_version}: "
            f"{', '.join(missing)}. To fix it: {engine_memory.STAGE_FIX}."
        )
    os.environ[_EXT_DIR_ENV] = str(named)
    failed: list[str] = []
    for name in required_extensions():
        con = connect()
        try:
            engine_memory.load(con, name, load_statement(name))
        except engine_memory.ExtensionNotStaged as refused:
            failed.append(f"{name}: {refused}")
        finally:
            con.close()
    if failed:
        raise BundledExtensionsMissing("; ".join(failed))
    return len(required_extensions())


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
        count = verify_staged_extensions()
    except BundledExtensionsMissing as refused:
        sys.exit(f"DuckDB extension check failed: {refused}")
    print(f"{count} DuckDB extensions staged")
