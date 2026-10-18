# Copyright (c) 2026 Kenneth Stott
# Canary: 22212ef7-ecc7-4ad8-a069-abdd0193e9d0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An embedded Postgres for tests: the pgserver wheel's PostgreSQL, on a plain TCP address.

pgserver is a product dependency (the desktop profile and the embedded engine run on it). This
module runs its server the way a test reaches a real one: on ``127.0.0.1`` and a leased port, with
the ``provisa`` role and database the docker stack has. pgserver's own ``get_server`` listens on a
Unix socket only; this starts the same binaries with ``pg_ctl`` and a TCP listener instead. The
binaries are only run, never written to. The data is throwaway, so durability is off (fsync,
synchronous_commit, full_page_writes).

A test that needs Postgres gets its own server here rather than whatever listens on the machine's
5432, which other jobs start and stop.

A test that needs the FDW set (``start_local_postgres(fdws=True)``) runs from a PRIVATE copy of
pgserver's ``pginstall`` with the provisa-pg-ext wheel's bundle (postgres_fdw, file_fdw,
sqlite_fdw, pg_duckdb, ...) staged into it by ``provisa.pg_extensions.staging``, as the product
stages it; ``mongo_fdw=True`` adds mongo_fdw, built by scripts/build_mongo_fdw.sh. The copy is
cached under ``~/.cache/provisa-test-pg``; the venv's own pgserver package is never written to.

A server does not outlive the process that started it. A session ended by a signal (a stopped job,
a runner's timeout) runs no teardown, and its server kept running, holding the machine's shared
memory until no further one could start. Each server's directory records its starter's pid
(``owner.pid``), and every start first stops the servers whose starter is gone; a running
session's is left alone.
"""

from __future__ import annotations

import fcntl
import os
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

from provisa.core.pg_shared_memory import release_segment

ROLE = "provisa"
PASSWORD = "provisa"
DATABASE = "provisa"

_PREFIX = "pv-pg-"
_OWNER = "owner.pid"

_SETTINGS = {
    "fsync": "off",
    "synchronous_commit": "off",
    "full_page_writes": "off",
    "max_connections": "300",
    # The zone a session reads a timestamptz back in: UTC, as the postgres image the docker stack and
    # CI run is. pgserver's initdb takes the machine's, so a test on a laptop read 04:00-04:00 for
    # 08:00+00:00 where CI reads the latter.
    "timezone": "UTC",
}


_REPO = Path(__file__).resolve().parent.parent
_CACHE = Path(os.environ.get("PROVISA_FDW_CACHE", Path.home() / ".cache" / "provisa-fdw"))
#: The PostgreSQL mongo_fdw is built against: pgserver's major, its minor ABI-compatible within 16.
_PG_VERSION = "16.2"


def _build_pg_source() -> Path:
    """PostgreSQL built from source once (cached under the FDW cache, restart-safe): the
    ``pg_config`` scripts/build_mongo_fdw.sh builds against. Returns its prefix."""
    prefix = _CACHE / f"pg{_PG_VERSION.replace('.', '')}"
    if (prefix / "bin" / "pg_config").exists():
        return prefix
    _CACHE.mkdir(parents=True, exist_ok=True)
    src = _CACHE / f"postgresql-{_PG_VERSION}"
    if not src.exists():
        tarball = _CACHE / f"postgresql-{_PG_VERSION}.tar.bz2"
        url = (
            f"https://ftp.postgresql.org/pub/source/v{_PG_VERSION}/postgresql-{_PG_VERSION}.tar.bz2"
        )
        subprocess.run(["curl", "-fsSL", "-o", str(tarball), url], check=True)
        subprocess.run(["tar", "xj", "-C", str(_CACHE), "-f", str(tarball)], check=True)
    with open(_CACHE / "build.log", "w") as log:
        for cmd in (
            ["./configure", "--without-icu", "--without-readline", "--without-zlib",
             "--without-gssapi", f"--prefix={prefix}"],
            ["make", f"-j{os.cpu_count() or 2}"],
            ["make", "install"],
        ):  # fmt: skip
            subprocess.run(cmd, cwd=src, check=True, stdout=log, stderr=log)
    return prefix


def _build_mongo_fdw(bundle: Path) -> None:
    """mongo_fdw and its driver libraries as a bundle tree at ``bundle``, its own manifest rows
    included (scripts/build_mongo_fdw.sh with OUT)."""
    _build_pg_source()
    bundle.mkdir(parents=True)
    (bundle / "manifest.json").write_text('{"artifacts": []}')
    log = bundle.parent / "mongo_fdw.log"
    with open(log, "w") as out:
        done = subprocess.run(
            ["bash", str(_REPO / "scripts" / "build_mongo_fdw.sh")],
            env={**os.environ, "OUT": str(bundle)},
            stdout=out,
            stderr=subprocess.STDOUT,
            check=False,
        )
    if done.returncode != 0:
        tail = log.read_text(errors="replace")[-4000:]
        raise RuntimeError(
            f"build_mongo_fdw.sh failed (exit {done.returncode}); log {log}:\n{tail}"
        )


def private_pginstall(*, mongo_fdw: bool = False) -> Path:
    """A private pginstall: pgserver's own files (as its wheel installed them, so nothing copied
    into the venv comes along) with the provisa-pg-ext wheel's FDW bundle staged into it as the
    product stages it -- and, with ``mongo_fdw``, mongo_fdw (REQ-1177's conformance target, not in
    the product's set) built and staged too. Built once per (pgserver, provisa-pg-ext, platform,
    mongo_fdw build script) under ``~/.cache/provisa-test-pg``, under an exclusive lock so
    concurrent sessions build it once. The venv's pgserver package is never written to."""
    import fcntl
    import hashlib
    import importlib.metadata as metadata

    from provisa.pg_extensions.staging import (
        bundle_platform,
        stage_bundled_pg_extensions,
        stage_pg_extension_bundle,
    )

    key = (
        f"pgserver-{metadata.version('pgserver')}_pgext-{metadata.version('provisa-pg-ext')}"
        f"_{bundle_platform()}"
    )
    if mongo_fdw:
        script = (_REPO / "scripts" / "build_mongo_fdw.sh").read_bytes()
        key += f"_mongo-{hashlib.sha256(script).hexdigest()[:12]}"
    home = Path.home() / ".cache" / "provisa-test-pg" / key
    home.mkdir(parents=True, exist_ok=True)
    site = home / "site"
    pginstall = site / "pgserver" / "pginstall"
    with open(home / ".lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if not (home / ".complete").exists():
            for stale in (site, home / "mongo_fdw"):  # an earlier build that did not complete
                if stale.exists():
                    shutil.rmtree(stale)
            # The wheel's own layout: its binaries load their bundled libraries relative to
            # themselves (pgserver/.dylibs on macOS, pgserver.libs on Linux).
            dist = metadata.distribution("pgserver")
            for f in dist.files or []:
                if Path(f).parts[0].startswith("pgserver"):
                    out = site / f
                    out.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(dist.locate_file(f), out)
            stage_bundled_pg_extensions(pginstall)
            if mongo_fdw:
                _build_mongo_fdw(home / "mongo_fdw")
                stage_pg_extension_bundle(home / "mongo_fdw", pginstall)
            (home / ".complete").touch()
    return pginstall


def _bin(name: str, pginstall: Path | None = None) -> str:
    """``name`` from ``pginstall`` (the private FDW install), else from pgserver's own."""
    if pginstall is not None:
        return str(pginstall / "bin" / name)
    from pgserver.postgres_server import POSTGRES_BIN_PATH

    return str(POSTGRES_BIN_PATH / name)


@dataclass
class LocalPostgres:
    """A running embedded Postgres on ``127.0.0.1:port`` (see the module docstring)."""

    port: int
    root: Path
    # Server settings over the defaults this session's clusters run with (e.g. wal_level).
    settings: dict[str, str] = field(default_factory=dict)
    # The private FDW install it runs from (private_pginstall), or None for pgserver's own.
    pginstall: Path | None = None

    @property
    def data(self) -> Path:
        return self.root / "data"

    def url(self, database: str = DATABASE, *, driver: str = "psycopg") -> str:
        return f"postgresql+{driver}://{ROLE}:{PASSWORD}@127.0.0.1:{self.port}/{database}"

    def dsn(self, database: str = DATABASE) -> str:
        """A libpq DSN (asyncpg, psycopg, postgres_fdw) for ``database`` as the ``provisa`` role."""
        return f"postgresql://{ROLE}:{PASSWORD}@127.0.0.1:{self.port}/{database}"

    def restart(self, *, preload: tuple[str, ...]) -> None:
        """Restart with ``shared_preload_libraries`` set to ``preload`` (pg_duckdb loads only so)."""
        self._ctl("stop", "-m", "fast")
        self._start(preload)

    def stop(self) -> None:
        self._ctl("stop", "-m", "fast")
        shutil.rmtree(self.root)

    def _ctl(self, *args: str) -> None:
        subprocess.run(
            [_bin("pg_ctl", self.pginstall), "-D", str(self.data), "-w", *args],
            check=True,
            capture_output=True,
        )

    def _start(self, preload: tuple[str, ...]) -> None:
        settings = {
            **_SETTINGS,
            **self.settings,
            **({"shared_preload_libraries": ",".join(preload)} if preload else {}),
        }
        options = " ".join(f"-c {k}={v}" for k, v in settings.items())
        started = subprocess.run(
            [
                _bin("pg_ctl", self.pginstall),
                "-D",
                str(self.data),
                "-l",
                str(self.root / "postgres.log"),
                "-w",
                "-o",
                f"-h 127.0.0.1 -p {self.port} -k {self.data} {options}",
                "start",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if started.returncode != 0:
            log = (self.root / "postgres.log").read_text(errors="replace")
            raise RuntimeError(
                f"embedded Postgres did not start on 127.0.0.1:{self.port}:\n{log[-4000:]}"
            )


def _running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # another user's process: running, and not this one's to judge
    return True


#: A directory with no recorded starter is an orphan once it is this old: a starter records itself
#: the moment it has made its directory, so only one made before starters recorded themselves (or
#: whose starter died between the two) has none, and it is never claimed again.
OWNERLESS_AFTER_SECONDS = 24 * 60 * 60


def _orphaned(root: Path) -> bool:
    owner = root / _OWNER
    if owner.is_file():
        return not _running(int(owner.read_text()))
    return time.time() - root.stat().st_mtime > OWNERLESS_AFTER_SECONDS


#: Held while a starter reaps. Every pytest worker reaps at its own start, and two reaping the same
#: orphan at once failed: one stopped the server, and the other's ``pg_ctl stop`` found no PID file,
#: or found the data directory half removed (a whole xdist worker's session fixture errored).
#: Outside the ``pv-pg-*`` glob.
_REAP_LOCK = "pv-pg.reap.lock"


def _postmaster_pid(data: Path) -> int | None:
    pid_file = data / "postmaster.pid"
    if not pid_file.exists():
        return None
    return int(pid_file.read_text().splitlines()[0])


def _stop_orphans() -> None:
    """Stop every embedded server whose starter is gone, and remove its directory: one whose
    recorded starter is not running, and one with no recorded starter older than
    ``OWNERLESS_AFTER_SECONDS``. One whose starter runs is left alone. One reaper at a time
    (``_REAP_LOCK``), so a directory is reaped whole by one of them."""
    temp = Path(tempfile.gettempdir())
    with open(temp / _REAP_LOCK, "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        for root in temp.glob(f"{_PREFIX}*"):
            try:
                if not _orphaned(root):
                    continue
            except FileNotFoundError:
                # A live starter's own stop removed it between the glob and this look: not an
                # orphan, and already gone.
                continue
            postmaster = _postmaster_pid(root / "data")
            # A postmaster that is gone (killed with its PID file left behind) is already stopped;
            # pg_ctl refuses to stop it, and there is nothing to stop.
            if postmaster is not None and _running(postmaster):
                subprocess.run(
                    [_bin("pg_ctl"), "-D", str(root / "data"), "-w", "stop", "-m", "immediate"],
                    check=True,
                    capture_output=True,
                )
            # A killed one left its System V segment, recorded only in the directory removed next:
            # released first, or it is held until the machine restarts (pg_shared_memory).
            release_segment(root / "data")
            shutil.rmtree(root)


def start_local_postgres(
    port: int | None = None,
    *,
    preload: tuple[str, ...] = (),
    settings: dict[str, str] | None = None,
    fdws: bool = False,
    mongo_fdw: bool = False,
) -> LocalPostgres:
    """Init a fresh cluster in a directory of its own and start it on ``127.0.0.1:port`` (a
    leased port when none is given), with ``preload`` as its ``shared_preload_libraries`` and
    ``settings`` over the session's defaults. With ``fdws``, it runs from a private install with
    the product's FDW bundle staged, and with ``mongo_fdw`` mongo_fdw too
    (:func:`private_pginstall`)."""
    from tests.port_lease import lease_port

    _stop_orphans()
    root = Path(tempfile.mkdtemp(prefix=_PREFIX))
    (root / _OWNER).write_text(str(os.getpid()))
    pg = LocalPostgres(
        port=lease_port() if port is None else port,
        root=root,
        settings=dict(settings or {}),
        pginstall=private_pginstall(mongo_fdw=mongo_fdw) if fdws or mongo_fdw else None,
    )
    subprocess.run(
        [
            _bin("initdb", pg.pginstall),
            "-D",
            str(pg.data),
            "-U",
            "postgres",
            "--auth=trust",
            "-E",
            "UTF8",
        ],
        check=True,
        capture_output=True,
    )
    pg._start(preload)
    psql = [_bin("psql", pg.pginstall), "-h", "127.0.0.1", "-p", str(pg.port), "-U", "postgres"]
    for sql in (
        f"CREATE ROLE {ROLE} LOGIN SUPERUSER PASSWORD '{PASSWORD}'",
        f"CREATE DATABASE {DATABASE} OWNER {ROLE}",
    ):
        subprocess.run([*psql, "-v", "ON_ERROR_STOP=1", "-c", sql], check=True, capture_output=True)
    return pg
