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
"""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

ROLE = "provisa"
PASSWORD = "provisa"
DATABASE = "provisa"

_SETTINGS = {
    "fsync": "off",
    "synchronous_commit": "off",
    "full_page_writes": "off",
    "max_connections": "300",
}


def _bin(name: str) -> str:
    from pgserver.postgres_server import POSTGRES_BIN_PATH

    return str(POSTGRES_BIN_PATH / name)


@dataclass
class LocalPostgres:
    """A running embedded Postgres on ``127.0.0.1:port`` (see the module docstring)."""

    port: int
    root: Path

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
            [_bin("pg_ctl"), "-D", str(self.data), "-w", *args], check=True, capture_output=True
        )

    def _start(self, preload: tuple[str, ...]) -> None:
        settings = {
            **_SETTINGS,
            **({"shared_preload_libraries": ",".join(preload)} if preload else {}),
        }
        options = " ".join(f"-c {k}={v}" for k, v in settings.items())
        started = subprocess.run(
            [
                _bin("pg_ctl"),
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


def start_local_postgres(
    port: int | None = None, *, preload: tuple[str, ...] = ()
) -> LocalPostgres:
    """Init a fresh cluster in a directory of its own and start it on ``127.0.0.1:port`` (a
    leased port when none is given), with ``preload`` as its ``shared_preload_libraries``."""
    from tests.port_lease import lease_port

    root = Path(tempfile.mkdtemp(prefix="pv-pg-"))
    pg = LocalPostgres(port=lease_port() if port is None else port, root=root)
    subprocess.run(
        [_bin("initdb"), "-D", str(pg.data), "-U", "postgres", "--auth=trust", "-E", "UTF8"],
        check=True,
        capture_output=True,
    )
    pg._start(preload)
    psql = [_bin("psql"), "-h", "127.0.0.1", "-p", str(pg.port), "-U", "postgres"]
    for sql in (
        f"CREATE ROLE {ROLE} LOGIN SUPERUSER PASSWORD '{PASSWORD}'",
        f"CREATE DATABASE {DATABASE} OWNER {ROLE}",
    ):
        subprocess.run([*psql, "-v", "ON_ERROR_STOP=1", "-c", sql], check=True, capture_output=True)
    return pg
