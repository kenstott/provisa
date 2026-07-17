# Copyright (c) 2026 Kenneth Stott
# Canary: 5a3c0301-fbd7-4107-a066-0003bbc24fcc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Boot the embedded control-plane PostgreSQL for the native (no-Docker) desktop tier.

Backed by pgserver (bundled postgres binaries), separate from the dedicated
telemetry instance (see ``provisa.observability.telemetry_pg``). The instance is
persistent (``cleanup_mode=None``): it keeps running after this process exits and
a later call to the same data dir reuses it. Requires a pgserver-capable
interpreter (cpython <= 3.12 today).

``start`` ensures a ``provisa`` role + ``provisa`` database, applies ``db/init.sql``
once, and prints the connection coordinates the backend needs as two shell-eval
lines (unix-socket host + port):

    PG_HOST=/Users/me/.provisa/control-pg
    PG_PORT=5432

Usage:
    python -m provisa.core.control_plane_pg start <datadir> [--init-sql db/init.sql]
    python -m provisa.core.control_plane_pg stop  <datadir>
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path


def _server(datadir: str):
    import pgserver  # lazy — optional dep; ImportError signals "not available"

    return pgserver.get_server(Path(datadir), cleanup_mode=None)


def _socket_port(sockdir: str) -> int:
    """The port pgserver's unix socket listens on, read from the socket filename
    (``.s.PGSQL.<port>``). Explicit — never guessed."""
    for name in os.listdir(sockdir):
        if name.startswith(".s.PGSQL.") and name[len(".s.PGSQL.") :].isdigit():
            return int(name[len(".s.PGSQL.") :])
    raise RuntimeError(f"no postgres unix socket found in {sockdir!r}")


def start(datadir: str, init_sql: str | None = None) -> tuple[str, int]:
    """Ensure a persistent control-plane postgres with a ``provisa`` role and
    ``provisa`` database, apply ``init_sql`` once, and return ``(host, port)`` for
    a unix-socket asyncpg connection."""
    srv = _server(datadir)
    if "1" not in srv.psql("SELECT 1 FROM pg_roles WHERE rolname='provisa'"):
        srv.psql("CREATE ROLE provisa LOGIN PASSWORD 'provisa' SUPERUSER")
    fresh = "1" not in srv.psql("SELECT 1 FROM pg_database WHERE datname='provisa'")
    if fresh:
        srv.psql("CREATE DATABASE provisa OWNER provisa")
    # Apply the base schema exactly once, on first database creation. The backend's
    # own init is idempotent, but seeding here means the pool comes up on a ready DB.
    if fresh and init_sql:
        sql = Path(init_sql).read_text()
        srv.psql(f"\\c provisa\n{sql}")
    sockdir = srv.get_uri().split("host=", 1)[-1]
    return sockdir, _socket_port(sockdir)


def stop(datadir: str) -> None:
    _server(datadir).cleanup()


def main() -> None:
    parser = argparse.ArgumentParser(description="Embedded control-plane postgres (native tier).")
    parser.add_argument("command", choices=["start", "stop"])
    parser.add_argument("datadir")
    parser.add_argument("--init-sql", default=None, help="schema applied once on first DB creation")
    args = parser.parse_args()
    if args.command == "start":
        host, port = start(args.datadir, args.init_sql)
        print(f"PG_HOST={host}")
        print(f"PG_PORT={port}")
    else:
        stop(args.datadir)


if __name__ == "__main__":
    main()
