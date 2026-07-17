# Copyright (c) 2026 Kenneth Stott
# Canary: 1f8f6b25-a006-46cc-8660-815d2d7d84c4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Boot a DEDICATED embedded PostgreSQL instance for the telemetry (ops) store —
separate from the control-plane database — and print its SQLAlchemy URL.

Backed by pgserver (bundled postgres binaries). The instance is persistent
(``cleanup_mode=None``): it keeps running after this process exits, and a later
call to the same data dir reuses it. Requires a pgserver-capable interpreter
(cpython <= 3.12 today).

Usage:
    python -m provisa.observability.telemetry_pg start <datadir>   # prints URL
    python -m provisa.observability.telemetry_pg stop  <datadir>
"""

from __future__ import annotations

import argparse
from pathlib import Path


def _server(datadir: str):
    import pgserver  # lazy — optional dep; ImportError signals "not available"

    return pgserver.get_server(Path(datadir), cleanup_mode=None)


def start(datadir: str) -> str:
    """Ensure a persistent telemetry postgres with a ``provisa`` role and a
    ``telemetry`` database; return its psycopg2 SQLAlchemy URL (unix socket)."""
    srv = _server(datadir)
    if "1" not in srv.psql("SELECT 1 FROM pg_roles WHERE rolname='provisa'"):
        srv.psql("CREATE ROLE provisa LOGIN PASSWORD 'provisa' SUPERUSER")
    if "1" not in srv.psql("SELECT 1 FROM pg_database WHERE datname='telemetry'"):
        srv.psql("CREATE DATABASE telemetry OWNER provisa")
    # get_uri() -> postgresql://postgres:@/postgres?host=<socketdir>
    sockdir = srv.get_uri().split("host=", 1)[-1]
    return f"postgresql+psycopg2://provisa:provisa@/telemetry?host={sockdir}"


def stop(datadir: str) -> None:
    _server(datadir).cleanup()


def main() -> None:
    parser = argparse.ArgumentParser(description="Dedicated telemetry (ops) postgres.")
    parser.add_argument("command", choices=["start", "stop"])
    parser.add_argument("datadir")
    args = parser.parse_args()
    if args.command == "start":
        print(start(args.datadir))
    else:
        stop(args.datadir)


if __name__ == "__main__":
    main()
