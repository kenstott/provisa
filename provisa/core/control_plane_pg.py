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

``reset`` drops the ``provisa`` database so the next ``start`` rebuilds it at the current schema
(V1 has no migrations), and ``dump-table``/``apply`` carry one table's rows across that wipe — which
is how the demo keeps its org settings while everything else is rebuilt from config.

``start`` ensures a ``provisa`` role + ``provisa`` database, applies ``db/init.sql``
once, and prints the connection coordinates the backend needs as two shell-eval
lines (unix-socket host + port):

    PG_HOST=/Users/me/.provisa/control-pg
    PG_PORT=5432

Usage:
    python -m provisa.core.control_plane_pg start <datadir> [--init-sql db/init.sql]
    python -m provisa.core.control_plane_pg stop  <datadir>
    python -m provisa.core.control_plane_pg reset <datadir>
    python -m provisa.core.control_plane_pg dump-table <datadir> --table T --out FILE
    python -m provisa.core.control_plane_pg apply <datadir> --file FILE
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path


def _server(datadir: str):
    import pgserver  # lazy — optional dep; ImportError signals "not available"

    return pgserver.get_server(Path(datadir), cleanup_mode=None)


def _stage_bundled_extensions() -> None:
    """REQ-1158: stage the PyPI-delivered provisa-pg-ext FDW/extension bundle into the SHARED pgserver
    ``pginstall`` so the pg federation engine (PgFederationRuntime), which connects to this same
    embedded Postgres, can ``CREATE EXTENSION`` sqlite_fdw/pg_duckdb/... offline — no github.com round
    trip. pgserver has one install dir for all data dirs, so staging here reaches every instance.

    Best-effort by DESIGN, not silent error-swallowing: only the embedded/air-gapped tier ships the
    provisa-pg-ext wheel; a BYO-Postgres deployment supplies its own FDWs, so an absent wheel is a
    no-op. But if the wheel IS installed yet lacks this platform's bundle, staging fails LOUD
    (BundledPgExtensionsMissing) — a packaging defect must not be papered over."""
    try:
        import provisa_pg_ext  # type: ignore[import-not-found]  # noqa: F401 — presence probe for the optional wheel
    except ModuleNotFoundError:
        return  # BYO / non-embedded tier: FDWs come from the system Postgres, not the wheel
    import pgserver

    from provisa.pg_extensions.staging import stage_bundled_pg_extensions

    stage_bundled_pg_extensions(Path(pgserver.__file__).parent / "pginstall")


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
    _stage_bundled_extensions()  # REQ-1158: make the PyPI-delivered FDWs loadable by the pg fed engine
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


def reset(datadir: str) -> None:
    """Drop the ``provisa`` database so the next :func:`start` rebuilds it at the current schema.

    V1 has no migrations, so a pristine start means a pristine database — this is the PostgreSQL
    analogue of deleting the sqlite control-plane files. Open backends are terminated first: a
    connection left by a previous run would otherwise hold DROP DATABASE off.
    """
    srv = _server(datadir)
    srv.psql(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='provisa' "
        "AND pid <> pg_backend_pid()"
    )
    srv.psql("DROP DATABASE IF EXISTS provisa")


def _has_table(srv, table: str) -> bool:
    return "1" in srv.psql(
        f"\\c provisa\nSELECT 1 FROM information_schema.tables WHERE table_name='{table}' LIMIT 1"
    )


def dump_table(datadir: str, table: str, out: str) -> bool:
    """Write every schema's rows of *table* to *out* as INSERTs, returning whether anything was written.

    Used to carry a table across :func:`reset`. A control plane that has never held the table yields
    ``False`` and no file — a fresh install has nothing to retain, which is a fact about the plane
    rather than a failure.
    """
    import subprocess

    from pgserver._commands import POSTGRES_BIN_PATH  # the bundled binaries, no PATH lookup

    srv = _server(datadir)
    if not _has_table(srv, table):
        return False
    uri = srv.get_uri(database="provisa")
    sql = subprocess.check_output(
        [
            str(POSTGRES_BIN_PATH / "pg_dump"),
            uri,
            "--data-only",
            "--inserts",
            "--on-conflict-do-nothing",
            f"--table=*.{table}",
        ]
    )
    Path(out).write_text(sql.decode())
    return True


def apply_sql(datadir: str, path: str) -> None:
    """Run a SQL file against the ``provisa`` database — the restore half of :func:`dump_table`."""
    srv = _server(datadir)
    srv.psql(f"\\c provisa\n{Path(path).read_text()}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Embedded control-plane postgres (native tier).")
    parser.add_argument("command", choices=["start", "stop", "reset", "dump-table", "apply"])
    parser.add_argument("datadir")
    parser.add_argument("--init-sql", default=None, help="schema applied once on first DB creation")
    parser.add_argument(
        "--table", default=None, help="dump-table: the table to carry across a reset"
    )
    parser.add_argument("--out", default=None, help="dump-table: file the INSERTs are written to")
    parser.add_argument("--file", default=None, help="apply: SQL file to run against the database")
    args = parser.parse_args()
    if args.command == "start":
        host, port = start(args.datadir, args.init_sql)
        print(f"PG_HOST={host}")
        print(f"PG_PORT={port}")
    elif args.command == "stop":
        stop(args.datadir)
    elif args.command == "reset":
        reset(args.datadir)
    elif args.command == "dump-table":
        if not args.table or not args.out:
            parser.error("dump-table needs --table and --out")
        print("DUMPED=1" if dump_table(args.datadir, args.table, args.out) else "DUMPED=0")
    else:
        if not args.file:
            parser.error("apply needs --file")
        apply_sql(args.datadir, args.file)


if __name__ == "__main__":
    main()
