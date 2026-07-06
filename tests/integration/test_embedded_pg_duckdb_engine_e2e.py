# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: pg_duckdb attaches csv + parquet IN PLACE inside a stock embedded PG, federated natively (REQ-901, REQ-902).

No docker. Provisions a pgserver embedded PG 16.2, installs pg_duckdb v1.0.0 (built against the cached
PG 16.2 source by scripts/build_pg_duckdb.sh — HEAD needs a backend symbol 16.2 lacks), then drives the
REAL connectors — PgDuckdbCsvConnector + PgDuckdbParquetConnector — whose read_csv/read_parquet views
expose the demo files as native PG relations. A native orders table joins BOTH file sources with a
governance (RLS) predicate; pg_duckdb executes the whole join inside PG via its embedded DuckDB, so the
DuckDB engine's file reach collapses into one containerless Postgres engine.

Scope / honest boundary: this proves the CONNECTORS + in-engine federation + governance filtering. It
does NOT run the compiler's nested-JSON GraphQL shape through pg_duckdb: pg_duckdb's transparent path
needs SQL that PG parses AND DuckDB executes, and the pipeline's SQL/JSON JSON_OBJECT(k : v) output
satisfies neither (DuckDB rejects the colon form; PG rejects DuckDB's comma json_object; pg_duckdb does
not map json_build_object). Federating the nested pipeline through pg_duckdb needs a pg_duckdb-specific
JSON emission in the transpiler — separate compiler work, not built here.

Skips unless the pg_duckdb artifact is prebuilt in the cache (the ~25-min build is a release/CI step).
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

asyncpg = pytest.importorskip("asyncpg")
pgserver = pytest.importorskip("pgserver")

from provisa.federation.connector import (  # noqa: E402
    PgDuckdbCsvConnector,
    PgDuckdbJsonConnector,
    PgDuckdbParquetConnector,
)

_FILES = Path(__file__).parent.parent.parent / "demo" / "files"
_CACHE = Path.home() / ".cache" / "provisa-fdw" / "pg162"
_PGDUCKDB_SO = _CACHE / "lib" / "postgresql" / "pg_duckdb.dylib"  # prebuilt marker (macOS suffix)


def _install_pg_duckdb_into_pgserver() -> None:
    """Copy pg_duckdb + libduckdb into pgserver, adding an @loader_path rpath so libduckdb resolves."""
    src_lib = _CACHE / "lib" / "postgresql"
    src_ext = _CACHE / "share" / "postgresql" / "extension"
    pginstall = Path(pgserver.__file__).parent / "pginstall"
    dst_lib = pginstall / "lib" / "postgresql"
    dst_ext = pginstall / "share" / "postgresql" / "extension"
    suffix = "dylib" if (dst_lib / "plpgsql.dylib").exists() else "so"
    for lib in ("pg_duckdb", "libduckdb"):
        shutil.copy(src_lib / f"{lib}.{suffix}", dst_lib / f"{lib}.{suffix}")
    # the fresh copy carries only the cache rpath; add @loader_path so the sibling libduckdb is found
    subprocess.run(
        ["install_name_tool", "-add_rpath", "@loader_path", str(dst_lib / f"pg_duckdb.{suffix}")],
        check=True, stderr=subprocess.DEVNULL,
    )  # fmt: skip
    shutil.copy(src_ext / "pg_duckdb.control", dst_ext / "pg_duckdb.control")
    for f in src_ext.glob("pg_duckdb--*.sql"):
        shutil.copy(f, dst_ext / f.name)


@pytest.fixture(scope="session")
def embedded_pg_duckdb():
    if sys.platform != "darwin":
        pytest.skip("prebuilt pg_duckdb artifact in this cache is macOS/arm64")
    if not _PGDUCKDB_SO.exists():
        pytest.skip("pg_duckdb not prebuilt — run scripts/build_pg_duckdb.sh (a ~25-min release/CI step)")
    _install_pg_duckdb_into_pgserver()
    base = tempfile.mkdtemp(prefix="provisa_pgduckdb_")
    server = pgserver.get_server(base)
    server.psql("ALTER SYSTEM SET shared_preload_libraries = 'pg_duckdb';")
    server.cleanup()  # stop (mode 'stop' keeps pgdata); restart picks up the preload
    server = pgserver.get_server(base)
    server.psql("CREATE EXTENSION pg_duckdb;")
    yield server


def _view_ddl_from_connector(connector, source, schema: str, table: str, cols: list[tuple[str, str]]) -> str:
    """Build the named-column view a real EngineRuntime would emit from the connector's scan detail."""
    scan = connector.details(source)["scan"]  # read_csv('..') / read_parquet('..')
    select = ", ".join(f"""r['{name}']::{typ} AS "{name}\"""" for name, typ in cols)
    return f'CREATE VIEW "{schema}"."{table}" AS SELECT {select} FROM {scan} r'


async def test_pg_duckdb_connectors_federate_csv_and_parquet(embedded_pg_duckdb):
    """PgDuckdb csv + parquet connectors attach demo files in place; native orders joins both, RLS TX.

    The views wrap read_csv/read_parquet (DuckDB-only functions), so pg_duckdb MUST execute the join in
    its embedded DuckDB — returning rows at all proves both files are read in place, not landed.
    """
    conn = await asyncpg.connect(dsn=embedded_pg_duckdb.get_uri())
    try:
        await conn.execute("CREATE SCHEMA IF NOT EXISTS e2e_files")
        # ---- attach customers.csv + products.parquet via the REAL pg_duckdb connectors ----
        await conn.execute(_view_ddl_from_connector(
            PgDuckdbCsvConnector(), SimpleNamespace(id="cust", path=str(_FILES / "customers.csv")),
            "e2e_files", "customers", [("id", "int"), ("first_name", "text"), ("state", "text")]))
        await conn.execute(_view_ddl_from_connector(
            PgDuckdbParquetConnector(), SimpleNamespace(id="prod", path=str(_FILES / "products.parquet")),
            "e2e_files", "products", [("id", "int"), ("name", "text"), ("category", "text")]))

        # ---- native orders table (the engine's own store) referencing both file sources ----
        await conn.execute("CREATE SCHEMA IF NOT EXISTS e2e_engine")
        await conn.execute(
            # amount is double precision: pg_duckdb rejects a NUMERIC with unset precision
            "CREATE TABLE e2e_engine.orders(id int, customer_id int, product_id int, amount double precision)")
        await conn.execute(
            "INSERT INTO e2e_engine.orders VALUES "
            "(10,4,1,19.99),(11,2,2,49.99),(12,7,3,5.00),(13,9,4,7.50),(14,1,5,3.25)")
        # customers 4/7/9 are TX; 2 is CA, 1 is NY

        # ---- federated join executed by pg_duckdb: orders x customers(csv) x products(parquet) ----
        # WHERE c.state = 'TX' is the governance (RLS) predicate the stage-2 layer applies to customers.
        rows = await conn.fetch(
            """
            SELECT o.id, o.amount, c.first_name, c.state, p.name AS product, p.category
            FROM e2e_engine.orders o
            JOIN e2e_files.customers c ON c.id = o.customer_id
            JOIN e2e_files.products  p ON p.id = o.product_id
            WHERE c.state = 'TX'
            ORDER BY o.id
            """
        )

        # orders for TX customers 4/7/9 survive the governance filter; both file sources read in place
        assert {r["first_name"] for r in rows} == {"David", "Grace", "Iris"}  # csv, TX-only
        assert {r["state"] for r in rows} == {"TX"}
        assert all(r["product"] and r["category"] for r in rows)  # parquet read in place
        assert {r["id"] for r in rows} == {10, 12, 13}  # non-TX orders (11 CA, 14 NY) filtered out
    finally:
        await conn.execute("DROP SCHEMA IF EXISTS e2e_files CASCADE")
        await conn.execute("DROP SCHEMA IF EXISTS e2e_engine CASCADE")
        await conn.close()


async def test_discover_reports_pg_duckdb_only_after_preload():
    """REQ-904: probe is functional truth — the pg_duckdb connectors are unavailable until preloaded.

    This is the case a static 'is it installed' flag gets wrong: the .dylib is present the whole time,
    but pg_duckdb only works once it is in shared_preload_libraries. discover() reflects that live.
    """
    if sys.platform != "darwin" or not _PGDUCKDB_SO.exists():
        pytest.skip("pg_duckdb not prebuilt — run scripts/build_pg_duckdb.sh (a ~25-min release/CI step)")
    from provisa.federation.engine import build_pg_engine

    _install_pg_duckdb_into_pgserver()  # the extension files are present the whole test
    base = tempfile.mkdtemp(prefix="provisa_pgduckdb_probe_")
    server = pgserver.get_server(base)  # started WITHOUT preload

    conn = await asyncpg.connect(dsn=server.get_uri())
    try:
        report = await build_pg_engine().discover(conn.fetch)
        assert report["pg_duckdb_parquet"].available is False
        assert "shared_preload_libraries" in report["pg_duckdb_parquet"].reason  # the reason, live
    finally:
        await conn.close()

    server.psql("ALTER SYSTEM SET shared_preload_libraries = 'pg_duckdb';")
    server.cleanup()
    server = pgserver.get_server(base)  # restart with preload
    server.psql("CREATE EXTENSION pg_duckdb;")
    conn = await asyncpg.connect(dsn=server.get_uri())
    try:
        eng = build_pg_engine()
        report = await eng.discover(conn.fetch)
        assert report["pg_duckdb_parquet"].available is True  # same files, now functional
        assert eng.reachable("parquet") is True and eng.reachable("json") is True
    finally:
        await conn.close()


async def test_pg_duckdb_reads_json_file(embedded_pg_duckdb, tmp_path):
    """PgDuckdbJsonConnector attaches a JSON file in place via pg_duckdb's read_json (DuckDB json ext)."""
    jpath = tmp_path / "events.json"
    jpath.write_text(json.dumps([
        {"id": 1, "kind": "shipped", "state": "TX"},
        {"id": 2, "kind": "placed", "state": "CA"},
        {"id": 3, "kind": "shipped", "state": "TX"},
    ]))  # fmt: skip
    conn = await asyncpg.connect(dsn=embedded_pg_duckdb.get_uri())
    try:
        await conn.execute("CREATE SCHEMA IF NOT EXISTS e2e_json")
        await conn.execute(_view_ddl_from_connector(
            PgDuckdbJsonConnector(), SimpleNamespace(id="evt", path=str(jpath)),
            "e2e_json", "events", [("id", "int"), ("kind", "text"), ("state", "text")]))
        rows = await conn.fetch("SELECT id, kind, state FROM e2e_json.events WHERE state='TX' ORDER BY id")
        assert [(r["id"], r["kind"]) for r in rows] == [(1, "shipped"), (3, "shipped")]  # json read in place
    finally:
        await conn.execute("DROP SCHEMA IF EXISTS e2e_json CASCADE")
        await conn.close()
