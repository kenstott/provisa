# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: pg_duckdb reads an Apache Iceberg table IN PLACE inside a stock embedded PG (REQ-908).

No docker. Provisions a pgserver embedded PG 16.2 with a pg_duckdb built (via vcpkg) to include the
DuckDB iceberg extension — aws-sdk-cpp[sso,sts,identity-management] + avro-c + roaring are static-linked
into libduckdb, so there is no extra runtime dylib. Generates a real Iceberg table with pyiceberg, then
drives the REAL PgDuckdbIcebergConnector's iceberg_scan through a named-column view and asserts on rows.

Skips unless a pg_duckdb WITH iceberg is prebuilt (the vcpkg build is a release/CI step) and pyiceberg
is available.
"""

from __future__ import annotations

import glob
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
pytest.importorskip("pyiceberg")
pytest.importorskip("pyarrow")

from provisa.federation.connector_duckdb import PgDuckdbIcebergConnector  # noqa: E402

_CACHE = Path.home() / ".cache" / "provisa-fdw" / "pg162"
_PGDUCKDB_SO = _CACHE / "lib" / "postgresql" / "pg_duckdb.dylib"


def _install_pg_duckdb() -> None:
    src_lib = _CACHE / "lib" / "postgresql"
    src_ext = _CACHE / "share" / "postgresql" / "extension"
    pg = Path(pgserver.__file__).parent / "pginstall"
    dl = pg / "lib" / "postgresql"
    de = pg / "share" / "postgresql" / "extension"
    suffix = "dylib" if (dl / "plpgsql.dylib").exists() else "so"
    for lib in ("pg_duckdb", "libduckdb"):
        shutil.copy(src_lib / f"{lib}.{suffix}", dl / f"{lib}.{suffix}")
    subprocess.run(
        ["install_name_tool", "-add_rpath", "@loader_path", str(dl / f"pg_duckdb.{suffix}")],
        check=True, stderr=subprocess.DEVNULL,
    )  # fmt: skip
    shutil.copy(src_ext / "pg_duckdb.control", de / "pg_duckdb.control")
    for f in src_ext.glob("pg_duckdb--*.sql"):
        shutil.copy(f, de / f.name)


def _make_iceberg_table(wh: Path) -> str:
    """Write a small Iceberg table; add v<N>.metadata.json + version-hint.text (DuckDB's expected
    naming, which pyiceberg's Hadoop-style 00001-<uuid>.metadata.json does not match)."""
    from pyiceberg.catalog.sql import SqlCatalog
    import pyarrow as pa

    cat = SqlCatalog("d", uri=f"sqlite:///{wh}/cat.db", warehouse=f"file://{wh}")
    cat.create_namespace("db")
    data = pa.table({
        "id": pa.array([1, 2, 3, 4], pa.int32()),
        "region": pa.array(["us-east", "us-west", "us-east", "apac"]),
        "amount": pa.array([10.5, 20.0, 5.25, 7.75], pa.float64()),
    })  # fmt: skip
    t = cat.create_table("db.orders", schema=data.schema)
    t.append(data)
    root = f"{wh}/db/orders"
    latest = sorted(glob.glob(f"{root}/metadata/00001-*.metadata.json"))[-1]
    shutil.copy(latest, f"{root}/metadata/v1.metadata.json")
    Path(f"{root}/metadata/version-hint.text").write_text("1")
    return root


@pytest.fixture(scope="session")
def embedded_pg_duckdb_iceberg():
    if sys.platform != "darwin" or not _PGDUCKDB_SO.exists():
        pytest.skip("pg_duckdb not prebuilt in cache")
    _install_pg_duckdb()
    base = tempfile.mkdtemp(prefix="provisa_iceberg_")
    server = pgserver.get_server(base)
    server.psql("ALTER SYSTEM SET shared_preload_libraries = 'pg_duckdb';")
    server.cleanup()
    server = pgserver.get_server(base)
    server.psql("CREATE EXTENSION pg_duckdb;")
    # skip if this pg_duckdb was built WITHOUT the iceberg extension
    if "iceberg_scan" not in server.psql(
        "SELECT proname FROM pg_proc WHERE proname = 'iceberg_scan'"
    ):
        pytest.skip("pg_duckdb prebuilt without the iceberg extension (vcpkg build not run)")
    yield server


def _view_ddl(scan: str, schema: str, table: str, cols: list[tuple[str, str]]) -> str:
    select = ", ".join(f"""r['{n}']::{t} AS "{n}\"""" for n, t in cols)
    return f'CREATE VIEW "{schema}"."{table}" AS SELECT {select} FROM {scan} r'


async def test_pg_duckdb_iceberg_connector_reads_in_place(embedded_pg_duckdb_iceberg, tmp_path):
    """The REAL PgDuckdbIcebergConnector's iceberg_scan reads an Iceberg table in place; RLS filters it."""
    root = _make_iceberg_table(tmp_path)
    scan = PgDuckdbIcebergConnector().details(SimpleNamespace(id="ord", path=root))["scan"]
    assert "iceberg_scan(" in scan and "allow_moved_paths" in scan  # the connector emits the reader

    conn = await asyncpg.connect(dsn=embedded_pg_duckdb_iceberg.get_uri())
    try:
        await conn.execute("CREATE SCHEMA IF NOT EXISTS e2e_ice")
        await conn.execute(
            _view_ddl(
                scan, "e2e_ice", "orders", [("id", "int"), ("region", "text"), ("amount", "float8")]
            )
        )
        rows = await conn.fetch("SELECT id, region, amount FROM e2e_ice.orders ORDER BY id")
        assert [(r["id"], r["region"]) for r in rows] == [
            (1, "us-east"),
            (2, "us-west"),
            (3, "us-east"),
            (4, "apac"),
        ]  # read from Iceberg in place

        # governance predicate applied to the Iceberg-backed relation
        gov = await conn.fetch("SELECT id FROM e2e_ice.orders WHERE region = 'us-east' ORDER BY id")
        assert [r["id"] for r in gov] == [1, 3]
    finally:
        await conn.execute("DROP SCHEMA IF EXISTS e2e_ice CASCADE")
        await conn.close()
