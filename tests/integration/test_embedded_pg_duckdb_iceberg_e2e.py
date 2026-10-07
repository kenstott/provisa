# Copyright (c) 2026 Kenneth Stott
# Canary: ec9a45fa-a265-4922-ad63-cf09f7bd258d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: pg_duckdb reads an Apache Iceberg table IN PLACE inside a stock embedded PG (REQ-908).

No docker. Provisions a pgserver embedded PG 16.2 with the pg_duckdb the product ships: the
provisa-pg-ext wheel's bundle for this platform (darwin-arm64, linux-x64), staged the way the
embedded tier stages it (stage_bundled_pg_extensions). That pg_duckdb is built (via vcpkg) to include
the DuckDB iceberg extension — aws-sdk-cpp[sso,sts,identity-management] + avro-c + roaring are
static-linked into libduckdb, so there is no extra runtime library. Generates a real Iceberg table
with pyiceberg, then drives the REAL PgDuckdbIcebergConnector's iceberg_scan through a named-column
view and asserts on rows.

A platform the wheel ships no bundle for fails loudly (BundledPgExtensionsMissing), and so does a
bundle whose pg_duckdb lacks iceberg — never a skip.
"""

from __future__ import annotations

import glob
import shutil
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
from provisa.pg_extensions.staging import stage_bundled_pg_extensions  # noqa: E402


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
    stage_bundled_pg_extensions(Path(pgserver.__file__).parent / "pginstall")
    base = tempfile.mkdtemp(prefix="provisa_iceberg_")
    server = pgserver.get_server(base)
    server.psql("ALTER SYSTEM SET shared_preload_libraries = 'pg_duckdb';")
    server.cleanup()
    server = pgserver.get_server(base)
    server.psql("CREATE EXTENSION pg_duckdb;")
    # The shipped pg_duckdb carries the iceberg extension; one without it is a packaging defect.
    assert "iceberg_scan" in server.psql(
        "SELECT proname FROM pg_proc WHERE proname = 'iceberg_scan'"
    ), "the bundled pg_duckdb has no iceberg_scan: it was built without the iceberg extension"
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
