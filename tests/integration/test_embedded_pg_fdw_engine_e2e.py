# Copyright (c) 2026 Kenneth Stott
# Canary: afd62da7-9f9e-4372-a4fb-2fb423c25e86
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: the Postgres FDW connectors work on a STOCK embedded PG once core-contrib FDWs are installed.

No docker. Provisions a pgserver embedded PG 16.2, installs file_fdw + postgres_fdw built from PG
source (cached under ~/.cache/provisa-fdw; the route proven by scripts/prove_embedded_fdw.sh), then
drives the REAL connectors — FileFdwConnector and PostgresFdwConnector — and the REAL compiler pipeline
(compile -> apply_governance -> rewrite_semantic_to_physical -> transpile("postgres")) against the
FDW-attached demo sources, asserting on returned rows:

  - customers  <- customers.csv        attached via FileFdwConnector (file_fdw)
  - orders     <- loopback PG schema    attached via PostgresFdwConnector (postgres_fdw)

The connectors' emitted DDL is executed verbatim (not hand-written), so a green run proves the
connectors themselves function on the embedded engine. REQ-893.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

asyncpg = pytest.importorskip("asyncpg")
pgserver = pytest.importorskip("pgserver")

from provisa.compiler.introspect import ColumnMetadata  # noqa: E402
from provisa.compiler.parser import parse_query  # noqa: E402
from provisa.compiler.rls import RLSContext  # noqa: E402
from provisa.compiler.schema_gen import SchemaInput, generate_schema  # noqa: E402
from provisa.compiler.sql_gen import compile_query  # noqa: E402
from provisa.compiler.context import build_context  # noqa: E402
from provisa.compiler.sql_rewrite import rewrite_semantic_to_physical  # noqa: E402
from provisa.compiler.stage2 import apply_governance, build_governance_context  # noqa: E402
from provisa.federation.connector_duckdb import FileFdwConnector, PostgresFdwConnector  # noqa: E402
from provisa.transpiler.transpile import transpile  # noqa: E402

_FILES = Path(__file__).parent.parent.parent / "demo" / "files"
_ADMIN = {"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]}
_PG_VERSION = "16.2"  # must match the embedded PG major; minor is ABI-compatible within 16
_FDWS = ("file_fdw", "postgres_fdw")  # core contrib — no external deps
_CACHE = Path(os.environ.get("PROVISA_FDW_CACHE", Path.home() / ".cache" / "provisa-fdw"))

# All CSV columns in file order (file_fdw is positional — the foreign table must list every column).
_CSV_COLS = [
    "id int", "first_name text", "last_name text", "email text", "phone text",
    "city text", "state text", "country text", "created_at text", "lifetime_value text",
]  # fmt: skip


def _have_build_tools() -> bool:
    tools = all(shutil.which(t) for t in ("cc", "make", "tar"))
    fetch = shutil.which("curl") or shutil.which("wget")
    return bool(tools and fetch)


def _build_fdw_artifacts() -> Path:
    """Build core-contrib FDWs from PG source once (cached, restart-safe); return the prefix dir."""
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
        if shutil.which("curl"):
            subprocess.run(["curl", "-fsSL", "-o", str(tarball), url], check=True)
        else:
            subprocess.run(["wget", "-qO", str(tarball), url], check=True)
        subprocess.run(["tar", "xj", "-C", str(_CACHE), "-f", str(tarball)], check=True)
    log = open(_CACHE / "build.log", "w")
    run = lambda cmd, cwd=src: subprocess.run(cmd, cwd=cwd, check=True, stdout=log, stderr=log)  # noqa: E731
    run(["./configure", "--without-icu", "--without-readline", "--without-zlib",
         "--without-gssapi", f"--prefix={prefix}"])  # fmt: skip
    run(["make", f"-j{os.cpu_count() or 2}"])
    run(["make", "install"])
    for fdw in _FDWS:
        run(["make", "-C", f"contrib/{fdw}", "install"])
    log.close()
    return prefix


def _install_fdws_into_pgserver() -> None:
    """Copy built FDW artifacts into the pgserver install, matching the server's DLSUFFIX."""
    prefix = _build_fdw_artifacts()
    pgc = str(prefix / "bin" / "pg_config")
    src_lib = Path(subprocess.check_output([pgc, "--pkglibdir"]).decode().strip())
    src_ext = Path(subprocess.check_output([pgc, "--sharedir"]).decode().strip()) / "extension"
    pginstall = Path(pgserver.__file__).parent / "pginstall"
    dst_lib = pginstall / "lib" / "postgresql"
    dst_ext = pginstall / "share" / "postgresql" / "extension"
    suffix = "dylib" if (dst_lib / "plpgsql.dylib").exists() else "so"
    for fdw in _FDWS:
        built = src_lib / f"{fdw}.dylib"
        if not built.exists():
            built = src_lib / f"{fdw}.so"
        shutil.copy(built, dst_lib / f"{fdw}.{suffix}")  # rename to the server's expected suffix
        shutil.copy(src_ext / f"{fdw}.control", dst_ext / f"{fdw}.control")
        for sql in src_ext.glob(f"{fdw}--*.sql"):
            shutil.copy(sql, dst_ext / sql.name)


@pytest.fixture(scope="session")
def embedded_pg_with_fdw():
    if sys.platform not in ("darwin", "linux") or not _have_build_tools():
        pytest.skip("needs a C toolchain (cc/make/tar + curl/wget) to build the contrib FDWs")
    base = tempfile.mkdtemp(prefix="provisa_embedded_fdw_")
    server = pgserver.get_server(base)
    _install_fdws_into_pgserver()
    yield server


def _loopback_source_params(server) -> dict:
    """host/port/dbname/user for postgres_fdw to reach this same embedded PG (unix socket)."""
    u = urlparse(server.get_uri())
    q = parse_qs(u.query)
    return {
        "host": q["host"][0] if "host" in q else (u.hostname or "localhost"),
        "port": u.port or 5432,
        "database": (u.path.lstrip("/") or "postgres"),
        "username": u.username or "postgres",
    }


def _col(n: str, d: str = "varchar", nl: bool = True) -> ColumnMetadata:
    return ColumnMetadata(column_name=n, data_type=d, is_nullable=nl)


def _si(customers_schema: str, orders_schema: str) -> SchemaInput:
    return SchemaInput(
        tables=[
            {
                "id": 1,
                "source_id": "cust",
                "domain_id": "sales",
                "schema_name": customers_schema,
                "table_name": "customers",
                "columns": [
                    {"column_name": c, "visible_to": ["admin"]}
                    for c in ("id", "first_name", "state")
                ],
            },
            {
                "id": 2,
                "source_id": "ord",
                "domain_id": "sales",
                "schema_name": orders_schema,
                "table_name": "orders",
                "columns": [
                    {"column_name": c, "visible_to": ["admin"]}
                    for c in ("id", "customer_id", "amount")
                ],
            },
        ],  # fmt: skip
        relationships=[
            {
                "id": "ord2cust",
                "source_table_id": 2,
                "target_table_id": 1,
                "source_column": "customer_id",
                "target_column": "id",
                "cardinality": "many-to-one",
            }
        ],
        column_types={
            1: [_col("id", "integer", False), _col("first_name"), _col("state")],
            2: [
                _col("id", "integer", False),
                _col("customer_id", "integer"),
                _col("amount", "double"),
            ],
        },
        naming_rules=[],
        role=_ADMIN,
        domains=[{"id": "sales", "description": "Sales"}],
        source_types={"cust": "csv", "ord": "postgresql"},
    )


def _compile(si: SchemaInput, gql: str, rls: RLSContext) -> str:
    ctx = build_context(si)
    compiled = compile_query(parse_query(generate_schema(si), gql, {}), ctx)[0]
    gov = build_governance_context("admin", rls, {}, ctx, si.tables, role=_ADMIN)
    return transpile(
        rewrite_semantic_to_physical(apply_governance(compiled.sql, gov), ctx), "postgres"
    )


async def test_embedded_pg_fdw_connectors_federate(embedded_pg_with_fdw):
    """FileFdwConnector + PostgresFdwConnector DDL, executed verbatim on the embedded PG, federate."""
    server = embedded_pg_with_fdw
    conn = await asyncpg.connect(dsn=server.get_uri())
    try:
        # ---- loopback "remote" that postgres_fdw will attach: orders in demo_remote ----
        await conn.execute("CREATE SCHEMA IF NOT EXISTS demo_remote")
        await conn.execute("DROP TABLE IF EXISTS demo_remote.orders CASCADE")
        await conn.execute(
            "CREATE TABLE demo_remote.orders(id int, customer_id int, amount numeric)"
        )
        await conn.execute(
            "INSERT INTO demo_remote.orders VALUES "
            "(10,4,19.99),(11,2,49.99),(12,7,5.00),(13,9,7.50),(14,1,3.25)"
        )  # customers 4/7/9 are TX; 2 is CA, 1 is NY

        # ---- attach customers.csv via the REAL FileFdwConnector ----
        csv_src = SimpleNamespace(
            id="cust", path=str(_FILES / "customers.csv"), federation_hints={}
        )
        fdet = FileFdwConnector().details(csv_src)
        for ddl in fdet["server_ddl"]:
            await conn.execute(ddl)
        await conn.execute("CREATE SCHEMA IF NOT EXISTS e2e_files")
        await conn.execute("DROP FOREIGN TABLE IF EXISTS e2e_files.customers")
        await conn.execute(
            f"CREATE FOREIGN TABLE e2e_files.customers ({', '.join(_CSV_COLS)}) "
            f"SERVER {fdet['server']} {fdet['table_options']}"
        )

        # ---- attach demo_remote via the REAL PostgresFdwConnector (loopback to self) ----
        pg_src = SimpleNamespace(
            id="ord",
            schema="demo_remote",
            password="",
            federation_hints={"schema": "demo_remote"},
            **_loopback_source_params(server),
        )
        pdet = PostgresFdwConnector().details(pg_src)
        for ddl in pdet["attach_ddl"]:
            await conn.execute(ddl)
        orders_schema = pdet["local_schema"]  # fdw_ord

        # ---- REAL pipeline: orders (postgres_fdw) x customers (file_fdw), RLS TX-only ----
        rls = RLSContext(rules={1: "state = 'TX'"}, domain_rules={})
        sql = _compile(_si("e2e_files", orders_schema),
                       "{ orders { id amount customer { firstName state } } }", rls)  # fmt: skip
        rows = await conn.fetch(sql)

        # all 5 orders return; nested customer governed to TX-only, non-TX masked to null
        assert len(rows) == 5
        parsed = [json.loads(r["customer"]) if r["customer"] else None for r in rows]
        visible = [c for c in parsed if c]
        assert visible and all(c["state"] == "TX" for c in visible)  # RLS through the FDW join
        assert any(c is None for c in parsed)  # CA/NY customers masked
        assert {c["firstName"] for c in visible} == {"David", "Grace", "Iris"}  # TX ids 4,7,9
    finally:
        await conn.execute("DROP SCHEMA IF EXISTS e2e_files CASCADE")
        await conn.execute("DROP SERVER IF EXISTS fdw_file_srv CASCADE")
        await conn.execute("DROP SCHEMA IF EXISTS fdw_ord CASCADE")
        await conn.execute("DROP SERVER IF EXISTS fdw_ord CASCADE")
        await conn.execute("DROP SCHEMA IF EXISTS demo_remote CASCADE")
        await conn.close()
