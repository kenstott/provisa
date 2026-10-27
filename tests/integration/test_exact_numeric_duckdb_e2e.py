# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2167: an unsized PostgreSQL numeric is read exactly on the DuckDB route.

DuckDB's PostgreSQL reader takes an unsized numeric as a double: 12345678901234567890.123456 comes
back rounded. Registration records the column as numeric(38, s) with s read from its values, and
the DuckDB backend reads the table through postgres_query, the column cast at its source; the
value comes back as the DECIMAL(38, 6) PostgreSQL holds, exactly. Self-provisioned embedded
Postgres; no docker.
"""

from __future__ import annotations

import tempfile
from decimal import Decimal
from types import SimpleNamespace

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

pgserver = pytest.importorskip("pixeltable_pgserver")
psycopg2 = pytest.importorskip("psycopg2")

from provisa.federation.duckdb_runtime import DuckDBFederationRuntime  # noqa: E402
from provisa.federation.exact_numeric import WideTable, exact_numeric_reads  # noqa: E402

from tests.integration.test_duckdb_postgres_sorted_pages_e2e import _libpq  # noqa: E402

_VALUE = Decimal("12345678901234567890.123456")


@pytest.fixture(scope="session")
def pg():
    server = pgserver.get_server(tempfile.mkdtemp(prefix="provisa_pg_exact_numeric_"))
    uri = server.get_uri()
    conn = psycopg2.connect(uri)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS ledger")
        cur.execute("DROP TABLE IF EXISTS ledger.entries")
        cur.execute("CREATE TABLE ledger.entries(id int PRIMARY KEY, amount numeric)")
        cur.execute("INSERT INTO ledger.entries VALUES (1, %s)", (_VALUE,))
    yield SimpleNamespace(uri=uri, conn=conn)
    conn.close()


async def test_an_unsized_numeric_reads_back_exactly_on_duckdb(pg, tmp_path):
    from provisa.api.admin.introspect import _sized_numeric

    p = _libpq(pg.uri)

    class _Pool:
        async def execute(self, source_id, sql, params=None):
            with pg.conn.cursor() as cur:
                cur.execute(sql)
                return SimpleNamespace(rows=cur.fetchall())

    # Registration: the column's scale is read from its values.
    typing = await _sized_numeric(_Pool(), "pgsrc", "ledger", "entries", "amount")
    assert typing.data_type == "numeric(38,6)"

    src = SimpleNamespace(
        id="pgsrc",
        catalog="pgsrc",
        type=SimpleNamespace(value="postgresql"),
        host=p["host"],
        port=p["port"],
        database=p["database"],
        username=p["username"],
        password=None,
        schema_name="ledger",
        table_name="entries",
        path=None,
        max_live_concurrency=None,
    )
    rt = DuckDBFederationRuntime(materialize_dsn=f"duckdb:///{tmp_path / 'store.duckdb'}")
    rt.attach_source(src)
    catalog, schema, table = (part.strip('"') for part in rt._phys_name(src).split("."))
    sql = f'SELECT "e"."amount" FROM "{catalog}"."{schema}"."{table}" AS "e" WHERE "e"."id" = 1'

    # The attached scan alone: the value a double holds, not the one PostgreSQL holds.
    (scanned,) = (await rt.run(sql)).rows
    assert Decimal(str(scanned[0])) != _VALUE

    wide = {(catalog, schema, table): WideTable(("id", "amount"), {"amount": typing.data_type})}
    (exact,) = (await rt.run(exact_numeric_reads(sql, wide, rt.remote_table))).rows
    assert exact[0] == _VALUE
    (exact_sync,) = rt.run_sync(exact_numeric_reads(sql, wide, rt.remote_table)).rows
    assert exact_sync[0] == _VALUE
