# Copyright (c) 2026 Kenneth Stott
# Canary: 8f31f5bf-40bc-4a13-bced-7175c583432a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a session's own temporary tables (REQ-615, REQ-1926, REQ-1942), through the one
pipeline on a real server -- created, written, read beside a governed table, dropped, ended with
the session, and no mutation -- while every other definition stays refused."""

# Requirements: REQ-615, REQ-1926, REQ-1942

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]


@pytest.fixture(scope="module")
def boot():
    server = WorkerBoot(
        1,
        pg_host=os.environ.get("PG_HOST", "localhost"),
        pg_port=int(os.environ.get("PG_PORT", "5432")),
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    server.create_database()
    try:
        engine = sa.create_engine(server.url, isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            # The harness's own orders table holds (1, east) and (2, west).
            conn.execute(sa.text("INSERT INTO public.orders VALUES (3, 'north')"))
        engine.dispose()
        server.start()
        server.wait_all_ready(timeout=300)
        yield server
    finally:
        server.cleanup()


def _sql(boot, sql: str, env: str | None = None) -> tuple[int, Any]:
    headers = {"Content-Type": "application/json", "x-provisa-role": "org_admin"}
    if env is not None:
        headers["x-provisa-env"] = env
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}/data/sql",
        data=json.dumps({"sql": sql}).encode(),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=600) as resp:
            return resp.status, json.loads(resp.read().decode())["data"]["sql"]
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()


def test_a_session_creates_writes_and_reads_its_temporary_table(boot):
    status, rows = _sql(
        boot,
        "CREATE TEMP TABLE t AS SELECT id, region FROM sales.orders;"
        "INSERT INTO t VALUES (100, 'mars');"
        "UPDATE t SET region = 'venus' WHERE id = 1;"
        "DELETE FROM t WHERE id = 2;"
        "SELECT t.id, t.region FROM t ORDER BY t.id",
    )
    assert status == 200, rows
    assert [(r["id"], r["region"]) for r in rows] == [(1, "venus"), (3, "north"), (100, "mars")]


def test_a_temporary_table_is_read_beside_a_governed_table(boot):
    status, rows = _sql(
        boot,
        "CREATE TEMPORARY TABLE k (id INTEGER, label TEXT);"
        "INSERT INTO k VALUES (1, 'a'), (3, 'c');"
        "SELECT o.region, k.label FROM sales.orders o JOIN k ON k.id = o.id ORDER BY o.id",
    )
    assert status == 200, rows
    assert [(r["region"], r["label"]) for r in rows] == [("east", "a"), ("north", "c")]


def test_a_temporary_table_ends_with_its_session_and_no_other_sees_it(boot):
    status, rows = _sql(boot, "CREATE TEMP TABLE gone (id INTEGER); SELECT COUNT(*) AS n FROM gone")
    assert status == 200 and rows == [{"n": 0}], rows
    status, body = _sql(boot, "SELECT * FROM gone")
    assert status != 200 and "'gone' is not a registered table" in str(body), body
    status, body = _sql(boot, "CREATE TEMP TABLE d (id INTEGER); DROP TABLE d; SELECT * FROM d")
    assert status != 200 and "'d' is not a registered table" in str(body), body


@pytest.mark.parametrize(
    ("sql", "kind"),
    [
        ("CREATE TABLE made (id INT)", "CREATE TABLE"),
        ("CREATE TABLE made AS SELECT id FROM sales.orders", "CREATE TABLE"),
        ("DROP TABLE orders", "DROP TABLE"),
        ("CREATE TEMP VIEW v AS SELECT 1", "CREATE VIEW"),
    ],
)
def test_every_other_definition_stays_refused(boot, sql, kind):
    status, body = _sql(boot, sql)
    assert status != 200 and f"{kind} is not available here" in str(body), body


def test_a_write_into_a_temporary_table_is_no_mutation(boot):
    """An environment whose mutation handling is Refused refuses every mutation; a temporary
    table's rows never reach a source or the environment's data, so writing one is none."""
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}/admin/orgs/{boot.org_id}/environments",
        data=json.dumps({"name": "ro", "data_mode": "inherit"}).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": "org_admin"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=600) as resp:
        assert resp.status == 200
    status, body = _sql(boot, "INSERT INTO sales.orders VALUES (9, 'x')", env="ro")
    assert status != 200 and "ro" in str(body), body
    status, rows = _sql(
        boot,
        "CREATE TEMP TABLE w AS SELECT id FROM sales.orders;"
        "INSERT INTO w VALUES (9);"
        "SELECT COUNT(*) AS n FROM w",
        env="ro",
    )
    assert status == 200 and rows == [{"n": 4}], rows


def test_a_decimal_keeps_its_precision_and_an_unsupported_type_is_refused(boot):
    status, rows = _sql(
        boot,
        "CREATE TEMP TABLE prices (id INTEGER, price DECIMAL(12, 4));"
        "INSERT INTO prices VALUES (1, 1.2345), (2, 100000.0001);"
        "SELECT CAST(SUM(price) AS VARCHAR) AS total FROM prices",
    )
    assert status == 200 and rows == [{"total": "100001.2346"}], rows
    status, body = _sql(boot, "CREATE TEMP TABLE docs (id INTEGER, doc JSONB)")
    assert status != 200 and "column 'doc': type JSONB is not supported" in str(body), body


def _pgwire(boot):
    import psycopg

    return psycopg.connect(
        host="127.0.0.1",
        port=boot.ports["pgwire"],
        user="org_admin",
        password="provisa",
        dbname="provisa",
        autocommit=True,
        connect_timeout=30,
    )


def test_on_pgwire_the_session_is_the_connection(boot):
    """A connection's temporary table lives across its statements, no other connection sees it,
    and it ends with the connection."""
    with _pgwire(boot) as first, _pgwire(boot) as second:
        first.execute("CREATE TEMP TABLE mine AS SELECT id, region FROM sales.orders")
        first.execute("INSERT INTO mine VALUES (50, 'mars')")
        first.execute("DELETE FROM mine WHERE id = 1")
        rows = first.execute("SELECT mine.id, mine.region FROM mine ORDER BY mine.id").fetchall()
        assert rows == [(2, "west"), (3, "north"), (50, "mars")]
        joined = first.execute(
            "SELECT o.region FROM sales.orders o JOIN mine ON mine.id = o.id ORDER BY o.id"
        ).fetchall()
        assert joined == [("west",), ("north",)]
        # Another connection is another session: the name means nothing there, and it may
        # hold a table of the same name with rows of its own.
        with pytest.raises(Exception, match="'mine' is not a registered table"):
            second.execute("SELECT * FROM mine")
        second.execute("CREATE TEMP TABLE mine (id INTEGER)")
        assert second.execute("SELECT COUNT(*) FROM mine").fetchone() == (0,)
        assert first.execute("SELECT COUNT(*) FROM mine").fetchone() == (3,)
        first.execute("DROP TABLE mine")
        with pytest.raises(Exception, match="'mine' is not a registered table"):
            first.execute("SELECT * FROM mine")
        # Every other definition is refused on the connection as before.
        with pytest.raises(Exception, match="CREATE TABLE is not available here"):
            first.execute("CREATE TABLE made (id INT)")
    # A new connection starts with none.
    with _pgwire(boot) as third:
        with pytest.raises(Exception, match="'mine' is not a registered table"):
            third.execute("SELECT * FROM mine")
