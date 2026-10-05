# Copyright (c) 2026 Kenneth Stott
# Canary: d1457774-f31d-45c7-a442-ab598472a2f6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a row-level table of a SQL source is read through ANY filter (REQ-1915).

A real isolated server (DuckDB engine, which cannot attach MariaDB, so ``row_materialize`` is the
table's reach; SQLite control plane) over a real MariaDB, whose general log counts the statements
that reach it. The row-level copy serves only reads the engine federates (maintainer ruling
2026-10-05): a single-source read goes DIRECT through MariaDB's own driver, live and read once,
nothing fetched into the copy, an unfiltered one not refused. A read joining a small SQLite table
federates: with no filter it is refused at planning; a range filter is pushed to MariaDB as its own
WHERE, read through the driver's streaming cursor, and answered from the copy with exactly the
matching rows. The same filter within its TTL is served from the copy without reading the source
(a change made at the source since is not seen); a different filter is fetched fresh. A key read
keeps its own path.

MariaDB is a heavy-marked service, up for one test at a time, so the whole flow is one test."""

# Requirements: REQ-1915, REQ-1865, REQ-1190

from __future__ import annotations

import os

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration, pytest.mark.requires_mariadb]

_ROLE = "org_admin"
_ORG = "row_level_filter_sql_e2e"
_TABLE = "rl_filter_orders"
_SEED = [(1, "east", 50), (2, "west", 150), (3, "east", 250), (4, "west", 90)]
_REPO = os.path.join(os.path.dirname(__file__), "..", "..")


def _port() -> int:
    return int(os.environ["MARIADB_PORT"])


def _mariadb():
    import pymysql

    return pymysql.connect(
        host="localhost",
        port=_port(),
        user="root",
        password="provisa",
        database="provisa",
        autocommit=True,
    )


def _seed_sqlite(path) -> None:
    import sqlite3

    con = sqlite3.connect(path)
    try:
        con.execute("CREATE TABLE rl_regions (region TEXT PRIMARY KEY, label TEXT)")
        con.executemany(
            "INSERT INTO rl_regions VALUES (?, ?)",
            [("east", "East"), ("west", "West"), ("north", "North")],
        )
        con.commit()
    finally:
        con.close()


def _config(regions_db) -> dict:
    with open(os.path.join(_REPO, "tests/fixtures/sample_config.yaml")) as f:
        base = yaml.safe_load(f)
    column = {"visible_to": [_ROLE]}
    return {
        "naming": base["naming"],
        "roles": base["roles"],
        "relationships": [
            {
                "id": "orders-to-regions",
                "source_table_id": _TABLE,
                "target_table_id": "rl_regions",
                "source_column": "region",
                "target_column": "region",
                "cardinality": "many-to-one",
            }
        ],
        "sources": [
            {
                "id": "rl-mariadb",
                "type": "mariadb",
                "host": "localhost",
                "port": _port(),
                "database": "provisa",
                "username": "root",
                "password": "provisa",
            },
            {"id": "rl-sqlite", "type": "sqlite", "path": str(regions_db)},
        ],
        "domains": [{"id": "rowlevel", "description": "row-level filter e2e"}],
        "tables": [
            {
                "source_id": "rl-mariadb",
                "domain_id": "rowlevel",
                "schema": "provisa",
                "table": _TABLE,
                "row_materialize": True,
                "cache_ttl": 300,
                "columns": [
                    {"name": "order_id", "data_type": "integer", "is_primary_key": True, **column},
                    {"name": "region", "data_type": "varchar", **column},
                    {"name": "amount", "data_type": "integer", **column},
                ],
            },
            {
                "source_id": "rl-sqlite",
                "domain_id": "rowlevel",
                "schema": "default",
                "table": "rl_regions",
                "columns": [
                    {"name": "region", "data_type": "varchar", "is_primary_key": True, **column},
                    {"name": "label", "data_type": "varchar", **column},
                ],
            },
        ],
    }


def _sql(server, sql: str) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}/data/sql",
        json={"sql": sql},
        headers={"X-Provisa-Role": _ROLE},
        timeout=server.request_timeout + 30,
    )


def _rows(resp: httpx.Response) -> list[dict]:
    assert resp.status_code == 200, resp.text
    return resp.json()["data"]["sql"]


def _source_reads(seed) -> int:
    """Statements naming the table that reached MariaDB from any connection but the test's own
    (its general log, on for this test's own container)."""
    with seed.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM mysql.general_log WHERE thread_id <> CONNECTION_ID() "
            "AND command_type IN ('Query', 'Execute') AND argument LIKE %s",
            (f"%{_TABLE}%",),
        )
        return cur.fetchone()[0]


def test_a_row_level_sql_table_is_read_through_any_filter(tmp_path):
    from tests.integration.isolated_server import IsolatedServer

    seed = _mariadb()
    with seed.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {_TABLE}")
        cur.execute(
            f"CREATE TABLE {_TABLE} (order_id INT PRIMARY KEY, region VARCHAR(20), amount INT)"
        )
        cur.executemany(f"INSERT INTO {_TABLE} VALUES (%s, %s, %s)", _SEED)
        cur.execute("SET GLOBAL log_output = 'TABLE'")
        cur.execute("SET GLOBAL general_log = 'ON'")
    regions_db = tmp_path / "regions.sqlite"
    _seed_sqlite(regions_db)
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(_config(regions_db)))
    server = IsolatedServer(_ORG, engine="duckdb", config=str(path), control_plane="sqlite")
    server.start(timeout=240)

    def read(sql: str) -> tuple[httpx.Response, int]:
        """The response, and how many statements naming the table reached the source for it."""
        before = _source_reads(seed)
        resp = _sql(server, sql)
        return resp, _source_reads(seed) - before

    # The row-level table joined to the SQLite one: a federated read, which the engine serves
    # from the row-level copy.
    def joined(where: str) -> str:
        return (
            f"SELECT o.order_id, r.label FROM rowlevel.{_TABLE} o "
            f"JOIN rowlevel.rl_regions r ON r.region = o.region WHERE {where} "
            "ORDER BY o.order_id"
        )

    single_over_100 = (
        f"SELECT order_id, region FROM rowlevel.{_TABLE} WHERE amount > 100 ORDER BY order_id"
    )
    try:
        # DIRECT: a single-source read goes live through MariaDB's own driver -- read once, with
        # nothing fetched into the copy, and an unfiltered read is not refused.
        resp, reads = read(f"SELECT order_id FROM rowlevel.{_TABLE} ORDER BY order_id")
        assert _rows(resp) == [{"order_id": n} for n in (1, 2, 3, 4)]
        assert reads == 1
        resp, reads = read(single_over_100)
        assert _rows(resp) == [{"order_id": 2, "region": "west"}, {"order_id": 3, "region": "east"}]
        assert reads == 1

        # Federated, no filter: refused at planning, naming the table; the source is not read.
        resp, reads = read(
            f"SELECT o.order_id FROM rowlevel.{_TABLE} o "
            "JOIN rowlevel.rl_regions r ON r.region = o.region"
        )
        assert resp.status_code == 400, resp.text
        assert resp.json()["code"] == "data.row_level_filter_required"
        assert resp.json()["params"] == {"table": _TABLE}
        assert reads == 0

        # Federated, a range filter: pushed to the source once, answered with exactly its rows.
        over_100 = joined("o.amount > 100")
        resp, reads = read(over_100)
        assert _rows(resp) == [{"order_id": 2, "label": "West"}, {"order_id": 3, "label": "East"}]
        assert reads == 1

        # The same filter within its TTL is served from the copy, without reading the source: a
        # change made at the source since is not seen.
        with seed.cursor() as cur:
            cur.execute(f"UPDATE {_TABLE} SET region = 'north' WHERE order_id = 2")
        resp, reads = read(over_100)
        assert _rows(resp)[0] == {"order_id": 2, "label": "West"}
        assert reads == 0

        # A DIRECT read meanwhile is live: it sees the change, whatever the copy holds.
        resp, reads = read(single_over_100)
        assert _rows(resp)[0] == {"order_id": 2, "region": "north"}
        assert reads == 1

        # A different filter has no fetch of its own yet: read from the source, change included.
        resp, reads = read(joined("o.amount BETWEEN 100 AND 200"))
        assert _rows(resp) == [{"order_id": 2, "label": "North"}]
        assert reads == 1

        # A key read keeps its own path.
        resp, _reads = read(joined("o.order_id = 4"))
        assert _rows(resp) == [{"order_id": 4, "label": "West"}]
    except BaseException:
        print(server.dump_stderr_debug()[-20000:])  # the server's own account of the failure
        raise
    finally:
        server.stop_process()
        with seed.cursor() as cur:
            cur.execute("SET GLOBAL general_log = 'OFF'")
            cur.execute(f"DROP TABLE IF EXISTS {_TABLE}")
        seed.close()
