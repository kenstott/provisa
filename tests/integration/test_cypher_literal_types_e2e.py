# Copyright (c) 2026 Kenneth Stott
# Canary: 1d6f9b28-7c4a-4e31-b5d0-9a2e8c7f3b16
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a Cypher filter compares a column to a value of the value's own type (issue #130).

The same table — an integer key, a double, a boolean, a string and a nullable string — is reached
on both routes a single-source statement can take, through a real isolated server (DuckDB engine,
SQLite control plane):

* **DIRECT** — the table lives in the stack's Postgres, so the statement runs on the source's own
  driver. Postgres has no ``integer = text`` operator: a literal or parameter the translator had
  cast to TEXT is refused here.
* **ENGINE** — the table lives in a SQLite file the DuckDB engine attaches, so the statement runs
  in DuckDB.

Every literal kind and every bound parameter must select the same rows on both.
"""

# Requirements: REQ-345, REQ-352

from __future__ import annotations

import os
import sqlite3
import tempfile
from pathlib import Path

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration]

pytest.importorskip("duckdb")

_ROLE = "org_admin"
_TABLE = "cypher_literal_types"
_LABEL = "CypherLiteralTypes"
_ROWS = [(7, 10.5, True, "seven", None), (8, 2.25, False, "eight", "n")]
_COLUMNS = [
    ("id", "integer"),
    ("score", "double"),
    ("active", "boolean"),
    ("name", "varchar"),
    ("note", "varchar"),
]
_CAPABILITIES = ["source_registration", "table_registration", "query_development", "full_results"]


def _config(source: dict, schema: str) -> dict:
    return {
        "federation_engine": "duckdb",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "lit", "description": "Literal types"}],
        "roles": [{"id": _ROLE, "capabilities": _CAPABILITIES, "domain_access": ["*"]}],
        "sources": [source],
        "tables": [
            {
                "source_id": source["id"],
                "table": _TABLE,
                "schema": schema,
                "domain_id": "lit",
                "columns": [
                    {"name": name, "data_type": data_type, "visible_to": [_ROLE]}
                    for name, data_type in _COLUMNS
                ],
            }
        ],
    }


def _postgres_source() -> dict:
    """The table in the stack's Postgres; returns the source that reaches it."""
    import psycopg

    conn_args = {
        "host": os.environ["PG_HOST"],
        "port": int(os.environ["PG_PORT"]),
        "dbname": os.environ["PG_DATABASE"],
        "user": os.environ["PG_USER"],
        "password": os.environ["PG_PASSWORD"],
    }
    with psycopg.connect(**conn_args, autocommit=True) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {_TABLE}")
        conn.execute(
            f"CREATE TABLE {_TABLE} (id INTEGER PRIMARY KEY, score DOUBLE PRECISION, "
            "active BOOLEAN, name TEXT, note TEXT)"
        )
        with conn.cursor() as cur:
            cur.executemany(f"INSERT INTO {_TABLE} VALUES (%s, %s, %s, %s, %s)", _ROWS)
    return {
        "id": "lit-pg",
        "type": "postgresql",
        "host": conn_args["host"],
        "port": conn_args["port"],
        "database": conn_args["dbname"],
        "username": conn_args["user"],
        "password": conn_args["password"],
    }


def _drop_postgres_table() -> None:
    import psycopg

    with psycopg.connect(
        host=os.environ["PG_HOST"],
        port=int(os.environ["PG_PORT"]),
        dbname=os.environ["PG_DATABASE"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
        autocommit=True,
    ) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {_TABLE}")


def _sqlite_source(directory: Path) -> dict:
    """The table in a SQLite file; returns the source that reaches it."""
    path = directory / "literal_types.sqlite"
    conn = sqlite3.connect(path)
    conn.execute(
        f"CREATE TABLE {_TABLE} (id INTEGER PRIMARY KEY, score DOUBLE, active BOOLEAN, "
        "name TEXT, note TEXT)"
    )
    conn.executemany(f"INSERT INTO {_TABLE} VALUES (?, ?, ?, ?, ?)", _ROWS)
    conn.commit()
    conn.close()
    return {"id": "lit-sqlite", "type": "sqlite", "path": str(path)}


@pytest.fixture(scope="module", params=["direct-postgres", "engine-duckdb"])
def server(request):
    from tests.integration.isolated_server import IsolatedServer

    work = tempfile.TemporaryDirectory()
    directory = Path(work.name)
    on_postgres = request.param == "direct-postgres"
    if on_postgres:
        config = _config(_postgres_source(), "public")
    else:
        config = _config(_sqlite_source(directory), "default")
    config_path = directory / "config.yaml"
    config_path.write_text(yaml.safe_dump(config))
    srv = IsolatedServer(
        f"cypher_literal_{request.param.replace('-', '_')}",
        engine="duckdb",
        config=str(config_path),
        control_plane="sqlite",
        materialize_store_url=f"duckdb:///{directory / 'materialize.duckdb'}",
    )
    try:
        srv.start()
        yield srv
    finally:
        srv.stop_process()
        if on_postgres:
            _drop_postgres_table()
        work.cleanup()


def _names(server, predicate: str, params: dict | None = None) -> list[str]:
    response = httpx.post(
        f"{server.base_url}/data/cypher",
        json={
            "query": f"MATCH (t:{_LABEL}) WHERE {predicate} RETURN t.name AS name ORDER BY name",
            "params": params or {},
        },
        headers={"X-Provisa-Role": _ROLE},
        timeout=server.request_timeout + 10,
    )
    assert response.status_code == 200, response.text
    return [row["name"] for row in response.json()["rows"]]


@pytest.mark.parametrize(
    "predicate, expected",
    [
        ("t.id = 7", ["seven"]),
        ("7 = t.id", ["seven"]),
        ("t.id <> 7", ["eight"]),
        ("t.id = 9", []),
        ("t.score = 10.5", ["seven"]),
        ("t.score > 2.5", ["seven"]),
        ("t.active = true", ["seven"]),
        ("t.active = false", ["eight"]),
        ("t.name = 'eight'", ["eight"]),
        ("t.note IS NULL", ["seven"]),
        ("t.note IS NOT NULL", ["eight"]),
        ("t.note = null", []),  # a comparison with null is never true
        ("t.id = null", []),
    ],
)
def test_a_literal_filter_selects_by_the_columns_own_type(server, predicate, expected):
    assert _names(server, predicate) == expected


@pytest.mark.parametrize(
    "predicate, value, expected",
    [
        ("t.id = $v", 7, ["seven"]),
        ("$v = t.id", 8, ["eight"]),
        ("t.id <> $v", 7, ["eight"]),
        ("t.score = $v", 10.5, ["seven"]),
        ("t.active = $v", True, ["seven"]),
        ("t.active = $v", False, ["eight"]),
        ("t.name = $v", "eight", ["eight"]),
        ("t.id = $v", None, []),
        ("t.note = $v", None, []),
    ],
)
def test_a_bound_parameter_selects_by_the_columns_own_type(server, predicate, value, expected):
    assert _names(server, predicate, {"v": value}) == expected
