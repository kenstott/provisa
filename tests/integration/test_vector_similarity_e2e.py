# Copyright (c) 2026 Kenneth Stott
# Canary: 1cbcc820-fa23-4360-a854-4d8b95217a38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: ``cosine_similarity`` through the one pipeline, on a pgvector PostgreSQL (REQ-422/423/426).

Everything here is the ``test`` instance: a pgvector PostgreSQL this module starts on a leased
port and removes, and an isolated DuckDB server. Tables:

* ``docs``: an embedding column of pgvector's ``vector`` type -- read on the source, where the
  extension is detected (REQ-422) and the search is written with pgvector's own function;
* ``notes``: an embedding column of ``real[]`` -- read on the source with plain SQL, and through
  the engine (``route=federated``), which computes it itself;

and an analyst role that is not granted ``notes.vec``.
"""

# Requirements: REQ-422, REQ-423, REQ-426, REQ-429

from __future__ import annotations

import os
import subprocess
import time

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration]

_ROLE = "org_admin"
_ANALYST = "analyst"
_IMAGE = "pgvector/pgvector:pg16"
_ROWS = [(1, "[1, 0, 0]"), (2, "[0, 1, 0]"), (3, "[1, 1, 0]")]


class _PgVector:
    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.port,) = lease_ports(1)
        self._name = f"provisa-itest-pgvector-{os.getpid()}"

    def url(self) -> str:
        return f"postgresql://provisa:provisa@127.0.0.1:{self.port}/shop"

    def start(self) -> None:
        import psycopg

        subprocess.run(
            ["docker", "run", "-d", "--rm", "--name", self._name]
            + ["-e", "POSTGRES_USER=provisa", "-e", "POSTGRES_PASSWORD=provisa"]
            + ["-e", "POSTGRES_DB=shop", "-p", f"127.0.0.1:{self.port}:5432", _IMAGE],
            check=True,
            capture_output=True,
        )
        # The image restarts the server once after initdb: ready means it answers repeatedly.
        deadline, answered = time.monotonic() + 120, 0
        while answered < 3:
            try:
                psycopg.connect(self.url(), connect_timeout=2).close()
                answered += 1
            except psycopg.OperationalError:
                answered = 0
                if time.monotonic() > deadline:
                    raise
            time.sleep(1)
        with psycopg.connect(self.url(), autocommit=True) as conn:
            conn.execute("CREATE EXTENSION vector")
            conn.execute("CREATE TABLE docs (id INT PRIMARY KEY, title TEXT, vec vector(3))")
            conn.execute("CREATE TABLE notes (id INT PRIMARY KEY, vec real[])")
            for row_id, vec in _ROWS:
                conn.execute("INSERT INTO docs VALUES (%s, %s, %s)", (row_id, f"d{row_id}", vec))
                conn.execute(
                    "INSERT INTO notes VALUES (%s, %s::real[])",
                    (row_id, vec.replace("[", "{").replace("]", "}")),
                )

    def stop(self) -> None:
        subprocess.run(["docker", "rm", "-f", self._name], capture_output=True)


@pytest.fixture(scope="module")
def pgvector():
    db = _PgVector()
    try:
        db.start()
        yield db
    finally:
        db.stop()


def _col(name: str, data_type: str, roles: list[str], **kw) -> dict:
    return {"name": name, "data_type": data_type, "visible_to": roles, **kw}


def _embedding(name: str, roles: list[str]) -> dict:
    return _col(name, "array(float)", roles, embedding=True, embedding_model="mini")


@pytest.fixture(scope="module")
def server(pgvector, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    both = [_ROLE, _ANALYST]
    cfg = {
        "federation_engine": "duckdb",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "shop", "description": "vector similarity e2e"}],
        "roles": [
            {
                "id": _ANALYST,
                "description": "Test role.",
                "capabilities": ["query_development"],
                "domain_access": ["shop"],
            }
        ],
        # The model is only read for its dimensions here: every query is a vector.
        "vector_models": [
            {"id": "mini", "provider": "openai", "dimensions": 3, "base_url": "http://127.0.0.1:9"}
        ],
        "sources": [
            {
                "id": "pgv",
                "type": "postgresql",
                "host": "127.0.0.1",
                "port": pgvector.port,
                "database": "shop",
                "username": "provisa",
                "password": "provisa",
            }
        ],
        "tables": [
            {
                "source_id": "pgv",
                "domain_id": "shop",
                "schema": "public",
                "table": "docs",
                "columns": [
                    _col("id", "integer", both),
                    _col("title", "varchar", both),
                    _embedding("vec", both),
                ],
            },
            {
                "source_id": "pgv",
                "domain_id": "shop",
                "schema": "public",
                "table": "notes",
                "columns": [_col("id", "integer", both), _embedding("vec", [_ROLE])],
            },
        ],
        # The join the tests make is a registered relationship: a join outside the registered
        # ones is refused to a role that may not ignore them (REQ-603).
        "relationships": [
            {
                "id": "doc-note",
                "source_table_id": "docs",
                "target_table_id": "notes",
                "source_column": "id",
                "target_column": "id",
                "cardinality": "one-to-one",
            }
        ],
    }
    work = tmp_path_factory.mktemp("vector_similarity_e2e")
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(cfg))
    srv = IsolatedServer(
        "vector_similarity",
        engine="duckdb",
        config=str(path),
        control_plane="own",
        materialize_store_url=f"duckdb:///{work / 'store.duckdb'}",
    )
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


def _sql(server, sql: str, role: str = _ROLE, params: list | None = None) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}/data/sql",
        json={"sql": sql, **({"params": params} if params is not None else {})},
        headers={"X-Provisa-Role": role},
        timeout=server.request_timeout + 30,
    )


def _rows(resp: httpx.Response) -> list[dict]:
    assert resp.status_code == 200, resp.text
    return resp.json()["data"]["sql"]


_TOP2 = "SELECT id, cosine_similarity(vec, '[1, 0, 0]') AS s FROM {table} ORDER BY s DESC LIMIT 2"


def test_a_pgvector_source_ranks_by_similarity(server):
    rows = _rows(_sql(server, _TOP2.format(table="docs")))
    assert [r["id"] for r in rows] == [1, 3]
    assert rows[0]["s"] == pytest.approx(1.0)
    assert rows[1]["s"] == pytest.approx(0.7071, abs=1e-4)


def test_a_plain_array_column_ranks_by_similarity_on_the_source(server):
    rows = _rows(_sql(server, _TOP2.format(table="notes")))
    assert [r["id"] for r in rows] == [1, 3]


def test_the_engine_computes_it_when_the_statement_is_federated(server):
    sql = "-- @provisa route=federated\n" + _TOP2.format(table="notes")
    rows = _rows(_sql(server, sql))
    assert [r["id"] for r in rows] == [1, 3]
    assert rows[1]["s"] == pytest.approx(0.7071, abs=1e-4)


def test_a_bound_query_vector_is_bound_as_a_vector(server):  # REQ-429
    sql = "SELECT id, cosine_similarity(vec, $1) AS s FROM docs ORDER BY s DESC LIMIT 1"
    assert [r["id"] for r in _rows(_sql(server, sql, params=[[0, 1, 0]]))] == [2]


def test_two_embedding_columns_of_one_model_are_compared(server):
    rows = _rows(
        _sql(
            server,
            "SELECT d.id, cosine_similarity(d.vec, n.vec) AS s FROM docs d "
            "JOIN notes n ON n.id = d.id ORDER BY d.id",
        )
    )
    assert [round(r["s"], 4) for r in rows] == [1.0, 1.0, 1.0]


def test_a_role_not_served_the_column_may_not_search_it(server):  # REQ-426
    # The column is not served to the role: the statement is refused for naming it, before
    # its search is governed.
    resp = _sql(server, _TOP2.format(table="notes"), role=_ANALYST)
    assert resp.status_code != 200
    assert "Column 'vec' is not visible to this role" in resp.text


def test_a_column_that_is_no_embedding_is_refused(server):
    resp = _sql(server, "SELECT cosine_similarity(title, '[1, 0, 0]') FROM docs")
    assert resp.status_code == 400, resp.text
    assert "is not an embedding column" in resp.text


def test_a_vector_of_other_dimensions_is_refused(server):  # REQ-429
    resp = _sql(server, "SELECT cosine_similarity(vec, '[1, 0]') FROM docs")
    assert resp.status_code == 400, resp.text
    assert "2 dimensions" in resp.text
