# Copyright (c) 2026 Kenneth Stott
# Canary: 3f0ff746-bda7-4c6d-8d77-51e7d6f9814a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a MERGE whose USING reads a code table replaces a PostgreSQL table's rows (REQ-2001,
amended 2026-11-14).

A real server (DuckDB engine, its own control plane) over two sources: a PostgreSQL 16 database
holding ``er.bridge`` and ``er.staged``, and a Python code source whose ``@table matches`` is the
matching. A MERGE INTO er.bridge USING the code table, WHEN NOT MATCHED BY SOURCE THEN DELETE
included, reads the code table first under the caller's governance and then runs on PostgreSQL
as one MERGE: matched rows updated, unmatched rows inserted, rows not in the source deleted, its
command tag counting all three. A registered statement command holding that MERGE runs it. A
MERGE reading only PostgreSQL is routed as before, and one that deletes the rows not in
its source carries that delete in its one statement on PostgreSQL 16 too, as does a statement
command whose MERGE reads a code query's rows. The code table's row rule and column
visibility hold on the read; a role without the delete grant may not merge with a delete.
"""

# Requirements: REQ-2001

from __future__ import annotations

import io
import json
import zipfile

import httpx
import psycopg
import pytest
import yaml

from tests.embedded_pg import DATABASE, PASSWORD, ROLE, start_local_postgres
from tests.helpers import config_write_grants

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_ADMIN = "org_admin"
_CLERK = "clerk"  # reads the code table under a row rule, its secret not served
_NO_DELETE = "nodelete"  # may insert and update the bridge, not delete from it
_ROLES = [_ADMIN, _CLERK, _NO_DELETE]
_SEED = [(1, "old"), (2, "globex"), (9, "stale")]
_CODE = {
    "er/er.py": '''
from dataclasses import dataclass


@dataclass
class Match:
    id: int
    org: str
    secret: str


def matches() -> list[Match]:
    """@table"""
    return [
        Match(1, "acme", "s1"),
        Match(2, "globex", "s2"),
        Match(3, "initech", "s3"),
        Match(4, "hidden", "s4"),
    ]


def fresh_matches() -> list[Match]:
    """@query"""
    return [Match(1, "acme", "s1"), Match(3, "initech", "s3")]
''',
}
_MERGE = (
    "MERGE INTO er.bridge AS t USING (SELECT m.id, m.org FROM er.matches AS m) AS s "
    "ON t.id = s.id "
    "WHEN MATCHED THEN UPDATE SET org = s.org "
    "WHEN NOT MATCHED THEN INSERT (id, org) VALUES (s.id, s.org) "
    "WHEN NOT MATCHED BY SOURCE THEN DELETE"
)

_SAME_SOURCE_DELETE = (
    "MERGE INTO er.bridge AS t USING er.staged AS s ON t.id = s.id "
    "WHEN MATCHED THEN UPDATE SET org = s.org "
    "WHEN NOT MATCHED THEN INSERT (id, org) VALUES (s.id, s.org) "
    "WHEN NOT MATCHED BY SOURCE THEN DELETE"
)
# The USING a code query: the command is run first and stands in the USING as its rows.
_FROM_QUERY = (
    "MERGE INTO er.bridge AS t USING (SELECT q.id, q.org FROM fresh_matches() AS q) AS s "
    "ON t.id = s.id "
    "WHEN MATCHED THEN UPDATE SET org = s.org "
    "WHEN NOT MATCHED THEN INSERT (id, org) VALUES (s.id, s.org) "
    "WHEN NOT MATCHED BY SOURCE THEN DELETE"
)


def _archive(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, text in files.items():
            z.writestr(name, text)
    return buf.getvalue()


def _pg_table(name: str) -> dict:
    return {
        "source_id": "er-pg",
        "domain_id": "er",
        "schema": "er",
        "table": name,
        "columns": [
            {"name": "id", "data_type": "bigint", "visible_to": _ROLES, "is_primary_key": True},
            {"name": "org", "data_type": "text", "visible_to": _ROLES},
        ],
        "write_grants": [
            *config_write_grants([_ADMIN, _CLERK], ["id", "org"]),
            *(
                {"role": _NO_DELETE, "operation": op, "columns": ["id", "org"]}
                for op in ("insert", "update")
            ),
        ],
    }


def _config(pg) -> dict:
    role = {"capabilities": ["query_development", "write", "command_execution"]}
    return {
        "federation_engine": "duckdb",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": r} for r in _ROLES],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "er", "description": "merge reads its source first"}],
        "roles": [
            {"id": r, "description": "Test role.", "domain_access": ["*"], **role}
            for r in (_CLERK, _NO_DELETE)
        ],
        "sources": [
            {
                "id": "er-pg",
                "type": "postgresql",
                "host": "127.0.0.1",
                "port": pg.port,
                "database": DATABASE,
                "username": ROLE,
                "password": PASSWORD,
            }
        ],
        "tables": [_pg_table("bridge"), _pg_table("staged")],
    }


@pytest.fixture(scope="module")
def source_pg():
    pg = start_local_postgres()
    try:
        with psycopg.connect(pg.dsn(), autocommit=True) as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS er")
            for name in ("bridge", "staged"):
                conn.execute(f"DROP TABLE IF EXISTS er.{name}")
                conn.execute(f"CREATE TABLE er.{name} (id bigint PRIMARY KEY, org text)")
            conn.execute("INSERT INTO er.staged VALUES (1, 'staged-acme'), (5, 'staged-new')")
        yield pg
    finally:
        pg.stop()


@pytest.fixture(scope="module")
def server(source_pg, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    work = tmp_path_factory.mktemp("merge_reads_source_first")
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(_config(source_pg)))
    srv = IsolatedServer(
        "merge_reads_source_first",
        engine="duckdb",
        config=str(path),
        control_plane="own",
        enable_pgwire=True,
        materialize_store_url=f"duckdb:///{work / 'store.duckdb'}",
        env={"PROVISA_RUNTIME_DEPS_CACHE": str(tmp_path_factory.mktemp("runtime-deps"))},
    )
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


def _admin(srv, mutation: str) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": f"mutation {{ {mutation} {{ success message }} }}"},
        headers={"X-Provisa-Role": _ADMIN},
        timeout=srv.request_timeout + 600,
    )
    assert response.status_code == 200, response.text
    assert "errors" not in response.json(), response.text
    (result,) = response.json()["data"].values()
    assert result["success"], result["message"]
    return result


def _column(name: str, data_type: str, visible: list[str]) -> str:
    roles = ", ".join(f'"{r}"' for r in visible)
    return f'{{name: "{name}", visibleTo: [{roles}], dataType: "{data_type}"}}'


@pytest.fixture(scope="module")
def code(server):
    """The code source, its matches table registered and released: its secret served to the
    administrator alone, and a row rule that keeps 'hidden' from the clerk. Then the statement
    command holding the MERGE."""
    upload = httpx.post(
        f"{server.base_url}/admin/orgs/{server.org_id}/code-uploads/archive",
        params={"filename": "er.zip"},
        content=_archive(_CODE),
        headers={"X-Provisa-Role": _ADMIN, "Content-Type": "application/octet-stream"},
        timeout=server.request_timeout + 30,
    )
    assert upload.status_code == 200, upload.text
    mapping = {
        "runtime": "python",
        "code": {"kind": "upload", "upload": upload.json()["content_hash"]},
    }
    _admin(
        server,
        f'createSource(input: {{id: "er-code", type: "python", host: "", port: 0, '
        f'database: "", mappingJson: {json.dumps(json.dumps(mapping))}}})',
    )
    columns = [
        _column("id", "bigint", _ROLES),
        _column("org", "text", _ROLES),
        _column("secret", "text", [_ADMIN]),
    ]
    _admin(
        server,
        'registerTable(input: {sourceId: "er-code", domainId: "er", schemaName: "default", '
        f'tableName: "matches", cacheTtl: 300, columns: [{", ".join(columns)}]}})',
    )
    _admin(
        server,
        f'upsertRlsRule(input: {{tableId: "matches", roleId: "{_CLERK}", '
        'filterExpr: "org <> \'hidden\'", description: "The clerk does not see it."})',
    )
    commands = [
        {"name": "fresh_matches", "sourceId": "er-code", "functionName": "fresh_matches"},
        {"name": "rerun_bridge", "implKind": "statement", "binding": {"sql": _MERGE}},
        {"name": "rerun_from_query", "implKind": "statement", "binding": {"sql": _FROM_QUERY}},
    ]
    for body in commands:
        command = httpx.post(
            f"{server.base_url}/admin/actions/functions",
            json={**body, "domainId": "er", "visibleTo": [_ADMIN]},
            headers={"X-Provisa-Role": _ADMIN},
            timeout=server.request_timeout + 120,
        )
        assert command.status_code == 200, command.text


@pytest.fixture
def bridge(source_pg):
    """er.bridge seeded with rows the source updates (1), keeps (2) and no longer has (9); the
    rows at the source after the test, read on PostgreSQL itself."""
    with psycopg.connect(source_pg.dsn(), autocommit=True) as conn:
        conn.execute("DELETE FROM er.bridge")
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO er.bridge VALUES (%s, %s)", _SEED)

    def rows() -> list[tuple]:
        with psycopg.connect(source_pg.dsn(), autocommit=True) as conn:
            return conn.execute("SELECT id, org FROM er.bridge ORDER BY id").fetchall()

    return rows


def _pgwire(srv, role: str):
    return psycopg.connect(
        host="127.0.0.1",
        port=srv.pgwire_port,
        user=role,
        password="provisa",
        dbname="provisa",
        autocommit=True,
        connect_timeout=30,
    )


def _debug(srv) -> str:
    return srv.dump_stderr_debug()[-8000:]


@pytest.mark.usefixtures("code")
def test_a_merge_from_a_code_table_replaces_the_rows_in_one_statement(server, bridge):
    try:
        with _pgwire(server, _ADMIN) as conn:
            tag = conn.execute(_MERGE).rowcount
    except psycopg.Error as exc:
        pytest.fail(f"{exc}\n{_debug(server)}")
    assert bridge() == [(1, "acme"), (2, "globex"), (3, "initech"), (4, "hidden")]
    # One MERGE on PostgreSQL: two updated, two inserted and one deleted, in one command tag.
    assert tag == 5


@pytest.mark.usefixtures("code")
def test_a_statement_command_holding_the_merge_runs_it(server, bridge):
    try:
        with _pgwire(server, _ADMIN) as conn:
            conn.execute("SELECT * FROM rerun_bridge()").fetchall()
    except psycopg.Error as exc:
        pytest.fail(f"{exc}\n{_debug(server)}")
    assert bridge() == [(1, "acme"), (2, "globex"), (3, "initech"), (4, "hidden")]


@pytest.mark.usefixtures("code")
def test_the_code_tables_row_rule_holds_on_the_read(server, bridge):
    with _pgwire(server, _CLERK) as conn:
        conn.execute(_MERGE)
    # 'hidden' is no row the clerk reads: not merged in.
    assert bridge() == [(1, "acme"), (2, "globex"), (3, "initech")]


@pytest.mark.usefixtures("code")
def test_a_column_the_role_is_not_served_is_refused_and_nothing_is_written(server, bridge):
    with _pgwire(server, _CLERK) as conn, pytest.raises(psycopg.Error, match="secret"):
        conn.execute(_MERGE.replace("SELECT m.id, m.org", "SELECT m.id, m.secret AS org"))
    assert bridge() == _SEED


@pytest.mark.usefixtures("code")
def test_a_role_without_the_delete_grant_may_not_merge_with_a_delete(server, bridge):
    with _pgwire(server, _NO_DELETE) as conn, pytest.raises(psycopg.Error, match="delete grant"):
        conn.execute(_MERGE)
    assert bridge() == _SEED


@pytest.mark.usefixtures("code")
def test_a_merge_reading_only_its_targets_source_is_routed_as_before(server, bridge):
    with _pgwire(server, _ADMIN) as conn:
        tag = conn.execute(
            "MERGE INTO er.bridge AS t USING er.staged AS s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET org = s.org "
            "WHEN NOT MATCHED THEN INSERT (id, org) VALUES (s.id, s.org)"
        ).rowcount
    assert tag == 2
    assert bridge() == [(1, "staged-acme"), (2, "globex"), (5, "staged-new"), (9, "stale")]


@pytest.mark.usefixtures("code")
def test_a_same_source_merge_deletes_the_rows_not_in_the_source_in_one_statement(server, bridge):
    try:
        with _pgwire(server, _ADMIN) as conn:
            tag = conn.execute(_SAME_SOURCE_DELETE).rowcount
    except psycopg.Error as exc:
        pytest.fail(f"{exc}\n{_debug(server)}")
    assert bridge() == [(1, "staged-acme"), (5, "staged-new")]
    # One MERGE on PostgreSQL 16: one updated, one inserted and two deleted, in one command tag.
    assert tag == 4


@pytest.mark.usefixtures("code")
def test_a_statement_command_merging_a_code_querys_rows_deletes_in_one_statement(server, bridge):
    try:
        with _pgwire(server, _ADMIN) as conn:
            conn.execute("SELECT * FROM rerun_from_query()").fetchall()
    except psycopg.Error as exc:
        pytest.fail(f"{exc}\n{_debug(server)}")
    assert bridge() == [(1, "acme"), (3, "initech")]
