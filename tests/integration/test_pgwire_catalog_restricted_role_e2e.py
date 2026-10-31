# Copyright (c) 2026 Kenneth Stott
# Canary: 3b9e1f47-6a02-4d58-8c13-f7a0d2e6c914
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A restricted role reading PostgreSQL's catalog over pgwire on a real server (REQ-2190): every
statement is addressed to the meta domain, governed as any read of it, and run on the control plane
as the client role. Nothing here is stubbed: it is the row scope, the column rules and the settings
of the real statement that are under test, against a role reaching all of it."""

# Requirements: REQ-2190

from __future__ import annotations

import asyncio
from pathlib import Path

import psycopg2
import pytest
import yaml

from sqlalchemy.engine import make_url

from tests.integration.isolated_server import IsolatedServer, drop_org_schema

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_ORG = "catalog_restricted_role"
_REPO_ROOT = Path(__file__).parents[2]
_CAPABILITIES = ["query_development", "full_results", "usage"]
_RESTRICTED = "catalog_restricted"
_EVERYTHING = "catalog_everything"
_CATALOG_READER = "viewer"  # REQ-2154: the catalog is read by every role built on Viewer
_ITS_DOMAIN = "sales-analytics"
_WITHHELD_COLUMN = ("customers", "email")


def _config(tmp_dir: Path) -> str:
    """sample_config.yaml with a role of one domain, one of whose columns it is not served, and a
    role reaching every domain and column."""
    with open(_REPO_ROOT / "tests" / "fixtures" / "sample_config.yaml") as f:
        cfg = yaml.safe_load(f)
    cfg["roles"] += [
        {
            "id": _RESTRICTED,
            "parents": [_CATALOG_READER],
            "description": "Test role.",
            "capabilities": list(_CAPABILITIES),
            "domain_access": [_ITS_DOMAIN],
        },
        {
            "id": _EVERYTHING,
            "parents": [_CATALOG_READER],
            "description": "Test role.",
            "capabilities": list(_CAPABILITIES),
            "domain_access": ["*"],
        },
    ]
    for table in cfg["tables"]:
        for column in table["columns"]:
            column["visible_to"] = [*column["visible_to"], _EVERYTHING]
            if (
                table["domain_id"] == _ITS_DOMAIN
                and (table["table"], column["name"]) != _WITHHELD_COLUMN
            ):
                column["visible_to"].append(_RESTRICTED)
    path = tmp_dir / "catalog-restricted-role.yaml"
    path.write_text(yaml.safe_dump(cfg, sort_keys=False))
    return str(path)


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    srv = IsolatedServer(
        _ORG,
        engine="duckdb",
        enable_pgwire=True,
        config=_config(tmp_path_factory.mktemp("catalog-restricted-role")),
        control_plane="own",
    )
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()
        asyncio.run(drop_org_schema(_ORG))


def _read(srv: IsolatedServer, role: str, sql: str) -> list[tuple]:
    conn = psycopg2.connect(
        host="127.0.0.1", port=srv.pgwire_port, dbname="provisa", user=role, password="provisa"
    )
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


_TABLES = "SELECT relname::text FROM pg_catalog.pg_class WHERE relkind = 'r'"


def _names(rows: list[tuple]) -> set[str]:
    return {row[0] for row in rows}


def test_a_role_lists_the_tables_it_reaches_and_no_other(server):
    listed = _names(_read(server, _RESTRICTED, _TABLES))
    assert {"orders", "customers"} <= listed
    assert "products" not in listed


def test_the_other_role_lists_what_this_one_does_not(server):
    assert "products" in _names(_read(server, _EVERYTHING, _TABLES))


def test_no_way_of_asking_the_catalog_names_a_table_out_of_reach(server):
    for sql in (
        "SELECT relname::text FROM pg_class WHERE relname = 'products'",
        "SELECT table_name::text FROM information_schema.tables WHERE table_name = 'products'",
        "SELECT column_name::text FROM information_schema.columns WHERE table_name = 'products'",
        "SELECT a.attname::text FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
        "WHERE c.relname = 'products'",
    ):
        assert _read(server, _RESTRICTED, sql) == [], sql


def test_a_listed_column_the_role_is_not_served_answers_false(server):
    columns = _names(
        _read(
            server,
            _RESTRICTED,
            "SELECT a.attname::text FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
            "WHERE c.relname = 'customers' AND a.attnum > 0",
        )
    )
    assert "email" in columns
    assert _read(
        server,
        _RESTRICTED,
        "SELECT has_column_privilege('sales_analytics.customers', 'email', 'SELECT'), "
        "has_column_privilege('sales_analytics.customers', 'name', 'SELECT')",
    ) == [(False, True)]


def test_a_column_the_role_is_not_served_is_refused_on_read(server):
    with pytest.raises(psycopg2.Error) as refused:
        _read(server, _RESTRICTED, "SELECT email FROM customers")
    assert refused.value.pgcode == "42501"


def test_every_system_row_is_every_roles(server):
    assert _read(server, _RESTRICTED, "SELECT typname::text FROM pg_type WHERE oid = 23") == [
        ("int4",)
    ]
    assert _read(
        server, _RESTRICTED, "SELECT relname::text FROM pg_class WHERE relname = 'pg_class'"
    ) == [("pg_class",)]


def test_a_clients_set_config_widens_no_scope(server):
    """What the views trust is the admin terminal's, in the statement's own transaction."""
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=server.pgwire_port,
        dbname="provisa",
        user=_RESTRICTED,
        password="provisa",
    )
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT set_config('provisa.catalog.scope', '{1,2,3,4,5,6}', false)")
        cur.fetchall()
        cur.execute(_TABLES)
        listed = _names(cur.fetchall())
    finally:
        conn.close()
    assert "products" not in listed


def test_a_catalog_view_is_not_read_with_a_table_of_a_source(server):
    with pytest.raises(psycopg2.Error) as refused:
        _read(
            server,
            _RESTRICTED,
            "SELECT c.relname FROM pg_catalog.pg_class c JOIN orders o ON o.id = c.oid",
        )
    assert refused.value.pgcode == "0A000"
    assert "catalog_pg_class" in str(refused.value)


#: The statements J312b found answered by the control plane itself, and their kin. Each reads no
#: table, or only a view of meta, and asks about the server it runs on.
_ASKS_THE_SERVER = (
    "SELECT inet_server_port(), NULL::box",
    "SELECT inet_server_port(), 'x'::name",
    "SELECT inet_server_port(), pg_get_userbyid(10)",
    "SELECT inet_server_port(), current_user",
    "SELECT inet_server_port(), relname::text FROM pg_class LIMIT 1",
    'SELECT "inet_server_port"(), relname::text FROM pg_catalog.pg_class LIMIT 1',
    "SELECT pg_catalog.inet_server_port(), relname::text FROM pg_class LIMIT 1",
    "SELECT current_query(), relname::text FROM pg_class LIMIT 1",
    "SELECT pg_sleep(0), relname::text FROM pg_class LIMIT 1",
)


def _plane_port(srv: IsolatedServer) -> int:
    assert srv.plane is not None
    return make_url(srv.plane.url).port


def _answer_or_refusal(srv: IsolatedServer, sql: str) -> str:
    try:
        return repr(_read(srv, _RESTRICTED, sql))
    except psycopg2.Error as refused:
        return f"{refused.pgcode}: {refused}"


@pytest.mark.parametrize("sql", _ASKS_THE_SERVER)
def test_a_statement_learns_nothing_of_the_control_plane(server, sql):
    answered = _answer_or_refusal(server, sql)
    assert str(_plane_port(server)) not in answered, answered
    assert "catalog_pg_class" not in answered, answered


def test_a_statement_with_no_table_runs_on_the_engine(server):
    """PostgreSQL's current_query() is the statement as it reached it: POINT is a type the plane has
    and the engine holds as text, so the engine's statement does not name it."""
    answered = _answer_or_refusal(server, "SELECT current_query(), NULL::point")
    assert "POINT" not in answered.upper(), answered


def test_a_function_only_the_plane_has_is_not_answered_for_a_statement_with_no_table(server):
    """The plane answered box(point, point) as '(1,1),(0,0)'; the engine has no such function."""
    sql = "SELECT pg_database_size(1::oid), box('(0,0)'::point, '(1,1)'::point)"
    with pytest.raises(psycopg2.Error) as refused:
        _read(server, _RESTRICTED, sql)
    assert "box" in str(refused.value)


def test_a_server_function_beside_a_view_is_refused_by_name(server):
    with pytest.raises(psycopg2.Error) as refused:
        _read(server, _RESTRICTED, "SELECT inet_server_port(), relname::text FROM pg_class")
    assert refused.value.pgcode == "0A000"
    assert "inet_server_port" in str(refused.value)


def test_privilege_and_visibility_functions_are_the_catalogs(server):
    assert _read(
        server,
        _RESTRICTED,
        "SELECT has_schema_privilege('public', 'usage'), pg_type_is_visible(23), "
        "pg_function_is_visible(1), pg_has_role('x', 'member')",
    ) == [(True, True, True, True)]
