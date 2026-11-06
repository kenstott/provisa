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

# Requirements: REQ-2190, REQ-1132

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
from pathlib import Path

import psycopg2
import pytest
import yaml

from sqlalchemy.engine import make_url

from provisa.core.environments import org_schema
from provisa.api._meta_views import _OPS_LOG_TABLE_ALIAS
from provisa.api._ontology_views import ONTOLOGY_TABLE_ALIAS
from provisa.api.app_loaders import _META_TABLE_ALIAS
from provisa.transpiler.metadata_settings import CLIENT_ROLE
from tests.integration.isolated_server import IsolatedServer, drop_org_schema

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_ORG = "catalog_restricted_role"
_REPO_ROOT = Path(__file__).parents[2]
_CAPABILITIES = [
    "catalog_read",
    "data_product_read",
    "glossary_read",
    "query_development",
    "usage",
    "full_results",
]
_RESTRICTED = "catalog_restricted"
_NO_META = "catalog_no_meta"
_EVERYTHING = "catalog_everything"
_CATALOG_READER = "developer"  # REQ-2155: a parent holding what the role is given
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
        {
            # REQ-1132: no catalog_read: the catalog is still its.
            "id": _NO_META,
            "parents": [],
            "description": "Test role.",
            "capabilities": ["usage", "query_development"],
            "domain_access": [_ITS_DOMAIN],
        },
    ]
    for table in cfg["tables"]:
        for column in table["columns"]:
            column["visible_to"] = [*column["visible_to"], _EVERYTHING]
            if (
                table["domain_id"] == _ITS_DOMAIN
                and (table["table"], column["name"]) != _WITHHELD_COLUMN
            ):
                column["visible_to"] += [_RESTRICTED, _NO_META]
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
    """current_query() is the statement the client wrote (pgwire.server._current_query_said,
    REQ-2171), and the NULL of a type only the plane has comes back from the engine as a NULL."""
    sql = "SELECT current_query(), NULL::point"
    assert _answer_or_refusal(server, sql) == repr([(sql, None)])


def test_a_function_only_the_plane_has_is_not_answered_for_a_statement_with_no_table(server):
    """The plane would answer pg_notify('c', 'p'); no engine has a form of it (REQ-2167,
    pg_forms_enum.NOT_SUPPORTED), so the statement is refused by name. box(point, point) was this
    test's function until the geometric forms (J455) gave every engine one."""
    sql = "SELECT pg_database_size(1::oid), pg_notify('c', 'p')"
    with pytest.raises(psycopg2.Error) as refused:
        _read(server, _RESTRICTED, sql)
    assert "pg_notify" in str(refused.value)


def test_a_server_function_beside_a_view_is_refused_by_name(server):
    with pytest.raises(psycopg2.Error) as refused:
        _read(server, _RESTRICTED, "SELECT inet_server_port(), relname::text FROM pg_class")
    assert refused.value.pgcode == "0A000"
    assert "inet_server_port" in str(refused.value)


def test_privilege_and_visibility_functions_are_the_catalogs(server):
    # As PostgreSQL answers: oid 1 is no function, and the visibility of an object no catalog
    # holds is NULL (metadata_functions._VISIBLE_IN); oid 23 is int4, on the search path.
    assert _read(
        server,
        _RESTRICTED,
        "SELECT has_schema_privilege('public', 'usage'), pg_type_is_visible(23), "
        "pg_function_is_visible(1), pg_has_role('x', 'member')",
    ) == [(True, True, None, True)]


#: A table of meta of each kind: a catalog view, told who asks, and a table of the plane, which is
#: told nothing and whose statement ran as the pool role.
_META_TABLES = ("meta.table_keys", "meta.domains")
#: A function no list names: made on the plane, and held from PUBLIC as a server read is.
_ON_NO_LIST = "held_from_public"


def _http_sql(srv: IsolatedServer, role: str, sql: str) -> tuple[bool, str]:
    req = urllib.request.Request(
        f"{srv.base_url}/data/sql",
        data=json.dumps({"sql": sql}).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": role},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status == 200, resp.read().decode()
    except urllib.error.HTTPError as refused:
        return False, f"{refused.code} {refused.read().decode()}"


def _on_the_plane(srv: IsolatedServer, sql: str) -> list[tuple]:
    """What the control plane itself holds, read with the server's own credentials."""
    assert srv.plane is not None
    url = make_url(srv.plane.url)
    conn = psycopg2.connect(
        host=url.host,
        port=url.port,
        dbname=url.database,
        user=url.username,
        password=url.password,
        options=f"-csearch_path={org_schema(_ORG)}",
    )
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


@pytest.fixture(scope="module")
def function_on_no_list(server):
    _on_the_plane(
        server,
        f"CREATE FUNCTION {_ON_NO_LIST}() RETURNS TEXT LANGUAGE SQL AS $$ SELECT 'answered' $$; "
        f"REVOKE ALL ON FUNCTION {_ON_NO_LIST}() FROM PUBLIC; SELECT 1",
    )
    return _ON_NO_LIST


@pytest.mark.parametrize("table", _META_TABLES)
def test_a_client_statement_runs_as_the_client_role_over_http(server, table):
    for asked in ("current_user", "current_setting('role')"):
        ok, body = _http_sql(server, _EVERYTHING, f"SELECT {asked} FROM {table} LIMIT 1")
        assert ok, body
        assert CLIENT_ROLE in body, body


@pytest.mark.parametrize("table", _META_TABLES)
def test_pgwire_answers_current_user_as_the_connection_that_asks(server, table):
    """PostgreSQL's answer to a client, written by the rewrite: it says nothing of the database's
    role, which the tests below hold."""
    sql = f"SELECT current_user FROM {table} LIMIT 1"
    assert _read(server, _EVERYTHING, sql) == [(_EVERYTHING,)]


@pytest.mark.parametrize("table", _META_TABLES)
def test_a_server_file_is_not_read_over_pgwire(server, table):
    sql = f"SELECT pg_read_file('postgresql.conf') FROM {table} LIMIT 1"
    with pytest.raises(psycopg2.Error) as refused:
        _read(server, _EVERYTHING, sql)
    assert refused.value.pgcode == "58P01", refused.value  # the form: every file is missing


@pytest.mark.parametrize("table", _META_TABLES)
def test_a_server_file_is_not_read_over_http(server, table):
    sql = f"SELECT pg_read_file('postgresql.conf') FROM {table} LIMIT 1"
    ok, body = _http_sql(server, _EVERYTHING, sql)
    assert not ok, body
    assert "pg_read_file" in body and "max_connections" not in body


@pytest.mark.parametrize("table", _META_TABLES)
def test_a_function_on_no_list_fails_at_the_database_over_pgwire(
    server, function_on_no_list, table
):
    sql = f"SELECT {function_on_no_list}() FROM {table} LIMIT 1"
    with pytest.raises(psycopg2.Error) as refused:
        _read(server, _EVERYTHING, sql)
    assert refused.value.pgcode == "42501", refused.value
    assert function_on_no_list in str(refused.value)


@pytest.mark.parametrize("table", _META_TABLES)
def test_a_function_on_no_list_fails_at_the_database_over_http(server, function_on_no_list, table):
    sql = f"SELECT {function_on_no_list}() FROM {table} LIMIT 1"
    ok, body = _http_sql(server, _EVERYTHING, sql)
    assert not ok, body
    assert f"permission denied for function {function_on_no_list}" in body


def _terminal_relations(srv: IsolatedServer) -> set[tuple[str, str]]:
    """Every (schema, relation) the admin terminal can be asked to read: what is registered under
    its source, by its physical name."""
    physical = {**_META_TABLE_ALIAS, **ONTOLOGY_TABLE_ALIAS, **_OPS_LOG_TABLE_ALIAS}
    rows = _on_the_plane(
        srv,
        "SELECT schema_name, table_name FROM registered_tables WHERE source_id = 'provisa-admin'",
    )
    return {(schema, physical.get(table, table)) for schema, table in rows}


def test_the_client_role_reads_every_table_of_the_terminal(server):
    relations = _terminal_relations(server)
    assert len(relations) > 200
    missing = [
        (schema, relation)
        for schema, relation in sorted(relations)
        if _on_the_plane(
            server,
            f"SELECT has_table_privilege('{CLIENT_ROLE}', '\"{schema}\".\"{relation}\"', 'SELECT')",
        )
        != [(True,)]
    ]
    assert not missing


def test_the_client_role_holds_no_table_but_the_terminals(server):
    """A view reads as its owner, so its base tables are no grant of this role's."""
    relations = {relation for _schema, relation in _terminal_relations(server)}
    granted = _on_the_plane(
        server,
        "SELECT DISTINCT table_name::text, privilege_type::text "
        f"FROM information_schema.role_table_grants WHERE grantee = '{CLIENT_ROLE}'",
    )
    assert {privilege for _table, privilege in granted} == {"SELECT"}
    assert {table for table, _privilege in granted} <= relations


def test_the_client_role_holds_no_base_table_a_meta_view_restates(server):
    held = _on_the_plane(
        server,
        f"SELECT has_table_privilege('{CLIENT_ROLE}', 'sources', 'SELECT'), "
        f"has_table_privilege('{CLIENT_ROLE}', 'roles', 'SELECT')",
    )
    assert held == [(False, False)]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT id FROM meta.sources",  # a view named apart from its table
        "SELECT id FROM meta.domains",  # a table of the plane itself
        "SELECT id FROM ontology._terms",
    ],
)
def test_the_meta_and_ontology_tables_answer_a_client(server, sql):
    assert isinstance(_read(server, _EVERYTHING, sql), list)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT id FROM ops.query_audit_log",  # a log's view
        "SELECT id FROM ops.queries",  # a report
    ],
)
def test_the_ops_tables_answer_their_steward(server, sql):
    assert isinstance(_read(server, "org_admin", sql), list)


def test_the_org_registry_is_a_table_of_the_terminal(server):
    """REQ-1301: the root org's, in its own schema, rebuilt with no grant unless the seed gives it."""
    assert ("org_" + _ORG, "org_registry") in _terminal_relations(server)


def test_a_write_to_meta_is_refused_and_writes_nothing(server):
    before = _on_the_plane(server, "SELECT id, description FROM domains ORDER BY id")
    for write in (
        "UPDATE meta.domains SET description = 'written'",
        "DELETE FROM meta.domains",
    ):
        with pytest.raises(psycopg2.Error):
            _read(server, _EVERYTHING, write)
    assert _on_the_plane(server, "SELECT id, description FROM domains ORDER BY id") == before


def test_a_role_with_no_meta_reads_pg_catalog(server):
    """REQ-1132: the catalog passes metadata along; it is not gated by meta."""
    assert "orders" in _names(_read(server, _NO_META, _TABLES))
    assert _read(server, _NO_META, "SELECT typname::text FROM pg_type WHERE oid = 23") == [
        ("int4",)
    ]


def test_a_role_with_no_meta_reads_information_schema(server):
    """REQ-1132: the catalog passes metadata along; it is not gated by meta."""
    listed = _read(server, _NO_META, "SELECT table_name::text FROM information_schema.tables")
    assert "orders" in _names(listed)
    assert "products" not in _names(listed)
