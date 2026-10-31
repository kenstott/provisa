# Copyright (c) 2026 Kenneth Stott
# Canary: 4c0b97e2-d163-4f8a-b5a7-1e3c68f2d095
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2190: pg_catalog and information_schema are views in the meta domain. On a control plane
with the real views: every relation answers in PostgreSQL 17's shape, whatever the server's
version; the model's rows are what a catalog lists of it; a role reads the rows of what it lists
and every system row; a view is told who asks by the statement's settings and by nothing else; the
names a view writes are the compiler's."""

# Requirements: REQ-2190

from __future__ import annotations

from types import SimpleNamespace

import pytest
import sqlalchemy as sa

from provisa.api._meta_views import _META_TABLE_VIEWS
from provisa.compiler import naming
from provisa.compiler.sql_rewrite import semantic_table_name
from provisa.core import schema_org
from provisa.core.naming_rule_check import NamingRuleRefused, refuse_divergent_rule
from provisa.pgwire.catalog_pg17 import RELATIONS
from provisa.security.rights import compute_meta_row_scope
from provisa.transpiler.metadata_names import camel_case_sql, snake_case_sql
from provisa.transpiler.metadata_oids import metric_oid, table_oid
from provisa.transpiler.metadata_settings import (
    ClientSettingRefused,
    refuse_client_settings,
    relation_names_literal,
    set_statement,
    statement_settings,
)
from provisa.transpiler.metadata_views import (
    CATALOG_ROW_SCOPES,
    CATALOG_VIEW_NAMES,
    NEUTRAL_VIEW_NAMES,
    SCOPE_COLUMN,
    SCOPED_BY_COLUMN,
    catalog_view_name,
)

_SCHEMA = "org_default"


def _insert(conn, table, **values):
    conn.execute(table.insert().values(**values))


def _model(conn) -> None:
    t = schema_org
    _insert(conn, t.sources, id="src", type="postgresql")
    for domain in ("sales", "sales-eu", "hr"):
        _insert(conn, t.domains, id=domain, description=f"The {domain} domain")
    for table_id, domain, name, alias in (
        (1, "sales", "CustomerOrders", None),
        (2, "sales-eu", "lines", "OrderLines"),
        (3, "hr", "salaries", None),
    ):
        _insert(
            conn,
            t.registered_tables,
            id=table_id,
            source_id="src",
            domain_id=domain,
            schema_name="public",
            table_name=name,
            alias=alias,
            description="What was ordered" if table_id == 1 else None,
        )
    _insert(
        conn,
        t.registered_tables,
        id=4,
        source_id="src",
        domain_id="sales",
        schema_name="public",
        table_name="drafted",
        draft=True,
    )
    for table_id, name, data_type, extras in (
        (1, "orderId", "integer", {"is_primary_key": True, "not_null": True}),
        (1, "total", "numeric(10,2)", {"description": "What it came to"}),
        (1, "code", "text", {"native_type": "varchar(20)", "not_null": True}),
        (1, "ip", "text", {"subtype": "inet", "alias": "address"}),
        (2, "id", "integer", {"is_primary_key": True}),
        (3, "id", "integer", {"is_primary_key": True}),
    ):
        _insert(
            conn,
            t.table_columns,
            table_id=table_id,
            column_name=name,
            data_type=data_type,
            **extras,
        )
    _insert(conn, t.metrics, name="revenue", expression="SUM(x)", description="All of it")
    _insert(conn, t.roles, id="reader_role", description="reads")
    _insert(conn, t.roles, id="clerk_role", description="enters", parents=["reader_role"])
    _insert(
        conn,
        t.table_write_grants,
        table_id=1,
        role_id="reader_role",
        operation="insert",
        presets=[{"column": "total", "source": "value", "value": "0"}],
    )
    _insert(
        conn,
        t.table_write_grants,
        table_id=1,
        role_id="clerk_role",
        operation="insert",
        presets=[{"column": "code", "source": "now"}],
    )
    _insert(
        conn,
        t.relationships,
        id="r1",
        source_table_id=2,
        source_column="id",
        target_table_id=1,
        target_column="orderId",
        cardinality="many-to-one",
    )


@pytest.fixture(scope="module")
def plane(embedded_postgres, control_plane_template):
    from tests.pg_control_plane import clone

    held = clone(embedded_postgres, control_plane_template)
    engine = held.engine(f"{held.url}?options=-csearch_path%3D{_SCHEMA},public")
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA meta")
        _model(conn)
        for ddl in _META_TABLE_VIEWS.values():
            conn.exec_driver_sql(ddl.replace("%", "%%"))
        for name in (*NEUTRAL_VIEW_NAMES, *CATALOG_VIEW_NAMES):
            conn.exec_driver_sql(f"CREATE VIEW meta.{name} AS SELECT * FROM {_SCHEMA}.{name}")
    yield engine
    held.close()


def _told(conn, role="reader_role", can_write=True, **session) -> None:
    sql, values = set_statement(
        statement_settings(role, can_write, "provisa", False, session=session)
    )
    for n, value in reversed(list(enumerate(values, 1))):
        sql = sql.replace(f"${n}", "'" + value.replace("'", "''") + "'")
    conn.exec_driver_sql(sql)


def _read(conn, sql: str) -> list[tuple]:
    return [tuple(r) for r in conn.exec_driver_sql(sql.replace("%", "%%")).fetchall()]


def test_every_relation_answers_in_postgresql_17s_columns_and_order(plane):
    with plane.begin() as conn:
        _told(conn)
        for (schema, name), columns in RELATIONS.items():
            view = catalog_view_name(schema, name)
            result = conn.exec_driver_sql(f"SELECT * FROM meta.{view} LIMIT 1")
            expected = [c for c, _, _ in columns]
            if view in SCOPED_BY_COLUMN:
                expected.append(SCOPE_COLUMN)
            assert list(result.keys()) == expected, view


def test_a_view_of_no_rows_of_the_server_shows_none_of_the_control_plane(plane):
    """A statistics or activity relation is the catalog's, never the server's it runs on."""
    with plane.begin() as conn:
        _told(conn)
        for view in (
            "catalog_pg_stat_all_tables",
            "catalog_pg_locks",
            "catalog_pg_stat_user_tables",
            "catalog_pg_stat_database",
        ):
            assert _read(conn, f"SELECT count(*) FROM meta.{view}") == [(0,)], view
        assert _read(
            conn, "SELECT count(*) FROM meta.catalog_pg_class WHERE relname = 'table_columns'"
        )[0] == (0,)


def test_the_catalog_lists_the_model_and_not_itself(plane):
    with plane.begin() as conn:
        _told(conn)
        rows = _read(
            conn,
            "SELECT c.relname, n.nspname FROM meta.catalog_pg_class c "
            "JOIN meta.catalog_pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relkind = 'r' AND n.nspname IN ('sales', 'sales_eu', 'hr', 'metrics')",
        )
        names = _read(conn, "SELECT relname FROM meta.catalog_pg_class")
    assert sorted(rows) == [
        ("customer_orders", "sales"),
        ("order_lines", "sales_eu"),
        ("revenue", "metrics"),
        ("salaries", "hr"),
    ]
    held = {n for (n,) in names}
    assert "drafted" not in held
    assert "table_keys" not in held and "is_tables" not in held
    assert "pg_class" in held and "columns" in held  # the system relations list themselves


def test_a_column_is_what_the_catalog_says_of_it(plane):
    with plane.begin() as conn:
        _told(conn)
        rows = _read(
            conn,
            "SELECT attname, attnum, atttypid::int, atttypmod, attnotnull, attlen "
            f"FROM meta.catalog_pg_attribute WHERE attrelid = {table_oid(1)} ORDER BY attnum",
        )
    assert rows == [
        ("order_id", 1, 23, -1, True, 4),
        ("total", 2, 1700, 655366, False, -1),
        ("code", 3, 1043, 24, True, -1),
        ("address", 4, 869, -1, False, -1),
        ("_name_", 5, 1043, -1, False, -1),
        ("_domain_", 6, 1043, -1, False, -1),
    ]


def test_a_metric_is_a_relation_of_one_value_with_its_description(plane):
    oid = metric_oid("revenue")
    with plane.begin() as conn:
        _told(conn)
        assert _read(conn, f"SELECT relname FROM meta.catalog_pg_class WHERE oid = {oid}") == [
            ("revenue",)
        ]
        assert _read(
            conn,
            f"SELECT attname, atttypid::int FROM meta.catalog_pg_attribute WHERE attrelid = {oid}",
        ) == [("value", 701)]
        assert _read(
            conn, f"SELECT description FROM meta.catalog_pg_description WHERE objoid = {oid}"
        ) == [("All of it",)]


def test_information_schema_columns_lists_a_column_as_pg_attribute_does(plane):
    with plane.begin() as conn:
        _told(conn)
        rows = _read(
            conn,
            "SELECT column_name, ordinal_position, is_nullable, data_type, column_default "
            f"FROM meta.is_columns WHERE {SCOPE_COLUMN} = 1 AND ordinal_position <= 4 "
            "ORDER BY ordinal_position",
        )
    assert rows == [
        ("order_id", 1, "NO", "integer", None),
        ("total", 2, "YES", "numeric", "0"),
        ("code", 3, "NO", "character varying", None),
        ("address", 4, "YES", "inet", None),
    ]


def test_a_column_default_is_the_asking_roles(plane):
    query = f"SELECT adnum, adbin FROM meta.catalog_pg_attrdef WHERE adrelid = {table_oid(1)}"
    with plane.begin() as conn:
        _told(conn, role="reader_role")
        assert _read(conn, query) == [(2, "0")]
    with plane.begin() as conn:
        _told(conn, role="clerk_role")
        assert _read(conn, query) == [(3, "now()")]
    with plane.begin() as conn:
        _told(conn, role="reader_role", can_write=False)
        assert _read(conn, query) == []


def test_a_view_told_nothing_lists_no_role_default_and_no_session(plane):
    with plane.begin() as conn:
        assert _read(conn, "SELECT count(*) FROM meta.catalog_pg_attrdef") == [(0,)]
        assert _read(conn, "SELECT count(*) FROM meta.catalog_pg_stat_activity") == [(0,)]


def test_settings_end_with_the_statements_transaction(plane):
    with plane.begin() as conn:
        _told(conn, role="clerk_role")
    with plane.begin() as conn:
        assert _read(conn, "SELECT current_setting('provisa.catalog.role', true)") in (
            [("",)],
            [(None,)],
        )


def test_the_connection_asking_is_the_one_row_of_activity(plane):
    with plane.begin() as conn:
        _told(
            conn,
            pid="77",
            application_name="psql",
            backend_start="2026-10-30 10:00:00+00",
            query="SELECT 1",
        )
        rows = _read(
            conn,
            "SELECT datname, pid, usename, application_name, query, state "
            "FROM meta.catalog_pg_stat_activity",
        )
    assert rows == [("provisa", 77, "reader_role", "psql", "SELECT 1", "active")]


def test_the_role_asking_is_listed_with_the_models_and_postgresqls(plane):
    with plane.begin() as conn:
        _told(conn, role="visitor")
        names = {n for (n,) in _read(conn, "SELECT rolname FROM meta.catalog_pg_roles")}
        members = _read(
            conn,
            "SELECT count(*) FROM meta.catalog_pg_auth_members m JOIN meta.catalog_pg_roles r ON r.oid = m.member "
            "WHERE r.rolname = 'clerk_role'",
        )
    assert {"reader_role", "clerk_role", "visitor", "provisa", "pg_monitor"} <= names
    assert members == [(1,)]


def test_a_foreign_key_and_its_key_read_as_a_client_reads_them(plane):
    with plane.begin() as conn:
        _told(conn)
        keys = _read(
            conn,
            "SELECT conname, contype, conkey FROM meta.catalog_pg_constraint "
            f"WHERE conrelid = {table_oid(2)} AND conname NOT LIKE '%___meta%' ORDER BY contype",
        )
        indexed = _read(
            conn,
            f"SELECT indkey::text, indisprimary FROM meta.catalog_pg_index WHERE indrelid = {table_oid(1)}",
        )
        referential = _read(
            conn,
            "SELECT constraint_name, unique_constraint_name FROM meta.is_referential_constraints "
            f"WHERE {SCOPE_COLUMN} = 2 AND constraint_name LIKE 'fk_order_lines__id'",
        )
        link = _read(
            conn,
            "SELECT count(*) FROM meta.table_keys WHERE table_id = 2 AND kind = 'f' "
            "AND column_name IN ('_domain_', '_name_')",
        )
    assert keys == [("fk_order_lines__id", "f", [1]), ("pk_order_lines", "p", [1])]
    assert indexed == [("1", True)]
    assert referential == [("fk_order_lines__id", "pk_customer_orders")]
    assert link == [(0,)]  # the registration's key is in no plane without meta's tables


# --- row scope: who sees what does not change ---------------------------------------------------


def _scoped(view: str, listed: set[int]) -> str:
    """The predicate stage2 writes on ``view`` for a role listing ``listed``."""
    id_list = ",".join(str(i) for i in sorted(listed)) or "-1"
    return " AND ".join(
        f"(({expr}) IS NULL OR ({expr}) IN ({id_list}))" for expr in CATALOG_ROW_SCOPES[view]
    )


def test_a_role_reads_what_it_lists_and_every_system_row(plane):
    with plane.begin() as conn:
        _told(conn)
        unscoped = _read(conn, "SELECT count(*) FROM meta.catalog_pg_class WHERE oid < 16384")
        system = _read(
            conn,
            f"SELECT count(*) FROM meta.catalog_pg_class WHERE oid < 16384 AND {_scoped('catalog_pg_class', {1})}",
        )
        model = _read(
            conn,
            "SELECT relname FROM meta.catalog_pg_class WHERE oid >= 16384 AND relkind = 'r' "
            f"AND {_scoped('catalog_pg_class', {1})} ORDER BY relname",
        )
        attributes = _read(
            conn,
            "SELECT DISTINCT attrelid::bigint FROM meta.catalog_pg_attribute "
            f"WHERE attrelid >= 16384 AND {_scoped('catalog_pg_attribute', {1})} ORDER BY 1",
        )
    assert unscoped == system and unscoped[0][0] == len(RELATIONS)
    assert model == [("customer_orders",), ("revenue",)]  # a metric belongs to no table
    assert attributes[0] == (table_oid(1),)
    assert (table_oid(3),) not in attributes


def test_the_scope_of_a_view_is_the_scope_of_table_columns(plane):
    """Under one neighbourhood, a role sees of each view the tables it sees of table_columns."""
    tables = [
        {"id": 1, "domain_id": "sales", "columns": [{"column_name": "x", "visible_to": ["a"]}]},
        {"id": 2, "domain_id": "sales-eu", "columns": [{"column_name": "x", "visible_to": []}]},
        {"id": 3, "domain_id": "hr", "columns": [{"column_name": "x", "visible_to": []}]},
    ]
    role = {"id": "a", "capabilities": [], "domain_access": ["sales"]}
    relationships = [{"source_table_id": 2, "target_table_id": 1}]
    listed = compute_meta_row_scope(role, tables, relationships)
    assert listed == {1, 2}
    with plane.begin() as conn:
        _told(conn)
        keys = _read(
            conn,
            f"SELECT DISTINCT {SCOPE_COLUMN} FROM meta.is_columns "
            f"WHERE {_scoped('is_columns', listed)} AND {SCOPE_COLUMN} IS NOT NULL ORDER BY 1",
        )
    assert keys == [(1,), (2,)]


_PINNED_RELATION = "customer_orders"


@pytest.mark.parametrize(
    "sql",
    (
        "SELECT c.oid, c.relname, c.relnatts, c.relkind FROM meta.catalog_pg_class c "
        "WHERE c.relname = '{name}'",
        "SELECT a.attname, a.attnum, a.atttypid, a.atttypmod, a.attnotnull "
        "FROM meta.catalog_pg_class c JOIN meta.catalog_pg_attribute a ON a.attrelid = c.oid "
        "WHERE c.relname = '{name}' ORDER BY a.attnum",
        "SELECT k.oid, k.conname, k.contype, k.conkey, k.confrelid, k.confkey "
        "FROM meta.catalog_pg_class c JOIN meta.catalog_pg_constraint k ON k.conrelid = c.oid "
        "WHERE c.relname = '{name}' ORDER BY k.oid",
        "SELECT i.indexrelid, i.indkey, x.relname, x.relnatts FROM meta.catalog_pg_class c "
        "JOIN meta.catalog_pg_index i ON i.indrelid = c.oid "
        "JOIN meta.catalog_pg_class x ON x.oid = i.indexrelid "
        "WHERE c.relname = '{name}' ORDER BY i.indexrelid",
        "SELECT d.objoid, d.objsubid, d.description FROM meta.catalog_pg_class c "
        "JOIN meta.catalog_pg_description d ON d.objoid = c.oid "
        "WHERE c.relname = '{name}' ORDER BY d.objsubid",
    ),
)
def test_a_statement_about_a_table_reads_what_the_whole_catalog_says_of_it(plane, sql):
    """The pin changes what a statement pays and nothing it gets: every row, and every oid, is
    the one a statement reading the model whole is shown."""
    statement = sql.replace("{name}", _PINNED_RELATION)
    with plane.begin() as conn:
        _told(conn)
        whole = _read(conn, statement)
    with plane.begin() as conn:
        _told(conn, asked=relation_names_literal([_PINNED_RELATION]))
        pinned = _read(conn, statement)
    assert whole
    assert pinned == whole


def test_an_index_goes_by_its_own_name_as_well_as_its_tables(plane):
    statement = "SELECT relname, relkind FROM meta.catalog_pg_class WHERE relname = 'pk_{}'"
    statement = statement.format(_PINNED_RELATION)
    with plane.begin() as conn:
        _told(conn)
        whole = _read(conn, statement)
    with plane.begin() as conn:
        _told(conn, asked=relation_names_literal([f"pk_{_PINNED_RELATION}"]))
        assert _read(conn, statement) == whole
    assert whole == [(f"pk_{_PINNED_RELATION}", "i")]


def test_a_key_keeps_its_number_whichever_tables_a_statement_asks_about(plane):
    statement = "SELECT DISTINCT key_id, table_id FROM meta.table_keys ORDER BY key_id"
    with plane.begin() as conn:
        _told(conn)
        whole = _read(conn, statement)
        tables = _read(
            conn,
            "SELECT DISTINCT table_name, table_id FROM meta.table_keys ORDER BY table_id",
        )
    for name, table_id in tables:
        with plane.begin() as conn:
            _told(conn, asked=relation_names_literal([name]))
            asked = _read(conn, statement)
        assert asked and set(asked) <= set(whole)
        assert {row for row in whole if row[1] == table_id} <= set(asked)


def test_a_foreign_key_to_a_table_not_listed_is_not_read(plane):
    with plane.begin() as conn:
        _told(conn)
        rows = _read(
            conn,
            "SELECT conname FROM meta.catalog_pg_constraint "
            f"WHERE contype = 'f' AND conname LIKE 'fk_order_lines__id' "
            f"AND {_scoped('catalog_pg_constraint', {2})}",
        )
    assert rows == []


# --- names --------------------------------------------------------------------------------------

_NAMES = (
    "customerId",
    "OrderDate",
    "HTTPServer",
    "order_date",
    "ABC",
    "aB",
    "A1b",
    "x9Y",
    "getHTTPResponseCode",
    "already_snake",
    "ID",
    "fooBAR",
    "__Weird__Name",
    "foo_123",
    "foo-bar",
    "a_b_c",
    "camelCaseName",
    "with_2_digits",
    "_leading",
    "trailing_",
)


@pytest.mark.parametrize("name", _NAMES)
def test_a_name_is_written_as_the_compiler_writes_it(plane, name):
    literal = "'" + name + "'"
    with plane.begin() as conn:
        snake, camel = conn.exec_driver_sql(
            f"SELECT {snake_case_sql(literal)}, {camel_case_sql(literal)}"
        ).one()
    assert snake == naming._to_snake_case(name)
    assert camel == naming._to_camel_case(name)


@pytest.mark.parametrize(
    ("registered", "alias", "convention"),
    (
        ("CustomerOrders", None, None),
        ("order_lines", None, None),
        ("order_2_lines", None, None),
        ("orders__eu", None, None),
        ("x", "OrderLines", None),
        ("order_lines", None, "snake"),
    ),
)
def test_a_table_is_named_as_a_served_table_is(plane, registered, alias, convention):
    from provisa.compiler.sql_gen import TableMeta

    field = naming.generate_name(
        registered, [], alias=alias, convention=convention or naming.active_gql_convention()
    )
    served = TableMeta(
        table_id=9,
        field_name=field,
        type_name="T",
        source_id="s",
        catalog_name="c",
        schema_name="public",
        table_name=registered,
        domain_id="sales",
        display_name=alias or "",
    )
    with plane.begin() as conn:
        _insert(
            conn,
            schema_org.registered_tables,
            id=9,
            source_id="src",
            domain_id="sales",
            schema_name="named",
            table_name=registered,
            alias=alias,
            gql_naming_convention=convention,
        )
        _told(conn)
        rows = _read(conn, "SELECT table_name FROM catalog_tables WHERE table_id = 9")
        conn.exec_driver_sql("DELETE FROM registered_tables WHERE id = 9")
    assert rows == [(semantic_table_name(served),)]


def test_naming_rules_apply_in_order_as_the_compiler_applies_them(plane):
    rules = [
        {"pattern": "^tbl_", "replacement": ""},
        {"pattern": "_v[0-9]+$", "replacement": ""},
    ]
    with plane.begin() as conn:
        for rule in rules:
            _insert(conn, schema_org.naming_rules, **rule)
        _insert(
            conn,
            schema_org.registered_tables,
            id=9,
            source_id="src",
            domain_id="sales",
            schema_name="named",
            table_name="tbl_invoices_v2",
        )
        _told(conn)
        rows = _read(conn, "SELECT table_name FROM catalog_tables WHERE table_id = 9")
        conn.exec_driver_sql("DELETE FROM registered_tables WHERE id = 9")
        conn.exec_driver_sql("DELETE FROM naming_rules")
    assert rows == [(naming.generate_name("tbl_invoices_v2", rules),)]


class _Connection:
    """The two calls refuse_divergent_rule makes, on a real server."""

    def __init__(self, conn) -> None:
        self.conn = conn

    def transaction(self):
        class _Savepoint:
            async def __aenter__(_):
                self.conn.exec_driver_sql("SAVEPOINT probe")

            async def __aexit__(_, kind, *_rest):
                if kind is not None:
                    self.conn.exec_driver_sql("ROLLBACK TO SAVEPOINT probe")
                return False

        return _Savepoint()

    async def fetchval(self, _sql, name, pattern, replacement):
        return self.conn.execute(
            sa.text("SELECT regexp_replace(:n, :p, :r, 'g')"),
            {"n": name, "p": pattern, "r": replacement},
        ).scalar()


@pytest.mark.parametrize(
    ("pattern", "replacement"),
    (
        ("^tbl_", ""),
        ("_v[0-9]+$", ""),
        ("(orders)_(v1)", r"\2_\1"),
        ("[0-9]+", "n"),
    ),
)
async def test_a_rule_both_engines_read_alike_is_taken(plane, pattern, replacement):
    with plane.begin() as conn:
        await refuse_divergent_rule(_Connection(conn), pattern, replacement)


@pytest.mark.parametrize(
    ("pattern", "replacement"),
    (
        (r"(?P<word>orders)", ""),  # a named group is Python's
        ("orders", r"\g<0>_x"),  # a replacement form Python's alone
        ("(", ""),  # no engine reads this
        (r"\borders", "x"),  # a word boundary to Python, a backspace to PostgreSQL
    ),
)
async def test_a_rule_the_engines_read_differently_is_refused(plane, pattern, replacement):
    with plane.begin() as conn:
        with pytest.raises(NamingRuleRefused):
            await refuse_divergent_rule(_Connection(conn), pattern, replacement)


# --- a client's set_config ----------------------------------------------------------------------


def _admin_plan(sql: str, settings: dict[str, str] | None):
    from provisa.pgwire._pipeline import _Plan
    from provisa.transpiler.router import Route

    return _Plan(
        route=Route.DIRECT,
        sql=sql,
        source_id="provisa-admin",
        dialect="postgres",
        catalog_settings=settings,
    )


async def test_the_admin_terminal_tells_a_statement_and_no_later_one(control_plane):
    from provisa.pgwire._pipeline import _run_plan_terminal

    state = SimpleNamespace(model_db=control_plane.org(), federation_engine=None)
    read = "SELECT current_setting('provisa.catalog.role', true) AS role"
    told = await _run_plan_terminal(
        _admin_plan(read, statement_settings("reader_role", False, "provisa", False)), state
    )
    assert told.rows == [("reader_role",)]
    after = await _run_plan_terminal(_admin_plan(read, None), state)
    assert after.rows in ([("",)], [(None,)])


async def test_the_admin_terminal_refuses_a_client_setting_what_a_view_trusts(control_plane):
    from provisa.pgwire._pipeline import _run_plan_terminal

    state = SimpleNamespace(model_db=control_plane.org(), federation_engine=None)
    with pytest.raises(ClientSettingRefused):
        await _run_plan_terminal(
            _admin_plan("SELECT set_config('provisa.catalog.role', 'admin', true)", None), state
        )


@pytest.mark.parametrize(
    "sql",
    (
        "SELECT set_config('provisa.catalog.role', 'admin', true)",
        "SELECT SET_CONFIG('provisa.catalog.role', 'admin', false), * FROM meta.catalog_pg_attrdef",
        "SELECT pg_catalog.set_config('x', 'y', true) FROM meta.catalog_pg_class",
        "SELECT \"set_config\"('x', 'y', true)",
        "WITH a AS (SELECT set_config('x', 'y', true)) SELECT * FROM a",
    ),
)
def test_a_client_calling_set_config_is_refused(sql):
    with pytest.raises(ClientSettingRefused):
        refuse_client_settings(sql)


@pytest.mark.parametrize(
    "sql",
    (
        "SELECT relname FROM meta.catalog_pg_class",
        "SELECT 'set_config' AS word",
        "SELECT set_config_note FROM meta.notes",
    ),
)
def test_a_statement_that_only_says_the_word_is_not(sql):
    refuse_client_settings(sql)
