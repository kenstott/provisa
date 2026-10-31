# Copyright (c) 2026 Kenneth Stott
# Canary: 3e9b7a14-c052-4d6f-8e21-96a1d4f07b38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2190's acceptance: each metadata function's form, run on PostgreSQL over the real meta views
of a model registered from real tables, answers what the real function answers of those tables."""

# Requirements: REQ-2190

from __future__ import annotations

import pytest
import sqlglot

from provisa.api._meta_views import _META_TABLE_VIEWS
from provisa.core import schema_org
from provisa.core.modeling_tags import append_modeling_tag, column_comment
from provisa.transpiler.metadata_facts import catalog_facts
from provisa.transpiler.metadata_functions import VALUE_FORMS, expand
from provisa.transpiler.metadata_oids import KEY_OID_BASE, table_oid
from provisa.transpiler.metadata_settings import set_statement, statement_settings
from provisa.transpiler.metadata_views import CATALOG_VIEW_NAMES, NEUTRAL_VIEW_NAMES

_REAL_MODEL = """
    CREATE ROLE analyst;
    CREATE SCHEMA sales;
    CREATE TABLE public.customers (
        id integer CONSTRAINT pk_customers PRIMARY KEY,
        code text CONSTRAINT uq_customers_code UNIQUE
    );
    CREATE TABLE sales.regions (
        a integer, b integer, label text, CONSTRAINT pk_regions PRIMARY KEY (b, a)
    );
    CREATE TABLE sales.orders (
        id integer CONSTRAINT pk_orders PRIMARY KEY,
        customer_id integer CONSTRAINT fk_orders_customer REFERENCES public.customers (id),
        region text,
        number integer,
        "order" text,
        ra integer,
        rb integer,
        CONSTRAINT uq_orders_region_number UNIQUE (region, number),
        CONSTRAINT fk_orders_region FOREIGN KEY (rb, ra) REFERENCES sales.regions (b, a)
    );
    COMMENT ON TABLE sales.orders IS 'The orders [fact, scd2, pii, pii-b]';
    COMMENT ON TABLE public.customers IS 'The customers';
    COMMENT ON COLUMN sales.orders.region IS 'Where it shipped [fact, geo]';
    COMMENT ON COLUMN sales.orders.number IS 'Its number';
    GRANT SELECT ON sales.orders TO analyst;
    GRANT INSERT ON sales.regions TO analyst;
"""

_TABLES = {
    1: ("public", "customers"),
    2: ("sales", "regions"),
    3: ("sales", "orders"),
}
# (table id, physical name, is primary key, alias). "customerId" is the source's name of a column
# the compiler's naming publishes as customer_id; a column with an alias is named by it.
_COLUMNS = (
    (1, "id", True, None),
    (1, "code", False, None),
    (2, "b", True, None),
    (2, "a", True, None),
    (2, "label", False, None),
    (3, "id", True, None),
    (3, "customerId", False, None),
    (3, "region", False, None),
    (3, "number", False, None),
    (3, "ord", False, "order"),
    (3, "ra", False, None),
    (3, "rb", False, None),
)


_ORG_SCHEMA = "org_default"


def _insert(conn, table, **values):
    conn.execute(table.insert().values(**values))


def _register(conn) -> None:
    t = schema_org
    _insert(conn, t.sources, id="src", type="postgresql")
    for domain in ("public", "sales"):
        _insert(conn, t.domains, id=domain, description=f"The {domain} domain")
    for table_id, (schema, name) in _TABLES.items():
        _insert(
            conn,
            t.registered_tables,
            id=table_id,
            source_id="src",
            domain_id=schema,
            schema_name=schema,
            table_name=name,
            description={3: "The orders", 1: "The customers"}.get(table_id),
            modeling_role="fact" if table_id == 3 else None,
            modeling_history="scd2" if table_id == 3 else None,
            unique_constraints={
                1: [{"name": "uq_customers_code", "columns": ["code"]}],
                3: [{"name": "uq_orders_region_number", "columns": ["region", "number"]}],
            }.get(table_id, []),
        )
    _insert(
        conn,
        t.registered_tables,
        id=4,
        source_id="src",
        domain_id="sales",
        schema_name="sales",
        table_name="drafted",
        draft=True,
    )
    descriptions = {"region": "Where it shipped", "number": "Its number"}
    for table_id, column, primary, alias in _COLUMNS:
        _insert(
            conn,
            t.table_columns,
            table_id=table_id,
            domain_id=_TABLES[table_id][0],
            column_name=column,
            is_primary_key=primary,
            alias=alias,
            description=descriptions.get(column),
        )
    relationships = (
        ("r1", 3, "customerId", 1, "id", "many-to-one"),
        ("r2", 3, "ra", 2, "a", "many-to-one"),
        ("r3", 3, "rb", 2, "b", "many-to-one"),
        # A semantic join: its target is no key of the target.
        ("r4", 3, "region", 2, "label", "many-to-one"),
        # Not a foreign key: the key is on the other side.
        ("r5", 1, "id", 3, "customerId", "one-to-many"),
    )
    for rel, source, source_column, target, target_column, cardinality in relationships:
        _insert(
            conn,
            t.relationships,
            id=rel,
            source_table_id=source,
            source_column=source_column,
            target_table_id=target,
            target_column=target_column,
            cardinality=cardinality,
        )
    tags = (
        ("pii-b", "table", 3, None),
        ("pii", "table", 3, None),
        ("geo", "column", 3, "region"),
    )
    for tag, kind, table_id, column in tags:
        _insert(
            conn,
            t.tag_assignments,
            tag_id=tag,
            base_tag_id=tag,
            object_type=kind,
            table_id=table_id,
            column_name=column,
            object_key=f"{tag}:{table_id}:{column}",
        )


@pytest.fixture(scope="module")
def live(embedded_postgres, control_plane_template):
    """A real model, and the control plane of org ``default`` it is registered in, with the real
    meta views and the meta schema a statement reads them by."""
    from tests.pg_control_plane import clone

    plane = clone(embedded_postgres, control_plane_template)
    engine = plane.engine(f"{plane.url}?options=-csearch_path%3D{_ORG_SCHEMA},public")
    with engine.begin() as conn:
        conn.exec_driver_sql("DROP ROLE IF EXISTS analyst")
        for statement in _REAL_MODEL.split(";"):
            if statement.strip():
                conn.exec_driver_sql(statement)
        conn.exec_driver_sql("CREATE SCHEMA meta")
        _register(conn)
        for ddl in _META_TABLE_VIEWS.values():
            conn.exec_driver_sql(ddl.replace("%", "%%"))
        for name in (*NEUTRAL_VIEW_NAMES, *CATALOG_VIEW_NAMES, "domains"):
            conn.exec_driver_sql(f"CREATE VIEW meta.{name} AS SELECT * FROM {_ORG_SCHEMA}.{name}")
    yield engine
    with engine.begin() as conn:
        conn.exec_driver_sql("DROP OWNED BY analyst")
        conn.exec_driver_sql("DROP ROLE analyst")
    plane.close()


def _facts(held: dict[int, set[str]] | None = None, role: str = "analyst", unserved=()):
    return catalog_facts(
        role, held if held is not None else {3: {"SELECT"}, 2: {"INSERT"}}, unserved
    )


def _told(conn) -> None:
    """What the admin terminal tells a statement's views, in this transaction."""
    sql, values = set_statement(statement_settings("analyst", True, "provisa", False))
    for n, value in enumerate(values, 1):
        sql = sql.replace(f"${n}", f"'{value}'", 1)
    conn.exec_driver_sql(sql)


def _one(conn, sql: str):
    _told(conn)
    return conn.exec_driver_sql(sql.replace("%", "%%")).scalar()


def _form(conn, call: str, facts=None):
    tree = sqlglot.parse_one(f"SELECT {call}", read="postgres")
    return _one(conn, expand(tree, facts or _facts()).sql(dialect="postgres"))


def _real(conn, call: str):
    conn.exec_driver_sql("SET LOCAL search_path = public")
    answer = _one(conn, f"SELECT {call}")
    conn.exec_driver_sql(f"SET LOCAL search_path = {_ORG_SCHEMA}, public")
    return answer


def _key_oid(conn, kind: str, table_id: int, column: str) -> int:
    key = _one(
        conn,
        f"SELECT key_id FROM meta.table_keys WHERE kind = '{kind}' AND table_id = {table_id} "
        f"AND column_name = '{column}'",
    )
    return KEY_OID_BASE + key


def _constraint(conn, name: str) -> int:
    return _one(conn, f"SELECT oid FROM pg_constraint WHERE conname = '{name}'")


def _index(conn, name: str) -> int:
    return _one(conn, f"SELECT conindid FROM pg_constraint WHERE conname = '{name}'")


_KEYS = (
    ("pk_customers", "p", 1, "id"),
    ("uq_customers_code", "u", 1, "code"),
    ("pk_regions", "p", 2, "b"),
    ("pk_orders", "p", 3, "id"),
    ("uq_orders_region_number", "u", 3, "region"),
    ("fk_orders_customer", "f", 3, "customerId"),
    ("fk_orders_region", "f", 3, "rb"),
)


@pytest.mark.parametrize(("name", "kind", "table_id", "column"), _KEYS)
@pytest.mark.parametrize("pretty", ("", ", false", ", true"))
def test_a_constraint_definition_is_the_real_one(live, name, kind, table_id, column, pretty):
    with live.begin() as conn:
        expected = _real(conn, f"pg_get_constraintdef({_constraint(conn, name)}{pretty})")
        oid = _key_oid(conn, kind, table_id, column)
        assert _form(conn, f"pg_get_constraintdef({oid}{pretty})") == expected


def test_a_key_named_for_its_alias_and_the_compilers_naming_is_named_so(live):
    with live.begin() as conn:
        oid = _key_oid(conn, "f", 3, "customerId")
        assert _form(conn, f"pg_get_constraintdef({oid})").startswith("FOREIGN KEY (customer_id)")
        reserved = sqlglot.parse_one(f"SELECT pg_get_indexdef({oid})", read="postgres")
        assert _one(conn, expand(reserved, _facts()).sql(dialect="postgres")) is None  # no index


@pytest.mark.parametrize(("name", "kind", "table_id", "column"), [k for k in _KEYS if k[1] != "f"])
def test_an_index_definition_is_the_real_one(live, name, kind, table_id, column):
    with live.begin() as conn:
        expected = _real(conn, f"pg_get_indexdef({_index(conn, name)})")
        oid = _key_oid(conn, kind, table_id, column)
        assert _form(conn, f"pg_get_indexdef({oid})") == expected
        real_whole = _real(conn, f"pg_get_indexdef({_index(conn, name)}, 0, false)")
        assert _form(conn, f"pg_get_indexdef({oid}, 0, false)") == real_whole


def test_an_index_column_is_the_real_one(live):
    with live.begin() as conn:
        oid = _key_oid(conn, "u", 3, "region")
        index = _index(conn, "uq_orders_region_number")
        for position in (1, 2, 3):
            expected = _real(conn, f"pg_get_indexdef({index}, {position}, false)")
            answered = _form(conn, f"pg_get_indexdef({oid}, {position}, false)")
            assert answered == (expected or None)


def test_an_oid_naming_no_key_has_no_definition(live):
    with live.begin() as conn:
        assert _form(conn, f"pg_get_constraintdef({KEY_OID_BASE + 1})") is None
        assert _form(conn, f"pg_get_indexdef({KEY_OID_BASE + 1})") is None


def test_the_views_list_only_what_a_catalog_lists_as_keys(live):
    with live.begin() as conn:
        _told(conn)
        rows = conn.exec_driver_sql(
            "SELECT kind, table_id, referenced_table_id, position, column_name, referenced_column "
            "FROM meta.table_keys WHERE kind = 'f' AND column_name NOT IN ('_domain_', '_name_') "
            "ORDER BY key_id, position"
        ).fetchall()
        # A composite key in the target's order; no semantic join; nothing on a draft.
        assert [tuple(r) for r in rows] == [
            ("f", 3, 1, 1, "customerId", "id"),
            ("f", 3, 2, 1, "rb", "b"),
            ("f", 3, 2, 2, "ra", "a"),
        ]
        ids = conn.exec_driver_sql(
            "SELECT DISTINCT key_id FROM meta.table_keys ORDER BY key_id"
        ).fetchall()
        again = conn.exec_driver_sql(
            "SELECT DISTINCT key_id FROM meta.table_keys ORDER BY key_id"
        ).fetchall()
        assert ids == again


@pytest.mark.parametrize(
    ("real_call", "form_call"),
    (
        ("obj_description('sales.orders'::regclass, 'pg_class')", f"{table_oid(3)}, 'pg_class'"),
        ("obj_description('public.customers'::regclass)", str(table_oid(1))),
        # No description is no row.
        ("obj_description('sales.regions'::regclass, 'pg_class')", f"{table_oid(2)}, 'pg_class'"),
    ),
)
def test_a_relation_comment_is_the_real_one(live, real_call, form_call):
    with live.begin() as conn:
        assert _form(conn, f"obj_description({form_call})") == _real(conn, real_call)


def test_a_comment_is_the_one_every_catalog_surface_writes(live):
    with live.begin() as conn:
        table = _one(conn, "SELECT comment FROM meta.table_details WHERE id = 3")
        column = _one(
            conn,
            "SELECT comment FROM meta.column_details WHERE table_id = 3 AND column_name = 'region'",
        )
    assert table == append_modeling_tag("The orders", "fact", "scd2", ["pii", "pii-b"])
    assert column == column_comment("Where it shipped", "fact", ["geo"])


@pytest.mark.parametrize(("attnum", "real_column"), ((4, "number"), (3, "region"), (1, "id")))
def test_a_column_comment_is_the_real_one(live, attnum, real_column):
    with live.begin() as conn:
        real_attnum = _one(
            conn,
            "SELECT attnum FROM pg_attribute WHERE attrelid = 'sales.orders'::regclass "
            f"AND attname = '{real_column}'",
        )
        expected = _real(conn, f"col_description('sales.orders'::regclass, {real_attnum})")
        assert _form(conn, f"col_description({table_oid(3)}, {attnum})") == expected


def test_a_schema_comment_comes_from_its_domain(live):
    with live.begin() as conn:
        sales_oid = _one(conn, "SELECT oid FROM meta.catalog_pg_namespace WHERE nspname = 'sales'")
        assert _form(conn, f"obj_description({sales_oid}, 'pg_namespace')") == "The sales domain"


@pytest.mark.parametrize(
    ("relation", "table_id"), (("sales.orders", 3), ("sales.regions", 2), ("public.customers", 1))
)
@pytest.mark.parametrize("privilege", ("SELECT", "INSERT", "select, insert", "UPDATE"))
def test_a_table_privilege_is_the_real_one(live, relation, table_id, privilege):
    with live.begin() as conn:
        real_relation = f"'{relation}'"
        expected = _real(conn, f"has_table_privilege('analyst', {real_relation}, '{privilege}')")
        by_name = _form(conn, f"has_table_privilege('{relation}', '{privilege}')")
        by_oid = _form(conn, f"has_table_privilege({table_oid(table_id)}, '{privilege}')")
        for_user = _form(
            conn, f"has_table_privilege('analyst', {table_oid(table_id)}, '{privilege}')"
        )
        assert by_name == by_oid == for_user == expected


def test_a_table_it_cannot_read_is_false_and_one_it_cannot_list_is_unknown(live):
    with live.begin() as conn:
        assert _form(conn, "has_table_privilege('sales.regions', 'SELECT')") is False
        assert _form(conn, "has_table_privilege(99999999, 'SELECT')") is None
        assert _form(conn, f"has_table_privilege('someone', {table_oid(3)}, 'SELECT')") is None


def test_visibility_of_a_relation_is_what_the_catalog_lists(live):
    with live.begin() as conn:
        assert _form(conn, f"pg_table_is_visible({table_oid(1)})") is True
        assert _form(conn, "pg_table_is_visible(1259)") is _real(conn, "pg_table_is_visible(1259)")
        assert _form(conn, "pg_table_is_visible(99999999)") is None
        assert _real(conn, "pg_table_is_visible(99999999)") is None
        assert _form(conn, f"pg_relation_is_publishable({table_oid(1)})") is True
        assert _form(conn, "pg_relation_is_publishable(99999999)") is False


def test_an_expression_is_its_text(live):
    with live.begin() as conn:
        for call in ("pg_get_expr('now()', 16385)", "pg_get_expr('1'::pg_node_tree, 16385, true)"):
            assert _form(conn, call) in ("now()", "1")


_TYPED = """
    CREATE TEMPORARY TABLE typed (
        a varchar(10), b char(3), c numeric(10, 2), d numeric(5, -2), e numeric(6), f bit(8),
        g bit varying(16), h time(3), i time(0) with time zone, j timestamp(6),
        k timestamp(2) with time zone, l interval(3), m interval year, n interval day to second(2),
        o interval hour to minute, p interval, q varchar(10)[], r numeric(4, 1)[], s char, t "char",
        u text, v varchar, w timestamp, x interval month, y interval second(1),
        z interval year to month
    ) ON COMMIT DROP
"""


def _value_form(name: str) -> tuple[tuple[str, ...], str]:
    return next((params, form) for n, params, form in VALUE_FORMS if n == name)


@pytest.mark.parametrize("typmod", ("NULL", "-1"))
def test_format_type_names_every_type_as_the_real_one(live, typmod):
    params, form = _value_form("format_type")
    assert params == ("type_oid", "typmod")
    with live.begin() as conn:
        wrong = conn.exec_driver_sql(
            f"SELECT type_oid, real, form FROM (SELECT oid::int AS type_oid, "
            f"format_type(oid, {typmod}) AS real, ({form}) AS form "
            f"FROM (SELECT oid, {typmod}::int AS typmod, oid::int AS type_oid FROM pg_type "
            "WHERE oid < 10000) t) s WHERE real IS DISTINCT FROM form"
        ).fetchall()
    assert wrong == []


def test_format_type_writes_every_typmod_as_the_real_one(live):
    _, form = _value_form("format_type")
    with live.begin() as conn:
        conn.exec_driver_sql(_TYPED)
        wrong = conn.exec_driver_sql(
            f"SELECT attname, real, form FROM (SELECT attname, "
            f"format_type(atttypid, atttypmod) AS real, ({form}) AS form "
            "FROM (SELECT attname, atttypid, atttypmod, atttypid::int AS type_oid, "
            "atttypmod AS typmod FROM pg_attribute WHERE attrelid = 'typed'::regclass "
            "AND attnum > 0) t) s WHERE real IS DISTINCT FROM form"
        ).fetchall()
    assert wrong == []


def test_format_type_answers_an_oid_naming_no_type_as_the_real_one(live):
    _, form = _value_form("format_type")
    with live.begin() as conn:
        for oid, typmod in ((999999, "-1"), (999999, "NULL"), ("NULL", "-1")):
            bound = form.replace("type_oid", f"{oid}::int").replace("typmod", f"{typmod}::int")
            assert _one(conn, f"SELECT {bound}") == _one(
                conn, f"SELECT format_type({oid}::oid, {typmod}::int)"
            )


def test_an_encoding_is_named_as_the_real_one(live):
    _, form = _value_form("pg_encoding_to_char")
    with live.begin() as conn:
        wrong = conn.exec_driver_sql(
            f"SELECT encoding, real, form FROM (SELECT encoding, pg_encoding_to_char(encoding) "
            f"AS real, ({form}) AS form FROM generate_series(-1, 50) AS g(encoding)) s "
            "WHERE real IS DISTINCT FROM form"
        ).fetchall()
    assert wrong == []
