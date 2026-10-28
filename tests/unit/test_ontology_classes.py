# Copyright (c) 2026 Kenneth Stott
# Canary: 3f84dbfc-dfe5-40e7-8c3e-291a9af801d1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2163: each Class term is a table of the ontology schema.

The table is the union of the tables the Class grounds, one row per instance, with
``_source_table`` naming the table it comes from. Each Property bound to a column of one of those
tables is a column: null on the rows of a table that does not bind it, a list where a table binds
several columns, and of the bound columns' common supertype. A reader is served a Class column
where it is served one of the columns it is bound to, and reads the Class through a body made for
it, governed as any view is. A Class that cannot be offered is refused by itself.
"""

# Requirements: REQ-2163

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from provisa.compiler.ontology_classes import (
    SOURCE_TABLE_COLUMN,
    class_table_id,
    class_tables,
    reader_body,
    served_class_columns,
    whole_body,
)
from provisa.compiler.supertype import common_supertype

CUSTOMERS, CLIENTS, ORDERS = 1, 2, 3
CUSTOMER, NAME, EMAIL, TIER, ORDER = 10, 11, 12, 13, 14
_TERMS = {
    CUSTOMER: "customer",
    NAME: "customer name",
    EMAIL: "email address",
    TIER: "loyalty tier",
    ORDER: "order",
}


def _col(name: str, data_type: str, visible_to: list[str] | None = None) -> dict:
    # REQ-2154: a grant names its roles; an empty one is no role.
    return {
        "column_name": name,
        "data_type": data_type,
        "visible_to": ["analyst", "clerk"] if visible_to is None else visible_to,
    }


def _tables() -> list[dict]:
    return [
        {
            "id": CUSTOMERS,
            "source_id": "pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "customers",
            "columns": [
                _col("id", "integer"),
                _col("name", "varchar(80)"),
                _col("email", "varchar", ["analyst"]),
            ],
        },
        {
            "id": CLIENTS,
            "source_id": "pg",
            "domain_id": "crm",
            "schema_name": "public",
            "table_name": "clients",
            "columns": [
                _col("id", "bigint"),
                _col("full_name", "text"),
                _col("mail1", "varchar"),
                _col("mail2", "varchar"),
                _col("tier", "varchar"),
            ],
        },
        {
            "id": ORDERS,
            "source_id": "pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "columns": [_col("id", "integer"), _col("total", "decimal(10,2)")],
        },
    ]


_GROUNDINGS = {CUSTOMERS: CUSTOMER, CLIENTS: CUSTOMER, ORDERS: ORDER}
_REFS = [
    (NAME, CUSTOMERS, "name"),
    (NAME, CLIENTS, "full_name"),
    (EMAIL, CUSTOMERS, "email"),
    (EMAIL, CLIENTS, "mail1"),
    (EMAIL, CLIENTS, "mail2"),
    (TIER, CLIENTS, "tier"),
]


def _customer(terms=_TERMS, refs=_REFS) -> dict:
    rows, refused = class_tables(_tables(), _GROUNDINGS, terms, refs)
    assert refused == {}
    return next(r for r in rows if r["table_name"] == "customer")


# --- the Class table -----------------------------------------------------------------------------


def test_each_class_is_a_table_of_the_ontology_schema() -> None:
    rows, refused = class_tables(_tables(), _GROUNDINGS, _TERMS, _REFS)
    assert refused == {}
    assert [(r["id"], r["domain_id"], r["table_name"]) for r in rows] == [
        (class_table_id(CUSTOMER), "ontology", "customer"),
        (class_table_id(ORDER), "ontology", "order"),
    ]
    assert all(r["id"] < 0 for r in rows)


def test_its_columns_are_the_source_table_and_one_per_bound_property() -> None:
    columns = {c["column_name"]: c for c in _customer()["columns"]}
    assert list(columns) == [SOURCE_TABLE_COLUMN, "customer_name", "email_address", "loyalty_tier"]
    # varchar(80) and text: varchar. A table binding two columns makes the column a list.
    assert columns["customer_name"]["data_type"] == "varchar"
    assert columns["email_address"]["data_type"] == "array(varchar)"
    assert (columns["email_address"]["array_depth"], columns["email_address"]["element_type"]) == (
        1,
        "varchar",
    )
    assert columns["loyalty_tier"]["data_type"] == "varchar"


def test_the_whole_body_is_the_union_of_its_tables() -> None:
    body = whole_body(_customer())
    branches = body.split(" UNION ALL ")
    assert len(branches) == 2
    assert branches[0] == (
        'SELECT \'sales.customers\' AS "_source_table", CAST("name" AS varchar) AS '
        '"customer_name", ARRAY["email"] AS "email_address", CAST(NULL AS varchar) AS '
        '"loyalty_tier" FROM sales.customers'
    )
    assert branches[1] == (
        'SELECT \'crm.clients\' AS "_source_table", CAST("full_name" AS varchar) AS '
        '"customer_name", ARRAY["mail1", "mail2"] AS "email_address", "tier" AS '
        '"loyalty_tier" FROM crm.clients'
    )


def test_a_class_with_no_bound_property_has_only_the_source_table() -> None:
    rows, _ = class_tables(_tables(), _GROUNDINGS, _TERMS, _REFS)
    order = next(r for r in rows if r["table_name"] == "order")
    assert [c["column_name"] for c in order["columns"]] == [SOURCE_TABLE_COLUMN]


def test_a_retired_class_or_property_is_not_offered() -> None:
    in_service = {k: v for k, v in _TERMS.items() if k not in (ORDER, TIER)}
    rows, _ = class_tables(_tables(), _GROUNDINGS, in_service, _REFS)
    assert [r["table_name"] for r in rows] == ["customer"]
    assert "loyalty_tier" not in {c["column_name"] for c in rows[0]["columns"]}


def test_a_class_that_cannot_be_offered_is_refused_alone() -> None:
    """Two Properties naming one column refuse their Class, naming both; the others are served."""
    terms = {**_TERMS, 15: "Customer-Name"}
    refs = [*_REFS, (15, CLIENTS, "id")]
    rows, refused = class_tables(_tables(), _GROUNDINGS, terms, refs)
    assert [r["table_name"] for r in rows] == ["order"]
    assert set(refused) == {CUSTOMER}
    assert "'customer name'" in refused[CUSTOMER] and "'Customer-Name'" in refused[CUSTOMER]


def test_a_property_naming_the_source_tables_field_refuses_its_class() -> None:
    """A field name drops the underscore: a Property "source table" would be _source_table's."""
    terms = {**_TERMS, 15: "source table"}
    rows, refused = class_tables(_tables(), _GROUNDINGS, terms, [*_REFS, (15, CLIENTS, "id")])
    assert [r["table_name"] for r in rows] == ["order"]
    assert "'source table'" in refused[CUSTOMER]


def test_a_name_that_gives_no_identifier_refuses_its_class() -> None:
    rows, refused = class_tables(_tables(), _GROUNDINGS, {**_TERMS, ORDER: "2nd order"}, _REFS)
    assert [r["table_name"] for r in rows] == ["customer"]
    assert "gives no table name" in refused[ORDER]


def test_two_classes_naming_one_table_refuse_the_second() -> None:
    terms = {**_TERMS, ORDER: "Customer"}
    rows, refused = class_tables(_tables(), _GROUNDINGS, terms, _REFS)
    assert [r["table_name"] for r in rows] == ["customer"]
    assert "both name the table 'customer'" in refused[ORDER]


# --- read rights ---------------------------------------------------------------------------------


def test_a_reader_is_served_a_class_column_where_it_is_served_a_bound_column() -> None:
    row = _customer()
    everything = {CUSTOMERS: None, CLIENTS: None}
    assert served_class_columns(row, everything.__getitem__) == {
        SOURCE_TABLE_COLUMN,
        "customer_name",
        "email_address",
        "loyalty_tier",
    }
    no_crm = {CUSTOMERS: frozenset({"id", "name"}), CLIENTS: frozenset()}
    assert served_class_columns(row, no_crm.__getitem__) == {SOURCE_TABLE_COLUMN, "customer_name"}
    nothing = {CUSTOMERS: frozenset(), CLIENTS: frozenset()}
    assert served_class_columns(row, nothing.__getitem__) == frozenset()


def test_a_readers_body_reads_only_what_it_is_served() -> None:
    """No branch for a table it is served nothing of; null for a column it is not served."""
    body = reader_body(
        _customer(), {CUSTOMERS: frozenset({"id", "name"}), CLIENTS: frozenset()}.__getitem__
    )
    assert body == (
        'SELECT \'sales.customers\' AS "_source_table", CAST("name" AS varchar) AS '
        '"customer_name", CAST(NULL AS varchar[]) AS "email_address", CAST(NULL AS varchar) AS '
        '"loyalty_tier" FROM sales.customers'
    )
    partly = reader_body(
        _customer(),
        {CUSTOMERS: frozenset(), CLIENTS: frozenset({"mail2", "full_name"})}.__getitem__,
    )
    assert 'ARRAY["mail2"] AS "email_address"' in partly and "mail1" not in partly


def test_a_reader_served_none_of_its_tables_reads_no_rows() -> None:
    body = reader_body(_customer(), {CUSTOMERS: frozenset(), CLIENTS: frozenset()}.__getitem__)
    assert body.endswith("WHERE 1 = 0") and "FROM" not in body


# --- the common supertype ------------------------------------------------------------------------


@pytest.mark.parametrize(
    "types, expected",
    [
        (["integer", "INTEGER"], "integer"),
        (["integer", "bigint"], "bigint"),
        (["int2", "int4"], "integer"),
        (["date", "timestamp"], "timestamp"),
        (["timestamp(3)", "timestamp(6)", "date"], "timestamp(6)"),
        (["decimal(10,2)", "decimal(12,4)"], "decimal(12,4)"),
        (["decimal(10,2)", "bigint"], "decimal(21,2)"),
        (["integer", "double"], "double"),
        (["real", "float4"], "real"),
        (["varchar(80)", "text"], "varchar"),
        (["integer", "varchar"], "varchar"),
        (["date", "boolean"], "varchar"),
        (["uuid", "uuid"], "uuid"),
    ],
)
def test_bound_columns_of_differing_types_take_their_common_supertype(types, expected) -> None:
    assert common_supertype(types) == expected


# --- the surfaces --------------------------------------------------------------------------------


@pytest.fixture
def built(monkeypatch):
    """The real role build over customers and clients and their Class, for two readers: the
    analyst, granted sales and crm, and the clerk, granted sales only."""
    import provisa.api.app as appmod
    from provisa.api import app_loaders
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.compiler.sql_rewrite import lower_to_catalog_physical
    from provisa.core import domain_policy

    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)
    state = appmod.state
    for name in ("roles", "schemas", "contexts", "rls_contexts", "table_path_maps", "proto_files"):
        monkeypatch.setattr(state, name, {}, raising=False)
    monkeypatch.setattr(state, "graphql_remote_sources", {}, raising=False)
    # The derived source's type is the deployment's engine (startup_seed._seed_built_in_sources).
    monkeypatch.setattr(
        state, "source_types", {"pg": "postgresql", "__derived__": "postgresql"}, raising=False
    )
    monkeypatch.setattr(state, "source_catalogs", {"pg": "pg"}, raising=False)
    tables = [t for t in _tables() if t["id"] != ORDERS]
    for t in tables:
        t.update(write_ops=[], write_returns_rows=False, write_refused_forms=[])
        t["governance"] = "pre-approved"
    rows, _ = class_tables(tables, _GROUNDINGS, _TERMS, _REFS)
    for r in rows:
        r.update(governance="pre-approved", write_ops=[], write_returns_rows=False)
        r["write_refused_forms"] = []
    tables = [*tables, *rows]
    app_loaders._build_and_register_schemas(
        roles=[
            {
                "id": "analyst",
                "description": "t",
                "capabilities": ["usage"],
                "domain_access": ["sales", "crm"],
            },
            {
                "id": "clerk",
                "description": "t",
                "capabilities": ["usage"],
                "domain_access": ["sales"],
            },
        ],
        tables=tables,
        relationships=[],
        col_types_converted={
            t["id"]: [
                ColumnMetadata(
                    column_name=c["column_name"], data_type=c["data_type"], is_nullable=True
                )
                for c in t["columns"]
            ]
            for t in tables
        },
        naming_rules=[],
        domains=[{"id": "sales"}, {"id": "crm"}, {"id": "ontology"}],
        domain_prefix=False,
        kafka_physical={},
        tracked_functions=[],
        tracked_webhooks=[],
        gql_object_cols={},
        rls_rules=[],
        metrics=[],
        draft_tables=[],
    )
    monkeypatch.setattr(state, "tables", tables, raising=False)
    monkeypatch.setattr(state, "class_tables", {r["table_name"]: r for r in rows}, raising=False)
    monkeypatch.setattr(state, "class_bodies", {}, raising=False)
    monkeypatch.setattr(
        state,
        "view_sql_map",
        {
            r["table_name"]: lower_to_catalog_physical(whole_body(r), state.view_context)
            for r in rows
        },
        raising=False,
    )
    return state


def test_the_class_is_a_graphql_type_with_the_columns_its_reader_is_served(built) -> None:
    from graphql import GraphQLObjectType

    from provisa.api.role_surface import role_surface

    def fields(role: str) -> set[str]:
        schema = role_surface(built, role).schema
        found = [
            t
            for name, t in schema.type_map.items()
            if isinstance(t, GraphQLObjectType) and "customerName" in t.fields
        ]
        assert [t.name for t in found] == ["Customer"]
        return set(found[0].fields)

    assert {"customerName", "emailAddress", "loyaltyTier", "sourceTable"} <= fields("analyst")
    clerk = fields("clerk")
    assert "loyaltyTier" not in clerk and "customerName" in clerk


async def _govern(state, sql: str, role: str) -> str:
    from provisa.pgwire import _pipeline
    from provisa.transpiler.router import Route, RouteDecision

    decision = RouteDecision(route=Route.DIRECT, source_id="pg", dialect="postgres", reason="t")

    async def _route(exec_sql, governed_sql, gov_ctx, ctx, state, **kwargs):
        return exec_sql, decision, False, {"pg"}, ()

    with (
        patch.object(_pipeline, "_optimize_and_route", side_effect=_route),
        patch("provisa.audit.pipeline.write_audit", AsyncMock(return_value=None)),
    ):
        return (await _pipeline._govern_and_route(sql, role)).sql


async def test_sql_reads_the_union_for_a_reader_of_both_tables(built) -> None:
    governed = await _govern(built, "SELECT * FROM ontology.customer", "analyst")
    assert governed.count("UNION ALL") == 1
    assert "mail1" in governed and "full_name" in governed


async def test_sql_reads_only_the_tables_its_reader_is_served(built) -> None:
    governed = await _govern(built, "SELECT * FROM ontology.customer", "clerk")
    assert "UNION ALL" not in governed
    assert "clients" not in governed and "customers" in governed
    # A column the reader is not served is not projected by the Class's own rules, as for any
    # view: the clerk is served the name only.
    assert '"customer"."customer_name"' in governed
    assert (
        '"customer"."loyalty_tier"' not in governed and '"customer"."email_address"' not in governed
    )


# --- the build -----------------------------------------------------------------------------------


class _Rows:
    """The control plane's answers, in the order the build asks: groundings, terms, bindings."""

    def __init__(self, *answers: list) -> None:
        self._answers = list(answers)

    async def execute_core(self, _stmt):
        rows = self._answers.pop(0)
        return SimpleNamespace(fetchall=lambda: rows)


async def test_the_build_serves_each_class_and_keeps_why_one_is_refused(
    monkeypatch, caplog
) -> None:
    import provisa.api.app as appmod
    from provisa.api._ontology_classes import register_class_tables

    state = SimpleNamespace(view_sql_map={"a_view": "SELECT 1"})
    monkeypatch.setattr(appmod, "state", state, raising=False)
    terms = {**_TERMS, ORDER: "2nd order"}
    conn = _Rows(
        [SimpleNamespace(id=t, class_term_id=c) for t, c in _GROUNDINGS.items()],
        [SimpleNamespace(id=i, name=n) for i, n in terms.items()],
        [SimpleNamespace(term_id=t, table_id=i, column_name=c) for t, i, c in _REFS],
    )
    tables = _tables()
    await register_class_tables(conn, tables)
    assert [t["table_name"] for t in tables][-1] == "customer" and len(tables) == 4
    assert set(state.view_sql_map) == {"a_view", "customer"}
    assert state.view_sql_map["customer"] == whole_body(state.class_tables["customer"])
    assert list(state.refused_classes) == [ORDER]
    assert any(r.levelname == "ERROR" and "2nd order" in r.getMessage() for r in caplog.records)


def test_a_terms_api_row_says_why_its_class_is_not_offered(monkeypatch) -> None:
    import provisa.api.app as appmod
    from provisa.api.admin.glossary_router import _with_class_refusal

    monkeypatch.setattr(
        appmod, "state", SimpleNamespace(refused_classes={ORDER: "why"}), raising=False
    )
    assert _with_class_refusal({"id": ORDER})["class_refused"] == "why"
    assert _with_class_refusal({"id": CUSTOMER})["class_refused"] is None


def test_in_cypher_the_class_is_a_label_a_reader_may_match(built) -> None:
    """A role granted no ontology domain matches a Class as a root: no relationship reaches it,
    and its rows are read under its tables' read rights."""
    from provisa.api.role_surface import role_surface
    from provisa.gql.label_map import CypherLabelMap

    ctx = role_surface(built, "clerk").context
    label_map = CypherLabelMap.from_schema(ctx, domain_access=["sales"], all_tables=built.tables)
    (node,) = [nm for nm in label_map.nodes.values() if nm.table_id == class_table_id(CUSTOMER)]
    assert not node.traversal_only


# --- every surface reads the governed Class ------------------------------------------------------


class _DuckRows:
    """The pipeline's execution, answered from real tables: the governed statement each surface
    hands it runs in DuckDB over one customer and one client."""

    def __init__(self) -> None:
        import duckdb

        self.con = duckdb.connect()
        self.con.execute("CREATE SCHEMA public")
        self.con.execute("CREATE TABLE public.customers (id INT, name VARCHAR, email VARCHAR)")
        self.con.execute("INSERT INTO public.customers VALUES (1, 'Ada', 'ada@example.com')")
        self.con.execute(
            "CREATE TABLE public.clients"
            " (id BIGINT, full_name VARCHAR, mail1 VARCHAR, mail2 VARCHAR, tier VARCHAR)"
        )
        self.con.execute("INSERT INTO public.clients VALUES (2, 'Bo', 'b1@x', 'b2@x', 'gold')")

    async def execute(self, plan, _state):
        from provisa.executor.result import QueryResult
        from provisa.transpiler.transpile import transpile_to_duckdb

        cur = self.con.execute(transpile_to_duckdb(plan.sql))
        return QueryResult(
            rows=[tuple(r) for r in cur.fetchall()],
            column_names=[d[0] for d in cur.description],
        )


@pytest.fixture
def surfaces(built, monkeypatch):
    from provisa.pgwire import _pipeline
    from provisa.transpiler.router import Route, RouteDecision

    decision = RouteDecision(route=Route.DIRECT, source_id="pg", dialect="postgres", reason="t")

    async def _route(exec_sql, governed_sql, gov_ctx, ctx, state, **kwargs):
        return exec_sql, decision, False, {"pg"}, ()

    rows = _DuckRows()
    monkeypatch.setattr(_pipeline, "_optimize_and_route", _route)
    monkeypatch.setattr(_pipeline, "_execute_plan", rows.execute)
    monkeypatch.setattr("provisa.audit.pipeline.write_audit", AsyncMock(return_value=None))
    return built


async def test_rest_serves_a_readers_class_rows_from_only_the_tables_it_is_served(surfaces):
    import httpx
    from fastapi import FastAPI, Request

    from provisa.api.rest import generator as rest_generator

    app = FastAPI()

    @app.middleware("http")
    async def _role(request: Request, call_next):  # pyright: ignore[reportUnusedFunction]
        request.state.role = request.headers.get("x-provisa-role")
        return await call_next(request)

    app.include_router(rest_generator.create_rest_router(surfaces))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/data/rest/ontology/customer", headers={"x-provisa-role": "clerk"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    rows = body["data"] if isinstance(body, dict) else body
    assert [(r["sourceTable"], r["customerName"]) for r in rows] == [("sales.customers", "Ada")]


async def test_mcp_serves_a_readers_class_rows_from_only_the_tables_it_is_served(surfaces):
    from provisa.api.mcp import tools

    result = await tools.run_sql(surfaces, "clerk", "SELECT * FROM ontology.customer")
    assert [(r["_source_table"], r["customer_name"]) for r in result["rows"]] == [
        ("sales.customers", "Ada")
    ]
