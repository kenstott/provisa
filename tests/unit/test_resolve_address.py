# Copyright (c) 2026 Kenneth Stott
# Canary: 9d3a5e72-1c4b-4f08-a6d9-7b2e0c8f5a13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1365: a semantic address is parsed, checked against the role's schema, and lowered to ONE
bound-parameter statement that the settled pipeline governs and runs."""

# Requirements: REQ-1365, REQ-1385, REQ-1929

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.data import resolve as resolve_mod
from provisa.api.data.resolve import (
    AddressError,
    AddressNotFound,
    SemanticAddress,
    build_statement,
    parse_address,
    resolve,
)


def _ctx(pk=("id",)):
    meta = SimpleNamespace(
        table_id=7,
        domain_id="sales",
        display_name="",
        original_table_name="",
        table_name="orders",
        field_name="sales__orders",
    )
    other = SimpleNamespace(
        table_id=8,
        domain_id="hr",
        display_name="People",
        original_table_name="",
        table_name="employees",
        field_name="hr__people",
    )
    return SimpleNamespace(
        tables={"sales__orders": meta, "sales__orders_aggregate": meta, "hr__people": other},
        pk_columns={7: list(pk), 8: []},
        physical_to_sql={(7, "id"): "id", (7, "amount"): "amount", (7, "region"): "region"},
    )


def test_a_row_and_column_address_parses():
    assert parse_address(
        "provisa://acme/sales/tables/orders/pk=7/amount", multitenant=True
    ) == SemanticAddress("acme", "sales", "orders", ("7",), "amount")


def test_a_single_tenant_address_carries_no_org():
    assert parse_address("provisa://sales/tables/orders/pk=7", multitenant=False) == (
        SemanticAddress(None, "sales", "orders", ("7",), None)
    )


def test_a_composite_key_and_encoded_values_parse():
    address = parse_address(
        "provisa://acme/sales/emea/tables/order%20lines/pk=7,a%2Fb%2Cc", multitenant=True
    )
    assert address.domain_id == "sales/emea"
    assert address.table == "order lines"
    assert address.key == ("7", "a/b,c")


def test_the_field_fragment_names_the_column():
    address = parse_address(
        "provisa://acme/sales/tables/orders/pk=7#field:amount", multitenant=True
    )
    assert address.column == "amount"


@pytest.mark.parametrize(
    "uri",
    [
        "postgresql://db:5432/sales/public/orders#amount",
        "s3://bucket/orders.parquet",
        "wh.public.orders",
        "https://example.test/sales/tables/orders/pk=7",
    ],
)
def test_a_physical_address_is_refused(uri):
    with pytest.raises(AddressError, match="physical address is refused"):
        parse_address(uri, multitenant=True)


@pytest.mark.parametrize(
    "uri",
    [
        "provisa://acme/sales/tables/orders",
        "provisa://acme/sales/tables/orders/amount",
        "provisa://acme/sales/tables/orders/pk=",
        "provisa://acme/sales/tables/orders/pk=7/amount/extra",
        "provisa://acme/sales/tables/orders/pk=7/amount#field:region",
        "provisa://acme/sources/wh",
        "provisa://acme/sales//tables/orders/pk=7",
    ],
)
def test_an_address_that_names_no_row_is_refused(uri):
    with pytest.raises(AddressError):
        parse_address(uri, multitenant=True)


def test_the_statement_selects_by_key_with_bound_values():
    address = SemanticAddress("acme", "sales", "orders", ("7'; DROP TABLE x; --",), "amount")
    statement = build_statement("u", address, _ctx(), as_of=None, bitemporal=set())
    assert statement.sql == 'SELECT "amount" FROM "sales"."orders" WHERE "id" = $1'
    assert statement.params == ["7'; DROP TABLE x; --"]


def test_no_column_selects_the_whole_row_and_a_composite_key_binds_in_order():
    address = SemanticAddress("acme", "sales", "orders", ("7", "emea"), None)
    statement = build_statement(
        "u", address, _ctx(pk=("id", "region")), as_of=None, bitemporal=set()
    )
    assert statement.sql == ('SELECT * FROM "sales"."orders" WHERE "id" = $1 AND "region" = $2')
    assert statement.params == ["7", "emea"]


def test_a_table_the_role_cannot_see_is_not_found():
    address = SemanticAddress("acme", "finance", "ledger", ("1",), None)
    with pytest.raises(AddressNotFound, match="names nothing visible"):
        build_statement("u", address, _ctx(), as_of=None, bitemporal=set())


def test_a_hidden_column_reads_the_same_as_an_absent_one():
    address = SemanticAddress("acme", "sales", "orders", ("1",), "margin")
    with pytest.raises(AddressNotFound, match="names nothing visible"):
        build_statement("u", address, _ctx(), as_of=None, bitemporal=set())


def test_a_table_is_found_by_its_alias():
    address = SemanticAddress("acme", "hr", "People", ("1",), None)
    with pytest.raises(AddressError, match="no primary key"):
        build_statement("u", address, _ctx(), as_of=None, bitemporal=set())


def test_the_wrong_number_of_key_values_is_refused_by_name():
    address = SemanticAddress("acme", "sales", "orders", ("7", "8"), None)
    with pytest.raises(AddressError, match=r"keyed by 1 column\(s\) \(id\)"):
        build_statement("u", address, _ctx(), as_of=None, bitemporal=set())


def test_as_of_on_a_table_with_no_history_is_refused_by_name():
    address = SemanticAddress("acme", "sales", "orders", ("7",), None)
    with pytest.raises(AddressError, match="keeps no history"):
        build_statement("u", address, _ctx(), as_of="TIMESTAMP '2026-01-01'", bitemporal=set())
    build_statement("u", address, _ctx(), as_of="TIMESTAMP '2026-01-01'", bitemporal={"orders"})


# -- the one function runs the one pipeline --------------------------------------------------------


RESOLVER = "m__resolve"  # meta's resolve, where domains prefix their fields (meta's alias "m")


def _schema():
    """A role's schema as far as the resolver is concerned: meta's resolve and a table."""
    from graphql import GraphQLField, GraphQLList, GraphQLObjectType, GraphQLSchema, GraphQLString

    from provisa.compiler.schema_gen import _add_resolve_field

    fields = {"orders": GraphQLField(GraphQLList(GraphQLString))}
    _add_resolve_field(fields, RESOLVER)
    return GraphQLSchema(query=GraphQLObjectType("Query", fields))


def _state():
    return SimpleNamespace(
        multitenancy=True,
        surfaces={"analyst": SimpleNamespace(context=_ctx(), schema=_schema())},
        bitemporal_view_reads={},
    )


@pytest.fixture
def pipeline(monkeypatch):
    import provisa.pgwire._pipeline as pipe

    calls: list[dict] = []

    async def _govern(sql, role_id, *, session_vars=None, as_of=None, params=None):
        calls.append({"sql": sql, "role": role_id, "as_of": as_of, "params": params})
        return "plan"

    async def _execute(plan, state=None):
        assert plan == "plan"
        return SimpleNamespace(column_names=["amount"], rows=calls[0].get("rows", [(12.5,)]))

    monkeypatch.setattr(pipe, "_govern_and_route", _govern)
    monkeypatch.setattr(pipe, "_execute_plan", _execute)
    return calls


async def test_resolve_governs_and_runs_one_statement(pipeline):
    result = await resolve(
        _state(), "analyst", "provisa://acme/sales/tables/orders/pk=7/amount", org_id="acme"
    )
    assert pipeline == [
        {
            "sql": 'SELECT "amount" FROM "sales"."orders" WHERE "id" = $1',
            "role": "analyst",
            "as_of": None,
            "params": ["7"],
        }
    ]
    assert result == {
        "uri": "provisa://acme/sales/tables/orders/pk=7/amount",
        "as_of": None,
        "columns": ["amount"],
        "row": {"amount": 12.5},
    }


async def test_an_address_in_another_org_is_refused_before_anything_runs(pipeline):
    with pytest.raises(AddressError, match="another organization"):
        await resolve(
            _state(), "analyst", "provisa://globex/sales/tables/orders/pk=7", org_id="acme"
        )
    assert pipeline == []


async def test_a_malformed_as_of_is_refused(pipeline):
    with pytest.raises(AddressError, match="invalid as_of"):
        await resolve(
            _state(),
            "analyst",
            "provisa://acme/sales/tables/orders/pk=7",
            org_id="acme",
            as_of="yesterday; DROP",
        )
    assert pipeline == []


async def test_an_unknown_role_is_refused(pipeline):
    with pytest.raises(PermissionError):
        await resolve(_state(), "nobody", "provisa://acme/sales/tables/orders/pk=7", org_id="acme")


# -- the surfaces are the GraphQL field's (REQ-1929) ----------------------------------------------


def test_every_roles_graphql_schema_offers_meta_s_resolve_and_no_root_resolver():
    from provisa.compiler.schema_gen import resolve_field
    from tests.unit.test_sql_gen_aggregate import _fact_schema_and_ctx

    schema, _ = _fact_schema_and_ctx()
    assert "_resolve" not in schema.query_type.fields
    field = schema.query_type.fields[resolve_field(schema)]
    assert set(field.args) == {"uri", "as_of", "as_recorded"}
    assert str(field.args["uri"].type) == "String!"


@pytest.mark.parametrize(
    ("domain_prefix", "aliases", "name"),
    [
        (True, {"meta": "m"}, "m__resolve"),
        (True, {}, "meta__resolve"),
        (False, {"meta": "m"}, "resolve"),
    ],
)
def test_the_resolver_is_named_as_a_field_of_the_meta_domain(domain_prefix, aliases, name):
    from provisa.compiler.schema_gen import resolve_field_name

    assert resolve_field_name(domain_prefix, aliases) == name


def test_resolve_selections_split_a_document():
    from graphql import parse

    document = parse('{ a: m__resolve(uri: "provisa://x") orders { id } }')
    found, others = resolve_mod.resolve_selections(document, RESOLVER)
    assert [s.alias.value for s in found] == ["a"]
    assert others == ["orders"]


# -- each surface hands the address to the one function -------------------------------------------


@pytest.fixture
def resolved(monkeypatch, bind_org):
    """``resolve`` replaced by a recorder; the request bound to org acme."""
    calls: list[dict] = []

    async def _resolve(state, role_id, uri, *, org_id, as_of=None, as_recorded=None):
        calls.append({"role": role_id, "uri": uri, "org": org_id, "as_of": as_of})
        return {"uri": uri, "row": {"amount": 1}}

    monkeypatch.setattr(resolve_mod, "resolve", _resolve)
    bind_org("acme")
    return calls


async def test_the_graphql_field_answers_with_the_bound_org_at_its_own_as_of(resolved):
    import json

    from graphql import parse

    from provisa.api.data.endpoint import _handle_query

    document = parse(
        '{ a: m__resolve(uri: "provisa://acme/sales/tables/orders/pk=7", as_of: "2026-01-01") }'
    )
    response = await _handle_query(
        document,
        None,
        _state(),
        None,
        {"id": "analyst"},
        role_id="analyst",
        cache_ttl=None,
        cache_opt_in=False,
        debug_trace=None,
    )
    assert json.loads(response.body) == {
        "data": {"a": {"uri": "provisa://acme/sales/tables/orders/pk=7", "row": {"amount": 1}}}
    }
    assert resolved == [
        {
            "role": "analyst",
            "uri": "provisa://acme/sales/tables/orders/pk=7",
            "org": "acme",
            "as_of": "2026-01-01",
        }
    ]


async def test_resolve_mixed_with_another_root_field_is_refused(resolved):
    from graphql import parse

    from provisa.api.data.endpoint import _handle_query
    from provisa.api.errors import ApiError

    document = parse('{ m__resolve(uri: "provisa://x") orders { id } }')
    with pytest.raises(ApiError) as refused:
        await _handle_query(
            document,
            None,
            _state(),
            None,
            {"id": "analyst"},
            role_id="analyst",
            cache_ttl=None,
            cache_opt_in=False,
            debug_trace=None,
        )
    assert refused.value.status_code == 400
    assert resolved == []


def test_rest_answers_under_meta_with_the_bound_org(resolved):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from provisa.api.rest.generator import create_rest_router
    from provisa.core.request_context import reset_current_org, set_current_org

    app = FastAPI()

    @app.middleware("http")
    async def _auth(request, call_next):
        request.state.role = "analyst"
        token = set_current_org("acme")  # what the auth middleware binds
        try:
            return await call_next(request)
        finally:
            reset_current_org(token)

    app.include_router(create_rest_router(_state()))
    uri = "provisa://acme/sales/tables/orders/pk=7"
    response = TestClient(app).get(
        "/data/rest/meta/resolve", params={"uri": uri, "as_of": "2026-01-01"}
    )
    assert response.status_code == 200, response.text
    assert response.json() == {"uri": uri, "row": {"amount": 1}}
    assert [(c["role"], c["org"]) for c in resolved] == [("analyst", "acme")]


async def test_the_mcp_tool_answers_under_meta_with_the_bound_org(resolved, monkeypatch):
    import json

    from provisa.api.mcp import server
    from tests.unit.test_mcp_server import _make_state

    token = server._request_role.set("analyst")  # what the bearer token resolves to
    try:
        content = await server.build_mcp_server(_make_state()).call_tool(
            "meta_resolve", {"uri": "provisa://acme/sales/tables/orders/pk=7"}
        )
    finally:
        server._request_role.reset(token)
    blocks = content[0] if isinstance(content, tuple) else content
    assert json.loads(blocks[0].text)["row"] == {"amount": 1}
    assert [(c["role"], c["org"]) for c in resolved] == [("analyst", "acme")]
