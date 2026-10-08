# Copyright (c) 2026 Kenneth Stott
# Canary: 9a4f2c61-7e3b-4d05-b8a1-c6e2d9f0b374
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A source's write operations are commands registered one at a time and passed through as is
(REQ-1924).

A remote source -- OpenAPI, remote GraphQL, gRPC -- offers its write operations where a command
is added, and registers none. One the steward registers is a ``source_operation`` command whose
arguments follow from the operation, each a JSON value. A call goes to the remote unchanged with
the source's credential, and the remote's answer, a refusal included, comes back unchanged. A
write is an action: composed in a larger statement it is refused."""

from __future__ import annotations

import json
from types import SimpleNamespace

import httpx
import pytest
import respx
import sqlglot
from graphql import build_schema

from provisa.api.errors import ApiError
from provisa.api_source.models import ApiSource
from provisa.core.auth_models import ApiAuthBearer
from provisa.core.models import GraphQLRemoteConfig
from provisa.executor import source_operation as ops

SHOP = "https://shop.example"
GQL = "https://gql.example/graphql"

SPEC = {
    "openapi": "3.0.0",
    "info": {"title": "shop", "version": "1"},
    "paths": {
        "/orders": {
            "get": {
                "operationId": "listOrders",
                "responses": {
                    "200": {
                        "description": "ok",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {"id": {"type": "integer"}},
                                    },
                                }
                            }
                        },
                    }
                },
            },
            "post": {
                "operationId": "createOrder",
                "summary": "Place an order",
                "requestBody": {
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {"sku": {"type": "string"}},
                            }
                        }
                    }
                },
                "responses": {
                    "201": {
                        "description": "made",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {"id": {"type": "integer"}},
                                }
                            }
                        },
                    }
                },
            },
        },
        "/orders/{order_id}": {
            "delete": {
                "operationId": "cancelOrder",
                "responses": {"204": {"description": "gone"}},
            }
        },
    },
}

SDL = """
type Query { issues: [Issue!]! }
type Mutation {
  createIssue(input: CreateIssueInput!, dryRun: Boolean): CreateIssuePayload
  closeIssue(id: ID!): Issue
}
input CreateIssueInput { title: String! body: String }
type CreateIssuePayload { clientMutationId: String issue: Issue }
type Issue { id: ID! title: String author: User comments(first: Int!): [String] }
type User { login: String }
"""


def _type_ref(t) -> dict:
    """A GraphQL type as introspection renders it, input objects included."""
    from graphql import (
        GraphQLList,
        GraphQLNonNull,
        is_enum_type,
        is_input_object_type,
        is_scalar_type,
    )

    if isinstance(t, GraphQLNonNull):
        return {"kind": "NON_NULL", "name": None, "ofType": _type_ref(t.of_type)}
    if isinstance(t, GraphQLList):
        return {"kind": "LIST", "name": None, "ofType": _type_ref(t.of_type)}
    kind = (
        "SCALAR"
        if is_scalar_type(t)
        else "ENUM"
        if is_enum_type(t)
        else "INPUT_OBJECT"
        if is_input_object_type(t)
        else "OBJECT"
    )
    return {"kind": kind, "name": t.name, "ofType": None}


def _introspected(sdl: str) -> dict:
    schema = build_schema(sdl)
    types = []
    for name, t in schema.type_map.items():
        if name.startswith("__"):
            continue
        declared = getattr(t, "fields", None)
        fields = (
            [
                {
                    "name": fname,
                    "description": f"{fname} it",
                    "type": _type_ref(f.type),
                    "args": [
                        {"name": an, "defaultValue": None, "type": _type_ref(a.type)}
                        for an, a in (getattr(f, "args", None) or {}).items()
                    ],
                }
                for fname, f in declared.items()
            ]
            if declared is not None
            else None
        )
        types.append({"kind": _type_ref(t)["kind"], "name": name, "fields": fields})
    return {"queryType": {"name": "Query"}, "mutationType": {"name": "Mutation"}, "types": types}


def _grpc_mutation(method: str):
    return SimpleNamespace(
        service="Orders",
        method=method,
        full_method_path=f"/shop.Orders/{method}",
        input_message=f"{method}Request",
        output_message=f"{method}Reply",
        input_fields=[SimpleNamespace(name="sku"), SimpleNamespace(name="qty")],
    )


@pytest.fixture
def state(monkeypatch) -> SimpleNamespace:
    st = SimpleNamespace(
        source_types={
            "shop": "openapi",
            "gh": "graphql_remote",
            "orders": "grpc_remote",
            "pg": "postgresql",
        },
        openapi_specs={"shop": {"spec": SPEC, "base_url": SHOP}},
        # The address and credential a command is called with: the source's stored ones.
        api_sources={
            "shop": ApiSource(
                id="shop", type="openapi", base_url=SHOP, auth=ApiAuthBearer(token="s3cret")
            )
        },
        graphql_remote_sources={
            "gh": {
                "source_id": "gh",
                "url": GQL,
                "namespace": "",
                "domain_id": "",
                "auth": {"type": "bearer", "token": "ghp_x"},
                "tables": [],
                "schema": _introspected(SDL),
            }
        },
        grpc_remote_sources={
            "orders": {
                "server_address": "orders.internal:50051",
                "pb2": object(),
                "mutations": [_grpc_mutation("PlaceOrder")],
            }
        },
        config=SimpleNamespace(graphql_remote=GraphQLRemoteConfig()),
    )
    monkeypatch.setattr("provisa.api.app.state", st)
    return st


# --- what a source offers ------------------------------------------------------------------------


async def test_an_openapi_source_offers_its_operations_that_are_not_gets(state):
    offered = {op.name: op for op in await ops.offered_operations(state, "shop") or []}
    assert set(offered) == {"createOrder", "cancelOrder"}
    assert offered["createOrder"].arguments == ("body",)
    assert offered["createOrder"].comment == "[POST] /orders — Place an order"
    assert offered["cancelOrder"].arguments == ("order_id",)  # no body to pass


async def test_a_graphql_source_offers_its_mutations(state):
    offered = {op.name: op for op in await ops.offered_operations(state, "gh") or []}
    assert set(offered) == {"createIssue", "closeIssue"}
    assert offered["createIssue"].arguments == ("input", "dryRun")
    assert offered["createIssue"].comment == "createIssue it"


async def test_a_grpc_source_offers_its_mutation_methods(state):
    (op,) = await ops.offered_operations(state, "orders") or []
    assert (op.name, op.arguments) == ("Orders.PlaceOrder", ("sku", "qty"))


async def test_a_source_that_is_not_remote_offers_nothing_here(state):
    assert await ops.offered_operations(state, "pg") is None
    with pytest.raises(ApiError) as refused:
        await ops.offered_operation(state, "pg", "anything")
    assert refused.value.code == "functions.not_a_remote_source"


async def test_an_operation_the_source_does_not_offer_is_refused(state):
    with pytest.raises(ApiError) as refused:
        await ops.offered_operation(state, "gh", "deleteRepository")
    assert refused.value.status_code == 422
    assert refused.value.code == "functions.operation_not_offered"


# --- calling an OpenAPI operation -----------------------------------------------------------------


@respx.mock
async def test_an_openapi_operation_gets_its_body_as_given_and_the_sources_credential(state):
    route = respx.post(f"{SHOP}/orders").mock(
        return_value=httpx.Response(201, json={"id": 7, "extra": [1, 2]})
    )
    body = {"sku": "A-1", "gift": {"wrap": True}}  # gift is not in the spec: still passed
    rows = await ops.call_operation(state, "shop", "createOrder", {"body": body})
    sent = route.calls.last.request
    assert json.loads(sent.content) == body
    assert sent.headers["authorization"] == "Bearer s3cret"
    assert rows == [{"id": 7, "extra": [1, 2]}]


@respx.mock
async def test_an_openapi_operation_is_sent_its_sources_own_headers(state):
    route = respx.post(f"{SHOP}/orders").mock(return_value=httpx.Response(201, json={"id": 7}))
    shop = state.api_sources["shop"]
    state.api_sources["shop"] = shop.model_copy(update={"headers": {"Shop-Version": "2026-01"}})
    await ops.call_operation(state, "shop", "createOrder", {"body": {"sku": "A-1"}})
    sent = route.calls.last.request
    assert (sent.headers["shop-version"], sent.headers["authorization"]) == (
        "2026-01",
        "Bearer s3cret",
    )


@respx.mock
async def test_a_body_the_operation_declares_as_a_form_is_sent_as_one(state):
    form = {
        "requestBody": {
            "content": {
                "application/x-www-form-urlencoded": {
                    "schema": {"type": "object", "properties": {"email": {"type": "string"}}}
                }
            }
        },
        "responses": {"200": {"description": "ok"}},
    }
    spec = {**SPEC, "paths": {"/customers": {"post": {"operationId": "createCustomer", **form}}}}
    state.openapi_specs["shop"]["spec"] = spec
    route = respx.post(f"{SHOP}/customers").mock(return_value=httpx.Response(200, json={"id": 1}))
    body = {
        "email": "a@b.co",
        "metadata": {"tier": "gold"},
        "items": [{"price": "p_1", "quantity": 2}],
        "livemode": False,
    }
    await ops.call_operation(state, "shop", "createCustomer", {"body": body})
    sent = route.calls.last.request
    assert sent.headers["content-type"] == "application/x-www-form-urlencoded"
    assert dict(httpx.QueryParams(sent.content.decode())) == {
        "email": "a@b.co",
        "metadata[tier]": "gold",
        "items[0][price]": "p_1",
        "items[0][quantity]": "2",
        "livemode": "false",
    }
    assert sent.headers["authorization"] == "Bearer s3cret"


@respx.mock
async def test_path_arguments_fill_the_path_and_the_rest_go_on_the_query_string(state):
    route = respx.delete(f"{SHOP}/orders/42").mock(return_value=httpx.Response(204))
    rows = await ops.call_operation(
        state, "shop", "cancelOrder", {"order_id": 42, "reason": "late"}
    )
    assert route.calls.last.request.url.params["reason"] == "late"
    assert rows == [{"result": ""}]


@respx.mock
async def test_an_openapi_refusal_comes_back_as_the_remote_stated_it(state):
    respx.post(f"{SHOP}/orders").mock(
        return_value=httpx.Response(400, json={"error": "sku A-9 is discontinued"})
    )
    with pytest.raises(ApiError) as refused:
        await ops.call_operation(state, "shop", "createOrder", {"body": {"sku": "A-9"}})
    assert refused.value.status_code == 422
    assert refused.value.params["remote_status"] == 400
    assert "sku A-9 is discontinued" in refused.value.params["answer"]


@respx.mock
async def test_a_remote_that_fails_is_a_bad_gateway(state):
    respx.post(f"{SHOP}/orders").mock(return_value=httpx.Response(503, text="down"))
    with pytest.raises(ApiError) as refused:
        await ops.call_operation(state, "shop", "createOrder", {"body": {}})
    assert refused.value.status_code == 502


# --- calling a GraphQL mutation ------------------------------------------------------------------


def test_the_mutation_document_passes_each_given_argument_as_a_typed_variable():
    schema = _introspected(SDL)
    document = ops.mutation_document(schema, "createIssue", ["input"])
    assert document.startswith("mutation($input: CreateIssueInput!) { createIssue(input: $input) {")
    # Its answer: the scalars, and those of the objects it holds; never a field that needs an
    # argument to be asked for.
    assert "issue { __typename id title author { __typename login } }" in document
    assert "comments" not in document
    assert "dryRun" not in document  # not given, not sent


def test_a_mutation_called_with_nothing_has_no_variables():
    schema = _introspected("type Query { a: Int } type Mutation { ping: Boolean }")
    assert ops.mutation_document(schema, "ping", []) == "mutation { ping }"


@respx.mock
async def test_a_graphql_mutation_is_sent_with_the_callers_object_and_the_sources_token(state):
    route = respx.post(GQL).mock(
        return_value=httpx.Response(
            200, json={"data": {"createIssue": {"issue": {"id": "I_1", "title": "Bug"}}}}
        )
    )
    given = {"input": {"title": "Bug", "repositoryId": "R_1", "labelIds": ["L_1"]}}
    rows = await ops.call_operation(state, "gh", "createIssue", given)
    sent = json.loads(route.calls.last.request.content)
    assert sent["variables"] == given
    assert route.calls.last.request.headers["authorization"] == "Bearer ghp_x"
    assert rows == [{"issue": {"id": "I_1", "title": "Bug"}}]


@respx.mock
async def test_a_graphql_refusal_comes_back_as_the_remote_stated_it(state):
    errors = [{"type": "INSUFFICIENT_SCOPES", "message": "needs repo scope"}]
    respx.post(GQL).mock(return_value=httpx.Response(200, json={"errors": errors}))
    with pytest.raises(ApiError) as refused:
        await ops.call_operation(state, "gh", "closeIssue", {"id": "I_1"})
    assert refused.value.status_code == 422
    assert json.loads(refused.value.params["answer"]) == errors


# --- calling a gRPC mutation method --------------------------------------------------------------


async def test_a_grpc_method_is_called_on_the_sources_channel_with_the_arguments_given(
    state, monkeypatch
):
    seen = {}

    async def _execute(channel, path, pb2, request_name, reply_name, args):
        seen.update(path=path, request=request_name, args=args, channel=channel)
        return {"order_id": "O-1"}

    monkeypatch.setattr("provisa.grpc_remote.executor.execute_mutation", _execute)
    rows = await ops.call_operation(state, "orders", "Orders.PlaceOrder", {"sku": "A", "qty": 2})
    assert seen["path"] == "/shop.Orders/PlaceOrder"
    assert seen["request"] == "PlaceOrderRequest"
    assert seen["args"] == {"sku": "A", "qty": 2}
    # The source's channel on this request's loop, opened for it and kept for the next call.
    (channel,) = state.grpc_remote_sources["orders"]["channels"].values()
    assert seen["channel"] is channel
    await ops.call_operation(state, "orders", "Orders.PlaceOrder", {"sku": "B"})
    assert seen["channel"] is channel
    assert rows == [{"order_id": "O-1"}]


# --- through the command dispatcher --------------------------------------------------------------


def _command(**over) -> dict:
    return {
        "name": "create_issue",
        "source_id": "gh",
        "schema_name": "graphql",
        "function_name": "createIssue",
        "impl_kind": "source_operation",
        "arguments": [{"name": "input", "type": "json"}, {"name": "dryRun", "type": "json"}],
        **over,
    }


@respx.mock
async def test_a_sql_call_writes_each_argument_as_a_json_literal(state):
    from provisa.executor.function_dispatch import dispatch_function

    route = respx.post(GQL).mock(
        return_value=httpx.Response(200, json={"data": {"createIssue": {"issue": None}}})
    )
    # The SQL surfaces pass arguments by position, as the literals the statement holds.
    await dispatch_function(_command(), {"a0": '{"title": "Bug"}', "a1": "true"}, state, None)
    assert json.loads(route.calls.last.request.content)["variables"] == {
        "input": {"title": "Bug"},
        "dryRun": True,
    }


async def test_a_sql_argument_that_is_not_a_json_literal_is_refused(state):
    from provisa.executor.function_dispatch import dispatch_function

    with pytest.raises(ApiError) as refused:
        await dispatch_function(_command(), {"a0": "{title: Bug}"}, state, None)
    assert refused.value.code == "functions.json_argument_invalid"


@respx.mock
async def test_a_call_by_name_passes_its_values_as_they_are(state):
    from provisa.executor.function_dispatch import dispatch_function

    route = respx.post(GQL).mock(
        return_value=httpx.Response(200, json={"data": {"createIssue": {"issue": None}}})
    )
    # A string is a string here: the surface carried a JSON value, not a SQL literal.
    await dispatch_function(_command(), {"input": '{"title": "Bug"}'}, state, None)
    assert json.loads(route.calls.last.request.content)["variables"] == {
        "input": '{"title": "Bug"}'
    }


# --- registering one ------------------------------------------------------------------------------


def _form(**over):
    from provisa.api.admin.actions_router import FunctionInput

    return FunctionInput(name="create_issue", domainId="eng", **over)


async def test_registering_an_operation_takes_what_it_is_from_the_source(state):
    from provisa.api.admin.actions_router import _as_source_operation

    body = _form(
        sourceId="gh",
        functionName="createIssue",
        implKind="http",
        kind="query",
        returns="x.y",
        arguments=[{"name": "made_up", "type": "Int"}],
        binding={"url": "https://elsewhere"},
        materialize=True,
    )
    await _as_source_operation(body)
    assert body.implKind == "source_operation"
    assert body.kind == "mutation"
    assert body.schemaName == "graphql"
    assert (body.returns, body.binding, body.materialize) == ("", {}, False)
    assert body.arguments == [
        {"name": "input", "type": "json"},
        {"name": "dryRun", "type": "json"},
    ]


async def test_registering_an_operation_the_source_does_not_offer_is_refused(state):
    from provisa.api.admin.actions_router import _as_source_operation

    with pytest.raises(ApiError) as refused:
        await _as_source_operation(_form(sourceId="shop", functionName="listOrders"))
    assert refused.value.code == "functions.operation_not_offered"  # a GET is a table


async def test_a_command_on_a_database_source_is_left_as_the_form_states_it(state):
    from provisa.api.admin.actions_router import _as_source_operation

    body = _form(sourceId="pg", functionName="refresh_totals", kind="query")
    await _as_source_operation(body)
    assert (body.implKind, body.kind) == ("source_procedure", "query")


# --- an action, not a transform -------------------------------------------------------------------


def test_a_write_operation_composed_in_a_statement_is_refused():
    from provisa.pgwire._pipeline import _refuse_composed_mutators

    commands = {
        "create_issue": _command(),
        "enrich": {"name": "enrich", "impl_kind": "http"},
    }
    composed = sqlglot.parse_one(
        "SELECT o.id FROM orders o JOIN create_issue('{}') c ON true", dialect="postgres"
    )
    with pytest.raises(PermissionError, match="cannot be composed in a query, a view"):
        _refuse_composed_mutators(composed, commands)
    # A transform composed the same way is not this rule's to refuse.
    _refuse_composed_mutators(
        sqlglot.parse_one(
            "SELECT o.id FROM orders o JOIN enrich('{}') e ON true", dialect="postgres"
        ),
        commands,
    )


# --- the GraphQL surface -------------------------------------------------------------------------


def test_its_arguments_and_answer_are_json_on_the_graphql_surface():
    from graphql import GraphQLNonNull, get_named_type

    from provisa.compiler.actions_schema import _build_action_fields
    from provisa.compiler.schema_types import SchemaInput
    from provisa.compiler.type_map import JSONScalar

    fn = {**_command(), "domain_id": "eng", "visible_to": [], "kind": "mutation", "returns": ""}
    si = SchemaInput(
        tables=[],
        relationships=[],
        column_types={},
        naming_rules=[],
        role={"id": "dev", "capabilities": [], "domain_access": ["*"]},
        domains=[],
        functions=[fn],
    )
    _, mutation_fields = _build_action_fields(si, {}, [])
    (field,) = mutation_fields.values()
    assert get_named_type(field.type) is JSONScalar
    assert not isinstance(field.args["input"].type, GraphQLNonNull)
    assert field.args["input"].type is JSONScalar


# --- a call that needs approval -----------------------------------------------------------------


class _Hook:
    def __init__(self, approved: bool) -> None:
        self.approved = approved
        self.asked = []

    async def evaluate(self, request):
        from provisa.auth.approval_hook import ApprovalResponse

        self.asked.append(request)
        return ApprovalResponse(approved=self.approved, reason="change window is closed")


def _registered(state, **over) -> None:
    # The role may write and is granted this command: approval is the one gate left to pass.
    fn = _command(requires_approval=True, writable_by=["dev"], visible_to=[], **over)
    state.tracked_functions = {fn["name"]: fn}
    state.roles = {"dev": {"id": "dev", "capabilities": ["write"], "domain_access": ["*"]}}


async def test_a_call_that_needs_approval_is_refused_with_no_hook_to_give_it(state):
    from provisa.api.data.action_exec import invoke_tracked_function

    _registered(state)
    state.approval_hook = None
    with pytest.raises(ApiError) as refused:
        await invoke_tracked_function("create_issue", {"input": {}}, state, "dev")
    assert (refused.value.status_code, refused.value.code) == (
        403,
        "functions.approval_unavailable",
    )


@respx.mock
async def test_a_call_the_hook_denies_never_reaches_the_remote(state):
    from provisa.api.data.action_exec import invoke_tracked_function

    route = respx.post(GQL).mock(return_value=httpx.Response(200, json={"data": {}}))
    _registered(state)
    state.approval_hook = _Hook(approved=False)
    with pytest.raises(ApiError) as refused:
        await invoke_tracked_function("create_issue", {"input": {"title": "x"}}, state, "dev")
    assert refused.value.code == "functions.approval_denied"
    assert "change window is closed" in str(refused.value.detail)
    assert not route.called


@respx.mock
async def test_the_hook_is_shown_who_calls_what_with_which_arguments(state):
    from provisa.api.data.action_exec import invoke_tracked_function

    respx.post(GQL).mock(
        return_value=httpx.Response(200, json={"data": {"createIssue": {"issue": None}}})
    )
    _registered(state)
    hook = _Hook(approved=True)
    state.approval_hook = hook
    await invoke_tracked_function("create_issue", {"input": {"title": "x"}}, state, "dev")
    (asked,) = hook.asked
    assert (asked.user, asked.roles, asked.operation) == ("dev", ["dev"], "command")
    assert (asked.command, asked.arguments) == ("create_issue", {"input": {"title": "x"}})


def test_the_flag_is_kept_with_the_command():
    from provisa.core.repositories.function import function_from_dict

    stored = function_from_dict(
        {**_command(requires_approval=True), "returns": "", "arguments": []}
    )
    assert stored.requires_approval is True


# --- the table it writes -------------------------------------------------------------------------


def test_the_table_it_writes_must_be_one_of_its_sources_registered_tables(state):
    from provisa.api.admin.actions_router import _check_written_table

    state.tables = [
        {"id": 3, "source_id": "gh", "schema_name": "graphql", "table_name": "issues"},
        {"id": 4, "source_id": "shop", "schema_name": "openapi", "table_name": "orders"},
    ]
    _check_written_table(_form(sourceId="gh", writesTable="graphql.issues"))
    _check_written_table(_form(sourceId="gh"))  # where it is not known, nothing is named
    with pytest.raises(ApiError) as refused:
        _check_written_table(_form(sourceId="gh", writesTable="openapi.orders"))
    assert refused.value.code == "actions.written_table_not_registered"


@respx.mock
async def test_after_a_call_what_is_held_of_the_written_table_stops_being_served(
    state, monkeypatch
):
    from provisa.api.data.action_exec import invoke_tracked_function

    respx.post(GQL).mock(
        return_value=httpx.Response(200, json={"data": {"createIssue": {"issue": None}}})
    )
    _registered(state, writes_table="graphql.issues")
    state.approval_hook = _Hook(approved=True)
    state.tables = [{"id": 3, "source_id": "gh", "schema_name": "graphql", "table_name": "issues"}]
    state.catalog_for = lambda source_id: f"cat_{source_id}"
    written = []

    async def _after(st, **table):
        written.append(table)

    monkeypatch.setattr("provisa.api.data.table_written.after_table_written", _after)
    await invoke_tracked_function("create_issue", {"input": {"title": "x"}}, state, "dev")
    assert written == [
        {
            "table_id": 3,
            "table_name": "issues",
            "source_id": "gh",
        }
    ]


@respx.mock
async def test_a_refused_call_wrote_nothing(state, monkeypatch):
    from provisa.api.data.action_exec import invoke_tracked_function

    respx.post(GQL).mock(
        return_value=httpx.Response(200, json={"errors": [{"message": "no such repository"}]})
    )
    _registered(state, writes_table="graphql.issues")
    state.approval_hook = _Hook(approved=True)
    written = []

    async def _after(st, **table):
        written.append(table)

    monkeypatch.setattr("provisa.api.data.table_written.after_table_written", _after)
    with pytest.raises(ApiError):
        await invoke_tracked_function("create_issue", {"input": {}}, state, "dev")
    assert written == []


async def test_a_grpc_refusal_comes_back_with_the_status_the_remote_gave(state, monkeypatch):
    import grpc
    from grpc.aio import AioRpcError, Metadata

    async def _refuse(*_a):
        raise AioRpcError(
            grpc.StatusCode.FAILED_PRECONDITION, Metadata(), Metadata(), details="out of stock"
        )

    monkeypatch.setattr("provisa.grpc_remote.executor.execute_mutation", _refuse)
    with pytest.raises(ApiError) as refused:
        await ops.call_operation(state, "orders", "Orders.PlaceOrder", {"sku": "A"})
    assert refused.value.status_code == 422
    assert refused.value.params["answer"] == "FAILED_PRECONDITION: out of stock"


def test_each_event_loop_gets_its_own_channel_to_a_grpc_source():
    """A grpc.aio channel belongs to the loop it was opened on, and each request runs on a loop
    of its own (REQ-1882): a channel opened on one loop is never handed to another."""
    import asyncio

    from provisa.grpc_remote.executor import channel_for

    reg = {"server_address": "orders.internal:50051", "tls": False}

    async def _channel():
        return channel_for(reg), channel_for(reg)

    first_a, first_b = asyncio.run(_channel())
    second_a, _ = asyncio.run(_channel())
    assert first_a is first_b
    assert second_a is not first_a


def test_a_write_operation_called_as_a_value_in_a_statement_is_refused_too():
    from provisa.pgwire._pipeline import _refuse_composed_mutators

    commands = {"create_issue": _command()}
    projected = sqlglot.parse_one("SELECT create_issue('{}') FROM orders", dialect="postgres")
    with pytest.raises(PermissionError, match="create_issue"):
        _refuse_composed_mutators(projected, commands)


def test_a_view_or_mv_definition_that_calls_a_write_operation_is_refused():
    commands = {"create_issue": _command(), "enrich": {"name": "enrich", "impl_kind": "http"}}
    with pytest.raises(ValueError, match="view 'v' calls create_issue"):
        ops.refuse_writes_in_definition(
            "SELECT * FROM orders o JOIN create_issue('{}') c ON true", commands, "view 'v'"
        )
    ops.refuse_writes_in_definition("SELECT * FROM enrich('{}')", commands, "view 'v'")
    with pytest.raises(ValueError, match="does not parse"):
        ops.refuse_writes_in_definition("SELECT FROM WHERE (", commands, "view 'v'")


def test_with_no_write_operation_registered_a_definition_is_not_this_checks_to_read():
    ops.refuse_writes_in_definition("SELECT FROM WHERE (", {"enrich": {"impl_kind": "http"}}, "v")


# --- an OpenAPI GET that answers no rows: a command that reads (REQ-1924) -------------------------

READS_SPEC = {
    "swagger": "2.0",
    "info": {"title": "shop", "version": "1"},
    "paths": {
        "/orders/{order_id}/diff": {
            "get": {"operationId": "getDiff", "responses": {"200": {"description": "text"}}}
        },
        "/orders/{order_id}/invoice": {
            "get": {
                "operationId": "getInvoice",
                "produces": ["application/octet-stream"],
                "responses": {"200": {"description": "the file"}},
            }
        },
    },
}


@pytest.fixture
def reads(state) -> SimpleNamespace:
    state.openapi_specs["shop"]["spec"] = READS_SPEC
    return state


async def test_an_openapi_source_offers_its_gets_that_answer_no_rows(reads):
    offered = {op.name: op for op in await ops.offered_operations(reads, "shop") or []}
    assert {n: (op.reads, op.binary) for n, op in offered.items()} == {
        "getDiff": (True, False),
        "getInvoice": (True, True),
    }
    assert offered["getDiff"].arguments == ("order_id",)


async def test_registering_a_get_makes_a_query_and_a_file_answers_bytea(reads):
    from provisa.api.admin.actions_router import _as_source_operation

    diff = _form(sourceId="shop", functionName="getDiff", kind="mutation")
    await _as_source_operation(diff)
    assert (diff.implKind, diff.kind, diff.outputColumns) == ("source_operation", "query", None)

    invoice = _form(sourceId="shop", functionName="getInvoice")
    await _as_source_operation(invoice)
    assert invoice.kind == "query"
    assert invoice.outputColumns == [{"name": "result", "type": "bytea"}]


@respx.mock
async def test_a_get_that_answers_text_is_one_row_holding_it(reads):
    respx.get(f"{SHOP}/orders/42/diff").mock(return_value=httpx.Response(200, text="- a\n+ b\n"))
    rows = await ops.call_operation(reads, "shop", "getDiff", {"order_id": 42})
    assert rows == [{"result": "- a\n+ b\n"}]


@respx.mock
async def test_a_file_is_one_row_holding_it_as_a_bytea(reads):
    from provisa.api.json_response import dumps
    from provisa.executor.function_dispatch import _schema_from_columns, _validate_against

    blob = bytes(range(256))  # not text in any encoding
    respx.get(f"{SHOP}/orders/42/invoice").mock(return_value=httpx.Response(200, content=blob))
    rows = await ops.call_operation(reads, "shop", "getInvoice", {"order_id": 42})
    assert bytes.fromhex(rows[0]["result"].removeprefix("\\x")) == blob
    # It meets the command's declared output and is carried in a JSON answer.
    assert _validate_against(rows, _schema_from_columns(ops.BINARY_ANSWER), where="t") == rows
    assert json.loads(dumps(rows)) == rows


def test_a_file_is_a_bytea_column_in_sql():
    from provisa.executor.command_localize import _output_spec, _values_source

    command = {"name": "get_invoice", "output_columns": list(ops.BINARY_ANSWER)}
    rows = [{"result": "\\x0001ff"}]
    columns, types = _output_spec(command, rows)
    relation = _values_source(rows, "c", columns, types, "postgres").sql(dialect="postgres")
    assert "AS BYTEA" in relation.upper()


def test_a_command_that_reads_is_composed_like_any_query():
    from provisa.pgwire._pipeline import _refuse_composed_mutators

    commands = {"get_diff": _command(name="get_diff", function_name="getDiff", kind="query")}
    sql = "SELECT o.id FROM orders o JOIN get_diff('{}') d ON true"
    _refuse_composed_mutators(sqlglot.parse_one(sql, dialect="postgres"), commands)
    ops.refuse_writes_in_definition(sql, commands, "view 'v'")


# --- a file upload, an operation's own address, and a spec read once ----------------------------

_UPLOAD = {
    "operationId": "postFile",
    "servers": [{"url": "https://files.shop.example/"}],
    "requestBody": {
        "content": {
            "multipart/form-data": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "file": {"type": "string", "format": "binary"},
                        "purpose": {"type": "string"},
                        "link": {"type": "object", "properties": {"create": {"type": "boolean"}}},
                    },
                }
            }
        }
    },
    "responses": {"200": {"description": "ok"}},
}


@pytest.fixture
def uploads(state) -> SimpleNamespace:
    state.openapi_specs["shop"]["spec"] = {**SPEC, "paths": {"/files": {"post": _UPLOAD}}}
    return state


@respx.mock
async def test_a_file_is_uploaded_as_a_multipart_body_at_the_operations_own_address(uploads):
    route = respx.post("https://files.shop.example/files").mock(
        return_value=httpx.Response(200, json={"id": "file_1"})
    )
    body = {"file": "\\x25504446", "purpose": "evidence", "link": {"create": True}}
    rows = await ops.call_operation(uploads, "shop", "postFile", {"body": body})
    sent = route.calls.last.request
    assert sent.headers["content-type"].startswith("multipart/form-data; boundary=")
    assert sent.headers["authorization"] == "Bearer s3cret"  # the source's credential goes too
    content = sent.content
    assert b'name="file"; filename="file"' in content and b"%PDF" in content
    assert b'name="purpose"\r\n\r\nevidence' in content
    assert b'name="link[create]"\r\n\r\ntrue' in content
    assert rows == [{"id": "file_1"}]


async def test_a_file_that_is_not_a_bytea_in_text_form_is_refused_before_any_call(uploads):
    with pytest.raises(ApiError) as refused:
        await ops.call_operation(uploads, "shop", "postFile", {"body": {"file": "JVBERi0="}})
    assert (refused.value.code, refused.value.params["field"]) == (
        "source_operation.file_not_bytea",
        "file",
    )


@respx.mock
async def test_a_sources_spec_is_read_once_and_again_when_it_is_replaced(state, monkeypatch):
    from provisa.openapi import mapper

    respx.post(f"{SHOP}/orders").mock(return_value=httpx.Response(201, json={"id": 7}))
    read = []
    parse = mapper.parse_spec
    monkeypatch.setattr(mapper, "parse_spec", lambda spec: read.append(1) or parse(spec))
    ops._COMMANDS.clear()
    for _ in range(3):
        await ops.call_operation(state, "shop", "createOrder", {"body": {"sku": "A-1"}})
    assert len(read) == 1
    state.openapi_specs["shop"]["spec"] = dict(SPEC)  # a refresh: the spec is a new one
    await ops.call_operation(state, "shop", "createOrder", {"body": {"sku": "A-1"}})
    assert len(read) == 2
