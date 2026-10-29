# Copyright (c) 2026 Kenneth Stott
# Canary: 4e8b1d27-9c53-4a6f-8d02-7b3e5a1c9f68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2001 (amended 2026-10-28): a VOID command declares no outputs -- calling it runs it and
answers an empty result with no columns on every surface that calls commands; and in Cypher a
standalone ``CALL name`` binds each argument from the statement parameter of the same name, a
missing one refused by name. (openCypher TCK Call1[1]-[4], Call2[3].)"""

# Requirements: REQ-2001

from __future__ import annotations

from types import SimpleNamespace

import duckdb
import pytest
import sqlglot

from provisa.api.errors import ApiError

pytestmark = [pytest.mark.unit]


def _command(name: str, cypher: str, *, outputs, arguments=()) -> dict:
    return {
        "name": name,
        "source_id": "",
        "function_name": name,
        "returns": "",
        "return_schema": None,
        "impl_kind": "python",
        "binding": {"callable": "x:y"},
        "kind": "query",
        "domain_id": "shelter",
        "visible_to": ["reader"],
        "arguments": [{"name": a, "type": t} for a, t in arguments],
        "output_columns": outputs,
        "language_bindings": {"cypher": {"name": cypher}},
        "writes_table": None,
    }


_DO_NOTHING = _command("tck_do_nothing", "test.doNothing", outputs=[])
_MY_PROC = _command(
    "tck_my_proc",
    "test.my.proc",
    outputs=[{"name": "city", "type": "varchar"}, {"name": "country_code", "type": "bigint"}],
    arguments=(("name", "text"), ("id", "bigint")),
)
_ROWS = {
    ("Stefan", 1): [{"city": "Berlin", "country_code": 49}],
    ("Stefan", 2): [{"city": "München", "country_code": 49}],
}


@pytest.fixture
def state(monkeypatch):
    ran: list[tuple[str, dict]] = []

    async def _dispatch(fn, args, _state, _role):
        ran.append((fn["name"], dict(args)))
        if fn["name"] == "tck_do_nothing":
            # What a VOID command's implementation answers is not its result.
            return [{"anything": 1}]
        return _ROWS.get((args["name"], args["id"]), [])

    async def _governed(rows, _fn, _role, _state):
        return rows, None  # the column governance of a command's rows is not what is tested here

    monkeypatch.setattr("provisa.api.data.action_exec.dispatch_function", _dispatch)
    monkeypatch.setattr("provisa.api.data.action_exec.govern_action_rows", _governed)
    return SimpleNamespace(
        roles={"reader": {"id": "reader", "capabilities": [], "domain_access": ["shelter"]}},
        tracked_functions={"tck_do_nothing": _DO_NOTHING, "tck_my_proc": _MY_PROC},
        tracked_webhooks={},
        undefined_commands={},
        model_stamp=None,
        ran=ran,
    )


# --- the command model ---------------------------------------------------------------------------


def test_a_command_declaring_no_outputs_is_void():
    from provisa.core.void_command import is_void

    assert is_void(_DO_NOTHING)
    assert not is_void(_MY_PROC)
    # Outputs left undeclared are not "none": the command's output is unvalidated, not VOID.
    assert not is_void({**_DO_NOTHING, "output_columns": None})


def test_the_model_takes_a_void_command_and_refuses_one_that_also_names_a_return():
    from provisa.core.models import Function

    fields = {k: v for k, v in _DO_NOTHING.items() if k not in ("writes_table",)}
    assert Function(**fields).output_columns == []
    with pytest.raises(ValueError, match="VOID"):
        Function(**{**fields, "return_schema": {"type": "array", "items": {"type": "object"}}})


async def test_a_void_command_is_stored_as_declaring_no_outputs():
    # An empty output list is a declaration (VOID), never collapsed to "undeclared".
    from provisa.core.models import Function
    from provisa.core.repositories.function import upsert_function

    stored: list[dict] = []

    class _Conn:
        async def upsert_returning(self, _table, vals, **_kw):
            stored.append(vals)
            return 1

    fields = {k: v for k, v in _DO_NOTHING.items() if k != "writes_table"}
    http = {"impl_kind": "http", "binding": {"url": "http://x"}}
    await upsert_function(_Conn(), Function(**{**fields, **http}))  # type: ignore[arg-type]
    await upsert_function(_Conn(), Function(**{**fields, **http, "output_columns": None}))  # type: ignore[arg-type]
    assert [v["output_columns"] for v in stored] == [[], None]


# --- the one executor ----------------------------------------------------------------------------


async def test_a_void_command_runs_and_answers_no_rows(state):
    from provisa.api.data.action_exec import invoke_command

    rows, enforcement = await invoke_command("tck_do_nothing", {}, state, "reader")
    assert rows == []
    assert enforcement is None
    assert state.ran == [("tck_do_nothing", {})]


def test_a_void_command_is_listed_as_answering_nothing(state):
    from provisa.api.data.action_exec import list_visible_commands
    from provisa.cypher.command_call import command_signature as _command_signature

    listed = {c["command"]: c for c in list_visible_commands(state, "reader", "cypher")}
    assert listed["tck_do_nothing"]["void"] is True
    assert listed["tck_my_proc"]["void"] is False
    assert _command_signature(listed["tck_do_nothing"]) == "test.doNothing() :: ()"


# --- Cypher: a standalone call ------------------------------------------------------------------


async def _http(query: str, params: dict, state):
    import json

    from provisa.api.rest.registered_call import intercept_precompile

    body = SimpleNamespace(query=query, params=params)
    response = await intercept_precompile(body, state, "reader", None)
    return json.loads(response.body)


@pytest.mark.parametrize("query", ["CALL test.doNothing()", "CALL test.doNothing"])
async def test_a_standalone_call_of_a_void_command_answers_no_columns(query, state):
    assert await _http(query, {}, state) == {"columns": [], "rows": []}
    assert state.ran == [("tck_do_nothing", {})]


async def test_a_call_without_parentheses_binds_its_arguments_from_same_named_parameters(state):
    answer = await _http("CALL test.my.proc", {"name": "Stefan", "id": 1}, state)
    assert answer == {
        "columns": ["city", "country_code"],
        "rows": [{"city": "Berlin", "country_code": 49}],
    }
    assert state.ran == [("tck_my_proc", {"name": "Stefan", "id": 1})]


async def test_an_implicit_call_takes_yield_and_return(state):
    answer = await _http(
        "CALL test.my.proc YIELD city AS c RETURN c", {"name": "Stefan", "id": 2}, state
    )
    assert answer == {"columns": ["c"], "rows": [{"c": "München"}]}


async def test_an_implicit_call_missing_a_parameter_is_refused_by_name(state):
    with pytest.raises(ApiError, match=r"\$id"):
        await _http("CALL test.my.proc", {"name": "Stefan"}, state)
    assert state.ran == []


def test_an_implicit_call_is_read_without_its_arguments():
    from provisa.cypher.command_call import ImplicitArguments, parse_command_call

    call = parse_command_call("CALL test.my.proc", {"name": "Stefan"})
    assert call is not None
    assert call.name == "test.my.proc"
    assert isinstance(call.values, ImplicitArguments)
    assert dict(call.values) == {"name": "Stefan"}
    # Neo4j's own and Provisa's procedures are answered elsewhere, with or without parentheses.
    assert parse_command_call("CALL db.labels", {}) is None


def test_implicit_arguments_are_bound_by_the_command_s_declared_names(state):
    from provisa.api.data.action_exec import bind_command_args
    from provisa.cypher.command_call import ImplicitArguments

    args = bind_command_args(
        "tck_my_proc", ImplicitArguments({"id": 1, "name": "Stefan", "extra": 0}), state, "reader"
    )
    assert list(args.items()) == [("name", "Stefan"), ("id", 1)]


# --- Cypher: a call composed in a statement ------------------------------------------------------


async def _composed(query: str, state) -> list:
    from provisa.api.data.cypher_commands import put_command_rows
    from provisa.cypher.parser import parse_cypher
    from provisa.gql.label_map import CypherLabelMap
    from provisa.gql.translator import cypher_to_sql

    graph = CypherLabelMap(nodes={}, relationships={})
    sql_ast, _, _ = cypher_to_sql(parse_cypher(query), graph, {})
    sql = await put_command_rows(sql_ast.sql(dialect="postgres"), "reader", state)
    return duckdb.sql(sqlglot.transpile(sql, read="postgres", write="duckdb")[0]).fetchall()


async def test_a_void_call_in_a_statement_keeps_every_row(state):
    rows = await _composed(
        "UNWIND ['a', 'b', 'c'] AS name CALL test.doNothing() RETURN name", state
    )
    assert sorted(rows) == [("a",), ("b",), ("c",)]
    assert state.ran == [("tck_do_nothing", {})]


async def test_a_void_call_in_a_statement_over_no_rows_answers_none(state):
    rows = await _composed("UNWIND [] AS name CALL test.doNothing() RETURN name", state)
    assert rows == []


async def test_a_call_of_a_command_with_outputs_in_a_statement_names_what_it_yields(state):
    with pytest.raises(ApiError, match="name the columns it returns with YIELD"):
        await _composed("UNWIND [1] AS x CALL test.my.proc('Stefan', 1) RETURN x", state)


async def test_a_void_call_yields_nothing(state):
    with pytest.raises(ApiError, match="VOID"):
        await _composed("UNWIND [1] AS x CALL test.doNothing() YIELD x2 RETURN x", state)


def test_an_implicit_call_in_a_statement_is_refused():
    from provisa.cypher.parser import CypherParseError, parse_cypher

    with pytest.raises(CypherParseError, match="parameters"):
        parse_cypher("UNWIND [1] AS x CALL test.my.proc RETURN x")


# --- SQL ------------------------------------------------------------------------------------------


async def test_a_sql_call_of_a_void_command_answers_no_columns(state, monkeypatch):
    from provisa.pgwire.function_call import maybe_invoke_registered_function

    monkeypatch.setattr(
        "provisa.api.data.action_exec.usable_commands",
        lambda _s, _r, **_k: {"tck_do_nothing": _DO_NOTHING},
    )
    result = await maybe_invoke_registered_function(
        "SELECT * FROM tck_do_nothing()", "reader", state
    )
    assert (result.column_names, result.rows) == ([], [])


def test_a_void_command_s_call_is_described_with_no_columns(state):
    from provisa.pgwire._pipeline import _function_call_shape

    assert _function_call_shape("tck_do_nothing", state) == []


def test_a_void_command_read_as_a_relation_is_refused_by_name():
    from provisa.executor.command_localize import _output_spec

    with pytest.raises(ValueError, match="VOID"):
        _output_spec(_DO_NOTHING, [])


# --- GraphQL --------------------------------------------------------------------------------------


def test_a_void_command_s_graphql_field_is_an_empty_list():
    from graphql import GraphQLList, GraphQLNonNull

    from provisa.compiler.actions_schema import void_return_type

    gql = void_return_type()
    assert isinstance(gql, GraphQLNonNull)
    assert isinstance(gql.of_type, GraphQLList)


# --- a standalone YIELD of a VOID command; its signature (J186) -----------------------------------


async def _bolt(query: str, params: dict, state):
    from provisa.bolt.session import _maybe_invoke_command_call

    return await _maybe_invoke_command_call(query, params, "reader", state)


@pytest.mark.parametrize(
    "query",
    [
        "CALL test.doNothing() YIELD x",
        "CALL test.doNothing() YIELD *",
        "CALL test.doNothing YIELD x",
    ],
)
async def test_a_standalone_yield_of_a_void_command_is_refused_over_http_naming_void(query, state):
    with pytest.raises(ApiError, match="VOID") as raised:
        await _http(query, {}, state)
    assert raised.value.status_code == 400
    assert state.ran == []


@pytest.mark.parametrize(
    "query", ["CALL test.doNothing() YIELD x", "CALL test.doNothing() YIELD x RETURN x"]
)
async def test_a_standalone_yield_of_a_void_command_is_refused_over_bolt_naming_void(query, state):
    from provisa.cypher.command_call import CommandCallRefused

    with pytest.raises(CommandCallRefused, match="VOID"):
        await _bolt(query, {}, state)
    assert state.ran == []


async def test_a_standalone_void_call_without_yield_still_runs_over_bolt(state):
    assert await _bolt("CALL test.doNothing()", {}, state) == ([], [])
    assert state.ran == [("tck_do_nothing", {})]


def test_a_void_command_s_signature_is_the_same_on_bolt_and_http(state):
    from provisa.api.data.action_exec import list_visible_commands
    from provisa.bolt import session
    from provisa.cypher.command_call import command_signature  # what Cypher over HTTP lists

    listed = {c["command"]: c for c in list_visible_commands(state, "reader", "cypher")}
    void = listed["tck_do_nothing"]
    assert session._command_signature(void) == "test.doNothing() :: ()"
    assert command_signature(void) == "test.doNothing() :: ()"
    with_default = {
        **listed["tck_my_proc"],
        "arguments": [{"name": "name", "type": "text", "default": "x"}],
    }
    expected = 'test.my.proc(name = "x" :: TEXT) :: (MAP)'
    assert session._command_signature(with_default) == expected
    assert command_signature(with_default) == expected
