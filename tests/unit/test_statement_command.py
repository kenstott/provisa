# Copyright (c) 2026 Kenneth Stott
# Canary: e07466fc-fb90-47f9-a883-1ca816745682
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A command implemented by a Provisa SQL statement (REQ-2001).

An argument is written ``{{name}}`` and bound as a value; the statement declares the command's
kind and the table it writes, and one declared beside it that says otherwise is refused. A call
runs the statement through the one pipeline as the caller's role.
"""

# Requirements: REQ-2001

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from provisa.core.statement_declaration import (
    StatementCommandRefused,
    check,
    declared,
    positional,
)

_INSERT = (
    "INSERT INTO kb.docs (id, title) SELECT ticket_id, subject FROM support.tickets "
    "WHERE updated_at > {{since}} AND team = {{team}}"
)


def test_each_argument_becomes_its_positional_parameter_in_declared_order():
    sql = positional("c", _INSERT, ["team", "since"])
    assert "updated_at > $2 AND team = $1" in sql


def test_an_argument_used_twice_is_one_parameter():
    assert positional("c", "SELECT {{x}} + {{x}}", ["x"]) == "SELECT $1 + $1"


def test_a_placeholder_naming_no_argument_is_refused():
    with pytest.raises(StatementCommandRefused, match=r"names \['team'\], which the command"):
        positional("c", _INSERT, ["since"])


def test_a_declared_argument_the_statement_never_uses_is_refused():
    with pytest.raises(StatementCommandRefused, match=r"\['extra'\] appear nowhere"):
        positional("c", _INSERT, ["since", "team", "extra"])


def test_a_placeholder_inside_quotes_is_no_value_and_is_refused():
    with pytest.raises(StatementCommandRefused, match=r"\['since'\] are inside quotes"):
        declared("c", {"sql": "SELECT * FROM t WHERE d > '{{since}}'"}, ["since"])


def test_a_statement_command_runs_one_statement():
    with pytest.raises(StatementCommandRefused, match="holds 2"):
        declared("c", {"sql": "DELETE FROM a.b; DELETE FROM a.c"}, [])
    with pytest.raises(StatementCommandRefused, match="binding holds its SQL as 'sql'"):
        declared("c", {}, [])


def test_the_statement_declares_the_kind_and_the_written_table():
    write = declared("c", {"sql": _INSERT}, ["since", "team"])
    assert (write.kind, write.writes, write.returns_rows) == ("mutation", "kb.docs", False)
    read = declared("c", {"sql": "SELECT id FROM kb.docs WHERE id = {{id}}"}, ["id"])
    assert (read.kind, read.writes, read.returns_rows) == ("query", None, True)
    returning = declared("c", {"sql": "DELETE FROM kb.docs RETURNING id"}, [])
    assert returning.returns_rows


def test_a_declared_kind_or_written_table_that_contradicts_the_statement_is_refused():
    with pytest.raises(StatementCommandRefused, match="declared a query, but its statement is a"):
        check("c", {"sql": _INSERT}, ["since", "team"], "query", None)
    with pytest.raises(StatementCommandRefused, match="writing 'kb.other', but its statement"):
        check("c", {"sql": _INSERT}, ["since", "team"], None, "kb.other")
    with pytest.raises(StatementCommandRefused, match="but its statement writes nothing"):
        check("c", {"sql": "SELECT 1"}, [], None, "kb.docs")


# -- the model -----------------------------------------------------------------------------------


def _function(**kw):
    from provisa.core.models import Function

    base = {
        "name": "embed_tickets",
        "source_id": "",
        "function_name": "embed_tickets",
        "returns": "",
        "impl_kind": "statement",
        "arguments": [{"name": "since", "type": "timestamp"}, {"name": "team", "type": "text"}],
        "binding": {"sql": _INSERT},
    }
    return Function(**{**base, **kw})


def test_an_undeclared_kind_and_written_table_are_the_statements():
    fn = _function()
    assert (fn.kind, fn.writes_table) == ("mutation", "kb.docs")
    read = _function(binding={"sql": "SELECT {{since}}, {{team}}"})
    assert (read.kind, read.writes_table) == ("query", None)


def test_the_model_refuses_a_contradicting_declaration():
    with pytest.raises(ValueError, match="declared a query, but its statement is a mutation"):
        _function(kind="query")


# -- a call --------------------------------------------------------------------------------------


@pytest.fixture
def pipeline(monkeypatch):
    from provisa.executor.result import QueryResult
    from provisa.pgwire import _pipeline

    calls: dict = {}

    async def govern(sql, role_id, *, params=None):
        calls.update(sql=sql, role_id=role_id, params=params)
        return SimpleNamespace(plan=True, embedded=calls.get("embedded"))

    result = {"value": QueryResult(rows=[], column_names=[], rowcount=3)}

    async def execute(plan, state):
        del plan, state
        return result["value"]

    monkeypatch.setattr(_pipeline, "_govern_and_route", govern)
    monkeypatch.setattr(_pipeline, "_execute_plan", execute)
    return calls, result


def _fn_dict(sql: str = _INSERT) -> dict:
    return {
        "name": "embed_tickets",
        "impl_kind": "statement",
        "arguments": [{"name": "since"}, {"name": "team"}],
        "binding": {"sql": sql},
        "contract": {"broken": None},
    }


@pytest.mark.asyncio
async def test_a_call_runs_the_statement_through_the_pipeline_as_the_caller(pipeline):
    from provisa.executor.statement_command import run

    calls, _ = pipeline
    rows = await run(_fn_dict(), {"team": "billing", "since": "2026-10-11"}, object(), "steward")
    assert calls["role_id"] == "steward"
    assert calls["params"] == ["2026-10-11", "billing"]
    assert "$1" in calls["sql"] and "{{" not in calls["sql"]
    assert rows == [{"rows_affected": 3}]


@pytest.mark.asyncio
async def test_a_query_command_answers_its_rows(pipeline):
    from provisa.executor.result import QueryResult
    from provisa.executor.statement_command import run

    _, result = pipeline
    result["value"] = QueryResult(rows=[(1, "a")], column_names=["id", "title"])
    sql = "SELECT id, title FROM kb.docs WHERE id > {{since}} AND team = {{team}}"
    rows = await run(_fn_dict(sql), {"since": 0, "team": "x"}, object(), "steward")
    assert rows == [{"id": 1, "title": "a"}]


@pytest.mark.asyncio
async def test_a_broken_command_is_refused_naming_why(pipeline):
    from provisa.api.errors import ApiError
    from provisa.executor.statement_command import run

    calls, _ = pipeline
    fn = {**_fn_dict(), "contract": {"broken": "no registered table is named 'kb.docs'"}}
    with pytest.raises(ApiError) as refused:
        await run(fn, {"since": "x", "team": "y"}, object(), "steward")
    assert refused.value.code == "functions.statement_broken"
    assert "no registered table is named 'kb.docs'" in str(refused.value)
    assert calls == {}


@pytest.mark.asyncio
async def test_the_dispatcher_runs_a_statement_command_as_the_calling_role(pipeline, monkeypatch):
    from provisa.executor import function_dispatch as fd

    calls, _ = pipeline
    state = SimpleNamespace(udf_trace_sink=None, minted_sessions=None)
    rows = await fd.dispatch_function(_fn_dict(), {"a0": "2026-10-11", "a1": "ops"}, state, "ops")
    assert calls["role_id"] == "ops" and calls["params"] == ["2026-10-11", "ops"]
    assert rows == [{"rows_affected": 3}]


@pytest.mark.asyncio
async def test_the_command_executor_leaves_the_after_write_to_the_pipeline(monkeypatch):
    from provisa.api.data import action_exec

    fn = {**_fn_dict(), "writes_table": "kb.docs", "kind": "mutation"}
    state = SimpleNamespace(tracked_functions={"embed_tickets": fn})
    monkeypatch.setattr(action_exec, "admit_command", lambda *_a, **_k: {"id": "ops"})
    monkeypatch.setattr(action_exec, "_record_call", lambda *_a, **_k: None)
    monkeypatch.setattr(
        action_exec, "dispatch_function", AsyncMock(return_value=[{"rows_affected": 1}])
    )
    written = AsyncMock()
    monkeypatch.setattr(action_exec, "_table_was_written", written)
    monkeypatch.setattr(
        action_exec, "govern_action_rows", AsyncMock(side_effect=lambda rows, *_a: (rows, None))
    )
    await action_exec.invoke_command("embed_tickets", {}, state, "ops")
    written.assert_not_awaited()


# -- the admin API ---------------------------------------------------------------------------


def test_the_admin_api_takes_the_statements_kind_when_none_is_sent():
    from provisa.api.admin.actions_router import FunctionInput, _as_statement

    body = FunctionInput(
        name="embed_tickets",
        implKind="statement",
        arguments=[{"name": "since"}, {"name": "team"}],
        binding={"sql": _INSERT},
    )
    _as_statement(body)
    assert (body.kind, body.writesTable, body.functionName) == (
        "mutation",
        "kb.docs",
        "embed_tickets",
    )


def test_the_admin_api_refuses_a_sent_kind_that_contradicts_the_statement():
    from provisa.api.admin.actions_router import FunctionInput, _as_statement
    from provisa.api.errors import ApiError

    body = FunctionInput(
        name="embed_tickets",
        implKind="statement",
        kind="query",
        arguments=[{"name": "since"}, {"name": "team"}],
        binding={"sql": _INSERT},
    )
    with pytest.raises(ApiError) as refused:
        _as_statement(body)
    assert refused.value.code == "actions.statement_invalid"


# -- the run's provenance record -----------------------------------------------------------------


def _recorded_fn() -> dict:
    return {
        **_fn_dict(),
        "impl_kind": "statement",
        "contract": {
            "broken": None,
            "statement_hash": "abc123",
            "version": "v7",
            "reads": ["support.tickets"],
            "writes": "kb.docs",
        },
    }


@pytest.mark.asyncio
async def test_each_run_leaves_a_record_proving_what_produced_its_rows(pipeline, monkeypatch):
    from provisa.api.data import action_exec
    from provisa.audit.context import audit_identity_scope
    from provisa.vector.embed_fn import Folded

    calls, _ = pipeline
    used = ({"id": "mini", "provider": "ollama", "dimensions": 3, "version": "sha256:9f"},)
    calls["embedded"] = Folded("x", audit=used)
    recorded: list = []
    monkeypatch.setattr(
        "provisa.audit.pipeline.enqueue_audit",
        lambda pending, status, _state, **kw: recorded.append((pending, status, kw)),
    )
    state = SimpleNamespace(udf_trace_sink=None, minted_sessions=None, model_stamp=4)
    with audit_identity_scope("scheduler:hourly", "scheduler"):
        rows = await action_exec._run_recorded(
            _recorded_fn(), {"a0": "2026-10-11", "a1": "ops"}, state, "steward"
        )
    assert rows == [{"rows_affected": 3}]
    ((pending, status, kw),) = recorded
    assert status == 200 and kw["row_count"] == 3 and kw["route"] == "command"
    assert (pending.user_id, pending.surface, pending.role_id) == (
        "scheduler:hourly",
        "scheduler",
        "steward",
    )
    assert pending.query_text == _INSERT
    assert pending.enforced["command"] == {
        "name": "embed_tickets",
        "kind": "statement",
        "statement_hash": "abc123",
        "contract_version": "v7",
        "arguments": {"since": "2026-10-11", "team": "ops"},
        "reads": ["support.tickets"],
        "writes": "kb.docs",
        "embed": list(used),
    }


@pytest.mark.asyncio
async def test_a_failed_run_is_recorded_with_its_outcome(monkeypatch):
    from provisa.api.data import action_exec
    from provisa.api.errors import ApiError
    from provisa.audit.context import audit_identity_scope

    recorded: list = []
    monkeypatch.setattr(
        "provisa.audit.pipeline.enqueue_audit",
        lambda pending, status, _state, **kw: recorded.append((status, kw)),
    )
    monkeypatch.setattr(
        action_exec,
        "dispatch_function",
        AsyncMock(side_effect=ApiError(409, "functions.statement_broken", "broken")),
    )
    with audit_identity_scope("u", "sql"), pytest.raises(ApiError):
        await action_exec._run_recorded(
            _recorded_fn(), {"since": "x", "team": "y"}, SimpleNamespace(model_stamp=1), "ops"
        )
    assert recorded == [(409, {"route": "command", "row_count": None})]


@pytest.mark.asyncio
async def test_a_run_the_engine_fails_answers_naming_the_command_and_the_cause(monkeypatch):
    """An engine error under a statement command (a value that does not convert) is answered by
    name, the command and the engine's own message, and recorded with that outcome -- never the
    catch-all's bare "Internal server error"."""
    import duckdb

    from provisa.api.data import action_exec
    from provisa.api.errors import ApiError
    from provisa.audit.context import audit_identity_scope

    recorded: list = []
    monkeypatch.setattr(
        "provisa.audit.pipeline.enqueue_audit",
        lambda pending, status, _state, **kw: recorded.append(status),
    )
    cause = duckdb.ConversionException("Conversion Error: Malformed JSON at byte 0 of input")
    monkeypatch.setattr(action_exec, "dispatch_function", AsyncMock(side_effect=cause))
    with audit_identity_scope("u", "graphql"), pytest.raises(ApiError) as answered:
        await action_exec._run_recorded(
            _recorded_fn(), {"since": "x", "team": "y"}, SimpleNamespace(model_stamp=1), "ops"
        )
    assert answered.value.status_code == 500
    assert answered.value.code == "functions.statement_failed"
    assert answered.value.params == {"name": "embed_tickets", "error": str(cause)}
    assert answered.value.__cause__ is cause
    assert recorded == [500]


@pytest.mark.asyncio
async def test_a_refusal_under_a_run_keeps_its_own_answer(monkeypatch):
    from provisa.api.data import action_exec
    from provisa.audit.context import audit_identity_scope
    from provisa.core.parameter_binding import ParameterCallsOverCap

    monkeypatch.setattr("provisa.audit.pipeline.enqueue_audit", lambda *a, **kw: None)
    refusal = ParameterCallsOverCap("lines_like", 2155, 1000)
    monkeypatch.setattr(action_exec, "dispatch_function", AsyncMock(side_effect=refusal))
    with audit_identity_scope("u", "graphql"), pytest.raises(ParameterCallsOverCap) as raised:
        await action_exec._run_recorded(
            _recorded_fn(), {"since": "x", "team": "y"}, SimpleNamespace(model_stamp=1), "ops"
        )
    assert raised.value is refusal
