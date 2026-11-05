# Copyright (c) 2026 Kenneth Stott
# Canary: 004b8859-f0b7-4e71-b880-2de59dd93e7e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""CREATE DATABASE and DROP DATABASE over Bolt, and selecting a created database
(provisa.bolt.databases; REQ-807, REQ-2176, REQ-2167).

A created database is a name for the home database, kept in the principal's one graph session:
every connection of the principal selects it, no other principal does, and DROP DATABASE removes
the name. Both are administration commands run against ``system``. The Neo4j Python driver's
database selection example (tests/integration/examples/test_database_selection_example.py, driver
6.2.0) runs exactly this sequence."""

# Requirements: REQ-807, REQ-2176, REQ-2167

from __future__ import annotations

import contextlib
from unittest.mock import patch

import pytest

from provisa.bolt import databases
from provisa.ephemeral import store
from tests.unit.ephemeral_harness import Session
from tests.unit import test_bolt_graph_session as bolt_graph
from tests.unit.test_bolt_graph_session import _connection, _served_runs

# The graph sessions' control plane and the app's state serving one org, as Bolt's graph session
# tests have them.
dbs = bolt_graph.dbs
ended = bolt_graph.ended
served = bolt_graph.served

_async = pytest.mark.asyncio(loop_scope="session")


class _Refused(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code


def _refuse(code: str, message: str) -> None:
    raise _Refused(code, message)


async def _run(session, h: Session, cypher: str, db: str | None = None, params=None) -> list:
    """One RUN of ``session``'s naming ``db`` (None: none), its database resolved as served; its
    records pulled. A failure raises _Refused with its Neo4j code."""
    extra = {} if db is None else {"db": db}
    with (
        _served_runs(h),
        patch.object(session, "_derived_held", return_value=lambda _role: False),
        patch.object(session, "_principal_scope", return_value=contextlib.nullcontext()),
        patch.object(session, "send_failure", side_effect=_refuse),
    ):
        await session.handle_run([cypher, params or {}, extra])
    rows = session._result_rows
    session._end_all_requests()
    return rows


def _code(exc: pytest.ExceptionInfo[_Refused]) -> str:
    return exc.value.code


@_async
async def test_the_drivers_database_selection_example_runs_as_on_neo4j(served):
    _state_, h = served
    conn = _connection()
    assert await _run(conn, h, "DROP DATABASE example IF EXISTS WAIT", db="system") == [
        ["localhost:17687", "CaughtUp", "caught up", True]
    ]
    await _run(conn, h, "CREATE DATABASE example WAIT", db="system")
    await _run(conn, h, "CREATE (a:Note {message: 'Hello, Example-Database'}) RETURN a", "example")
    rows = await _run(conn, h, "MATCH (a:Note) RETURN a.message AS msg", db="example")
    assert rows == [["Hello, Example-Database"]]
    await _run(conn, h, "DROP DATABASE example WAIT", db="system")
    with pytest.raises(_Refused) as refused:
        await _run(conn, h, "MATCH (a:Note) RETURN a.message AS msg", db="example")
    assert _code(refused) == databases.NOT_FOUND


@_async
async def test_a_created_database_is_the_home_database(served):
    _state_, h = served
    conn = _connection()
    await _run(conn, h, "CREATE DATABASE example", db="system")
    await _run(conn, h, "CREATE (:Note {text: 'through example'})", db="example")
    assert await _run(conn, h, "MATCH (n:Note) RETURN n.text AS t", db="neo4j") == [
        ["through example"]
    ]


@_async
async def test_dropping_a_created_database_keeps_what_was_written_through_it(served):
    _state_, h = served
    conn = _connection()
    await _run(conn, h, "CREATE DATABASE example", db="system")
    await _run(conn, h, "CREATE (:Note {text: 'kept'})", db="example")
    await _run(conn, h, "DROP DATABASE example", db="system")
    assert await _run(conn, h, "MATCH (n:Note) RETURN n.text AS t") == [["kept"]]


@_async
async def test_every_connection_of_the_principal_selects_it_and_no_other_principal(served):
    _state_, h = served
    await _run(_connection("ada"), h, "CREATE DATABASE shared", db="system")
    assert await _run(_connection("ada"), h, "RETURN 1 AS one", db="shared") == [[1]]
    with pytest.raises(_Refused) as refused:
        await _run(_connection("bob"), h, "RETURN 1 AS one", db="shared")
    assert _code(refused) == databases.NOT_FOUND


@_async
async def test_names_are_compared_as_neo4j_compares_them_in_lower_case(served):
    _state_, h = served
    conn = _connection()
    await _run(conn, h, "CREATE DATABASE `Mixed-Case`", db="system")
    assert await _run(conn, h, "RETURN 1 AS one", db="MIXED-case") == [[1]]


@_async
async def test_a_dotted_name_is_refused_where_database_names_carry_no_org(served):
    """REQ-1235: a single-tenant deployment's database names have no org part, so a dotted name
    could never be selected: refused when created."""
    _state_, h = served
    with pytest.raises(_Refused) as refused:
        await _run(_connection(), h, "CREATE DATABASE `ex.ample`", db="system")
    assert _code(refused) == databases.ARGUMENT


@_async
async def test_a_name_may_be_a_parameter(served):
    _state_, h = served
    conn = _connection()
    await _run(conn, h, "CREATE DATABASE $name", db="system", params={"name": "fromparam"})
    assert await _run(conn, h, "RETURN 1 AS one", db="fromparam") == [[1]]


@_async
async def test_show_databases_lists_a_created_database(served):
    from provisa.api import graph_session
    from provisa.bolt.session import _system_query

    state, h = served
    conn = _connection()
    await _run(conn, h, "CREATE DATABASE listed", db="system")
    # The catalog answer as _execute_cypher gives it, in the principal's graph session.
    async with graph_session.session_for(state, "ada", ["analyst"], writes=False):
        answer = _system_query("SHOW DATABASES", None, "analyst", False, state, ["analyst"])
    assert answer is not None
    assert [r[0] for r in answer[1]] == ["provisa_analyst", "provisa_ops_analyst", "listed"]


@pytest.mark.parametrize("db", [None, "neo4j", "provisa_analyst"])
@_async
async def test_a_database_command_outside_system_is_refused_as_on_neo4j(served, db):
    _state_, h = served
    with pytest.raises(_Refused) as refused:
        await _run(_connection(), h, "CREATE DATABASE example", db=db)
    assert _code(refused) == databases.NOT_SYSTEM


@_async
async def test_creating_an_existing_database_is_refused_unless_if_not_exists(served):
    _state_, h = served
    conn = _connection()
    await _run(conn, h, "CREATE DATABASE twice", db="system")
    with pytest.raises(_Refused) as refused:
        await _run(conn, h, "CREATE DATABASE twice", db="system")
    assert _code(refused) == databases.EXISTS
    assert await _run(conn, h, "CREATE DATABASE twice IF NOT EXISTS", db="system") == []
    assert await _run(conn, h, "CREATE OR REPLACE DATABASE twice", db="system") == []


@_async
async def test_dropping_a_missing_database_is_refused_unless_if_exists(served):
    _state_, h = served
    conn = _connection()
    with pytest.raises(_Refused) as refused:
        await _run(conn, h, "DROP DATABASE missing", db="system")
    assert _code(refused) == databases.NOT_FOUND
    assert await _run(conn, h, "DROP DATABASE missing IF EXISTS DESTROY DATA", db="system") == []


@pytest.mark.parametrize(
    ("cypher", "code"),
    [
        ("CREATE DATABASE neo4j", databases.EXISTS),
        ("CREATE DATABASE system", databases.EXISTS),
        ("CREATE DATABASE `provisa_analyst`", databases.EXISTS),
        ("DROP DATABASE neo4j", databases.ARGUMENT),
        ("DROP DATABASE system", databases.ARGUMENT),
        ("CREATE DATABASE ab", databases.ARGUMENT),
        ("CREATE DATABASE `1st`", databases.ARGUMENT),
        ("CREATE DATABASE systems", databases.ARGUMENT),
        ("CREATE DATABASE `has space`", databases.ARGUMENT),
    ],
)
@_async
async def test_the_servers_own_and_invalid_names_are_refused(served, cypher, code):
    _state_, h = served
    with pytest.raises(_Refused) as refused:
        await _run(_connection(), h, cypher, db="system")
    assert _code(refused) == code


@_async
async def test_if_not_exists_on_a_servers_own_database_does_nothing(served):
    _state_, h = served
    assert await _run(_connection(), h, "CREATE DATABASE neo4j IF NOT EXISTS", db="system") == []


@_async
async def test_clearing_the_graph_session_ends_its_created_databases(served):
    from provisa.api.rest import cypher_session
    from tests.unit.test_cypher_page_session import _request

    _state_, h = served
    conn = _connection()
    await _run(conn, h, "CREATE DATABASE example", db="system")
    await cypher_session.clear(_request())
    with pytest.raises(_Refused) as refused:
        await _run(conn, h, "RETURN 1 AS one", db="example")
    assert _code(refused) == databases.NOT_FOUND


# --- the commands as written ------------------------------------------------------------------


def test_any_other_statement_is_no_database_command():
    assert databases.parse("MATCH (n) RETURN n") is None
    assert databases.parse("CREATE (:Database {name: 'x'})") is None


@pytest.mark.parametrize(
    ("cypher", "expected"),
    [
        ("CREATE DATABASE example WAIT", databases.DatabaseCommand(True, "example", False, False, True)),
        ("create database `ex`` q` nowait", databases.DatabaseCommand(True, "`ex`` q`", False, False, False)),
        ("CREATE OR REPLACE DATABASE e1x", databases.DatabaseCommand(True, "e1x", False, True, False)),
        ("CREATE DATABASE $n IF NOT EXISTS WAIT 5 SECONDS;", databases.DatabaseCommand(True, "$n", True, False, True)),
        ("DROP DATABASE example IF EXISTS WAIT", databases.DatabaseCommand(False, "example", True, False, True)),
        ("DROP DATABASE example DUMP DATA", databases.DatabaseCommand(False, "example", False, False, False)),
    ],
)  # fmt: skip
def test_the_supported_forms_parse(cypher, expected):
    assert databases.parse(cypher) == expected


@pytest.mark.parametrize(
    "cypher",
    [
        "CREATE DATABASE example TOPOLOGY 1 PRIMARY",
        "CREATE DATABASE example OPTIONS {storeFormat: 'block'}",
        "CREATE COMPOSITE DATABASE example",
        "DROP COMPOSITE DATABASE example",
    ],
)
def test_a_store_option_or_a_composite_database_is_refused_by_name(cypher):
    with pytest.raises(databases.DatabaseCommandError) as refused:
        databases.parse(cypher)
    assert refused.value.code == databases.UNSUPPORTED


def test_or_replace_with_if_not_exists_is_refused():
    with pytest.raises(databases.DatabaseCommandError) as refused:
        databases.parse("CREATE OR REPLACE DATABASE example IF NOT EXISTS")
    assert refused.value.code == databases.ARGUMENT


def test_a_parameter_name_must_be_a_string():
    with pytest.raises(databases.DatabaseCommandError) as refused:
        databases.database_name("$n", {"n": 3})
    assert refused.value.code == databases.ARGUMENT


def test_the_graph_session_keeps_its_databases_between_statements():
    graph = store.Graph(databases={"example", "other"})
    assert store.graph_from_json(store.graph_to_json(graph)).databases == {"example", "other"}
    # A session kept before the field existed holds none.
    held = store.graph_to_json(store.Graph())
    del held["databases"]
    assert store.graph_from_json(held).databases == set()


@pytest.mark.parametrize(
    ("name", "could"),
    [("example", True), ("Ex-1.b", True), ("provisa_analyst", False), ("neo4j", False),
     ("system", False), ("systemx", False), ("ab", False)],
)  # fmt: skip
def test_only_a_valid_name_none_of_the_servers_own_is_looked_up_as_created(name, could):
    """A role database never reaches the graph session's created names: no RUN naming one reads it."""
    assert databases.may_be_created(name) is could
