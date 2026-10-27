# Copyright (c) 2026 Kenneth Stott
# Canary: d4d771cf-4c37-402a-a7ab-05e0fb761794
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Bolt connection's ephemeral graph (REQ-2176): one temporary-table session per connection,
bound around each of its RUNs and ended with it; a CREATE the governing rule does not make wholly
the governed write is the ephemeral layer's, its governed parts through Bolt's governed write;
a read while the session holds ephemeral objects is never served from, nor kept in, the
translations a role's reads share."""

# Requirements: REQ-2176, REQ-615

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from provisa.compiler import temp_tables
from provisa.ephemeral import store
from provisa.ephemeral.write import Created

pytestmark = [pytest.mark.asyncio(loop_scope="session")]


async def _run(session, cypher: str) -> None:
    with patch.object(session, "_resolve_db", return_value=("analyst", False)):
        await session.handle_run([cypher, {}, {}])
    session._end_all_requests()


async def test_each_run_of_a_connection_belongs_to_its_one_temporary_session():
    from tests.unit.test_bolt_show_databases import _make_session

    session, _writer = _make_session(["analyst"])
    seen: list[object] = []

    async def execute(*_args, **_kwargs):
        seen.append(temp_tables.current())
        return ["id"], [[1]], None

    with patch("provisa.bolt.session._execute_cypher", execute):
        await _run(session, "MATCH (p:Pets) RETURN p.id AS id")
        await _run(session, "MATCH (p:Pets) RETURN p.id AS id")
    assert seen == [session.temp_tables, session.temp_tables]
    assert temp_tables.current() is None


async def test_the_connection_ending_ends_its_temporary_tables(monkeypatch):
    import asyncio

    from provisa.bolt import server
    from provisa.bolt.messages import MAGIC
    from provisa.pgwire import temp_exec

    ended: list[object] = []

    async def end(held, _state):
        ended.append(held)

    monkeypatch.setattr(temp_exec, "end", end)
    sessions: list[object] = []

    async def _loop(session, _reader, _writer):
        session.org_id = "org"
        session.temp_tables.stored = True
        sessions.append(session)

    monkeypatch.setattr(server, "_serve", _loop)

    class _Writer:
        def write(self, _data: bytes) -> None:
            pass

        async def drain(self) -> None:
            pass

    reader = asyncio.StreamReader()
    reader.feed_data(MAGIC + bytes([0, 0, 4, 5]) + bytes(12))  # proposes Bolt 5.4
    await server._bolt_handshake_and_serve(reader, _Writer())  # type: ignore[arg-type]
    [session] = sessions
    assert ended == [session.temp_tables]  # type: ignore[attr-defined]


class _Kept:
    """The translations a role's reads share: none is ever looked up or kept here."""

    def __init__(self, *_args, **_kwargs) -> None:
        pass

    def cached(self):
        raise AssertionError("served a kept translation")

    def record(self, _translation) -> None:
        raise AssertionError("kept a translation")


def _patched(**extra):
    """_execute_cypher with every branch ahead of the ephemeral seam answering nothing."""
    state = SimpleNamespace(contexts={"analyst": object()})
    return (
        patch("provisa.api.app.state", state),
        patch("provisa.bolt.session._system_query", return_value=None),
        patch("provisa.bolt.session._maybe_invoke_metric_call", return_value=None),
        patch("provisa.bolt.session._maybe_invoke_command_call", return_value=None),
        patch("provisa.bolt.session._bolt_label_map", return_value="model"),
        patch("provisa.bolt.session._bolt_domain_access", return_value=["*"]),
        patch("provisa.api.rest.cypher_plan.TranslationRequest", _Kept),
        *extra.values(),
    )


async def test_an_ephemeral_create_is_the_ephemeral_layers_its_governed_parts_bolts_write():
    from contextlib import ExitStack

    from provisa.bolt.session import _execute_cypher

    asked: list[tuple] = []
    written: list[tuple] = []

    async def write(statement, params, model, held, _state, run_read, run_write):
        asked.append((statement, model, held))
        await run_write("CREATE (n:Person {id: $id})", {"id": 1})
        return Created(1, 0, 1)

    async def governed_write(statement, values, *_args):
        written.append((statement, values))
        return [], []

    held = temp_tables.TempSession()
    token = temp_tables.bind(held)
    try:
        with ExitStack() as stack:
            for p in _patched(
                seam=patch("provisa.ephemeral.seam.write", write),
                governed=patch("provisa.bolt.session._execute_write_cypher", governed_write),
            ):
                stack.enter_context(p)
            answer = await _execute_cypher("CREATE (:Temp {k: 1})", {}, "analyst")
    finally:
        temp_tables.unbind(token)
    assert answer == ([], [], None)
    assert asked == [("CREATE (:Temp {k: 1})", "model", held)]
    assert written == [("CREATE (n:Person {id: $id})", {"id": 1})]


async def test_a_read_over_a_session_holding_ephemeral_objects_is_never_kept():
    from contextlib import ExitStack

    from provisa.bolt.session import _execute_cypher

    class Stop(Exception):
        pass

    def compiled_over(_ast, label_map, _params):
        assert label_map == "overlaid"
        raise Stop

    held = temp_tables.TempSession()
    store.graph(held).node_keys["e:1"] = ("Temp",)
    token = temp_tables.bind(held)
    try:
        with ExitStack() as stack:
            for p in _patched(
                overlay=patch("provisa.ephemeral.seam.label_map", return_value="overlaid"),
                parse=patch("provisa.cypher.parser.parse_cypher", return_value="ast"),
                translate=patch("provisa.gql.translator.cypher_to_sql", compiled_over),
            ):
                stack.enter_context(p)
            with pytest.raises(Stop):
                await _execute_cypher("MATCH (n:Temp) RETURN n.k", {}, "analyst")
    finally:
        temp_tables.unbind(token)
