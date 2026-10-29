# Copyright (c) 2026 Kenneth Stott
# Canary: 6c1f2a8e-3d47-4b9a-8e05-9a7d2c41b6e3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2176: a Bolt connection runs each RUN in its principal's one graph session -- the one the
Cypher page, HTTP Cypher and MCP run in -- so what a statement makes over Bolt is read over HTTP
and the reverse. A connection ending leaves it as it was: it ends only as every graph session
does (cleared, its retention lapsed, or its principal's rights changed). Two connections of one
principal share it, their writes done one at a time under its exclusive lock."""

# Requirements: REQ-2176

from __future__ import annotations

import contextlib
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from provisa.api import graph_session
from provisa.api.rest import cypher_session
from provisa.compiler import temp_tables
from provisa.ephemeral import seam
from tests.unit.ephemeral_harness import Session
from tests.unit import test_cypher_page_session as page
from tests.unit.test_cypher_page_session import _MODEL, _body, _request, _row, _state

pytestmark = [pytest.mark.asyncio(loop_scope="session")]

# The graph sessions' control plane, and the principal's store schema dropped, as the Cypher
# page's tests have them.
dbs = page.dbs
ended = page.ended


@pytest.fixture
def served(dbs, monkeypatch):
    """The app's state, serving one org: the graph sessions' control plane and the harness's
    store, as HTTP and Bolt read it."""
    import provisa.api.app as appmod

    h = Session(monkeypatch, _MODEL)
    state = _state(dbs, h)
    state.multitenancy = False
    state.user_attributes = {}
    monkeypatch.setattr(appmod, "state", state)
    return state, h


def _connection(user: str | None = "ada"):
    """A Bolt connection authenticated as ``user`` (None: an unsecured deployment's), its org
    resolved."""
    from tests.unit.test_bolt_show_databases import _make_session

    session, _writer = _make_session(["analyst"], graph_session=True)
    session.org_id, session._org_resolved = "default", True
    session.user_id = user or "neo4j"
    session._identity = (
        None if user is None else SimpleNamespace(user_id=user, roles=["analyst"], derived_roles={})
    )
    return session


def _executing(h: Session):
    """Bolt's execution of a statement, as the ephemeral layer does it over the harness's store:
    in whatever temporary-table session the RUN bound."""

    async def execute(cypher, params, *_args, **_kwargs):
        held = temp_tables.current()
        assert held is not None, "a Bolt RUN is bound to a graph session"
        h.session = held
        made = await seam.write(cypher, params, h.model, held, h.state, h.read, h.write)
        if made is not None:
            if any(seam.counters(made).values()):
                bound = graph_session.bound()
                assert bound is not None
                bound.wrote = True
            return (*seam.rows_of(made), None)
        rows = await h.read(cypher, params)
        columns = list(rows[0]) if rows else []
        return columns, [list(r.values()) for r in rows], None

    return patch("provisa.bolt.session._execute_cypher", execute)


def _failed(code: str, message: str) -> None:
    raise AssertionError(f"the RUN failed: {code}: {message}")


@contextlib.contextmanager
def _served_runs(h: Session):
    """What every RUN below is served by, patched once for the process: a module's patch made
    from two threads at once would be undone under the other."""
    with (
        patch("provisa.security.role_probe.suspended", return_value=False),
        _executing(h),
    ):
        yield


async def _run(session, cypher: str) -> list[list]:
    """One RUN of ``session``'s under :func:`_served_runs`, its records pulled."""
    with (
        patch.object(session, "_resolve_db", return_value=("analyst", False)),
        # What a role's rows are filtered by is no part of this: the principal binds nothing.
        patch.object(session, "_derived_held", return_value=lambda _role: False),
        patch.object(session, "_principal_scope", return_value=contextlib.nullcontext()),
        patch.object(session, "send_failure", side_effect=_failed),
    ):
        await session.handle_run([cypher, {}, {}])
    rows = session._result_rows
    session._end_all_requests()
    return rows


async def _bolt(session, h: Session, cypher: str) -> list[list]:
    """One RUN of ``session``'s, its records pulled."""
    with _served_runs(h):
        return await _run(session, cypher)


async def _http(h: Session, cypher: str, user: str = "ada") -> list[dict]:
    """One statement over HTTP Cypher, in the requesting principal's graph session."""
    async with cypher_session.statement(_request(user), cypher) as held:
        h.session = held.session
        made = await seam.write(cypher, {}, h.model, held.session, h.state, h.read, h.write)
        if made is not None:
            if any(seam.counters(made).values()):
                held.wrote = True
            return []
        return await h.read(cypher, {})


async def test_a_node_created_over_bolt_is_read_over_http(served):
    _state_, h = served
    await _bolt(_connection(), h, "CREATE (:Note {text: 'from bolt'})")
    assert await _http(h, "MATCH (n:Note) RETURN n.text AS t") == [{"t": "from bolt"}]


async def test_a_node_created_over_http_is_read_over_bolt(served):
    _state_, h = served
    await _http(h, "CREATE (:Note {text: 'from http'})")
    rows = await _bolt(_connection(), h, "MATCH (n:Note) RETURN n.text AS t")
    assert rows == [["from http"]]


async def test_an_unsecured_connection_is_the_anonymous_principals_as_an_unsecured_page_is(
    served,
):
    _state_, h = served
    await _bolt(_connection(None), h, "CREATE (:Note {text: 'anon'})")
    read = await _http(h, "MATCH (n:Note) RETURN n.text AS t", user=graph_session.ANONYMOUS)
    assert read == [{"t": "anon"}]


async def test_the_connection_ending_leaves_the_graph_session_as_it_was(served, ended, monkeypatch):
    import asyncio as _asyncio

    from provisa.bolt import server
    from provisa.bolt.messages import MAGIC

    _state_, h = served
    connections: list[object] = []

    async def _loop(session, _reader, _writer):
        session.org_id, session._org_resolved = "default", True
        session.roles, session.role_id = ["analyst"], "analyst"
        session._identity = SimpleNamespace(user_id="ada", roles=["analyst"], derived_roles={})
        await _bolt(session, h, "CREATE (:Note {text: 'kept'})")
        connections.append(session)

    monkeypatch.setattr(server, "_serve", _loop)

    class _Writer:
        def write(self, _data: bytes) -> None:
            pass

        async def drain(self) -> None:
            pass

    reader = _asyncio.StreamReader()
    reader.feed_data(MAGIC + bytes([0, 0, 4, 5]) + bytes(12))  # proposes Bolt 5.4
    await server._bolt_handshake_and_serve(reader, _Writer())  # type: ignore[arg-type]
    assert len(connections) == 1
    assert ended == []
    assert await _http(h, "MATCH (n:Note) RETURN n.text AS t") == [{"t": "kept"}]
    assert await _bolt(_connection(), h, "MATCH (n:Note) RETURN n.text AS t") == [["kept"]]


async def test_clearing_over_http_clears_what_bolt_made(served, ended):
    _state_, h = served
    await _bolt(_connection(), h, "CREATE (:Note {text: 'gone'})")
    assert _body(await cypher_session.clear(_request())) == {"cleared": True}
    assert ended == [graph_session.session_id("ada")]
    # The label went with it: a read naming it is refused, as for any label nothing declares.
    with pytest.raises(AssertionError, match="There is no label :Note"):
        await _bolt(_connection(), h, "MATCH (n:Note) RETURN n.text AS t")


async def test_two_connections_of_one_principal_share_it_and_neither_write_is_lost(served):
    """Each connection is served on its own thread (provisa.bolt.server): a second connection's
    write waits for the first's to be written back, then sees it."""
    from tests.unit.test_cypher_page_session import _in_thread

    _state_, h = served
    await _bolt(_connection(), h, "CREATE (:Note {text: 'zero'})")  # the row exists
    order: list[str] = []
    first_in = threading.Event()
    landed = seam.write

    async def write(cypher, *args):
        made = await landed(cypher, *args)
        if "'one'" in cypher:
            order.append("one in")
            first_in.set()
            time.sleep(0.5)
            order.append("one out")
        elif "'two'" in cypher:
            order.append("two in")
        return made

    async def first():
        await _run(_connection(), "CREATE (:Note {text: 'one'})")

    async def second():
        first_in.wait(timeout=10)
        await _run(_connection(), "CREATE (:Note {text: 'two'})")

    with _served_runs(h), patch.object(seam, "write", write):
        threads = [_in_thread(first), _in_thread(second)]
        for t in threads:
            t.join(timeout=30)
    assert order == ["one in", "one out", "two in"]
    rows = await _bolt(_connection(), h, "MATCH (n:Note) RETURN n.text AS t ORDER BY t")
    assert rows == [["one"], ["two"], ["zero"]]


async def test_a_bolt_write_advances_the_sessions_generation_and_a_read_does_not(served, dbs):
    _state_, h = served
    conn = _connection()
    await _bolt(conn, h, "CREATE (:Note {text: 'a'})")
    before = (await _row(dbs)).generation
    await _bolt(conn, h, "MATCH (n:Note) RETURN n.text AS t")
    assert (await _row(dbs)).generation == before
    await _bolt(conn, h, "MATCH (n:Note) SET n.text = 'b'")
    assert (await _row(dbs)).generation == before + 1


async def test_a_bolt_rollback_undoes_nothing_the_session_holds(served):
    """BEGIN/COMMIT/ROLLBACK frame no transaction over the graph session: each RUN is one
    statement of it, written back as it ends."""
    _state_, h = served
    conn = _connection()
    conn.handle_begin([{}])
    await _bolt(conn, h, "CREATE (:Note {text: 'stays'})")
    conn.handle_rollback()
    assert await _http(h, "MATCH (n:Note) RETURN n.text AS t") == [{"t": "stays"}]


async def test_a_written_back_generation_is_past_every_one_its_statement_keyed_plans_by(dbs):
    """A statement that creates tables advances the bound session's generation once for each, and
    a plan it keeps mid-statement is keyed by one of them (``temp_tables.slot_key``): the
    generation written back must be past them all, or the next statement -- holding every table
    -- would meet a plan kept for a session holding fewer."""
    from provisa.compiler.temp_tables import TempTable

    state = _state(dbs)
    keyed: list[list] = []
    async with graph_session.session_for(state, "ada", ["analyst"], writes=True) as held:
        for name in ("a", "b", "c"):
            held.session.tables[name] = TempTable(name, [("x", "INTEGER")])
            held.session.generation += 1
            keyed.append(temp_tables.slot_key())
    async with graph_session.session_for(state, "ada", ["analyst"], writes=False):
        assert temp_tables.slot_key() not in keyed
    assert (await _row(dbs)).generation > held.session.generation
