# Copyright (c) 2026 Kenneth Stott
# Canary: 2e7b9c40-81d6-4f3a-a5c2-6b0d9e14f873
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2176: a principal has one ephemeral graph session in an environment, the same on every
client.

Its tables are in the store under the principal's own schema and what it knows of them in one
control-plane row, read for each statement under a lock -- shared for a statement with no write
clause, exclusive for one with -- and written back, its generation advanced, by a statement that
changed it. So every tab, connection and worker a statement reaches sees what every earlier
statement made. It ends when the principal clears it, when its retention after last use lapses,
or when the principal's rights -- its roles, or their grants -- change; switching the role it acts
as is no change. The Cypher page reads its labels and types with their counts.
"""

# Requirements: REQ-2176

from __future__ import annotations

import asyncio
import contextvars
import json
import threading
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import insert, select
from starlette.requests import Request

from provisa.api import graph_session
from provisa.api.rest import cypher_session
from provisa.compiler import temp_tables
from provisa.compiler.temp_tables import TempSession
from provisa.core.schema_org import graph_sessions, roles as roles_table, user_role_assignments
from provisa.ephemeral import seam, store
from tests.unit.ephemeral_harness import Session

pytestmark = [pytest.mark.asyncio(loop_scope="session")]

_MODEL = {"Person": ({"id": "INTEGER", "name": "TEXT"}, "(1, 'Ada'), (2, 'Bo')")}


@pytest.fixture
def dbs(control_plane):
    return control_plane.org(holds="model"), control_plane.org(holds="state")


@pytest.fixture
def ended(monkeypatch):
    """The principal's store schema dropped, as temp_exec.end drops it."""
    from provisa.pgwire import temp_exec

    gone: list[str] = []

    async def end(session, _state):
        gone.append(session.id)
        session.tables.clear()
        session.extensions.clear()
        session.stored = False

    monkeypatch.setattr(temp_exec, "end", end)
    return gone


def _state(dbs, harness: Session | None = None) -> SimpleNamespace:
    model_db, state_db = dbs
    engine = (
        harness.state.federation_engine
        if harness is not None
        else SimpleNamespace(engine=SimpleNamespace(name="duckdb"))
    )
    return SimpleNamespace(
        federation_engine=engine,
        model_db=model_db,
        tenant_db=state_db,
        roles={"analyst": {"id": "analyst", "domain_access": ["sales"]}},
        role_build_inputs={
            "tables": [{"id": 1, "columns": [{"column_name": "name", "visible_to": ["analyst"]}]}],
            "rls_rules": [],
            "metrics": [],
        },
        org_registry=SimpleNamespace(
            runtimes=lambda: [SimpleNamespace(org_id="default", env=None)]
        ),
    )


async def _run(state, h: Session, statement: str, user="ada", claims=("analyst",), **params):
    """One statement of ``user``'s, in its graph session, as a surface runs it."""
    from provisa.cypher.parser import writes_in

    async with graph_session.session_for(
        state, user, list(claims), writes=writes_in(statement)
    ) as held:
        h.session = held.session
        made = await seam.write(statement, params, h.model, h.session, h.state, h.read, h.write)
        answer = made if made is not None else await h.read(statement, params)
    return answer, held


async def _filled(dbs, monkeypatch) -> tuple[SimpleNamespace, Session]:
    h = Session(monkeypatch, _MODEL)
    state = _state(dbs, h)
    await _run(state, h, "CREATE (:Note {text: 'hello'})")
    await _run(state, h, "CREATE (:Note {text: 'world'})")
    await _run(state, h, "MATCH (p:Person) WHERE p.id = 1 SET p:Starred")
    await _run(
        state,
        h,
        "MATCH (a:Note), (b:Note) WHERE a.text = 'hello' AND b.text = 'world' "
        "CREATE (a)-[:NEXT]->(b)",
    )
    return state, h


async def _row(dbs, user="ada"):
    async with dbs[1].acquire() as conn:
        return (
            await conn.execute_core(select(graph_sessions).where(graph_sessions.c.user_id == user))
        ).first()


# --- one graph, every client -------------------------------------------------------------------


async def test_every_statement_of_a_principal_sees_what_its_earlier_ones_made(dbs, monkeypatch):
    # Each statement loads the session from its row: a second tab, connection or worker is
    # another load of the same row.
    state, h = await _filled(dbs, monkeypatch)
    texts, _ = await _run(state, h, "MATCH (n:Note) RETURN n.text AS t ORDER BY t")
    assert texts == [{"t": "hello"}, {"t": "world"}]
    starred, _ = await _run(state, h, "MATCH (p:Starred) RETURN p.name AS name")
    assert starred == [{"name": "Ada"}]
    nexts, _ = await _run(state, h, "MATCH (:Note)-[r:NEXT]->(b:Note) RETURN b.text AS t")
    assert nexts == [{"t": "world"}]


async def test_its_store_schema_is_the_principals_own_on_every_statement(dbs, monkeypatch):
    state, h = await _filled(dbs, monkeypatch)
    _, first = await _run(state, h, "MATCH (n:Note) RETURN n.text AS t")
    _, second = await _run(state, h, "CREATE (:Note {text: 'again'})")
    _, other = await _run(state, h, "MATCH (p:Person) RETURN p.name AS name", user="bo")
    assert first.session.id == second.session.id == graph_session.session_id("ada")
    assert other.session.id == graph_session.session_id("bo") != first.session.id
    assert first.session.kept


async def test_a_statement_that_changed_it_advances_its_generation_and_a_read_does_not(
    dbs, monkeypatch
):
    state, h = await _filled(dbs, monkeypatch)
    before = (await _row(dbs)).generation
    _, read = await _run(state, h, "MATCH (n:Note) RETURN n.text AS t")
    assert not read.changed and (await _row(dbs)).generation == before
    _, wrote = await _run(state, h, "CREATE (:Note {text: 'third'})")
    assert wrote.changed and (await _row(dbs)).generation == before + 1


async def test_another_principal_never_reaches_it(dbs, monkeypatch):
    state, h = await _filled(dbs, monkeypatch)
    async with graph_session.session_for(state, "bo", ["analyst"], writes=False) as held:
        assert store.graph(held.session).empty and not held.session.tables


async def test_a_statement_made_within_one_is_in_its_session_already(dbs):
    state = _state(dbs)
    async with graph_session.session_for(state, "ada", ["analyst"], writes=True) as outer:
        assert temp_tables.current() is outer.session
        async with graph_session.session_for(state, "ada", ["analyst"], writes=False) as inner:
            assert inner is outer
    assert temp_tables.current() is None


async def test_what_a_failed_statement_landed_is_kept(dbs, monkeypatch):
    h = Session(monkeypatch, _MODEL)
    state = _state(dbs, h)
    with pytest.raises(RuntimeError, match="after landing"):
        async with graph_session.session_for(state, "ada", ["analyst"], writes=True) as held:
            h.session = held.session
            await seam.write(
                "CREATE (:Note {text: 'kept'})", {}, h.model, h.session, h.state, h.read, h.write
            )
            raise RuntimeError("after landing")
    texts, _ = await _run(state, h, "MATCH (n:Note) RETURN n.text AS t")
    assert texts == [{"t": "kept"}]


# --- the lock -----------------------------------------------------------------------------------


def _in_thread(coro_fn) -> threading.Thread:
    ctx = contextvars.copy_context()
    thread = threading.Thread(target=lambda: ctx.run(asyncio.run, coro_fn()))
    thread.start()
    return thread


async def test_writes_to_one_session_are_done_one_at_a_time(dbs):
    state = _state(dbs)
    async with graph_session.session_for(state, "ada", ["analyst"], writes=True):
        pass  # the row exists
    order: list[str] = []
    first_in = threading.Event()

    async def first():
        async with graph_session.session_for(state, "ada", ["analyst"], writes=True):
            order.append("first in")
            first_in.set()
            time.sleep(0.5)
            order.append("first out")

    async def second():
        first_in.wait()
        async with graph_session.session_for(state, "ada", ["analyst"], writes=True):
            order.append("second in")

    threads = [_in_thread(first), _in_thread(second)]
    for t in threads:
        t.join(timeout=30)
    assert order == ["first in", "first out", "second in"]


async def test_reads_of_one_session_do_not_wait_for_each_other(dbs):
    state = _state(dbs)
    async with graph_session.session_for(state, "ada", ["analyst"], writes=False):
        pass
    order: list[str] = []
    first_in = threading.Event()
    second_done = threading.Event()

    async def first():
        async with graph_session.session_for(state, "ada", ["analyst"], writes=False):
            order.append("first in")
            first_in.set()
            second_done.wait(timeout=10)
            order.append("first out")

    async def second():
        first_in.wait()
        async with graph_session.session_for(state, "ada", ["analyst"], writes=False):
            order.append("second in")
        second_done.set()

    threads = [_in_thread(first), _in_thread(second)]
    for t in threads:
        t.join(timeout=30)
    assert order == ["first in", "second in", "first out"]


# --- its end ------------------------------------------------------------------------------------


async def test_clearing_ends_it_and_the_next_statement_begins_a_new_one(dbs, monkeypatch, ended):
    state, h = await _filled(dbs, monkeypatch)
    await graph_session.clear(state, "ada")
    assert ended == [graph_session.session_id("ada")]
    assert await _row(dbs) is None
    _, held = await _run(state, h, "MATCH (p:Person) RETURN p.name AS name")
    assert store.graph(held.session).empty


async def test_one_unused_past_its_retention_is_ended(dbs, monkeypatch, ended):
    state, _h = await _filled(dbs, monkeypatch)
    assert await graph_session.sweep(state, datetime.now(UTC) + timedelta(days=6)) == []
    assert await graph_session.sweep(state, datetime.now(UTC) + timedelta(days=8)) == ["ada"]
    assert ended == [graph_session.session_id("ada")] and await _row(dbs) is None


async def test_holding_other_roles_ends_it_at_the_next_statement(dbs, monkeypatch, ended):
    state, h = await _filled(dbs, monkeypatch)
    _, held = await _run(
        state, h, "MATCH (p:Person) RETURN p.name AS name", claims=("analyst", "auditor")
    )
    assert ended == [graph_session.session_id("ada")]
    assert store.graph(held.session).empty


async def test_acting_as_another_of_its_roles_keeps_it(dbs, monkeypatch, ended):
    h = Session(monkeypatch, _MODEL)
    state = _state(dbs, h)
    await _run(state, h, "CREATE (:Note {text: 'x'})", claims=("analyst", "auditor"))
    # Its rights are the union of its roles, whichever it acts as.
    texts, _ = await _run(
        state, h, "MATCH (n:Note) RETURN n.text AS t", claims=("auditor", "analyst")
    )
    assert texts == [{"t": "x"}] and ended == []


async def test_a_change_of_a_roles_grants_ends_it_at_the_next_build(dbs, monkeypatch, ended):
    state, _h = await _filled(dbs, monkeypatch)
    assert await graph_session.sweep(state, datetime.now(UTC)) == []
    # The next build: its inputs are made anew, the analyst granted another column.
    state.role_build_inputs = {
        **state.role_build_inputs,
        "tables": [
            {
                "id": 1,
                "columns": [
                    {"column_name": "name", "visible_to": ["analyst"]},
                    {"column_name": "salary", "visible_to": ["analyst"]},
                ],
            }
        ],
    }
    state.role_build_inputs.pop("graph_session_role_prints", None)
    assert await graph_session.sweep(state, datetime.now(UTC)) == ["ada"]


async def test_a_change_of_the_roles_assigned_to_the_principal_ends_it(dbs, monkeypatch, ended):
    state, _h = await _filled(dbs, monkeypatch)
    async with dbs[0].acquire() as conn:
        await conn.execute_core(insert(roles_table).values(id="auditor", capabilities=[]))
        await conn.execute_core(
            insert(user_role_assignments).values(user_id="ada", role_id="auditor", domain_id="*")
        )
    assert await graph_session.sweep(state, datetime.now(UTC)) == ["ada"]


# --- a kept session's schema is no keeper's ------------------------------------------------------


async def test_a_kept_sessions_schema_is_never_recorded_for_a_keeper_and_an_unkept_one_is(
    dbs, monkeypatch
):
    from provisa.pgwire import temp_keeper

    recorded: list[str] = []

    async def note(_state, session, schema):
        assert not session.kept
        recorded.append(schema)

    monkeypatch.setattr(temp_keeper, "note_stored", note)
    landed: list[str] = []

    class _Replicator:
        def __init__(self, *_a, **_k):
            pass

        async def run(self, _progress):
            landed.append("rows")

    monkeypatch.setattr("provisa.federation.data_replicator.data_replicator", _Replicator)
    from provisa.pgwire import temp_exec

    backend = SimpleNamespace(replica_target=lambda *_a, **_k: object())
    state = SimpleNamespace(
        federation_engine=SimpleNamespace(engine=SimpleNamespace(backend=backend))
    )
    batch = temp_exec._batch([("a", "text")], [("x",)])
    kept, unkept = TempSession(kept=True), TempSession()
    await temp_exec._land(state, kept, "t", batch, [("a", "text")])
    await temp_exec._land(state, unkept, "t", batch, [("a", "text")])
    assert landed == ["rows", "rows"] and kept.stored and unkept.stored
    assert recorded == [temp_exec._schema(unkept)]


async def test_a_kept_session_refuses_a_keepers_record(dbs):
    from provisa.pgwire import temp_keeper

    with pytest.raises(AssertionError, match="its own sweep"):
        await temp_keeper.note_stored(_state(dbs), TempSession(kept=True), "s")


# --- the graph's JSON ---------------------------------------------------------------------------


async def test_the_graph_is_the_same_after_its_json(dbs, monkeypatch):
    _state_, h = await _filled(dbs, monkeypatch)
    g = store.graph(h.session)
    assert not g.empty and g.rel_pairs and g.node_keys
    again = store.graph_from_json(json.loads(json.dumps(store.graph_to_json(g))))
    assert again == g


# --- the page -----------------------------------------------------------------------------------


def _request(user="ada") -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/data/cypher/session",
            "headers": [],
            "query_string": b"",
            "state": {
                "identity": SimpleNamespace(user_id=user, roles=["analyst"], derived_roles={}),
                "role": "analyst",
            },
        }
    )


def _body(response) -> dict:
    return json.loads(bytes(response.body))


async def test_the_page_reads_its_labels_and_types_with_their_counts(dbs, monkeypatch):
    import provisa.api.app as appmod

    state, _h = await _filled(dbs, monkeypatch)
    monkeypatch.setattr(appmod, "state", state)
    body = _body(await cypher_session.describe(_request()))
    assert body == {
        "labels": [
            {"label": "Note", "nodes": 2, "tagged_model_nodes": 0},
            {"label": "Starred", "nodes": 0, "tagged_model_nodes": 1},
        ],
        "types": [{"type": "NEXT", "relationships": 1}],
        "empty": False,
    }


async def test_a_new_principal_reads_an_empty_session(dbs, monkeypatch):
    import provisa.api.app as appmod

    monkeypatch.setattr(appmod, "state", _state(dbs))
    assert _body(await cypher_session.describe(_request("new"))) == {
        "labels": [],
        "types": [],
        "empty": True,
    }


async def test_the_page_clears_it(dbs, monkeypatch, ended):
    import provisa.api.app as appmod

    state, _h = await _filled(dbs, monkeypatch)
    monkeypatch.setattr(appmod, "state", state)
    assert _body(await cypher_session.clear(_request())) == {"cleared": True}
    assert await _row(dbs) is None


async def test_a_cypher_answer_that_wrote_to_it_says_so(dbs, monkeypatch):
    import provisa.api.app as appmod
    from provisa.api.rest import cypher_router

    h = Session(monkeypatch, _MODEL)
    monkeypatch.setattr(appmod, "state", _state(dbs, h))
    monkeypatch.setattr(
        cypher_router, "role_surface", lambda _s, _r: SimpleNamespace(context=object())
    )
    monkeypatch.setattr(cypher_router, "_build_label_map", lambda *_a: h.model)

    real_write = seam.write

    async def write(statement, params, label_map, session, state, _read, _write):
        h.session = session
        return await real_write(statement, params, label_map, session, h.state, h.read, h.write)

    monkeypatch.setattr(seam, "write", write)
    answer = await cypher_router.cypher_query(
        cypher_router.CypherRequest(query="CREATE (:Note {text: 'x'})"), _request(), None, None
    )
    body = _body(answer)
    assert body["counters"] == {"nodes": 1, "relationships": 0, "labels": 1}
    assert body["session_changed"] is True


async def test_its_retention_is_a_setting_of_7_days():
    from provisa.core import settings_catalog

    declared = {s.key: s for s in settings_catalog.DECLARED}
    kept = declared["temp_tables.graph_session_retention_days"]
    assert (kept.default, kept.unit, kept.effect) == (7, "days", "live")


async def test_a_cached_answer_is_keyed_by_the_sessions_generation():
    from provisa.compiler.temp_tables import TempTable
    from provisa.pgwire._pipeline import _raw_cache_key

    unbound = _raw_cache_key("SELECT 1", [], "analyst", None)
    session = TempSession(id="p1", tables={"t": TempTable("t", [("a", "text")])}, kept=True)
    token = temp_tables.bind(session)
    try:
        first = _raw_cache_key("SELECT 1", [], "analyst", None)
        session.generation += 1
        second = _raw_cache_key("SELECT 1", [], "analyst", None)
    finally:
        temp_tables.unbind(token)
    assert len({unbound, first, second}) == 3
    # A statement read in no session keeps the key it had.
    assert _raw_cache_key("SELECT 1", [], "analyst", None) == unbound
