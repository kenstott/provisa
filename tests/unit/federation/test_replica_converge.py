# Copyright (c) 2026 Kenneth Stott
# Canary: cdec7333-ad37-4bd9-b21a-e9de8a1c4c30
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915, REQ-1919: replicas converge to the declared model. One step compares what the model
says is served from a replica with what the state store records: it requests the builds that
are missing or whose definition changed, and retires — then, after a grace, drops — the
replicas the model no longer declares. No save or delete path asks for either."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from provisa.core.request_context import current_org

from provisa.federation import replica_converge, replica_state
from provisa.federation.replica_address import ReplicaAddress, replica_table_name
from provisa.federation.replica_converge import (
    converge_logged,
    converge_replicas,
    definition_hash,
    drop_retired,
    still_answers,
)
from provisa.federation.replica_locks import BuildLocks
from provisa.federation.replica_state_view import view_for
from provisa.federation.replica_state import RetryPolicy

pytestmark = pytest.mark.unit

STORE = "store-a"
# PostgreSQL advisory locks are held server-wide, not per database, and every xdist worker's
# control plane is a database on one server: each worker process names an org of its own, or a
# replica lock another worker's test holds reads here as another build's.
ORG = f"org1{uuid.uuid4().hex[:8]}"
COLUMNS = [("id", "bigint"), ("name", "text")]


def _source(sid="src", **kw):
    base = dict(
        id=sid,
        type="postgresql",
        host="h",
        port=5432,
        database="d",
        path=None,
        region=None,
        cache_ttl=None,
    )
    base.update(kw)
    return SimpleNamespace(**base)


def _key(table: str, sid="src"):
    return (sid, "public", table)


@pytest.fixture
def plane(control_plane):
    yield control_plane.url, control_plane.org()


class _Store:
    """The engine's store: which replica tables it holds, and every drop made in it."""

    def __init__(self) -> None:
        self.tables: set[str] = set()
        self.dropped: list[str] = []

    def target(self, address):
        store = self

        class _Target:
            async def drop(self) -> None:
                store.dropped.append(address.table)
                store.tables.discard(address.table)

        return _Target()


class _Connectors:
    """The engine's connectors by source type: one that reads in place for the types the model
    says the engine attaches, none for the rest."""

    def __init__(self, model) -> None:
        self._model = model

    def get(self, source_type):
        if source_type in self._model.attaches:
            return SimpleNamespace(reads_in_place=True)
        return None


class _Model:
    """What this process's model declares: the tables served from a replica, with their
    columns, and the model stamp it was loaded at."""

    def __init__(self, plane, monkeypatch, *, stamp=10) -> None:
        self.url, self.db = plane
        self.store = _Store()
        self.tables: dict[tuple, tuple] = {}  # key -> (source, columns, pk)
        self.unresolved: set[tuple] = set()  # declared, a column's type not resolved yet
        self.row_level: set[tuple] = set()  # replicated row by row
        self.parameters: dict[tuple, tuple] = {}  # key -> its parameter columns
        self.regions: dict[tuple, str] = {}  # key -> the region the table names (REQ-1921)
        self.attaches: set[str] = set()  # source types the engine reads in place
        self.drafts: set[tuple] = set()  # declared, and draft: offered to no reader (REQ-1921)
        self.kicks = 0
        model = self
        backend = SimpleNamespace(
            replica_address=lambda state, *, source_id, schema_name, table_name: ReplicaAddress(
                "org_org1_replicas", replica_table_name(source_id, schema_name, table_name)
            ),
            replica_target=lambda state, *, address, args, engine: model.store.target(address),
        )
        self.state = SimpleNamespace(
            federation_engine=SimpleNamespace(
                engine=SimpleNamespace(backend=backend, name="e", connectors=_Connectors(self))
            ),
            model_db=self.db,
            tenant_db=self.db,
            model_stamp=stamp,
        )

        async def replica_tables(engine, state, *, draft=False):
            return [
                (
                    src,
                    {
                        "schema_name": key[1],
                        "table_name": key[2],
                        "row_materialize": key in model.row_level,
                        "region": model.regions.get(key),
                        "cache_ttl": None,
                        "columns": [{"name": name, "native_filter_type": None} for name, _ in cols]
                        + [
                            {"name": name, "native_filter_type": "query_param"}
                            for name in model.parameters.get(key, ())
                        ],
                    },
                )
                for key, (src, cols, _pk) in model.tables.items()
                if (key in model.drafts) == draft
            ]

        async def draft_replica_tables(engine, state):
            return await replica_tables(engine, state, draft=True)

        async def landing_worklist(engine, state, *, draft=False):
            return [
                (src, key[1], key[2], cols, pk, None)
                for key, (src, cols, pk) in model.tables.items()
                if key not in model.unresolved and (key in model.drafts) == draft
            ]

        async def draft_landing_worklist(engine, state):
            return await landing_worklist(engine, state, draft=True)

        def kick(org_id):
            model.kicks += 1

        monkeypatch.setattr("provisa.federation.replica_routing.replica_tables", replica_tables)
        monkeypatch.setattr("provisa.federation.replica_routing.landing_worklist", landing_worklist)
        monkeypatch.setattr(
            "provisa.federation.replica_routing.draft_replica_tables", draft_replica_tables
        )
        monkeypatch.setattr(
            "provisa.federation.replica_routing.draft_landing_worklist", draft_landing_worklist
        )
        monkeypatch.setattr("provisa.federation.replica_builds.store_identity", lambda s: STORE)
        monkeypatch.setattr("provisa.federation.replica_builds.kick", kick)
        monkeypatch.setattr(replica_converge, "last_error", {})

    def declare(self, table: str, *, columns=None, source=None, pk=("id",)) -> tuple:
        key = _key(table)
        self.tables[key] = (source or _source(), list(columns or COLUMNS), list(pk))
        return key

    def wanted_hash(self, key) -> str:
        src, cols, pk = self.tables[key]
        address = ReplicaAddress("org_org1_replicas", replica_table_name(*key))
        return definition_hash(src, address, cols, pk, landing_filter=None)

    async def build(self, key, *, stamp_hash=True) -> None:
        """Complete the requested build of ``key`` as the runner would."""
        now = datetime.now(UTC)
        src, cols, _pk = self.tables[key]
        async with self.db.acquire() as conn:
            assert await replica_state.claim(
                conn, key, holder="t:1", now=now, retry=RetryPolicy(60, 3600)
            )
            await replica_state.record_completed(
                conn,
                key,
                rows_copied=1,
                method="stream_batches",
                content_hash=None,
                store=STORE,
                next_refresh_at=None,
                now=now,
                definition_hash=self.wanted_hash(key) if stamp_hash else None,
                built_columns=[[n, t] for n, t in cols],
            )
        self.store.tables.add(replica_table_name(*key))

    async def record(self, key):
        async with self.db.acquire() as conn:
            return await replica_state.read(conn, key)


@pytest.fixture
def model(plane, monkeypatch):
    return _Model(plane, monkeypatch)


async def test_a_declared_table_with_no_replica_has_its_build_requested(model):
    key = model.declare("orders")
    done = await converge_replicas(model.state)
    assert done.requested == [key] and model.kicks == 1
    record = await model.record(key)
    assert (record.build_state, record.requested_reason, record.model_stamp) == (
        "requested",
        "model",
        10,
    )


async def test_running_it_again_changes_nothing(model):
    key = model.declare("orders")
    await converge_replicas(model.state)
    again = await converge_replicas(model.state)  # the build is requested: nothing to write
    assert (again.requested, again.retired, again.kept) == ([], [], [])
    await model.build(key)
    built = await converge_replicas(model.state)  # built from this definition: nothing to do
    assert (built.requested, built.retired, built.kept) == ([], [], [])
    assert model.kicks == 1


async def test_a_changed_definition_asks_for_a_rebuild(model):
    key = model.declare("orders")
    await converge_replicas(model.state)
    await model.build(key)
    model.declare("orders", source=_source(host="moved-host"))  # the source moved
    done = await converge_replicas(model.state)
    assert done.requested == [key]
    assert (await model.record(key)).requested_reason == "definition"


async def test_a_replica_built_before_definitions_were_recorded_is_rebuilt_once(model):
    key = model.declare("orders")
    await converge_replicas(model.state)
    await model.build(key, stamp_hash=False)
    assert (await converge_replicas(model.state)).requested == [key]


async def test_a_table_whose_types_are_not_resolved_is_declared_but_not_built_yet(model):
    key = model.declare("orders")
    model.unresolved.add(key)
    done = await converge_replicas(model.state)
    assert done.requested == [] and done.retired == []
    assert await model.record(key) is None


async def test_a_row_level_table_is_declared_and_never_built_whole(model):
    """REQ-1865: rows of a row-level table are fetched by key when a statement asks, never
    ahead of one. Convergence asks for no whole build of it and never retires its table."""
    key = model.declare("edges")
    model.row_level.add(key)
    done = await converge_replicas(model.state)
    assert done.requested == [] and done.retired == [] and await model.record(key) is None
    # Where the engine reads the source in place the flag is ignored, and a table the operator
    # puts on its replica is built whole.
    model.attaches.add("postgresql")
    assert (await converge_replicas(model.state)).requested == [key]


async def test_a_parameterized_table_is_declared_and_never_built(model):
    """A table with a parameter column is a function of its arguments: there is no whole to
    copy, so no build is asked for (a build would call the remote with no arguments)."""
    key = model.declare("pet_by_id")
    model.parameters[key] = ("pet_id",)
    plain = model.declare("pets")
    done = await converge_replicas(model.state)
    assert done.requested == [plain] and await model.record(key) is None


def test_the_standing_replica_serves_only_while_it_can_answer_the_model():
    built = [["id", "bigint"], ["name", "text"], ["old", "text"]]
    assert still_answers(built, [("id", "bigint"), ("name", "text")])  # a column removed
    assert not still_answers(built, [("id", "bigint"), ("added", "text")])  # a column added
    assert not still_answers(built, [("id", "text")])  # a column retyped
    assert not still_answers(None, [("id", "bigint")])


async def test_a_replica_that_can_no_longer_answer_is_not_served_until_rebuilt(model):
    org = current_org.get()  # the org the converge ran for (REQ-1266)
    key = model.declare("orders")
    await converge_replicas(model.state)
    await model.build(key)
    view = view_for(model.state)
    assert view.serves(org, key, await model.record(key))

    model.declare("orders", columns=COLUMNS + [("added", "text")])
    done = await converge_replicas(model.state)
    assert done.not_serving == [key] and done.requested == [key]
    assert not view.serves(org, key, await model.record(key))  # the old shape is not read
    await model.build(key)  # the rebuild of the model's definition lands
    assert view.serves(org, key, await model.record(key))

    # a column removed: the old rows can answer everything, and are served until the swap
    model.declare("orders", columns=[("id", "bigint")])
    done = await converge_replicas(model.state)
    assert done.requested == [key] and done.not_serving == []
    assert view.serves(org, key, await model.record(key))


async def test_a_replica_the_model_no_longer_declares_is_retired_then_dropped(model, monkeypatch):
    keep, gone = model.declare("keep"), model.declare("gone")
    await converge_replicas(model.state)
    await model.build(keep)
    await model.build(gone)
    del model.tables[gone]  # deleted, set to Never, demoted: it no longer routes to a replica

    first = await converge_replicas(model.state)
    assert first.retired == [gone]
    assert (await model.record(gone)).retired_at is not None
    assert model.store.dropped == []  # nothing is dropped by the pass that finds it

    locks = BuildLocks(model.url)
    monkeypatch.setattr(replica_converge, "drop_grace_seconds", lambda: 3600.0)
    assert await drop_retired(model.state, locks, ORG) == []  # the wait is not over
    assert model.store.dropped == []

    monkeypatch.setattr(replica_converge, "drop_grace_seconds", lambda: 0.0)
    assert await drop_retired(model.state, locks, ORG) == [gone]
    assert model.store.dropped == [replica_table_name(*gone)]  # only the recorded replica
    assert await model.record(gone) is None
    assert (await model.record(keep)).retired_at is None
    assert await drop_retired(model.state, locks, ORG) == []


async def test_a_retired_replica_is_neither_built_nor_requested_again(model):
    key = model.declare("orders")
    await converge_replicas(model.state)
    await model.build(key)
    del model.tables[key]
    await converge_replicas(model.state)
    now = datetime.now(UTC)
    async with model.db.acquire() as conn:
        assert not await replica_state.request_build(conn, key, replica_state.REASON_READ)
        assert not await replica_state.request_build(conn, key, replica_state.REASON_OPERATOR)
        due = now + timedelta(days=1)
        assert key not in await replica_state.candidates(
            conn, now=due, limit=50, retry=RetryPolicy(0, 3600)
        )
        assert not await replica_state.claim(
            conn, key, holder="t:1", now=due, retry=RetryPolicy(0, 3600)
        )


async def test_a_table_declared_again_before_the_drop_keeps_its_replica(model, monkeypatch):
    key = model.declare("orders")
    await converge_replicas(model.state)
    await model.build(key)
    declared = model.tables.pop(key)
    assert (await converge_replicas(model.state)).retired == [key]
    model.tables[key] = declared  # promoted again, or the setting changed back
    done = await converge_replicas(model.state)
    assert done.kept == [key] and done.requested == []
    assert (await model.record(key)).retired_at is None
    monkeypatch.setattr(replica_converge, "drop_grace_seconds", lambda: 0.0)
    assert await drop_retired(model.state, BuildLocks(model.url), ORG) == []
    assert model.store.dropped == []


async def test_a_table_declared_again_while_its_replica_is_dropped_is_built_again(
    model, monkeypatch
):
    key = model.declare("orders")
    await converge_replicas(model.state)
    await model.build(key)
    declared = model.tables.pop(key)
    assert (await converge_replicas(model.state)).retired == [key]
    target, during = model.store.target, []

    def racing(address):
        inner = target(address)

        class _Target:
            async def drop(self) -> None:
                await inner.drop()
                # Declared again after its table went and before its record did.
                model.tables[key] = declared
                during.append(await converge_replicas(model.state))

        return _Target()

    monkeypatch.setattr(model.store, "target", racing)
    monkeypatch.setattr(replica_converge, "drop_grace_seconds", lambda: 0.0)
    assert await drop_retired(model.state, BuildLocks(model.url), ORG) == []
    # Its table is gone, so the declaration asks for a build; the record stays to hold it.
    assert during[0].kept == [key] and during[0].requested == [key]
    assert (await model.record(key)).retired_at is None


async def test_a_node_with_an_older_model_does_not_retire_a_newer_tables_replica(
    plane, monkeypatch
):
    """Nodes reload at different moments. One that has not yet seen a table added since finds
    its record and must not take it for a table that is gone."""
    newer = _Model(plane, monkeypatch, stamp=11)
    key = newer.declare("added")
    await converge_replicas(newer.state)  # requested under model stamp 11
    older = _Model(plane, monkeypatch, stamp=10)  # has not reloaded: does not declare it
    done = await converge_replicas(older.state)
    assert done.retired == []
    assert (await older.record(key)).retired_at is None
    older.state.model_stamp = 11  # it reloads, and the table really is gone from the model
    assert (await converge_replicas(older.state)).retired == [key]


async def test_a_replica_whose_lock_is_held_is_left_for_the_next_pass(model, monkeypatch):
    key = model.declare("orders")
    await converge_replicas(model.state)
    await model.build(key)
    del model.tables[key]
    await converge_replicas(model.state)
    monkeypatch.setattr(replica_converge, "drop_grace_seconds", lambda: 0.0)
    locks = BuildLocks(model.url)
    builder = locks.claim()
    assert builder.try_replica(ORG, key)  # a build of it is still finishing
    try:
        assert await drop_retired(model.state, BuildLocks(model.url), ORG) == []
        assert model.store.dropped == []
    finally:
        builder.close()
    assert await drop_retired(model.state, BuildLocks(model.url), ORG) == [key]


async def test_a_failed_pass_is_recorded_for_the_operator_and_cleared_by_the_next(
    model, monkeypatch, caplog
):
    import logging

    model.declare("orders")

    async def _boom(engine, state):
        raise RuntimeError("store unreachable")

    monkeypatch.setattr("provisa.federation.replica_routing.landing_worklist", _boom)
    org = current_org.get()  # the org the converge ran for (REQ-1266)
    with caplog.at_level(logging.ERROR):
        await converge_logged(model.state)  # never raises into the model build it runs after
    assert "did not converge" in caplog.text and "store unreachable" in caplog.text
    error = replica_converge.last_error[org]
    assert error["cause"] == "RuntimeError: store unreachable" and error["at"] is not None

    async def _ok(engine, state):
        return []

    monkeypatch.setattr("provisa.federation.replica_routing.landing_worklist", _ok)
    await converge_logged(model.state)
    assert org not in replica_converge.last_error


def test_the_drop_waits_out_two_reloads_and_the_longest_request(monkeypatch):
    values = {
        "config.reload_interval": 2.0,
        "limits.request_timeout": 60.0,
        "limits.request_timeouts": {"flight": 3600.0, "pgwire": 300.0, "graphql": None},
    }
    monkeypatch.setattr("provisa.core.settings_registry.value", lambda key: values[key])
    assert replica_converge.drop_grace_seconds() == 2 * 2.0 + 3600.0
    values["limits.request_timeouts"] = {"flight": None}
    assert replica_converge.drop_grace_seconds() == 2 * 2.0 + 60.0


async def test_a_table_naming_a_region_is_built_only_there(model):
    """REQ-1922: the region a table names builds it; the org's other regions read it from there."""
    from provisa.core import process_region

    platform = {
        "regions": [
            {"id": "eu", "address": "https://eu.example.com"},
            {"id": "us", "address": "https://us.example.com"},
        ]
    }
    was = process_region._region
    try:
        eu_table, everywhere = model.declare("orders"), model.declare("items")
        model.regions[eu_table] = "eu"
        process_region.bind_launch(platform, requested="us")
        done = await converge_replicas(model.state)
        assert done.requested == [everywhere]  # its own copy here; the eu table is eu's
        process_region.bind_launch(platform, requested="eu")
        done = await converge_replicas(model.state)
        assert done.requested == [eu_table]
    finally:
        process_region._region = was


_PLATFORM = {
    "regions": [
        {"id": "eu", "address": "https://eu.example.com"},
        {"id": "us", "address": "https://us.example.com"},
    ]
}


@pytest.fixture
def node_region():
    """Bind this process to a platform region for the test, and unbind it after."""
    from provisa.core import process_region

    was = process_region._region
    yield lambda region: process_region.bind_launch(_PLATFORM, requested=region)
    process_region._region = was


async def _retired_then_dropped(model, monkeypatch, key) -> None:
    done = await converge_replicas(model.state)
    assert done.retired == [key] and done.requested == []
    monkeypatch.setattr(replica_converge, "drop_grace_seconds", lambda: 0.0)
    assert await drop_retired(model.state, BuildLocks(model.url), ORG) == [key]
    assert model.store.dropped == [replica_table_name(*key)]
    assert await model.record(key) is None
    assert (await converge_replicas(model.state)).requested == []  # nor built here again


async def test_a_copy_built_before_its_table_named_another_region_is_retired_and_dropped(
    model, monkeypatch, node_region
):
    """REQ-1922: a table that named no region was built in every region; once it names eu, the
    copy a us node built holds data us may no longer keep — it goes, like an undeclared one."""
    node_region("us")
    key = model.declare("orders")
    await converge_replicas(model.state)
    await model.build(key)
    model.regions[key] = "eu"
    await _retired_then_dropped(model, monkeypatch, key)


async def test_a_copy_left_behind_when_its_table_moves_region_is_retired_and_dropped(
    model, monkeypatch, node_region
):
    node_region("eu")
    key = model.declare("orders")
    model.regions[key] = "eu"
    await converge_replicas(model.state)
    await model.build(key)
    model.regions[key] = "us"
    await _retired_then_dropped(model, monkeypatch, key)


async def test_a_table_moved_back_before_the_drop_keeps_its_copy(model, node_region):
    node_region("eu")
    key = model.declare("orders")
    model.regions[key] = "eu"
    await converge_replicas(model.state)
    await model.build(key)
    model.regions[key] = "us"
    assert (await converge_replicas(model.state)).retired == [key]
    model.regions[key] = "eu"
    done = await converge_replicas(model.state)
    assert done.kept == [key] and done.requested == []


async def test_an_older_model_node_does_not_retire_a_copy_a_newer_model_wants_here(
    plane, monkeypatch, node_region
):
    """A node still on the model before the table was homed here must not retire the copy the
    newer model asked this region for."""
    node_region("eu")
    newer = _Model(plane, monkeypatch, stamp=11)
    key = newer.declare("orders")
    newer.regions[key] = "eu"
    await converge_replicas(newer.state)  # requested here under model stamp 11
    older = _Model(plane, monkeypatch, stamp=10)
    older.declare("orders")
    older.regions[key] = "us"  # the model before: the table was us's
    assert (await converge_replicas(older.state)).retired == []
    assert (await older.record(key)).retired_at is None


# --- REQ-1921: a draft may hold a replica; it is kept, and never built on the model's word -------


async def test_a_draft_table_has_no_build_asked_for_by_the_model(model):
    served, draft = model.declare("orders"), model.declare("refunds")
    model.drafts.add(draft)
    done = await converge_replicas(model.state)
    assert done.requested == [served]
    assert await model.record(draft) is None


async def test_a_table_made_draft_keeps_the_replica_it_has(model, monkeypatch):
    key = model.declare("orders")
    await converge_replicas(model.state)
    await model.build(key)
    model.drafts.add(key)  # taken out of service: offered to no reader, its data still landed

    done = await converge_replicas(model.state)
    assert (done.retired, done.requested) == ([], [])
    assert (await model.record(key)).retired_at is None
    monkeypatch.setattr(replica_converge, "drop_grace_seconds", lambda: 0.0)
    assert await drop_retired(model.state, BuildLocks(model.url), ORG) == []
    assert model.store.dropped == []


async def test_a_build_asked_for_a_draft_by_its_preview_is_not_retired(model):
    key = model.declare("refunds")
    model.drafts.add(key)
    async with model.db.acquire() as conn:  # as a steward's preview asks (query_residency)
        assert await replica_state.request_build(
            conn, key, replica_state.REASON_MODEL, model_stamp=10, now=datetime.now(UTC)
        )
    done = await converge_replicas(model.state)
    assert done.retired == []
    await model.build(key)
    assert (await converge_replicas(model.state)).retired == []
    assert (await model.record(key)).retired_at is None


async def test_a_draft_whose_definition_changed_is_rebuilt_only_when_previewed(model):
    key = model.declare("refunds")
    await converge_replicas(model.state)
    await model.build(key)
    model.drafts.add(key)
    model.declare("refunds", columns=[*COLUMNS, ("note", "text")])
    kicks = model.kicks
    done = await converge_replicas(model.state)
    assert done.requested == [] and model.kicks == kicks


async def test_a_draft_released_is_built_and_kept_as_any_table(model):
    key = model.declare("refunds")
    model.drafts.add(key)
    await converge_replicas(model.state)
    model.drafts.discard(key)
    assert (await converge_replicas(model.state)).requested == [key]


async def test_a_build_another_server_completes_after_this_one_read_its_records_is_not_asked_again(
    model, monkeypatch
):
    """Two servers converge the same model: the second reads the records while the first's
    build is running, and the first completes it before the second asks. The second sees the
    replica built from the model's definition and asks for nothing."""
    key = model.declare("orders")
    await converge_replicas(model.state)
    read_all = replica_state.read_all

    async def read_then_built_elsewhere(conn):
        records = await read_all(conn)
        await model.build(key)  # the other server's build completes here
        return records

    monkeypatch.setattr(replica_state, "read_all", read_then_built_elsewhere)
    done = await converge_replicas(model.state)
    assert done.requested == []
    assert (await model.record(key)).build_state == "idle"


# --- a table's region, while a draft and when it is released (REQ-1921, REQ-1922) ----------------


def test_a_draft_naming_a_region_is_previewed_only_on_that_regions_nodes(node_region):
    """A draft has no replica: its preview reads it, and lands it, where it is read. One that
    names eu is refused on a us node before any statement is built, and read on an eu node."""
    from provisa.core.region_stores import HomeRegionUnavailable
    from provisa.federation.query_residency import require_draft_read_in_its_region

    node_region("us")
    with pytest.raises(HomeRegionUnavailable):
        require_draft_read_in_its_region("refunds", "eu")
    node_region("eu")
    require_draft_read_in_its_region("refunds", "eu")


async def test_a_draft_given_a_region_then_released_is_built_only_in_that_region(
    model, node_region
):
    """Draft, then assigned eu, then released: eu's nodes build it into eu's store; us's never
    do, before the release or after."""
    key = model.declare("refunds")
    model.drafts.add(key)
    model.regions[key] = "eu"
    for region in ("us", "eu"):  # a draft: built by no node on the model's word
        node_region(region)
        assert (await converge_replicas(model.state)).requested == []
    model.drafts.discard(key)  # released
    node_region("us")
    assert (await converge_replicas(model.state)).requested == []
    assert await model.record(key) is None
    node_region("eu")
    assert (await converge_replicas(model.state)).requested == [key]


async def test_a_draft_copy_held_outside_the_region_it_is_given_is_retired(
    model, monkeypatch, node_region
):
    """A draft previewed on a us node while it named no region holds a copy there; once it names
    eu, that copy is data us may not keep, draft or not: it goes."""
    node_region("us")
    key = model.declare("refunds")
    model.drafts.add(key)
    async with model.db.acquire() as conn:  # its steward's preview on us, before a region
        await replica_state.request_build(
            conn, key, replica_state.REASON_MODEL, model_stamp=10, now=datetime.now(UTC)
        )
    await model.build(key)
    model.regions[key] = "eu"
    await _retired_then_dropped(model, monkeypatch, key)


async def test_a_table_moved_to_another_region_is_built_there(model, monkeypatch, node_region):
    """Its copy in the region it left goes (above); the region it moves to builds its own, in its
    own store."""
    from provisa.core import process_region

    monkeypatch.setattr(
        "provisa.federation.replica_builds.store_identity",
        lambda _state: f"{process_region.region()}-store",  # each region's store is its own
    )
    node_region("eu")
    key = model.declare("orders")
    model.regions[key] = "eu"
    await converge_replicas(model.state)
    await model.build(key)
    model.regions[key] = "us"
    node_region("us")
    assert (await converge_replicas(model.state)).requested == [key]
