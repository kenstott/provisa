# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
# ruff: noqa: F811  (the `wiring` fixture is imported and named as a test's argument)

"""REQ-1915: the replicas one read of a source gives are assembled into one build -- each
table its own address, build table, record of how it copies and stamp on its freshness node,
around ONE read. The fakes are the single build's."""

from types import SimpleNamespace

import pyarrow as pa
import pytest

from provisa.federation import replica_builds
from provisa.federation.data_replicator import SourceCaps, SourceRead
from tests.unit.federation.test_replica_builds import (  # noqa: F401  (wiring is a fixture)
    _Backend,
    _col,
    _source,
    _state,
    _Target,
    wiring,
)

pytestmark = pytest.mark.unit

NAMES = ("messages", "threads", "attachments")
KEYS = [("s1", "public", name) for name in NAMES]


def _table(name):
    return SimpleNamespace(
        source_id="s1",
        schema_name="public",
        table_name=name,
        change_signal=None,
        watermark_column=None,
        probe_type=None,
        live=None,
        row_materialize=False,
        columns=[_col("id", "bigint", pk=True), _col("name", "text")],
    )


class _Targets(_Backend):
    """A store that keeps each table's build table apart."""

    def __init__(self):
        super().__init__()
        self.targets: dict[str, _Target] = {}

    def replica_target(self, state, *, address, args, engine):
        target = _Target([], address, args)
        self.targets[address.table] = target
        return target


class _GroupRead:
    caps = SourceCaps(frozenset({SourceRead.CURSOR}))

    def __init__(self, tables, columns, seen, *, fail=False):
        self._tables, self._columns, self._fail = tables, columns, fail
        seen["asked"] = (tuple(tables), {t: list(c) for t, c in columns.items()})
        seen["reads"] = seen.get("reads", 0)
        self._seen = seen

    def notes(self):
        return []

    async def batches(self, batch_rows):
        self._seen["reads"] += 1
        for n, table in enumerate(self._tables):
            if self._fail and n == 1:
                raise RuntimeError("the source stopped answering")
            yield table, pa.RecordBatch.from_pylist([{"id": n, "name": table}])


@pytest.fixture
def group(wiring, monkeypatch):
    """The single build's wiring, with an adapter that gives NAMES from one read and the
    freshness node's stamps recorded."""
    seen = wiring
    seen["stamps"] = []
    seen["fail"] = False

    def loader(source, table):
        raise AssertionError("a group build never reads one table alone")

    loader.replica_group = lambda source, table: NAMES if table.table_name in NAMES else None
    loader.replica_group_source = lambda source, tables, columns: _GroupRead(
        tables, columns, seen, fail=seen["fail"]
    )
    monkeypatch.setattr(
        "provisa.events.app_wiring.build_adapter_loaders", lambda s, e: {"openapi": loader}
    )

    async def record_refresh(conn, node, *, at, ok):
        seen["stamps"].append((node, ok))

    monkeypatch.setattr("provisa.events.queue.record_refresh", record_refresh)
    started = seen.setdefault("all_started", [])

    async def _started(conn, key, *, method, load_kind):
        started.append((key, method, load_kind))

    monkeypatch.setattr("provisa.federation.replica_state.record_started", _started)
    return seen


def _group_state(backend, names=NAMES):
    return _state(backend, tables=[_table(n) for n in (*names, "folders")])


async def _noop(key, rows):
    return None


async def test_the_tables_of_one_read_are_found_among_the_declared_replicas(group):
    group["record"] = SimpleNamespace(retired_at=None)
    state = _group_state(_Targets())
    assert await replica_builds.group_of(state, KEYS[0]) == sorted(KEYS[1:])
    assert await replica_builds.group_of(state, ("s1", "public", "folders")) == []
    # A table the model does not declare, or one with no replica, is no sibling.
    assert await replica_builds.group_of(_group_state(_Targets(), NAMES[:2]), KEYS[0]) == [KEYS[1]]
    group["record"] = None
    assert await replica_builds.group_of(state, KEYS[0]) == []
    group["record"] = SimpleNamespace(retired_at="yesterday")
    assert await replica_builds.group_of(state, KEYS[0]) == []


async def test_a_source_that_gives_no_group_has_none(wiring):
    wiring["record"] = SimpleNamespace(retired_at=None)
    assert await replica_builds.group_of(_group_state(_Targets()), KEYS[0]) == []


async def test_one_read_builds_each_claimed_table_at_its_own_address(group):
    backend = _Targets()
    progress: list = []

    async def seen_progress(key, rows):
        progress.append((key, rows))

    built = await replica_builds.build_group(_group_state(backend), KEYS, seen_progress)

    assert group["reads"] == 1
    asked_tables, asked_columns = group["asked"]
    assert asked_tables == NAMES  # the claimed tables, and no other of the read's
    assert asked_columns["threads"] == [("id", "bigint"), ("name", "text")]
    assert len(backend.targets) == 3
    for target in backend.targets.values():
        assert target.log[0] == "begin" and target.log[-1] == "swap"
    for key in KEYS:
        outcome = built[key]
        assert outcome.rows_copied == 1 and outcome.changed
        assert outcome.definition_hash and outcome.built_columns == [
            ["id", "bigint"],
            ["name", "text"],
        ]
    assert sorted(k for k, _m, _l in group["all_started"]) == sorted(KEYS)
    assert {m for _k, m, _l in group["all_started"]} == {"stream_batches"}
    assert sorted(progress) == sorted((key, 1) for key in KEYS)
    assert [ok for _node, ok in group["stamps"]] == [True, True, True]


async def test_only_the_tables_claimed_are_built(group):
    backend = _Targets()
    built = await replica_builds.build_group(_group_state(backend), KEYS[:2], _noop)
    assert set(built) == set(KEYS[:2]) and len(backend.targets) == 2
    assert group["asked"][0] == NAMES[:2]


async def test_a_read_that_fails_swaps_nothing_and_stamps_every_table_as_failed(group):
    group["fail"] = True
    backend = _Targets()
    with pytest.raises(RuntimeError, match="stopped answering"):
        await replica_builds.build_group(_group_state(backend), KEYS, _noop)
    for target in backend.targets.values():
        assert "swap" not in target.log and target.log[-1] == "abort"
    assert [ok for _node, ok in group["stamps"]] == [False, False, False]


async def test_a_table_the_model_no_longer_declares_is_answered_and_the_rest_are_built(group):
    backend = _Targets()
    state = _group_state(backend, NAMES[:2])  # attachments is gone from the model
    built = await replica_builds.build_group(state, KEYS, _noop)
    assert isinstance(built[KEYS[2]], replica_builds.ReplicaTableGone)
    assert built[KEYS[0]].rows_copied == 1 and built[KEYS[1]].rows_copied == 1
    assert group["asked"][0] == NAMES[:2] and len(backend.targets) == 2


async def test_the_runner_is_given_the_group_calls():
    import inspect

    source = inspect.getsource(replica_builds.make_runner)
    assert "group_of=lambda key: group_of(state, key)" in source
    assert "build_group=lambda keys, progress: build_group(state, keys, progress)" in source
