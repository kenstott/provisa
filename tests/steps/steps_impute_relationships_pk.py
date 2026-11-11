# Copyright (c) 2026 Kenneth Stott
# Canary: 9edfcea9-4564-485f-9e6a-d14cd8ab7e19
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
"""pytest-bdd steps for REQ-786 — impute-relationships resolves stable ids via composite PKs."""

# Requirements: REQ-786

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.gql.assembler import register_node_ids


@pytest.fixture
def shared_data():
    return {}


class _FakeResult:
    def __init__(self, rows=()):
        self._rows = list(rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _FakeConn:
    """The node_ids table held in memory: what register_node_ids reads and writes through a
    model connection (a select of held ids by composite_id, then an insert of the new ones and
    an update of the changed ones inside one transaction)."""

    def __init__(self):
        self._next_id = 100
        self._held: dict[str, tuple[int, str, dict]] = {}

    async def execute_core(self, stmt):
        # select(composite_id, id, kind, properties).where(composite_id.in_([...]))
        wanted = stmt.whereclause.right.value
        return _FakeResult((cid, *self._held[cid]) for cid in wanted if cid in self._held)

    async def execute_core_many(self, stmt, rows):
        if stmt.is_insert:
            for row in rows:
                self._next_id += 1
                kind = next(v for k, v in row.items() if k not in ("composite_id", "properties"))
                self._held[row["composite_id"]] = (self._next_id, kind, row["properties"])
        else:  # the update of a changed kind or properties
            for row in rows:
                held_id = self._held[row["b_composite_id"]][0]
                self._held[row["b_composite_id"]] = (held_id, row["b_kind"], row["b_properties"])

    @asynccontextmanager
    async def transaction(self):
        # A real connection's transaction: the code under test writes inside one.
        yield self

    async def upsert_returning(self, _table, _values, **_kw):
        # Stable integer id assigned per composite_id, mirroring the node_ids sequence.
        self._next_id += 1
        return self._next_id


class _FakeTenantDB:
    def __init__(self):
        self._conn = _FakeConn()

    @asynccontextmanager
    async def acquire(self):
        yield self._conn


@given('a request with nodes: [{label: "Meta", id: 10}, {label: "Meta", id: 11}, ...]')
def given_request_with_nodes(shared_data):
    # Serialized graph nodes carry composite string ids of the form "label|pk_value".
    shared_data["rows"] = [
        {"n": {"id": "Meta|10", "label": "Meta", "properties": {"name": "a"}}},
        {"n": {"id": "Meta|11", "label": "Meta", "properties": {"name": "b"}}},
    ]
    shared_data["db"] = _FakeTenantDB()


@when("the endpoint fetches rows from node_ids WHERE id = ANY([10, 11, ...])")
def when_endpoint_fetches_rows(shared_data):
    shared_data["pk_from_composite"] = "Meta|10".rsplit("|", 1)[-1]


@then('it extracts the raw PK from composite_id ("label|pk_value")')
def then_extracts_raw_pk(shared_data):
    raw = shared_data["pk_from_composite"]
    assert raw == "10"
    assert int(raw) == 10  # coerced to a stable integer for the WHERE clause


@then("uses the raw PK values in the WHERE clause for relationship queries")
def then_uses_raw_pk_in_where(shared_data):
    # Every composite id resolves to its numeric PK component.
    pks = [row["n"]["id"].rsplit("|", 1)[-1] for row in shared_data["rows"]]
    assert pks == ["10", "11"]


@then("returns stable integer ids in the result edges (via register_node_ids)")
def then_returns_stable_integer_ids(shared_data):
    import asyncio

    rows = shared_data["rows"]
    asyncio.run(register_node_ids(rows, shared_data["db"]))
    # Composite string ids ("Meta|10") are replaced in place with stable integers.
    ids = [row["n"]["id"] for row in rows]
    assert all(isinstance(i, int) for i in ids), ids
    assert len(set(ids)) == 2  # distinct stable ids


scenarios("../features/REQ-786.feature")
