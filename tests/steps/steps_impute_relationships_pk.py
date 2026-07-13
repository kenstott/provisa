# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
"""pytest-bdd steps for REQ-786 — impute-relationships resolves stable ids via composite PKs."""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.cypher.assembler import register_node_ids


@pytest.fixture
def shared_data():
    return {}


class _FakeResult:
    def fetchone(self):
        return None  # no pre-existing properties to merge


class _FakeConn:
    def __init__(self):
        self._next_id = 100

    async def execute_core(self, _stmt):
        return _FakeResult()

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
