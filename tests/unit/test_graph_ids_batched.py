# Copyright (c) 2026 Kenneth Stott
# Canary: fc645ba0-a8c8-4502-b118-9aee92a77856
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Cypher read registers its nodes' and relationships' durable ids in batched statements (REQ-394).

Each node used to be a read and an upsert of its own -- four statements and a commit or two -- so
``MATCH (n:Label) RETURN n`` over a few hundred rows was hundreds of control-plane commits, and
the readers running at once (the neo4j export's discovery, eight at a time) queued on the store's
writer behind each other's: a read of one label took 30-60 s on a CI runner. The statements a
registration makes no longer grow with the rows it registers.
"""

# Requirements: REQ-394

from __future__ import annotations

import pytest
from sqlalchemy import event, select

from provisa.core import schema_org
from provisa.core.database import Database, create_engine_from_url
from provisa.gql.assembler import register_node_ids, register_rel_ids

pytestmark = pytest.mark.asyncio


@pytest.fixture
def db() -> Database:
    database = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="graph-ids")
    with database.engine.begin() as conn:
        schema_org.node_ids.create(conn)
        schema_org.rel_ids.create(conn)
    return database


def _count_statements(database: Database) -> list[str]:
    seen: list[str] = []

    @event.listens_for(database.engine, "before_cursor_execute")
    def _seen(conn, cursor, statement, parameters, context, executemany):  # noqa: ANN001
        del conn, cursor, parameters, context, executemany
        seen.append(statement)

    return seen


def _nodes(n: int, label: str = "Pet", **props: object) -> list[dict]:
    return [
        {"n": {"id": f"{label}|{i}", "label": label, "properties": {"pk": i, **props}}}
        for i in range(n)
    ]


async def _held(database: Database, table) -> dict[str, tuple[int, dict]]:  # noqa: ANN001
    async with database.acquire() as conn:
        result = await conn.execute_core(
            select(table.c.composite_id, table.c.id, table.c.properties)
        )
        return {r[0]: (r[1], r[2]) for r in result.fetchall()}


async def test_the_statements_do_not_grow_with_the_nodes(db: Database) -> None:
    small, large = _nodes(3), _nodes(300)
    seen = _count_statements(db)
    await register_node_ids(small, db)
    for_three = len(seen)
    seen.clear()
    await register_node_ids([{"n": {**r["n"], "id": f"Big|{i}"}} for i, r in enumerate(large)], db)
    assert len(seen) == for_three


async def test_every_node_gets_its_own_durable_id(db: Database) -> None:
    rows = _nodes(5)
    await register_node_ids(rows, db)
    held = await _held(db, schema_org.node_ids)
    assert sorted(r["n"]["id"] for r in rows) == sorted(v[0] for v in held.values())
    assert len(held) == 5


async def test_a_node_read_again_keeps_its_id_and_writes_nothing(db: Database) -> None:
    first = _nodes(4)
    await register_node_ids(first, db)
    seen = _count_statements(db)
    again = _nodes(4)
    await register_node_ids(again, db)
    assert [r["n"]["id"] for r in again] == [r["n"]["id"] for r in first]
    assert not [s for s in seen if s.lstrip().upper().startswith(("INSERT", "UPDATE"))]


async def test_a_held_nodes_properties_are_merged_the_reads_winning(db: Database) -> None:
    await register_node_ids(_nodes(1, colour="red", size=3), db)
    rows = _nodes(1, colour="blue", name=None)
    await register_node_ids(rows, db)
    ((node_id, props),) = (await _held(db, schema_org.node_ids)).values()
    assert rows[0]["n"]["id"] == node_id
    assert props == {"pk": 0, "colour": "blue", "size": 3}


async def test_relationships_are_registered_the_same_way(db: Database) -> None:
    def edges(n: int) -> list[dict]:
        return [
            {
                "r": {
                    "identity": f"Pet|{i}|OWNS|Owner|{i}",
                    "start": f"Pet|{i}",
                    "end": f"Owner|{i}",
                    "type": "OWNS",
                    "properties": {"since": i},
                    "startNode": {"id": f"Pet|{i}", "label": "Pet", "properties": {}},
                    "endNode": {"id": f"Owner|{i}", "label": "Owner", "properties": {}},
                }
            }
            for i in range(n)
        ]

    seen = _count_statements(db)
    await register_rel_ids(edges(2), db)
    for_two = len(seen)
    seen.clear()
    rows = edges(200)
    await register_rel_ids(rows, db)
    assert len(seen) == for_two
    held = await _held(db, schema_org.rel_ids)
    assert len(held) == 200
    assert {r["r"]["identity"] for r in rows} == {v[0] for v in held.values()}
