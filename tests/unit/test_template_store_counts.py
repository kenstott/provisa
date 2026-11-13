# Copyright (c) 2026 Kenneth Stott
# Canary: 79682ed5-7426-4726-bf9e-07b5e06aaa91
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A template's readiness record reads the rows its store holds while replicas it does not wait
for are still being built (REQ-2006).

The reviews template was ready once its Elasticsearch replica landed; its Neo4j tables, which it
does not wait for, were built beside it. Its record listed the store's tables, a Neo4j table's
build table among them, and counted each in a later operation: the build had swapped in between,
the build table was gone, and the template failed ("Table with name build__... does not exist"),
so a visitor's open was refused as sandbox.demo_failed (release 509, run 43491470642)."""

# Requirements: REQ-2006

from __future__ import annotations

import pyarrow as pa
import pytest

from provisa.api import demo_replicas
from provisa.federation import materialize_broker
from provisa.federation.materialize_broker import get_broker
from provisa.federation.replica_target import DuckDBStoreTarget

pytestmark = pytest.mark.unit

ORG = "sbxtplreviews"
COLUMNS = [("id", "bigint")]


def _batch(n: int) -> pa.RecordBatch:
    return pa.record_batch([pa.array(range(n), pa.int64())], names=["id"])


class _SwapsBetweenOperations:
    """The store as a reader sees it while a replica build runs beside it: the build's swap lands
    right after the reader's first operation on the store, before its next one."""

    def __init__(self, broker, swap) -> None:
        self._broker = broker
        self._swap = swap

    def __getattr__(self, name):
        method = getattr(self._broker, name)

        def call(*args, **kwargs):
            result = method(*args, **kwargs)
            if self._swap is not None:
                swap, self._swap = self._swap, None
                swap()
            return result

        return call


@pytest.fixture
def store(tmp_path, monkeypatch):
    path = str(tmp_path / "materialize.duckdb")
    monkeypatch.setattr(demo_replicas, "_store_path", lambda org: path)
    monkeypatch.setattr("provisa.core.org_files.folder", lambda org: tmp_path)
    return path


async def test_the_record_counts_the_landed_replicas_while_another_is_swapped_in(
    store, monkeypatch
):
    schema = demo_replicas._replicas_schema(ORG)
    broker = get_broker(store)
    landed = DuckDBStoreTarget(
        broker, schema=schema, table="reviews__default__reviews", columns=COLUMNS
    )
    await landed.begin()
    await landed.write(_batch(3), [])
    await landed.swap()
    building = DuckDBStoreTarget(
        broker, schema=schema, table="purchases__neo4j__customers", columns=COLUMNS
    )
    await building.begin()
    await building.write(_batch(5), [])

    def swap() -> None:
        broker.replica_swap(schema, "purchases__neo4j__customers", building._build)

    monkeypatch.setattr(
        materialize_broker,
        "get_broker",
        lambda path: _SwapsBetweenOperations(get_broker(path), swap),
    )

    counts = demo_replicas.store_counts(ORG)

    # What the store held at one moment: the landed replica, and not the build table, which is no
    # replica (nothing reads it until it is swapped in).
    assert counts == {"replicas": {"reviews__default__reviews": 3}, "views": {}}


async def test_the_record_leaves_out_a_build_in_progress(store):
    schema = demo_replicas._replicas_schema(ORG)
    broker = get_broker(store)
    landed = DuckDBStoreTarget(
        broker, schema=schema, table="reviews__default__reviews", columns=COLUMNS
    )
    await landed.begin()
    await landed.write(_batch(3), [])
    await landed.swap()
    building = DuckDBStoreTarget(
        broker, schema=schema, table="purchases__neo4j__customers", columns=COLUMNS
    )
    await building.begin()
    await building.write(_batch(5), [])

    assert demo_replicas.store_counts(ORG)["replicas"] == {"reviews__default__reviews": 3}
