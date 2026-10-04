# Copyright (c) 2026 Kenneth Stott
# Canary: 1ee4bed2-3b58-4447-bbe9-92e041af2a83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A direct-driver source the engine cannot read in place is landed from its own driver.

Trino as a source has a direct driver (the SQLAlchemy trino dialect) and no DuckDB connector. Its
replica is built by reading the source through that driver; reading it through the engine names a
catalog DuckDB never attached ("Catalog ... does not exist"). A source the engine does attach is
still read through the engine unless the operator floors it."""

# Requirements: REQ-030, REQ-1141, REQ-1732

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.events.source_loader import make_floored_direct_loader
from provisa.federation.engine import build_engine


class _Engine:
    """The engine runtime: records which read path a land took."""

    def __init__(self, kind: str) -> None:
        self.engine = build_engine(kind)
        self.reads: list[str] = []

    async def execute_engine(self, sql, authorization=None):
        self.reads.append("engine")
        return SimpleNamespace(column_names=["n"], rows=[(1,)])

    async def execute_native(self, pools, source_id, sql, params):
        self.reads.append("driver")
        return SimpleNamespace(column_names=["n"], rows=[(1,)])


def _state() -> SimpleNamespace:
    return SimpleNamespace(source_dialects={"src": "trino"}, source_pools=object())


def _source(stype: str, **settings) -> SimpleNamespace:
    return SimpleNamespace(id="src", type=stype, **settings)


_TABLE = SimpleNamespace(schema_name="tiny", table_name="nation")


@pytest.mark.asyncio
async def test_a_trino_source_on_duckdb_is_read_through_its_own_driver():
    engine = _Engine("duckdb")
    rows = await make_floored_direct_loader(_state(), engine)(_source("trino"), _TABLE)
    assert rows == [{"n": 1}]
    assert engine.reads == ["driver"]


@pytest.mark.asyncio
async def test_a_source_the_engine_attaches_is_read_through_the_engine():
    engine = _Engine("duckdb")
    await make_floored_direct_loader(_state(), engine)(_source("postgresql"), _TABLE)
    assert engine.reads == ["engine"]


@pytest.mark.asyncio
async def test_a_floored_source_the_engine_attaches_is_read_through_its_own_driver():
    engine = _Engine("duckdb")
    await make_floored_direct_loader(_state(), engine)(_source("postgresql", replicate=0), _TABLE)
    assert engine.reads == ["driver"]
