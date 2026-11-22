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
        self.dialect = self.engine.dialect  # the runtime speaks its engine's dialect
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


_TABLE = SimpleNamespace(schema_name="tiny", table_name="nation", columns=[])


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


async def test_a_natively_nested_column_is_landed_as_json_the_source_produces():
    """REQ-1965: the land reads a native array, struct or map column through the source's own
    to-JSON expression, by name, so the store receives JSON and no native value."""
    from provisa.transpiler.native_json import source_projection

    columns = [
        SimpleNamespace(name="id", native_filter_type=None, native_type=None),
        SimpleNamespace(
            name="tags",
            native_filter_type=None,
            native_type="varchar[]",
            array_depth=1,
            element_type="text",
        ),
        SimpleNamespace(
            name="amounts",
            native_filter_type=None,
            native_type="decimal(18,2)[]",
            array_depth=1,
            element_type="numeric(18,2)",
        ),
        SimpleNamespace(name="_nf_limit", native_filter_type="query_param", native_type=None),
    ]

    assert source_projection(columns, "duckdb") == (
        '"id", to_json("tags") AS "tags", '
        'to_json(list_transform("amounts", LAMBDA x1 : CAST(x1 AS VARCHAR))) AS "amounts"'
    )
    assert source_projection(columns, "postgres") == (
        '"id", to_jsonb("tags") AS "tags", '
        'to_jsonb(CASE WHEN "amounts" IS NULL THEN NULL ELSE ARRAY(SELECT CAST(x AS TEXT) '
        'FROM UNNEST("amounts") WITH ORDINALITY AS u(x, i) ORDER BY i) END) AS "amounts"'
    )
    assert source_projection(columns[:1], "duckdb") == "*"  # nothing nested: the table as it is
