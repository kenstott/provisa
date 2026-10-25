# Copyright (c) 2026 Kenneth Stott
# Canary: 8e4b2d19-6c7a-4f30-b5e1-2a9d7c40f6b3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A MongoDB source floored to its replica has no live attach on any engine (REQ-1912), so on an
engine that attaches MongoDB in place (DuckDB) its replica is built by reading the collection
over its own driver -- never by the engine's read of the attach the floor removed ("Table with
name order_docs does not exist")."""

from __future__ import annotations

from types import SimpleNamespace

import provisa.api.app  # noqa: F401 -- the API layer says whether the deployment holds many orgs, which reading a source's secrets asks
from provisa.core.operator_floor import floor_setting
from provisa.events import source_loader as sl
from provisa.federation.replica_source import BlockingCursorSource, EngineTableSource

COLUMNS = [("order_id", "integer"), ("status", "varchar")]


def _table():
    return SimpleNamespace(
        schema_name="provisa_bench",
        table_name="order_docs",
        columns=[SimpleNamespace(name=c, native_filter_type=None) for c, _ in COLUMNS],
        landing_filter=None,
        query_template=None,
    )


def _source(replicate):
    return SimpleNamespace(
        id="bench-mongodb",
        type="mongodb",
        host="localhost",
        port=27017,
        database="provisa_bench",
        username=None,
        password=None,
        federation_hints={},
        replicate=replicate,
    )


def _loader():
    # An engine that attaches MongoDB in place registers no MongoDB adapter loader.
    return sl.SourceRowLoader(object(), adapter_loaders={})


def test_a_floored_mongodb_source_builds_its_replica_over_its_driver():
    source = _source(0)
    assert floor_setting(source) is not None
    reader = _loader().replica_source(SimpleNamespace(source_pools=None), source, _table(), COLUMNS)
    assert not isinstance(reader, EngineTableSource)  # not the attach the floor removed
    assert isinstance(reader, BlockingCursorSource)


def test_a_mongodb_source_not_floored_keeps_the_engines_read_of_its_attach():
    source = _source(None)
    assert floor_setting(source) is None
    reader = _loader().replica_source(SimpleNamespace(source_pools=None), source, _table(), COLUMNS)
    assert isinstance(reader, EngineTableSource)
