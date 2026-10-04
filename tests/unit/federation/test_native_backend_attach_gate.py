# Copyright (c) 2026 Kenneth Stott
# Canary: 97f2d805-052a-4cdb-ab2d-12cdb3de1da8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""The native engine's registry walk attaches only sources it reads LIVE in place (REQ-947/951).

A FETCH/DIRECT source (openapi, graphql_remote, the dq sources) is read from its replica or the API
cache, never attached. The walk must decide that up front from the connector's declared reach, not
attempt an attach the connector has no face for and swallow the KeyError as "table not queryable"
for every such table at startup.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.federation.engine import build_duckdb_engine

pytestmark = pytest.mark.unit


class _RecordingRuntime:
    """Records every attach_source call by source type; never raises (the point under test is
    WHETHER attach is attempted, not how it fails)."""

    def __init__(self) -> None:
        self.attached: list[str] = []

    def attach_source(self, merged) -> None:  # noqa: ANN001
        self.attached.append(merged.type.value)

    def detach_source(self, *_a) -> None:
        pass


def _source(sid: str, type_value: str):
    return SimpleNamespace(
        id=sid,
        type=SimpleNamespace(value=type_value),
        replicate=None,
        load_protected=False,
        host="h",
        port=None,
        base_url=None,
        database=None,
        username=None,
        password=None,
        path=None,
        federation_hints={},
        mapping={},
    )


def _table(sid: str, table: str):
    return SimpleNamespace(source_id=sid, schema_name="api", table_name=table)


def test_the_walk_skips_fetch_sources_and_attaches_live_ones():
    engine = build_duckdb_engine()
    backend = engine.backend
    backend._runtime = _RecordingRuntime()  # noqa: SLF001

    config = SimpleNamespace(
        sources=[
            _source("petstore-api", "openapi"),  # FETCH: read from its replica, never attached
            _source("graphql-demo", "graphql_remote"),  # FETCH
            _source("pg", "postgresql"),  # the engine attaches this live
        ],
        tables=[
            _table("petstore-api", "list_pets"),
            _table("graphql-demo", "widgets"),
            _table("pg", "orders"),
        ],
    )
    state = SimpleNamespace(runtime_sources={}, tables=[])

    complete = backend._walk_registry(state, config)  # noqa: SLF001

    # The FETCH sources were skipped (no attach attempted); only the live-in-place one was attached.
    assert backend._runtime.attached == ["postgresql"]  # noqa: SLF001
    assert complete is True
    # The skipped FETCH tables are not recorded as refused — they are simply not attached here.
    assert ("petstore-api", "api", "list_pets") not in backend._refused  # noqa: SLF001
