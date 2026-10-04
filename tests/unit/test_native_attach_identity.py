# Copyright (c) 2026 Kenneth Stott
# Canary: 3b4c4d4a-a5c0-4f00-9fac-206c05cdad00
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A native engine attaches every registered table, each by its identity (REQ-1674).

The walk that attaches registered tables remembered what it had attached by ``schema.table``:
two sources that both hold ``public.orders`` were one entry, and the second source's table was
skipped as already attached — never queryable on the engine."""

# Requirements: REQ-1674, REQ-841

from __future__ import annotations

from types import SimpleNamespace

from provisa.federation.native_backend import NativeEngineBackend


class _Runtime:
    def __init__(self) -> None:
        self.attached: list[tuple[str, str, str]] = []

    def attach_source(self, source) -> None:
        self.attached.append((source.id, source.schema_name, source.table_name))


class _Backend(NativeEngineBackend):
    def __init__(self) -> None:  # no engine wiring: the walk alone is under test
        self._runtime = _Runtime()
        # The walk decides whether to attach a source from its connector's reach (reads_in_place):
        # a live-in-place source (anything but the land-only openapi here) is attached.
        self.engine = SimpleNamespace(
            name="duckdb",
            connector_for=lambda source_type: SimpleNamespace(
                reads_in_place=source_type != "openapi"
            ),
        )
        self._attached = set()
        self._detached = set()
        self._refused = set()


def _src(source_id: str):
    return SimpleNamespace(
        id=source_id,
        type=SimpleNamespace(value="postgresql"),
        host="h",
        port=5432,
        database="d",
        username="u",
        password="p",
        path=None,
        base_url=None,
        federation_hints={},
        mapping={},
    )


def test_two_sources_same_named_tables_are_each_attached(monkeypatch):
    monkeypatch.setattr("provisa.core.operator_floor.floor_setting", lambda _src: None)
    backend = _Backend()
    config = SimpleNamespace(
        sources=[_src("sales"), _src("crm")],
        tables=[
            SimpleNamespace(source_id="sales", schema_name="public", table_name="orders"),
            SimpleNamespace(source_id="crm", schema_name="public", table_name="orders"),
        ],
    )
    state = SimpleNamespace(runtime_sources={}, tables=[], model_db=None, tenant_db=None)

    assert backend._walk_registry(state, config) is True
    assert sorted(backend._runtime.attached) == [
        ("crm", "public", "orders"),
        ("sales", "public", "orders"),
    ]
