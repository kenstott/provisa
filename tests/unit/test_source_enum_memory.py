# Copyright (c) 2026 Kenneth Stott
# Canary: 7c1e94d2-5a3b-4f80-b6e2-0d9a8c4f1e37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The enum types a runtime serves are the union of what each PostgreSQL source declared when it
was last read (REQ-221, REQ-2171).

Each source's enums are held under its id, so reading one source (one just registered, or one
whose connection changed) leaves the others' as they were read, without contacting them, and a
deleted source's enums leave with it."""

# Requirements: REQ-221, REQ-2171

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from provisa.api import app_loaders
from provisa.api.org_runtime import OrgRuntime
from provisa.core.models import Source, SourceType


class _Driver:
    def __init__(self, registry: dict[str, list[str]]) -> None:
        self.registry = registry
        self.reads = 0

    async def fetch_enums(self) -> dict[str, list[str]]:
        self.reads += 1
        return self.registry

    async def fetch_source_enums(self) -> dict:
        return {("public", name): values for name, values in self.registry.items()}


def _pg(sid: str) -> Source:
    return Source(
        id=sid, type=SourceType.postgresql, host="h", port=5432, database="d", username="u"
    )


@pytest.fixture
def worker(monkeypatch):
    runtime = OrgRuntime(org_id="acme")
    drivers: dict[str, _Driver] = {}
    pools = SimpleNamespace(has=lambda sid: sid in drivers, get=lambda sid: drivers[sid])
    state = SimpleNamespace(_active_runtime=lambda: runtime, source_pools=pools)
    monkeypatch.setattr("provisa.api.app.state", state)
    return SimpleNamespace(runtime=runtime, drivers=drivers)


def test_reading_one_source_keeps_the_enums_read_from_another(worker):
    worker.drivers["a"] = _Driver({"mood": ["sad", "happy"]})
    asyncio.run(app_loaders._read_source_enums([_pg("a")]))
    worker.drivers["b"] = _Driver({"size": ["s", "m"]})
    asyncio.run(app_loaders._read_source_enums([_pg("b")]))
    assert set(worker.runtime.pg_enum_types) == {"mood", "size"}
    assert set(worker.runtime.pg_source_enums) == {("public", "mood"), ("public", "size")}
    assert worker.drivers["a"].reads == 1  # not contacted again


def test_a_forgotten_source_takes_its_enums_with_it(worker):
    worker.drivers["a"] = _Driver({"mood": ["sad"]})
    worker.drivers["b"] = _Driver({"size": ["s"]})
    asyncio.run(app_loaders._read_source_enums([_pg("a"), _pg("b")]))
    app_loaders.forget_source_enums(worker.runtime, "b")
    assert set(worker.runtime.pg_enum_types) == {"mood"}
    assert set(worker.runtime.pg_source_enums) == {("public", "mood")}


def test_a_source_read_again_replaces_what_it_declared_before(worker):
    worker.drivers["a"] = _Driver({"mood": ["sad"]})
    asyncio.run(app_loaders._read_source_enums([_pg("a")]))
    worker.drivers["a"] = _Driver({"tone": ["low"]})
    asyncio.run(app_loaders._read_source_enums([_pg("a")]))
    assert set(worker.runtime.pg_enum_types) == {"tone"}
