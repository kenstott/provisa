# Copyright (c) 2026 Kenneth Stott
# Canary: 3f7a2c91-6e4b-4d18-a5c0-9b2e8d1f6a47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A native engine's close releases its persistent runtime (REQ-825).

The runtime holds what it attached -- a DuckDB-attached Postgres source keeps a pool of its own --
so a runtime left open at shutdown kept those connections until the process exited. Close
releases it and forgets its attaches. A use after close is refused by name (``EngineClosed``,
REQ-1316) and builds nothing; once the next lifespan opens the engine, a use builds a new
runtime."""

# Requirements: REQ-825, REQ-1316

from __future__ import annotations

import pytest

from provisa.federation.engine import build_engine
from provisa.federation.native_backend import NativeEngineBackend
from provisa.federation.runtime_support import EngineClosed


class _Runtime:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _Backend(NativeEngineBackend):
    def __init__(self) -> None:
        super().__init__(engine=build_engine("duckdb"))
        self.built: list[_Runtime] = []

    def _new_runtime(self) -> _Runtime:
        runtime = _Runtime()
        self.built.append(runtime)
        return runtime


def test_close_releases_the_runtime_refuses_use_until_open_then_builds_a_new_one():
    backend = _Backend()
    first = backend._store_runtime()
    backend._attached.add(("cat", "src", "public", "orders"))
    backend.close(state=None)
    assert first.closed
    assert backend._attached == set()
    with pytest.raises(EngineClosed, match="duckdb"):
        backend._store_runtime()
    assert backend.built == [first]
    backend.open(state=None)
    second = backend._store_runtime()
    assert second is not first and not second.closed
    assert backend.built == [first, second]


def test_close_with_no_runtime_built_is_a_no_op():
    backend = _Backend()
    backend.close(state=None)
    assert backend.built == []
