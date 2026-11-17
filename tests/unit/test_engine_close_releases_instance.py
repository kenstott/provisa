# Copyright (c) 2026 Kenneth Stott
# Canary: acf3e7ac-1e69-4eb0-8f19-ccb65642064a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A closed native engine holds nothing and builds nothing (REQ-1316, REQ-258).

A server's shutdown closes its engine's runtime, and with it the DuckDB instance whose attached
PostgreSQL databases (the control plane among them) each keep a pool of backends. Two ways kept
that pool after the shutdown:

- work still running when the engine closed (a cancelled request's thread, a late walk) reached
  the backend and got a NEW runtime, which attached the control plane again and that nothing
  closed. A closed backend now refuses by name until the next lifespan opens it.
- a statement's result still open (a stream its consumer never drained or closed) kept the
  instance alive: DuckDB's Arrow reader holds it after its cursor and the connection are closed.
  The runtime's close now ends every stream still open, so the instance goes with the close.
"""

# Requirements: REQ-1316, REQ-258

from __future__ import annotations

import subprocess
import sys
from types import SimpleNamespace

import duckdb
import pytest

from provisa.federation.native_backend import NativeEngineBackend
from provisa.federation.runtime_support import EngineClosed


class _Runtime:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _Backend(NativeEngineBackend):
    def __init__(self) -> None:
        super().__init__(SimpleNamespace(name="duckdb"))
        self.built: list[_Runtime] = []

    def _new_runtime(self) -> _Runtime:
        runtime = _Runtime()
        self.built.append(runtime)
        return runtime


def test_a_closed_backend_refuses_by_name_and_builds_no_runtime():
    backend = _Backend()
    first = backend._store_runtime()
    backend.close(None)
    assert first.closed
    with pytest.raises(EngineClosed, match="duckdb"):
        backend._store_runtime()
    assert backend.built == [first]


def test_a_backend_opened_again_builds_a_fresh_runtime():
    backend = _Backend()
    first = backend._store_runtime()
    backend.close(None)
    backend.open(None)
    second = backend._store_runtime()
    assert second is not first and not second.closed


def _file_runtime(path: str):
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    runtime = DuckDBFederationRuntime()
    runtime._con.close()
    runtime._con = duckdb.connect(path)
    return runtime


def _instance_released(path: str) -> bool:
    """True when no instance holds ``path``: DuckDB locks an open database file against other
    processes, so a second process opens it only once this one's instance is gone. (An open in
    this process would wait on the live instance instead of failing.)"""
    opened = subprocess.run(
        [sys.executable, "-c", "import duckdb, sys; duckdb.connect(sys.argv[1]).close()", path],
        capture_output=True,
        timeout=60,
    )
    return opened.returncode == 0


def test_close_ends_an_undrained_arrow_stream_and_releases_the_instance(tmp_path):
    path = str(tmp_path / "engine.db")
    runtime = _file_runtime(path)
    _schema, batches = runtime.run_arrow_stream("SELECT * FROM range(1000000)")
    next(batches)
    runtime.close()
    assert _instance_released(path)
    with pytest.raises(EngineClosed):
        next(batches)


def test_close_ends_an_unread_result_stream_and_releases_the_instance(tmp_path):
    path = str(tmp_path / "engine.db")
    runtime = _file_runtime(path)
    result = runtime.run_sync("SELECT * FROM range(1000000)")
    runtime.close()
    assert _instance_released(path)
    del result


def test_a_stream_closed_by_its_consumer_is_forgotten(tmp_path):
    path = str(tmp_path / "engine.db")
    runtime = _file_runtime(path)
    _schema, batches = runtime.run_arrow_stream("SELECT * FROM range(10)")
    assert sum(b.num_rows for b in batches) == 10
    assert runtime._streams == set()
    runtime.close()
    assert _instance_released(path)
