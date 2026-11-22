# Copyright (c) 2026 Kenneth Stott
# Canary: 6d1f0b52-8a3e-4c47-9e1b-2f7c5a9d3e81
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Closing a DuckDB connection DETACHes every database it attached first.

The DuckDB mssql extension keeps its connection pools in a process-wide registry that only a
DETACH empties (``MSSQLCatalog::OnDetach`` -> ``RemovePool``): a connection closed without one
leaves its pool's sessions signed in to SQL Server for the life of the process (R511-S6A). The
federation runtime's close and the source loader's scratch reads now DETACH each attach before
closing their connection.
"""

from __future__ import annotations

from typing import Any

import duckdb

from provisa.events import source_loader
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime


class _Recording:
    """A DuckDB connection whose statements and close are written to ``log``, in order."""

    def __init__(self, con: Any, log: list[str]) -> None:
        self._con = con
        self._log = log

    def execute(self, sql: str, *args: Any) -> Any:
        self._log.append(sql)
        return self._con.execute(sql, *args)

    def cursor(self) -> Any:
        return self._con.cursor()

    def close(self) -> None:
        self._log.append("close")
        self._con.close()


def test_the_runtime_close_detaches_every_raw_attach_before_closing(monkeypatch):
    runtime = DuckDBFederationRuntime()
    for alias in ("src_a", "src_b"):
        runtime._on_instance(f"ATTACH ':memory:' AS \"{alias}\"")
        runtime._raw_attached.add(alias)
    log: list[str] = []
    real_on_instance = runtime._on_instance

    def recording_on_instance(sql: str, params: list | None = None) -> None:
        log.append(sql)
        real_on_instance(sql, params)

    monkeypatch.setattr(runtime, "_on_instance", recording_on_instance)
    monkeypatch.setattr(runtime, "_con", _Recording(runtime._con, log))

    runtime.close()

    assert log[-1] == "close"
    assert sorted(log[:-1]) == ['DETACH "src_a"', 'DETACH "src_b"']
    assert runtime._raw_attached == set()


def _scratch_details() -> dict:
    return {"attach": "ATTACH ':memory:' AS \"_src_scratch\"", "raw_alias": "_src_scratch"}


def _recording_connect(monkeypatch, log: list[str]) -> None:
    from provisa.core import engine_memory

    monkeypatch.setattr(engine_memory, "connect", lambda: _Recording(duckdb.connect(), log))


def test_a_scratch_read_detaches_its_attach_before_closing(monkeypatch):
    log: list[str] = []
    _recording_connect(monkeypatch, log)

    rows = source_loader._duckdb_extension_scratch_read(_scratch_details(), "SELECT 1 AS one")

    assert rows == [{"one": 1}]
    assert log[-2:] == ['DETACH "_src_scratch"', "close"]


def test_a_scratch_batch_read_detaches_its_attach_before_closing(monkeypatch):
    log: list[str] = []
    _recording_connect(monkeypatch, log)

    batches = list(
        source_loader._duckdb_extension_scratch_batches(
            _scratch_details(), "SELECT * FROM range(3) t(n)", 2
        )
    )

    assert batches == [[{"n": 0}, {"n": 1}], [{"n": 2}]]
    assert log[-2:] == ['DETACH "_src_scratch"', "close"]


def test_an_abandoned_scratch_batch_read_detaches_its_attach_before_closing(monkeypatch):
    log: list[str] = []
    _recording_connect(monkeypatch, log)

    batches = source_loader._duckdb_extension_scratch_batches(
        _scratch_details(), "SELECT * FROM range(3) t(n)", 1
    )
    assert next(batches) == [{"n": 0}]
    batches.close()

    assert log[-2:] == ['DETACH "_src_scratch"', "close"]


def test_a_scratch_read_of_a_file_names_no_attach_and_detaches_nothing(monkeypatch):
    log: list[str] = []
    _recording_connect(monkeypatch, log)

    rows = source_loader._duckdb_extension_scratch_read({}, "SELECT 2 AS two")

    assert rows == [{"two": 2}]
    assert not any(sql.startswith("DETACH") for sql in log)
    assert log[-1] == "close"
