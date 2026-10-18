# Copyright (c) 2026 Kenneth Stott
# Canary: 5b2d7e49-0c8a-4f13-96e1-a4d8c2f7b305
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A telemetry table registered with its metadata unreadable is said, in an ERROR naming it and
the location Trino could not read, and left as it is: no statement changes it, and the boot's
ensure leaves it out while making the others (observability.ops_trino.metadata_missing; the
reproducer is tests/integration/test_otel_missing_metadata.py)."""

from __future__ import annotations

import logging

from provisa.observability.ops_schema import OPS_TABLES
from provisa.observability.ops_trino import metadata_missing, seed_ops_trino

_LOCATION = "s3://provisa-otel/warehouse/signals/traces-1/metadata/00003.metadata.json"


class _Answer(Exception):
    def __init__(self, error_name: str) -> None:
        super().__init__(f"{error_name}: Metadata not found in metadata location {_LOCATION}")
        self.error_name = error_name


def _engine(probe_raises: Exception | None):
    ran: list[str] = []

    def execute(sql: str):
        ran.append(sql)
        if sql.startswith("SELECT") and probe_raises is not None:
            raise probe_raises

    return execute, ran


def test_a_table_whose_metadata_is_unreadable_is_said_and_left_as_it_is(caplog):
    execute, ran = _engine(_Answer("ICEBERG_MISSING_METADATA"))
    with caplog.at_level(logging.ERROR):
        assert metadata_missing(execute, "traces") is True
    assert ran == ["SELECT count(*) FROM otel.signals.traces"]  # nothing but the probe
    assert "otel.signals.traces" in caplog.text and _LOCATION in caplog.text


def test_an_engine_error_wrapping_it_is_found_through_its_cause():
    try:
        try:
            raise _Answer("ICEBERG_MISSING_METADATA")
        except _Answer as inner:
            raise RuntimeError("engine statement failed") from inner
    except RuntimeError as wrapped:
        execute, ran = _engine(wrapped)
    assert metadata_missing(execute, "metrics") is True
    assert len(ran) == 1


def test_a_readable_table_and_a_table_not_created_yet_are_not_missing():
    for probe in (None, _Answer("TABLE_NOT_FOUND")):
        execute, ran = _engine(probe)
        assert metadata_missing(execute, "logs") is False
        assert ran == ["SELECT count(*) FROM otel.signals.logs"]


class _Cursor:
    def __init__(self, ran: list[str]) -> None:
        self._ran = ran

    def execute(self, sql: str) -> None:
        self._ran.append(sql)
        if sql == "SELECT count(*) FROM otel.signals.traces":
            raise _Answer("ICEBERG_MISSING_METADATA")

    def fetchall(self) -> list:
        return []


class _Conn:
    def __init__(self) -> None:
        self.ran: list[str] = []

    def cursor(self) -> _Cursor:
        return _Cursor(self.ran)


def test_the_boot_ensure_leaves_an_unreadable_table_alone_and_makes_the_others():
    conn = _Conn()
    seed_ops_trino(conn, [])
    touching_traces = [
        sql for sql in conn.ran if "otel.signals.traces" in sql and not sql.startswith("SELECT")
    ]
    assert touching_traces == []
    for table in OPS_TABLES:
        if table != "traces":
            assert any(
                sql.startswith(f"CREATE TABLE IF NOT EXISTS otel.signals.{table} ")
                for sql in conn.ran
            ), table
