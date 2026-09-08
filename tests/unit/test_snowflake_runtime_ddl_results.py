# Copyright (c) 2026 Kenneth Stott
# Canary: 8d2c4f61-9a3e-4b57-b0c8-5e1f7a29d3c4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A DDL or SHOW statement run through the Snowflake runtime's Arrow terminals yields its JSON
result as a table instead of the connector's message-less NotSupportedError -- the failure that
stopped every MV refresh on Snowflake at its first statement, CREATE SCHEMA IF NOT EXISTS."""

from __future__ import annotations

import pytest

from provisa.federation.snowflake_runtime import SnowflakeFederationRuntime

pytestmark = pytest.mark.unit


class _Cursor:
    def __init__(self, fmt: str):
        self._query_result_format = fmt
        self.description = [("status",)]
        self.closed = False

    def execute(self, sql, params=None):
        self.sql = sql

    def fetchall(self):
        return [("Schema ORG_DEFAULT_MV_CACHE successfully created.",)]

    def fetch_arrow_all(self):
        raise AssertionError("arrow fetch on a JSON result")

    def fetch_arrow_batches(self):
        raise AssertionError("arrow fetch on a JSON result")

    def close(self):
        self.closed = True


class _Conn:
    def __init__(self, fmt: str):
        self.cur = _Cursor(fmt)

    def cursor(self):
        return self.cur


def _runtime(fmt: str) -> SnowflakeFederationRuntime:
    rt = object.__new__(SnowflakeFederationRuntime)
    rt._conn = _Conn(fmt)
    return rt


def test_run_arrow_returns_a_ddl_status_row_as_a_table():
    table = _runtime("json").run_arrow('CREATE SCHEMA IF NOT EXISTS "_landing"."x"')
    assert table.column_names == ["status"]
    assert table.num_rows == 1


def test_run_arrow_stream_yields_the_ddl_status_and_closes_the_cursor():
    rt = _runtime("json")
    schema, batches = rt.run_arrow_stream('CREATE SCHEMA IF NOT EXISTS "_landing"."x"')
    assert schema.names == ["status"]
    assert sum(b.num_rows for b in batches) == 1
    assert rt._conn.cur.closed


def test_run_sync_streams_the_ddl_status_row():
    result = _runtime("json").run_sync('CREATE SCHEMA IF NOT EXISTS "_landing"."x"')
    assert result.column_names == ["status"]
    assert len(list(result.iter_rows())) == 1
