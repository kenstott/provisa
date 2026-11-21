# Copyright (c) 2026 Kenneth Stott
# Canary: 0b6e2a41-5c7d-4f3e-9a18-d2c47e9b1f63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``zoned_times_as_text`` reads a relation as the engine's Arrow table (REQ-2173, REQ-2176)."""

# Requirements: REQ-2173, REQ-2176

from __future__ import annotations

import duckdb
import pyarrow as pa
import pytest

from provisa.federation.materialize_broker import ZONED_TIME_TYPE, zoned_times_as_text


@pytest.fixture
def con():
    c = duckdb.connect()
    yield c
    c.close()


def test_a_relation_without_a_zoned_time_is_its_arrow_table(con):
    con.execute("CREATE TABLE t AS SELECT 1::INTEGER AS a, 'x' AS b")
    table = zoned_times_as_text(con, "t")
    assert isinstance(table, pa.Table)
    assert table.to_pylist() == [{"a": 1, "b": "x"}]


def test_a_zoned_time_is_sent_as_its_text_and_marked_with_its_type(con):
    con.execute("CREATE TABLE t AS SELECT 1::INTEGER AS a, '01:02:03+05'::TIMETZ AS z")
    table = zoned_times_as_text(con, "t")
    assert table.column("z").to_pylist() == ["01:02:03+05"]
    assert table.schema.field("z").metadata == {ZONED_TIME_TYPE: b"TIME WITH TIME ZONE"}
    assert table.schema.field("a").metadata is None
