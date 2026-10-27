# Copyright (c) 2026 Kenneth Stott
# Canary: dcec7ba6-456b-489a-bd1e-1a9607252b53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A session's ephemeral column is typed by its values and only widens (REQ-2176)."""

# Requirements: REQ-2176

from __future__ import annotations

import datetime as dt

import pytest

from provisa.core.ir_types import to_sqlalchemy
from provisa.ephemeral import types
from provisa.gql.temporal import Duration

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("value", "kind"),
    [
        (None, None),
        (True, "boolean"),
        (3, "bigint"),
        (2.5, "double"),
        ("a", "text"),
        (dt.date(2020, 1, 2), "date"),
        (dt.datetime(2020, 1, 2, 3, 4), "timestamp"),
        (dt.datetime(2020, 1, 2, 3, 4, tzinfo=dt.timezone.utc), "timestamptz"),
        (dt.time(3, 4), "time"),
        (dt.time(3, 4, tzinfo=dt.timezone(dt.timedelta(hours=1))), "zoned_time"),
        (Duration(1, 2, 3), "interval"),
        ([1, 2], "list<bigint>"),
        ([1, None, 2.5], "list<double>"),
        (["a", "b"], "list<text>"),
        ([1, "a"], "json"),
        ([[1]], "json"),
        ([], "json"),
        ({"a": 1}, "json"),
    ],
)
def test_a_value_is_held_as_its_own_type(value, kind):
    assert types.value_type(value) == kind


@pytest.mark.parametrize(
    ("held", "given", "after"),
    [
        (None, "bigint", "bigint"),
        ("bigint", None, "bigint"),
        ("bigint", "double", "double"),
        ("double", "bigint", "double"),
        ("list<bigint>", "list<double>", "list<double>"),
        ("list<bigint>", "list<text>", "json"),
        ("bigint", "text", "json"),
        ("json", "bigint", "json"),
    ],
)
def test_a_column_only_ever_widens(held, given, after):
    assert types.widened(held, given) == after


@pytest.mark.parametrize("kind", ["interval", "zoned_time", "list<bigint>"])
def test_on_trino_what_its_connector_cannot_read_is_json(kind):
    assert types.for_engine(kind, "trino") == "json"
    assert types.for_engine(kind, "duckdb") == kind


def test_held_rows_are_converted_when_their_column_widens():
    columns, rows = types.relanded(
        [("node_id", "text"), ("v", "bigint")],
        (("node_id", "text"),),
        [("e:1", 1)],
        [{"node_id": "e:2", "v": 2.5, "w": "x"}, {"node_id": "e:3", "gone": None}],
        "duckdb",
    )
    assert columns == [("node_id", "text"), ("v", "double"), ("w", "text")]
    assert rows == [("e:1", 1.0, None), ("e:2", 2.5, "x"), ("e:3", None, None)]


def test_a_value_widened_to_json_is_its_json_text():
    _, rows = types.relanded(
        [("node_id", "text"), ("v", "bigint")],
        (("node_id", "text"),),
        [("e:1", 1)],
        [{"node_id": "e:2", "v": "x"}],
        "duckdb",
    )
    assert rows == [("e:1", "1"), ("e:2", '"x"')]


@pytest.mark.parametrize(
    ("kind", "postgres"),
    [("zoned_time", "TIME WITH TIME ZONE"), ("list<bigint>", "BIGINT[]"), ("list<text>", "TEXT[]")],
)
def test_a_session_only_type_lands_as_the_stores_own(kind, postgres):
    from sqlalchemy.dialects import postgresql

    assert str(to_sqlalchemy(kind).compile(dialect=postgresql.dialect())) == postgres


def test_a_sources_time_with_a_zone_is_still_a_time():
    from provisa.core.ir_types import to_ir

    assert to_ir("timetz") == "time"
