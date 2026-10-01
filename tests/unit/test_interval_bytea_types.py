# Copyright (c) 2026 Kenneth Stott
# Canary: 6e2a9d47-3b18-4c05-a7f3-9d1e5b8c2a60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``interval`` is a registrable IR type with one canonical text form (ISO 8601 duration) on every
text surface; ``timetz``/``timestamptz`` resolve on every schema face; a raw ``bytea`` value renders
as Postgres's own hex text instead of crashing a JSON response (REQ-010, REQ-306, REQ-846)."""

# Requirements: REQ-010, REQ-306, REQ-846

from __future__ import annotations

import json
from datetime import timedelta
from types import SimpleNamespace

import pytest
from google.protobuf.descriptor import FieldDescriptor

from provisa.compiler.type_map import (
    FILTER_TYPE_MAP,
    GraphQLString,
    Interval,
    IntervalFilter,
    column_type_to_graphql,
)
from provisa.core.ir_types import bytea_hex, iso8601_duration, to_ir, to_physical
from provisa.executor.serialize import _convert_value
from provisa.grpc.proto_gen import _physical_to_proto
from provisa.grpc.server import _proto_value


@pytest.mark.parametrize(
    ("value", "text"),
    [
        (timedelta(days=3, seconds=4, microseconds=5), "P3DT4.000005S"),
        (timedelta(0), "PT0S"),
        (timedelta(days=2), "P2D"),
        (timedelta(hours=1, minutes=30), "PT1H30M"),
        (timedelta(microseconds=500_000), "PT0.5S"),
        (timedelta(seconds=-90), "-PT1M30S"),
    ],
)
def test_an_interval_renders_as_its_iso_8601_duration(value, text):
    assert iso8601_duration(value) == text


def test_interval_and_timetz_resolve_in_the_ir():
    assert to_ir("interval") == "interval"
    assert to_ir("timetz") == "time"
    assert to_physical("interval", "postgresql") == "INTERVAL"


def test_interval_is_its_own_graphql_scalar_with_an_ordered_filter():
    assert column_type_to_graphql("interval") is Interval
    assert FILTER_TYPE_MAP[Interval] is IntervalFilter
    assert set(IntervalFilter.fields) >= {"eq", "gt", "gte", "lt", "lte", "in", "is_null"}
    assert column_type_to_graphql("timetz") is GraphQLString


def test_the_interval_scalar_serializes_a_timedelta_and_keeps_engine_text():
    assert Interval.serialize(timedelta(days=1, seconds=2)) == "P1DT2S"
    assert Interval.serialize("P1DT2S") == "P1DT2S"
    with pytest.raises(TypeError, match="Interval cannot represent int"):
        Interval.serialize(5)


def test_json_results_render_interval_and_bytea_as_their_canonical_text():
    assert _convert_value(timedelta(days=3, seconds=4, microseconds=5)) == "P3DT4.000005S"
    assert _convert_value(b"\x00\x01\xff") == "\\x0001ff"
    assert _convert_value(memoryview(b"\xab")) == "\\xab"
    json.dumps([_convert_value(b"\x00"), _convert_value(timedelta(seconds=1))])


def test_bytea_hex_is_postgres_hex_output():
    assert bytea_hex(b"\x00\x01\xff") == "\\x0001ff"
    assert bytea_hex(bytearray()) == "\\x"


@pytest.mark.parametrize(
    ("column_type", "proto"),
    [
        ("timestamptz", "google.protobuf.Timestamp"),
        ("timetz", "string"),
        ("interval", "string"),
        ("bytea", "bytes"),
    ],
)
def test_proto_maps_every_postgres_temporal_and_binary_alias(column_type, proto):
    assert _physical_to_proto(column_type) == proto


def test_grpc_renders_an_interval_string_field_as_iso_8601():
    field = SimpleNamespace(type=FieldDescriptor.TYPE_STRING)
    assert _proto_value(field, timedelta(days=3, seconds=4, microseconds=5)) == "P3DT4.000005S"


@pytest.mark.parametrize("scalar_name", ["Date", "DateTime", "Interval"])
@pytest.mark.parametrize("number", [1700000000, 1.5, True])
def test_a_temporal_scalar_never_serializes_a_number(scalar_name, number):
    """Dates, times and durations are ISO 8601 strings in every GraphQL response — a number (an
    epoch offset in unknown units) is refused, never passed through or stringified."""
    from provisa.compiler import type_map

    with pytest.raises(TypeError, match=f"{scalar_name} cannot represent"):
        getattr(type_map, scalar_name).serialize(number)


def test_date_and_datetime_serialize_as_iso_8601():
    from datetime import date, datetime, timezone

    from provisa.compiler.type_map import Date, DateTime

    assert DateTime.serialize(datetime(2026, 1, 2, 3, 4, 5)) == "2026-01-02T03:04:05"
    assert (
        DateTime.serialize(datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc))
        == "2026-01-02T03:04:05+00:00"
    )
    assert Date.serialize(date(2026, 1, 2)) == "2026-01-02"
    assert DateTime.serialize("2026-01-02T03:04:05") == "2026-01-02T03:04:05"
