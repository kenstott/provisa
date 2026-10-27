# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A timestamp column's fractional-second precision, as the model records it (REQ-1961): taken
from the source at registration, kept in the column's type, and read and listed in Arrow at that
precision's unit -- seconds, milliseconds, microseconds or nanoseconds. Before, every timestamp
was microseconds, whatever its column declared."""

# Requirements: REQ-1961

from __future__ import annotations

import pyarrow as pa
import pytest

from provisa.api.admin.introspect import _pg_column_typing
from provisa.compiler.result_shape import RegisteredType
from provisa.core.catalog_types import _physical_type_to_arrow
from provisa.core.ir_types import ColumnTyping, to_ir
from provisa.core.temporal_type import timestamp_precision
from provisa.federation.runtime_support import read_targets


@pytest.mark.parametrize(
    ("registered", "unit"),
    [
        ("timestamp(0)", "s"),
        ("timestamp(1)", "ms"),
        ("timestamp(3)", "ms"),
        ("timestamp(4)", "us"),
        ("timestamp(6)", "us"),
        ("timestamp(7)", "ns"),
        ("timestamp(9)", "ns"),
        ("timestamp", "us"),  # no precision declared: PostgreSQL's default, six digits
    ],
)
def test_a_timestamp_is_the_unit_its_precision_needs(registered, unit):
    assert _physical_type_to_arrow(registered) == pa.timestamp(unit)
    zoned = registered.replace("timestamp", "timestamptz")
    assert _physical_type_to_arrow(zoned) == pa.timestamp(unit, tz="UTC")


def test_a_sized_timestamp_is_still_a_timestamp():
    """Everything that asks what a column is reads the type without its precision."""
    assert (to_ir("timestamp(3)"), to_ir("timestamptz(0)")) == ("timestamp", "timestamptz")
    assert timestamp_precision("timestamp(3)") == 3
    assert timestamp_precision("timestamptz(0)") == 0
    assert timestamp_precision("timestamp") is None
    assert timestamp_precision("integer") is None


@pytest.mark.parametrize(
    ("data_type", "formatted", "registered"),
    [
        ("timestamp without time zone", "timestamp(3) without time zone", "timestamp(3)"),
        ("timestamp with time zone", "timestamp(0) with time zone", "timestamptz(0)"),
        ("timestamp without time zone", "timestamp without time zone", "timestamp"),
        ("timestamp with time zone", "timestamp with time zone", "timestamptz"),
    ],
)
def test_registration_keeps_a_postgresql_timestamps_precision(data_type, formatted, registered):
    typing = _pg_column_typing("postgresql", data_type, "timestamp", formatted, "b")
    assert typing == ColumnTyping(registered)


def test_a_read_of_a_sized_timestamp_column_is_held_to_its_unit():
    targets = read_targets(
        ["at", "seen"],
        [("at", RegisteredType("timestamp(3)")), ("seen", RegisteredType("timestamptz(0)"))],
    )
    assert targets == [pa.timestamp("ms"), pa.timestamp("s", tz="UTC")]
