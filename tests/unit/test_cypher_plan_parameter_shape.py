# Copyright (c) 2026 Kenneth Stott
# Canary: 1780292b-ad7c-4cb6-b65d-43544eda9a16
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A kept Cypher translation is reused only for parameters of the same shape (REQ-1877): a local
time or date time, a time or date time with an offset, and one with a zone are each translated
as their own kind (REQ-2173), so each keeps a translation of its own. Before, the shape was the
Python type alone, so ``RETURN $x`` translated for a zoned date time answered a local one with
that zone (the neo4j driver's test_local_datetime read 07:47 back as 03:47-04:00)."""

# Requirements: REQ-1877, REQ-2173

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

from provisa.api.rest.cypher_plan import parameter_shape

_EASTERN = dt.timezone(dt.timedelta(hours=-4))


def test_a_local_and_a_zoned_value_have_shapes_of_their_own():
    local = parameter_shape({"x": dt.datetime(2021, 11, 2, 7, 47)})
    offset = parameter_shape({"x": dt.datetime(2021, 11, 2, 7, 47, tzinfo=_EASTERN)})
    zoned = parameter_shape({"x": dt.datetime(2021, 11, 2, 7, 47, tzinfo=ZoneInfo("US/Eastern"))})
    assert len({tuple(local), tuple(offset), tuple(zoned)}) == 3
    local_time = parameter_shape({"x": dt.time(7, 47)})
    offset_time = parameter_shape({"x": dt.time(7, 47, tzinfo=_EASTERN)})
    assert local_time != offset_time


def test_values_of_one_kind_share_a_shape():
    assert parameter_shape({"x": dt.datetime(2021, 1, 1), "n": 1}) == parameter_shape(
        {"n": 2, "x": dt.datetime(1999, 5, 6, 1, 2)}
    )
    assert parameter_shape({"x": "a"}) == parameter_shape({"x": "b"})
