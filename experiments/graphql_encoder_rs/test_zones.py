# Copyright (c) 2026 Kenneth Stott
# Canary: 0b5e7d2a-6c4f-4a1b-93d8-e2f1c6a9b057
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every pytz zone: an aware µs timestamp column localized as the product localizes it (pyarrow
through pytz), from 1800 to 2100 every three days and at 20,000 random instants, written by the
GraphQL encoder against the product's path. Not collected by CI; run it as RESULTS.md says."""

from __future__ import annotations

import random

import pyarrow as pa
import pytest
import pytz

import graphql_encoder_rs as rs
import python_ref as ref
from cases import C
from check import compare

_r = random.Random(1)
VALUES = list(range(-5364662400 * 10**6, 4102444800 * 10**6, 3 * 86400 * 10**6))
VALUES += [_r.randint(-5364662400 * 10**6, 4102444800 * 10**6) for _ in range(20000)]


def test_tables_are_this_pytz():
    assert rs.PYTZ_VERSION == pytz.__version__


@pytest.mark.parametrize("zone", pytz.all_timezones)
def test_zone(zone):
    t = pa.table({"v": pa.array(VALUES, pa.timestamp("us", tz=zone))})
    cols = [C("v")]
    assert (
        compare(
            "zones",
            lambda: ref.encode(t.schema, t.to_batches(), cols, "r"),
            lambda: rs.encode_response(t.to_batches(), ref.plan_tuples(cols), "r"),
        )
        == "ok"
    )
