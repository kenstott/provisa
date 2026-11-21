# Copyright (c) 2026 Kenneth Stott
# Canary: 0b5e7d2a-6c4f-4a1b-93d8-e2f1c6a9b057
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every IANA zone zoneinfo knows (tzdata): an aware µs timestamp column, read as zoneinfo and
PostgreSQL read it (J498, the shared rust/pgwire tz: its transitions, then its POSIX rule), from
1800 to 2100 every three days, at 20,000 random instants, and to 2400 every thirty days (the
POSIX rule's years), written by the GraphQL encoder against the reference (oracle.column_values,
tzdata's ``fromutc``, the pinned release)."""

from __future__ import annotations

import importlib.resources
import random

import pyarrow as pa
import pytest
import tzdata

from tests.unit.json_encoder import encoders as rs
from tests.unit.json_encoder import python_ref as ref
from tests.unit.json_encoder.cases import C
from tests.unit.json_encoder.check import compare

_r = random.Random(1)
VALUES = list(range(-5364662400 * 10**6, 4102444800 * 10**6, 3 * 86400 * 10**6))
VALUES += [_r.randint(-5364662400 * 10**6, 4102444800 * 10**6) for _ in range(20000)]
VALUES += list(range(4102444800 * 10**6, 13569465600 * 10**6, 30 * 86400 * 10**6))
ZONES = sorted(
    line.strip()
    for line in importlib.resources.files("tzdata").joinpath("zones").read_text().splitlines()
    if line.strip()
)


def test_tables_are_this_tzdata():
    # The encoders' zones are IANA 2026f (rust/pgwire/third_party/chrono-tz), as is this reference.
    assert tzdata.IANA_VERSION == "2026f"


def test_factory_is_written():
    # tzdata's placeholder zone (its `factory` file) is held by the shared tz tables too: written at
    # offset 0, not refused.
    t = pa.table({"v": pa.array(VALUES[:3], pa.timestamp("us", tz="Factory"))})
    assert "Factory" in ZONES
    assert (
        compare(
            "zones",
            lambda: ref.encode(t.schema, t.to_batches(), [C("v")], "r"),
            lambda: rs.encode_response(t.to_batches(), [C("v")], "r"),
        )
        == "ok"
    )


@pytest.mark.parametrize("zone", ZONES)
def test_zone(zone):
    t = pa.table({"v": pa.array(VALUES, pa.timestamp("us", tz=zone))})
    cols = [C("v")]
    assert (
        compare(
            "zones",
            lambda: ref.encode(t.schema, t.to_batches(), cols, "r"),
            lambda: rs.encode_response(t.to_batches(), cols, "r"),
        )
        == "ok"
    )
