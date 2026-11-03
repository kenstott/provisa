# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Python and Rust answer a named zone from one IANA release: zoneinfo reads the pinned tzdata
package (provisa/__init__.py empties its search path), and the Rust encoders' tables
(rust/pgwire tz, tz_rules, vendored chrono-tz) hold the same zones with the same offsets, Factory
included. Offsets are compared from 1800 to 2100 every three days, at 20,000 random instants, and
to 2400 every thirty days (the POSIX rule's years), as tests/unit/json_encoder/test_zones.py
samples them."""

from __future__ import annotations

import datetime as dt
import os
import random
import zoneinfo

import pytest
import pytz
import tzdata

import provisa  # noqa: F401  # the zone setup under test runs at import
from provisa._encoders import pgwire

EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
_r = random.Random(1)
SECS = list(range(-5364662400, 4102444800, 3 * 86400))
SECS += [_r.randint(-5364662400, 4102444800) for _ in range(20000)]
SECS += list(range(4102444800, 13569465600, 30 * 86400))
ZONES = sorted(zoneinfo.available_timezones())


def test_zoneinfo_reads_the_package():
    assert zoneinfo.TZPATH == ()
    assert os.environ["PYTHONTZPATH"] == ""
    # With no search path a key resolves only through the tzdata package.
    assert zoneinfo.ZoneInfo("Factory").utcoffset(None) == dt.timedelta(0)


def test_one_release():
    assert tzdata.IANA_VERSION == "2026f"
    assert pytz.OLSON_VERSION == tzdata.IANA_VERSION
    assert pgwire.TZDB_VERSION == tzdata.IANA_VERSION
    assert pgwire.TZ_RULES_VERSION == tzdata.IANA_VERSION


def test_one_zone_set():
    assert set(pgwire.ZONE_NAMES) == set(ZONES)
    assert "Factory" in ZONES


@pytest.mark.parametrize("zone", ZONES)
def test_offsets_agree(zone):
    z = zoneinfo.ZoneInfo(zone)
    python = [
        int(offset.total_seconds())
        for s in SECS
        if (offset := (EPOCH + dt.timedelta(seconds=s)).astimezone(z).utcoffset()) is not None
    ]
    assert len(python) == len(SECS)
    rust = pgwire.zone_offsets(zone, SECS)
    assert rust is not None, f"{zone}: not held by the Rust tables"
    diffs = [(s, p, r) for s, p, r in zip(SECS, python, rust) if p != r]
    assert not diffs, f"{zone}: {len(diffs)} instants differ, first {diffs[:3]}"
