# Copyright (c) 2026 Kenneth Stott
# Canary: 5d8f2b73-9c1e-4a64-b3d7-1f6e9a4c8b25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A forced redirect asked for over Flight (REQ-1194, REQ-1224 amended 2026-10-04).

A ticket's ``redirect`` / ``redirect_format`` options force the delivery; do_get answers a one-row
table -- url, format, row_count, expires_at. Flight is a streaming transport and is never
delivered automatically. An option that cannot be read is refused by name."""

# Requirements: REQ-1194, REQ-1224

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.flight as fl
import pytest

from provisa.api.flight.server import _redirect_table, _ticket_delivery


@pytest.fixture(autouse=True)
def _redirect_config(monkeypatch):
    monkeypatch.setattr(
        "provisa.executor.redirect.request_redirect_config",
        lambda threshold: SimpleNamespace(default_format=None, threshold=threshold),
    )


def test_the_ticket_options_force_a_delivery_in_the_format_they_name():
    delivery = _ticket_delivery({"redirect": True, "redirect_format": "orc"}, "analyst")
    assert delivery is not None and (delivery.output_format, delivery.role) == ("orc", "analyst")
    assert _ticket_delivery({"query": "SELECT 1"}, "analyst") is None
    assert _ticket_delivery({"redirect": False, "redirect_format": "parquet"}, "analyst") is None


@pytest.mark.parametrize(
    "options",
    [
        {"redirect": "yes"},
        {"redirect": True, "redirect_format": "xlsx"},
        {"redirect": True, "redirect_format": 7},
    ],
)
def test_an_option_that_cannot_be_read_is_refused(options):
    with pytest.raises(fl.FlightServerError):
        _ticket_delivery(options, "analyst")


def test_the_answer_is_a_one_row_table_naming_the_delivery():
    handle = {"redirect_url": "http://store/o.parquet", "row_count": 2, "expires_in": 60}
    before = dt.datetime.now(dt.timezone.utc)
    table = _redirect_table(handle, SimpleNamespace(output_format="parquet"))
    assert table.schema.names == ["url", "format", "row_count", "expires_at"]
    assert table.schema.field("expires_at").type == pa.timestamp("us", tz="UTC")
    (row,) = table.to_pylist()
    assert (row["url"], row["format"], row["row_count"]) == ("http://store/o.parquet", "parquet", 2)
    assert dt.timedelta(seconds=59) <= row["expires_at"] - before <= dt.timedelta(seconds=61)
