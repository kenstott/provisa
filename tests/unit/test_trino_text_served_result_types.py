# Copyright (c) 2026 Kenneth Stott
# Canary: 9a4c7e21-6f38-4b05-b2d9-3e81f0a5c647
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A result Trino serves as text is described as the type PostgreSQL returns (REQ-2167, REQ-2171):
interval_in's folded text as interval, and jsonb_build_object's json text (Trino's json_object is a
varchar) as jsonb. Run 43288245572 found each described as text (oid 25). The wire's typing of a
result is pgwire's, not the polyfill grid's (provisa.pgwire.cast_types)."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from typing import Any, Callable

import pytest
from buenavista.core import BVType

from provisa._encoders import pgwire as encoder
from provisa.core.ir_types import interval_parts, interval_value
from provisa.pgwire.cast_types import interval_of_text
from provisa.pgwire.server import _describe_casts, coercer_names

_INTERVAL, _JSONB, _TEXT = 1186, 3802, 25


def _described(sql: str, served: BVType) -> tuple[BVType, int, list[str]]:
    types: list[BVType] = [served]
    oids: list[int | None] = [_TEXT]
    typmods = [-1]
    coerce: list[tuple[int, Callable[[Any], Any]]] = []
    _describe_casts(sql, types, oids, typmods, coerce)
    (names,) = coercer_names(coerce, 1)
    return types[0], oids[0] or 0, names


def test_interval_in_served_as_text_is_described_as_interval():
    sql = (
        "SELECT pg_catalog.\"interval_in\"('1 day 02:03:04'::cstring, "
        "'interval'::regtype::oid, (-1)::integer) AS v"
    )
    assert _described(sql, BVType.TEXT) == (BVType.INTERVAL, _INTERVAL, ["interval_text"])


def test_an_engines_own_interval_takes_no_coercer():
    sql = "SELECT pg_catalog.\"interval_in\"('1 day'::cstring, 0::oid, -1) AS v"
    assert _described(sql, BVType.INTERVAL) == (BVType.INTERVAL, _INTERVAL, [])


def test_a_cast_to_interval_served_as_text_is_still_text():
    """A cast's interval text is the engine's own form, which nothing reads: only a call's is
    interval_out's (interval_in's fold)."""
    assert _described("SELECT CAST(x AS interval) AS v FROM t", BVType.TEXT)[1] == _TEXT


@pytest.mark.parametrize(
    "sql",
    [
        'SELECT pg_catalog."jsonb_build_object"() AS v',
        "SELECT pg_catalog.\"jsonb_build_object\"('a'::text, 3::integer) AS v",
    ],
)
def test_jsonb_build_object_served_as_text_is_described_as_jsonb(sql):
    assert _described(sql, BVType.TEXT) == (BVType.JSONB, _JSONB, ["jsonb_out"])


@pytest.mark.parametrize(
    ("text", "parts"),
    [
        ("1 day 02:03:04", (0, 1, 7_384_000_000)),
        ("1 year 2 mons -3 days +04:05:06.5", (14, -3, 14_706_500_000)),
        ("-1 days -00:00:01.000001", (0, -1, -1_000_001)),
        ("26:00:00", (0, 0, 93_600_000_000)),
        ("00:00:00", (0, 0, 0)),
    ],
)
def test_interval_out_text_is_the_interval_it_reads_as(text, parts):
    assert interval_parts(interval_of_text(text)) == parts


@pytest.mark.parametrize("text", ["", "1 day 2", "1:2:3", "1 week", "infinity"])
def test_a_text_interval_out_does_not_write_is_refused(text):
    with pytest.raises(ValueError, match="invalid input syntax for type interval"):
        interval_of_text(text)


@pytest.mark.parametrize("fmt", [0, 1], ids=["text", "binary"])
def test_the_encoder_writes_the_text_as_the_interval(fmt):
    """The Rust encoder's coercer reads the text as interval_of_text does: the bytes of the
    interval itself, in either format."""
    as_text = encoder.encode_rows([("1 day 02:03:04",)], ["INTERVAL"], [fmt], [["interval_text"]])
    as_interval = encoder.encode_rows([(interval_value(0, 1, 7_384_000_000),)], ["INTERVAL"], [fmt])
    assert as_text == as_interval


def test_the_encoder_writes_trinos_json_text_as_jsonb():
    written = encoder.encode_rows([('{"a":3}',)], ["JSONB"], [0], [["jsonb_out"]])
    assert written.endswith(b'{"a": 3}')
