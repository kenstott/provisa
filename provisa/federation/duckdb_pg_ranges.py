# Copyright (c) 2026 Kenneth Stott
# Canary: 0d7e43a9-81f6-4bc2-9e05-3a6c17f2d8b4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The multirange values DuckDB builds from columns (REQ-2171, RANGES ARE EMULATED ON EVERY
ENGINE; REQ-2167). Union, intersection and difference sort and merge a variable number of ranges,
which is no one expression: a UDF over the values' text, by provisa.core.pg_range. Everything else
about a range or multirange is the portable SQL of provisa.transpiler.pg_ranges."""

# Requirements: REQ-2171, REQ-2167

from __future__ import annotations

import functools
from typing import Any

from provisa.core import pg_range
from provisa.core.pg_error import END, MARK, PostgresError
from provisa.core.pg_range import RANGE_TYPES, Multirange, Range, RangeType


def _reported(call: Any) -> Any:
    """PostgreSQL's error from the UDF, as provisa.core.pg_error reads it back."""

    @functools.wraps(call)
    def guarded(*args: Any) -> Any:
        try:
            return call(*args)
        except PostgresError as error:
            raise ValueError(f"{MARK} {error.sqlstate}: {error}{END}") from error

    return guarded


def _ranges_of(rtype: RangeType, written: str) -> list[Range]:
    """A range's or a multirange's text, as its ranges."""
    if written.lstrip().startswith("{"):
        return list(pg_range.parse_multirange(rtype, written).ranges)
    return [pg_range.parse_range(rtype, written)]


def _multirange(rtype: RangeType, written: str) -> Multirange:
    return pg_range.make_multirange(rtype, _ranges_of(rtype, written))


def multirange_op(verb: str, range_type: str, a: str, b: str) -> str:
    """a + b, a * b or a - b, each a multirange's text."""
    rtype = RANGE_TYPES[range_type]
    apply = {
        "union": pg_range.mr_union,
        "intersect": pg_range.mr_intersect,
        "minus": pg_range.mr_minus,
    }[verb]
    return pg_range.multirange_text(apply(_multirange(rtype, a), _multirange(rtype, b)))


def multirange_of(range_type: str, items: list[str | None]) -> str:
    """int4multirange(a, b, ...): the arguments' ranges, sorted and merged. NULL is not a range."""
    rtype = RANGE_TYPES[range_type]
    held = [r for written in items if written is not None for r in _ranges_of(rtype, written)]
    return pg_range.multirange_text(pg_range.make_multirange(rtype, held))


def range_agg(range_type: str, items: list[str | None]) -> str | None:
    """range_agg(range) or range_agg(multirange): NULL over no value, as a strict aggregate is."""
    if not any(written is not None for written in items):
        return None
    return multirange_of(range_type, items)


def range_intersect_agg(range_type: str, items: list[str | None]) -> str | None:
    """range_intersect_agg: the intersection of every value; a range's type for ranges, a
    multirange's for multiranges."""
    rtype = RANGE_TYPES[range_type]
    held = [written for written in items if written is not None]
    if not held:
        return None
    if held[0].lstrip().startswith("{"):
        out = _multirange(rtype, held[0])
        for written in held[1:]:
            out = pg_range.mr_intersect(out, _multirange(rtype, written))
        return pg_range.multirange_text(out)
    value = pg_range.parse_range(rtype, held[0])
    for written in held[1:]:
        value = pg_range.intersect(value, pg_range.parse_range(rtype, written))
    return pg_range.range_text(value)


def register(con: Any) -> None:
    """Create the functions on DuckDB connection ``con``."""
    con.create_function(
        "provisa_multirange_op",
        _reported(multirange_op),
        ["VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR"],
        "VARCHAR",
        side_effects=False,
    )
    for name, function in (
        ("provisa_multirange_of", multirange_of),
        ("provisa_range_agg", range_agg),
        ("provisa_range_intersect_agg", range_intersect_agg),
    ):
        con.create_function(
            name,
            _reported(function),
            ["VARCHAR", "VARCHAR[]"],
            "VARCHAR",
            null_handling="special",
            side_effects=False,
        )
