# Copyright (c) 2026 Kenneth Stott
# Canary: 1dfceb93-f852-49f2-94e5-c2a4b48712e4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A multirange of a VARIADIC array of ranges on Trino and ClickHouse (REQ-2236; the function
support measure, run 43337203459, REQ-2171).

provisa.transpiler.pg_ranges lowers every range call before the grid runs, so the grid holds the
refusal and pg_ranges raises it (THE REFUSAL LIVES IN THE GRID). A VARIADIC array is the arguments
it holds: an array's text of one dimension cast to an array of ranges is its ranges, each answered
as the constructor of them is. Any other cast to an array of ranges (of a value the rows hold, or of
more dimensions) would reach the engine as an array of a type it does not have: refused by name.
An array the engine holds (array_agg of a range column, held as text) is read as it was. DuckDB names the range types (the
catalog's CREATE TYPE), and reads such an array as its text."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import datetime

from provisa.core.pg_range import RANGE_TYPES
from provisa.transpiler.polyfill import instants, trino
from provisa.transpiler.polyfill.grid import Feature, Limitation, Side
from provisa.transpiler.polyfill.notation import Refusal

FAMILY = "ranges"

#: The dialects with no range type an array of ranges may be cast to.
NO_RANGE_ARRAYS = (*trino.TRINO, "clickhouse")
_BASES = {
    "int4": "integers",
    "int8": "bigints",
    "num": "numerics",
    "date": "dates",
    "ts": "timestamps",
    "tstz": "timestamps with time zone",
}


def variadic_refusal(constructor: str, dialect: str) -> Refusal:
    """The refusal of ``constructor`` of a VARIADIC array of no text the statement says."""
    return _REFUSALS[constructor][dialect]


_REFUSALS = {
    f"{base}multirange": trino.refused_on_trino(f"{base}multirange")
    | {"clickhouse": trino.no_form(f"{base}multirange", "clickhouse")}
    for base in _BASES
}

FEATURES: tuple[Feature, ...] = tuple(
    Feature(
        f"{base}multirange",
        FAMILY,
        f"A multirange of {elements}, of ranges or of a VARIADIC array of them.",
        read=Side(
            limitations={
                dialect: (
                    Limitation(
                        "Of a VARIADIC cast to an array of ranges, of anything but the text of an "
                        "array of one dimension: refused by name, as no array of a range type is "
                        "held.",
                        _REFUSALS[f"{base}multirange"][dialect],
                    ),
                )
                for dialect in NO_RANGE_ARRAYS
            }
        ),
    )
    for base, elements in _BASES.items()
)

#: What ClickHouse's type of a range's bound holds, by the element type: its first and last value
#: and their text. DateTime64 holds 1900-01-01 .. 2299-12-31 as wall times of its own zone (a
#: timestamp's, the session's; a timestamptz's, UTC), and Date32 those days; neither holds an
#: infinity. A date keeps a day from the end, so the day after it (an inclusive upper bound's, in
#: the canonical form) is held too. A range of numbers is held as its digits.
HELD_ON_CLICKHOUSE: dict[str, tuple[datetime.date, datetime.date, str]] = {
    "timestamptz": (instants.FIRST, instants.LAST, instants.BOUND),
    "timestamp": (instants.FIRST, instants.LAST, instants.STAMP_BOUND),
    "date": (datetime.date(1900, 1, 1), datetime.date(2299, 12, 30), "1900-01-01 .. 2299-12-30"),
}


def outside_held(constructor: str) -> str:
    """The message a range's literal bound outside what ClickHouse holds raises, SQLSTATE 0A000:
    instants' message of a timestamptz outside its bound, with the bound of the range's element."""
    held = HELD_ON_CLICKHOUSE.get(RANGE_TYPES[constructor].element)
    assert held is not None, constructor  # a range of numbers is held as its digits
    return instants.out_of_bound(constructor, held[2])


_HELD_STATED = (
    "A literal bound, and a literal element beside the range, is read where the statement is "
    "transpiled (provisa.transpiler.pg_ranges): one outside {bound} raises 0A000 ({message}), "
    "as ClickHouse would hold it at its type's end. Every bound a literal: the range is answered "
    "there, in full. A bound a row computes is ClickHouse's own value."
)

FEATURES += tuple(
    Feature(
        f"{name}({element}, {element})",
        FAMILY,
        f"A range of {elements} of its two bounds, and of its bounds' flags.",
        read=Side(
            limitations={
                "clickhouse": (
                    Limitation(
                        _HELD_STATED.format(
                            bound=HELD_ON_CLICKHOUSE[element][2], message=outside_held(name)
                        )
                    ),
                )
            }
        ),
    )
    for name, element, elements in (
        ("tsrange", "timestamp", "timestamps"),
        ("tstzrange", "timestamptz", "timestamps with time zone"),
        ("daterange", "date", "dates"),
    )
)
