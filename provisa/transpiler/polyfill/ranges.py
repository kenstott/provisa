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

from provisa.transpiler.polyfill import trino
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
