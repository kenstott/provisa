# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A timestamp column's fractional-second precision, as the model records it (REQ-1961).

As a numeric's size is (``core.numeric_type``), the declaration lives in the column's stored
type, in one spelling -- ``timestamp(p)`` or ``timestamptz(p)`` -- written by
:func:`sized_timestamp` from the source's own spelling and read by :func:`timestamp_precision`.
A column that declares none is a timestamp of six digits, PostgreSQL's default. Its Arrow unit
is the coarsest that holds the precision (:func:`timestamp_unit`)."""

# Requirements: REQ-1961

from __future__ import annotations

import re

_SIZED = re.compile(r"^\s*(timestamptz|timestamp)\s*\(\s*(\d)\s*\)\s*$", re.IGNORECASE)
# PostgreSQL's spelling (format_type): timestamp(3) with time zone.
_SOURCE = re.compile(
    r"^\s*timestamp\s*\(\s*(\d)\s*\)\s*(with|without)\s+time\s+zone\s*$", re.IGNORECASE
)


def timestamp_precision(registered: str) -> int | None:
    """The fractional-second digits a stored ``timestamp(p)`` or ``timestamptz(p)`` declares;
    None for any other type, a timestamp declaring none among them."""
    sized = _SIZED.match(registered)
    return int(sized.group(2)) if sized else None


def sized_timestamp(source_spelling: str) -> str | None:
    """The stored type of a PostgreSQL timestamp column spelled ``source_spelling`` (as
    format_type prints it) that declares its precision; None for one that declares none."""
    sized = _SOURCE.match(source_spelling)
    if sized is None:
        return None
    base = "timestamptz" if sized.group(2).lower() == "with" else "timestamp"
    return f"{base}({sized.group(1)})"


def timestamp_unit(precision: int | None) -> str:
    """The Arrow unit of a timestamp of ``precision`` digits: the coarsest that holds them."""
    if precision is None:
        return "us"
    if precision == 0:
        return "s"
    if precision <= 3:
        return "ms"
    if precision <= 6:
        return "us"
    return "ns"
