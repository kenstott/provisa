# Copyright (c) 2026 Kenneth Stott
# Canary: 8f2c6d14-b57a-4e3e-91c0-4a7e2d9b6f15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A NULL of an input function whose type only DuckDB holds (REQ-2236; replayed against
postgres:17.11 by the function support measure, REQ-2171).

interval_in, jsonb_in and xid8in are strict: PostgreSQL answers a NULL argument with a NULL of
interval, jsonb or xid8. Only DuckDB holds a NULL that pgwire describes as that type, and answers it
before the grid (provisa.transpiler.pg_forms_type_io folds a strict call of a NULL). Any other
dialect's NULL would be described as text, an answer that differs, so the call is refused by name,
with the message a call of a value it does not read gets. interval_in of a constant text is folded to
the text interval_out writes, which pgwire describes as interval (provisa.pgwire.cast_types); one
the fold leaves (infinity, which pgwire has no interval value of, or a text that is no constant) is
refused so too."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

from provisa.transpiler.polyfill.grid import DIALECTS, NATIVE, Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.notation import Refusal

FAMILY = "type input"

#: The dialects with no NULL of these types: all but PostgreSQL's own and DuckDB.
_UNHELD = tuple(d for d in DIALECTS if d not in {*NATIVE, "duckdb"})


def _refused(function: str) -> Refusal:
    return Refusal("0A000", f"{function} does not read this value")


def _null_refused(function: str, result: str, patterns: tuple[str, ...]) -> Feature:
    """``function``'s NULL calls, one pattern per NULL argument, each refused off DuckDB."""
    refusal = _refused(function)
    return Feature(
        function,
        FAMILY,
        f"{function} of a NULL: PostgreSQL's NULL of {result}.",
        read=Side(
            rules=tuple(Rule(p, dict.fromkeys(_UNHELD, refusal)) for p in patterns),
            limitations={
                d: (
                    Limitation(
                        f"A NULL of {result} would be described as text: the call is refused.",
                        refusal,
                    ),
                )
                for d in _UNHELD
            },
        ),
    )


_INTERVAL_IN_REFUSED = _refused("interval_in")

INTERVAL_IN = Feature(
    "interval_in",
    FAMILY,
    "interval_in of a NULL: PostgreSQL's NULL of interval; of a constant, its interval.",
    read=Side(
        rules=tuple(
            Rule(pattern, dict.fromkeys(_UNHELD, _INTERVAL_IN_REFUSED))
            for pattern in (
                "interval_in(NULL, $o, $t)",
                "interval_in($v, NULL, $t)",
                "interval_in($v, $o, NULL)",
                # Any other call the fold leaves (provisa.transpiler.pg_forms_type_io): a text
                # that is no constant, and infinity, which pgwire has no interval value of.
                "interval_in($v, $o, $t)",
            )
        ),
        limitations={
            d: (
                Limitation(
                    "A NULL of interval would be described as text: the call is refused.",
                    _INTERVAL_IN_REFUSED,
                ),
                Limitation(
                    "A finite constant is folded to its text, which pgwire describes as interval; "
                    "infinity, and a text that is no constant, are refused.",
                    _INTERVAL_IN_REFUSED,
                ),
            )
            for d in _UNHELD
        },
    ),
)
JSONB_IN = _null_refused("jsonb_in", "jsonb", ("jsonb_in(NULL)",))
XID8IN = _null_refused("xid8in", "xid8", ("xid8in(NULL)",))

FEATURES: tuple[Feature, ...] = (INTERVAL_IN, JSONB_IN, XID8IN)
