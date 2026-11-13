# Copyright (c) 2026 Kenneth Stott
# Canary: c2a97e51-4d0b-4b6f-8a3e-5f19d7e0c483
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Values of types Trino holds none of (REQ-2236; the function support measure, run 43307392116,
REQ-2171), each call of one refused by name there.

xid8 is a 64-bit unsigned number: sqlglot writes it as UBIGINT, a type Trino does not have, and the
old table's forms of it (provisa.transpiler.function_macros) do not parse there. pg_lsn's position
is a 64-bit unsigned number: the old table's forms of it (provisa.transpiler.pg_forms_types) add
and compare it as a number past a bigint, which Trino reads as no literal. A range or a multirange
the statement says of a NULL reaches Trino as a type it does not have; unnest of a tsvector, held as its text, is
refused likewise (run 43337203459). max and min of an inet, and
of an xid8, are refused likewise; every other max and min is Trino's own.

ClickHouse holds no range type either (a cast to one is an unknown type there, run 43410717058),
and no inet: lower, upper and unnest of a range or multirange, and max and min of an inet or a cidr
(whose order is not its text's), are refused by name on it likewise."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.notation import Refused, Rewrite

FAMILY = "types Trino holds none of"

_XID8 = "Refused by name: an xid8 is a 64-bit unsigned number, of a type Trino does not have."
_XID8_CALLS = {
    "xid": "The 32-bit transaction ID of an xid8.",
    "xid8cmp": "btree's comparison of two xid8 values.",
    "xid8_larger": "The larger of two xid8 values.",
    "xid8_smaller": "The smaller of two xid8 values.",
    "pg_xact_status": "A transaction's commit status.",
}

_LSN = (
    "Refused by name: a pg_lsn's position is a 64-bit unsigned number, past the literals and "
    "the integers Trino has."
)
_LSN_CALLS = {
    "pg_lsn_pli": "A WAL position moved forward by a number of bytes.",
    "pg_lsn_mii": "A WAL position moved back by a number of bytes.",
    "numeric_pl_pg_lsn": "A number of bytes added to a WAL position.",
    "pg_lsn_larger": "The later of two WAL positions.",
    "pg_lsn_smaller": "The earlier of two WAL positions.",
}

_RANGES = tuple(
    f"{base}{kind}"
    for base in ("int4", "int8", "num", "date", "ts", "tstz")
    for kind in ("range", "multirange")
)
_OF_A_RANGE = {
    "lower": "The lower bound of a range or multirange.",
    "upper": "The upper bound of a range or multirange.",
}
_AGGREGATES = {
    "max": "The largest value of an aggregate.",
    "min": "The smallest value of an aggregate.",
}
_UNORDERED = ("inet", "cidr", "xid8")


def xid8_value(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A value cast to xid8: refused by name, as Trino has no unsigned 64-bit type (sqlglot writes
    UBIGINT, which does not parse there). A server function's form reads its xid8 argument so."""
    if isinstance(node, exp.Cast) and trino.said(node) == "xid8":
        raise Refused(trino.no_form("xid8in", dialect))
    return None


XID8 = Feature(
    "xid8",
    FAMILY,
    "A value of type xid8.",
    read=Side(
        rules=(Rule(None, trino.on_trino(Rewrite(xid8_value, mentions=r"\bxid8\b"))),),
        limitations=dict.fromkeys(trino.TRINO, (Limitation(_XID8),)),
    ),
)

_TSVECTOR_ROWS = (
    "Of a tsvector: refused by name, as its rows are three columns (lexeme, positions, weights) "
    "of a text the engine holds it as."
)

#: unnest of a range, multirange or tsvector on Trino, and of a tsvector on ClickHouse (whose
#: ranges provisa.transpiler.pg_ranges writes as its own arrays before the grid runs).
UNNEST = Feature(
    "unnest",
    FAMILY,
    "The ranges of a multirange, or the lexemes of a tsvector, as rows.",
    read=Side(
        rules=(
            Rule(
                None,
                {
                    **trino.on_trino(
                        trino.refusing(
                            "unnest",
                            "unnest of a range, multirange or tsvector: refused by name.",
                            trino.of_said((*_RANGES, "tsvector")),
                        )
                    ),
                    "clickhouse": trino.refusing(
                        "unnest",
                        "unnest of a multirange or tsvector: refused by name.",
                        trino.of_said((*_RANGES, "tsvector")),
                    ),
                },
            ),
        ),
        limitations={
            **dict.fromkeys(
                trino.TRINO,
                (
                    Limitation(
                        "Of a range or multirange the statement says: refused by name, as Trino "
                        f"has no such type. {_TSVECTOR_ROWS} Of anything else, Trino's own."
                    ),
                ),
            ),
            "clickhouse": (
                Limitation(
                    "Of a range or multirange the statement says: refused by name, as ClickHouse "
                    f"has no such type. {_TSVECTOR_ROWS} Of anything else, ClickHouse's own."
                ),
            ),
        },
    ),
)

#: The dialects each of :data:`_OF_A_RANGE` and :data:`_AGGREGATES` is refused on, with the types
#: refused there and the limitation it reads.
_ON_TRINO_AND_CLICKHOUSE = {
    "range": {
        **dict.fromkeys(trino.TRINO, (_RANGES, "Trino")),
        "clickhouse": (_RANGES, "ClickHouse"),
    },
    "order": {
        **dict.fromkeys(trino.TRINO, (_UNORDERED, "Trino")),
        "clickhouse": (("inet", "cidr"), "ClickHouse"),
    },
}


def _refused_of(name: str, summary: str, kind: str) -> Feature:
    """``name`` refused by name on Trino and ClickHouse where an argument is said to be of a type
    the dialect does not hold (a range, or a type it does not order as PostgreSQL does)."""
    on = _ON_TRINO_AND_CLICKHOUSE[kind]
    said = "a range or multirange" if kind == "range" else "an inet, a cidr or an xid8"
    rules: dict = {}
    limitations: dict = {}
    for dialect, (types, title) in on.items():
        what = said if dialect != "clickhouse" or kind == "range" else "an inet or a cidr"
        rules[dialect] = trino.refusing(
            name,
            f"{name} of {what}: refused by name, as {title} has no such type.",
            trino.of_said(types),
        )
        limitations[dialect] = (
            Limitation(
                f"Of {what} the statement says: refused by name, as {title} has no such type. "
                f"Of anything else, {title}'s own."
            ),
        )
    return Feature(
        name, FAMILY, summary, read=Side(rules=(Rule(None, rules),), limitations=limitations)
    )


FEATURES: tuple[Feature, ...] = (
    XID8,
    *(trino.by_name(name, FAMILY, summary, _XID8) for name, summary in _XID8_CALLS.items()),
    *(trino.by_name(name, FAMILY, summary, _LSN) for name, summary in _LSN_CALLS.items()),
    *(_refused_of(name, summary, "range") for name, summary in _OF_A_RANGE.items()),
    UNNEST,
    *(_refused_of(name, summary, "order") for name, summary in _AGGREGATES.items()),
)
