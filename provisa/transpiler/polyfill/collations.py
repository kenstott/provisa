# Copyright (c) 2026 Kenneth Stott
# Canary: 8d1e4c72-5a39-4f0b-9e6c-2b7a0f53d914
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The collation a COLLATE clause gives a value (REQ-2236; replayed against postgres:17.11 by the
function support measure, REQ-2171).

pg_collation_for answers the collation of its argument as generate_collation_name writes it:
quote_ident of the name, so "C", "POSIX" and "default" are quoted and ucs_basic is not. A COLLATE
clause names the collation in the statement, so a built-in one (initdb makes them on every
PostgreSQL 17) is answered as its name. No engine has PostgreSQL's collations: any other is
refused by name, as is a COLLATE of a value of a type that has none (PostgreSQL's 42804). A value
with no COLLATE clause is the old table's (provisa.transpiler.pg_forms_system)."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import DIALECTS, NATIVE, Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.notation import Refusal

FAMILY = "collations"

_EVERY = tuple(d for d in DIALECTS if d not in NATIVE)

#: The collations every PostgreSQL 17 has, each as quote_ident writes it.
_BUILT_IN = {
    "default": '"default"',
    "C": '"C"',
    "POSIX": '"POSIX"',
    "ucs_basic": "ucs_basic",
    "unicode": "unicode",
    "pg_c_utf8": "pg_c_utf8",
}
#: The types a COLLATE clause applies to, as a statement says them.
_COLLATABLE = ("text", "character", "name")

REFUSED = Refusal("0A000", "pg_collation_for does not read this value")


def _written(name: str, quoted: bool) -> str:
    return f'"{name}"' if quoted else name


def _rules() -> tuple[Rule, ...]:
    """A rule per collatable type and spelling of each built-in collation's name: one PostgreSQL
    folds (unquoted) only where folding keeps it, then every other COLLATE, refused."""
    rules = []
    for name, answer in _BUILT_IN.items():
        spellings = (True, False) if name == name.lower() and name != "default" else (True,)
        for quoted in spellings:
            for sql_type in _COLLATABLE:
                rules.append(
                    Rule(
                        f"pg_collation_for($v::{sql_type} COLLATE {_written(name, quoted)})",
                        dict.fromkeys(_EVERY, "'" + answer.replace("'", "''") + "'"),
                    )
                )
    rules.append(Rule("pg_collation_for($v COLLATE $c)", dict.fromkeys(_EVERY, REFUSED)))
    return tuple(rules)


PG_COLLATION_FOR = Feature(
    "pg_collation_for",
    FAMILY,
    "The collation a COLLATE clause gives a value, as quote_ident writes its name.",
    read=Side(
        rules=_rules(),
        limitations=dict.fromkeys(
            _EVERY,
            (
                Limitation(
                    "Only the built-in collations (default, C, POSIX, ucs_basic, unicode, "
                    "pg_c_utf8) of a text, character or name value: any other COLLATE is refused.",
                    REFUSED,
                ),
            ),
        ),
    ),
)

#: The orders of text and character by the database's collation (en_US.UTF-8, as pg_database
#: says), each with its summary. Trino has no collation: its order of text is by code points, and
#: it reads no COLLATE clause (run 43307392116 sent one, a syntax error there). Each is refused by
#: name on Trino and on ClickHouse; every other dialect keeps the old table's form
#: (provisa.transpiler.pg_forms_types). An order by bytes (text_pattern_lt and its kin) is answered there as before.
_COLLATED_ORDERS = {
    "text_lt": "One text before another in the database's collation.",
    "text_le": "One text before or equal to another in the database's collation.",
    "text_gt": "One text after another in the database's collation.",
    "text_ge": "One text after or equal to another in the database's collation.",
    "text_larger": "The later of two texts in the database's collation.",
    "text_smaller": "The earlier of two texts in the database's collation.",
    "bpcharlt": "One character string before another in the database's collation.",
    "bpcharle": "One character string before or equal to another in the database's collation.",
    "bpchargt": "One character string after another in the database's collation.",
    "bpcharge": "One character string after or equal to another in the database's collation.",
    "bpcharcmp": "btree's comparison of two character strings in the database's collation.",
    "bpchar_larger": "The later of two character strings in the database's collation.",
    "bpchar_smaller": "The earlier of two character strings in the database's collation.",
}

#: ClickHouse compares no text by a collation either: a COLLATE clause in a comparison is a syntax
#: error there (run 43410717058), and its own order of text is by bytes.
_NO_COLLATION = {
    **dict.fromkeys(trino.TRINO, "Trino has no collation, and orders text by its code points"),
    "clickhouse": "ClickHouse compares no text by a collation, and orders text by its bytes",
}


def _collated_order(name: str, summary: str) -> Feature:
    rules = {
        dialect: trino.refusing(name, f"{name}: refused by name: {why}.", lambda *_: True)
        for dialect, why in _NO_COLLATION.items()
    }
    limitations = {
        dialect: (Limitation(f"Refused by name: {why}."),) for dialect, why in _NO_COLLATION.items()
    }
    return Feature(
        name, FAMILY, summary, read=Side(rules=(Rule(None, rules),), limitations=limitations)
    )


COLLATED_ORDERS: tuple[Feature, ...] = tuple(
    _collated_order(name, summary) for name, summary in _COLLATED_ORDERS.items()
)

FEATURES: tuple[Feature, ...] = (PG_COLLATION_FOR, *COLLATED_ORDERS)
