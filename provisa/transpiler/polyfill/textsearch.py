# Copyright (c) 2026 Kenneth Stott
# Canary: c47a91e2-0b6d-4f38-a5e1-6d29b8f0c375
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ts_delete of a list of lexemes (REQ-2236; replayed against postgres:17.11 by the function support
measure, REQ-2171).

A tsvector is on the engine as the text tsvectorout writes (provisa.transpiler.pg_forms_textsearch,
whose lowering writes ``ts_delete(tsvector, text[])`` as the call ``ts_delete_array`` of that text
and the list): each lexeme the list holds is dropped. PostgreSQL skips a NULL item of the list
(tsvector_op.c, tsvector_delete_arr). Trino's contains of a value a list does not hold is NULL where
the list holds a NULL, and a lexeme kept by ``NOT contains`` was then dropped: there, a lexeme is
kept where contains is not true. DuckDB's list_contains and ClickHouse's has answer false so."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

from provisa.transpiler import pg_forms_textsearch as ts
from provisa.transpiler.polyfill.grid import Feature, Rule, Side

FAMILY = "text search"


def _form(engine: ts.Engine, not_held: str) -> str:
    """The vector's tokens whose lexeme ``not_held`` says the list does not hold, joined again."""
    lexeme = ts._lexeme_of(engine, "_t")
    kept = engine.keep.format(ts._tokens(engine, "$v"), "_t", not_held.format(lexeme))
    return ts._read(engine, "ts_delete", ts._join_tokens(engine, kept), "$v")


def _forms() -> dict[str, str]:
    forms: dict[str, str] = {}
    for dialect, engine in ts.ENGINES:
        held = engine.has.format("$x", "{}")
        not_held = f"NOT coalesce({held}, false)" if dialect == "trino" else f"NOT {held}"
        forms.update(dict.fromkeys(ts._OF[dialect], _form(engine, not_held)))
    return forms


TS_DELETE = Feature(
    "ts_delete",
    FAMILY,
    "A vector without the lexemes a list holds; a NULL item of the list is skipped.",
    read=Side(rules=(Rule("ts_delete_array($v, $x)", _forms()),)),
)

FEATURES: tuple[Feature, ...] = (TS_DELETE,)
