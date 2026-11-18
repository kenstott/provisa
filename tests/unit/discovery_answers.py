# Copyright (c) 2026 Kenneth Stott
# Canary: 5b2e8f14-9c3d-4a71-b6e0-2f8d1c7a9e35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A stub model's answers to a discovery call (REQ-413, amended 2026-11-18): read the numbered
table pairs a call lists and answer each under its number.

A column pair is written by table and column names as the call lists them
(:func:`column_pair`); :func:`answer` places each under the number of the listed pair of its two
tables, in either order, and answers every other listed pair "none" unless it is omitted.
"""

# Requirements: REQ-413

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

from provisa.discovery.context import PAIRS_HEADER, TABLES_HEADER


def listed_pairs(prompt: str) -> list[dict[str, Any]]:
    """The numbered table pairs a call lists."""
    return json.loads(prompt.split(PAIRS_HEADER, 1)[1])


def listed_tables(prompt: str) -> list[dict[str, Any]]:
    """The tables a call lists, each once."""
    return json.loads(prompt.split(TABLES_HEADER, 1)[1].split("\n" + PAIRS_HEADER, 1)[0])


def number(prompt: str, a: str, b: str) -> int | None:
    """The number of the listed pair of tables ``a`` and ``b``, in either order; None when the
    call does not list it."""
    for p in listed_pairs(prompt):
        if {p["source"], p["target"]} == {a, b} and (a == b) == (p["source"] == p["target"]):
            return int(p["pair"])
    return None


def column_pair(
    source: tuple[str, str],
    target: tuple[str, str],
    confidence: float = 0.9,
    cardinality: str = "many-to-one",
    reasoning: str = "same name",
    suggested_name: str = "",
) -> dict[str, Any]:
    """One column pair of an answer, its tables and columns by the names the call lists."""
    return {
        "source_table": source[0],
        "source_column": source[1],
        "target_table": target[0],
        "target_column": target[1],
        "cardinality": cardinality,
        "confidence": confidence,
        "reasoning": reasoning,
        "suggested_name": suggested_name,
    }


def answer_items(
    prompt: str, pairs: Iterable[dict[str, Any]] = (), omit: Iterable[tuple[str, str]] = ()
) -> list[dict[str, Any]]:
    """The answer's elements: each listed pair under its number with the column pairs of
    ``pairs`` naming its two tables, or "none"; a listed pair of ``omit`` (by its two tables) is
    left unanswered. A column pair whose tables the call does not pair is not placed."""
    skipped = {frozenset(o) for o in omit}
    out: list[dict[str, Any]] = []
    given = list(pairs)
    for p in listed_pairs(prompt):
        tables = frozenset({p["source"], p["target"]})
        if tables in skipped:
            continue
        mine = [
            c
            for c in given
            if frozenset({c["source_table"], c["target_table"]}) == tables
            and (c["source_table"] == c["target_table"]) == (p["source"] == p["target"])
        ]
        out.append({"pair": p["pair"], "column_pairs": mine or "none"})
    return out


def answer(
    prompt: str, pairs: Iterable[dict[str, Any]] = (), omit: Iterable[tuple[str, str]] = ()
) -> str:
    """:func:`answer_items` as the model's text."""
    return json.dumps(answer_items(prompt, pairs, omit))
