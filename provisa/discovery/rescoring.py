# Copyright (c) 2026 Kenneth Stott
# Canary: 8a4e1f7c-3b62-4d95-9c0e-5f2d7b1a6e38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A second scorer for a discovery run's proposals (REQ-1789): with a Jev key configured, each
proposal the model made is put to Jev as a yes-or-no question, and Jev's calibrated confidence
replaces the model's own.

Jev judges on what it is shown, so each question comes with what a run knows of the two tables
and the two columns (:func:`evidence`): their names, descriptions, tags, the table's role and
column names, the columns' classes and glossary terms. Sample values are not sent: they went to
the model the run's person chose, and Jev is another vendor.
"""

# Requirements: REQ-413, REQ-1789

from __future__ import annotations

import logging
from typing import Any

from provisa.discovery import context
from provisa.discovery.columns import TableFacts
from provisa.discovery.direction import ColumnRef, Proposal

log = logging.getLogger(__name__)


def _column(tables: dict[int, TableFacts], ref: ColumnRef) -> dict[str, Any]:
    column = tables[ref[0]].column(ref[1])
    assert column is not None, f"table {ref[0]} has no column {ref[1]!r}"
    shown = context.column_context(column)
    shown.pop("sample_values", None)
    return shown


def evidence(found: list[Proposal], tables: dict[int, TableFacts]) -> dict[str, Any]:
    """What Jev is shown for ``found``: each table they name, with its context and the context
    of each of its columns they name, sample values left out."""
    named: dict[int, dict[str, dict[str, Any]]] = {}
    for p in found:
        for ref in (p.start, p.end):
            named.setdefault(ref[0], {})[ref[1]] = _column(tables, ref)
    return {
        "tables": [
            {"table": context.table_context(tables[tid]), "columns": list(columns.values())}
            for tid, columns in sorted(named.items())
        ]
    }


def question(index: int, p: Proposal, tables: dict[int, TableFacts]) -> dict[str, Any]:
    """The question one proposal is put as: its tables and columns by name."""
    start, end = tables[p.start[0]], tables[p.end[0]]
    return {
        "id": str(index),
        "type": "noul",
        "instructions": (
            f"Does column '{p.start[1]}' of table '{start.table_name}' (id {start.table_id}) "
            f"refer to column '{p.end[1]}' of table '{end.table_name}' (id {end.table_id}) as "
            "a foreign key refers to the key it names, many-to-one? The state holds each "
            "table's name, description, tags, role and column names, and for each of the two "
            "columns its description, classes, tags and glossary term. The proposing model "
            f"also read sample values of both columns, and reasoned: {p.reasoning}"
        ),
        "criteria": {
            "true": "a real relationship: the first column's values name rows of the second's table",
            "false": "not a real relationship, or the wrong way round",
        },
    }


async def jev_scores(
    found: list[Proposal], tables: dict[int, TableFacts], api_key: str
) -> list[float | None] | None:
    """Jev's confidence for each of ``found``, in order; None for one it gave no answer. None for
    all: the call failed."""
    from provisa.jev.client import evaluate

    questions = [question(i, p, tables) for i, p in enumerate(found)]
    try:
        result = await evaluate(api_key, evidence(found, tables), questions)
    except Exception:
        # REQ-1789 (provisa/jev/decisions.py): the second score is layered on proposals the model
        # has already made, so a failed Jev call keeps them on the model's confidence. It is not
        # silent: the caller counts it (Tally.scoring_failed) and the run's result says so.
        log.warning("Jev could not score %d relationship proposals", len(found), exc_info=True)
        return None
    answers: dict[str, Any] = result["answers"]
    scores: list[float | None] = []
    for i in range(len(found)):
        answer = answers.get(str(i))
        scores.append(None if answer is None else float(answer["noul"]))
    return scores
