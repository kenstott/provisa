# Copyright (c) 2026 Kenneth Stott
# Canary: 0751b927-cbd6-452f-ac0e-474804d24c37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A relationship candidate, and the list read from a model's answer (REQ-413)."""

# Requirements: REQ-018, REQ-167, REQ-413, REQ-612

from __future__ import annotations

import json
import re
from dataclasses import dataclass


@dataclass
class RelationshipCandidate:  # REQ-018, REQ-612
    source_table_id: int
    source_column: str
    target_table_id: int
    target_column: str
    cardinality: str
    confidence: float
    reasoning: str
    suggested_name: str = ""


#: Where a list of relationships can open: a ``[`` before an object or before its own end. A
#: bracket in prose (``[Note]``) or one inside an element is not taken for the list.
_LIST_OPENING = re.compile(r"\[(?=\s*[{\]])")


@dataclass
class AnswerList:  # REQ-413
    """The list read from a model's answer. ``items`` is None when no list could be read;
    ``cut_short`` says the answer stopped before its list closed, and ``items`` then holds the
    elements that were complete."""

    items: list | None
    cut_short: bool = False


def _complete_elements(text: str, start: int) -> list:
    """The elements of the list opening at ``text[start]`` that are complete, read one at a time
    until one is not: the list of an answer cut off before it closed."""
    decoder = json.JSONDecoder()
    items: list = []
    pos = start + 1
    while True:
        while pos < len(text) and text[pos] in " \t\r\n,":
            pos += 1
        try:
            item, pos = decoder.raw_decode(text, pos)
        except json.JSONDecodeError:
            return items
        items.append(item)


def read_answer_list(text: str, cut_off: bool) -> AnswerList:  # REQ-413
    """The JSON list in a model's answer, whatever text or code fence is around it: read by a JSON
    decoder from the first ``[`` that opens a list of objects (:data:`_LIST_OPENING`), so a
    bracket in the text after the list is never taken for its end. An answer that stopped at its token limit (``cut_off``) and whose
    list never closed gives the elements that were complete, marked ``cut_short``."""
    decoder = json.JSONDecoder()
    opens = [m.start() for m in _LIST_OPENING.finditer(text)]
    for i in opens:
        try:
            value, _ = decoder.raw_decode(text, i)
        except json.JSONDecodeError:
            continue
        if isinstance(value, list):
            return AnswerList(value)
    if cut_off and opens:
        return AnswerList(_complete_elements(text, opens[0]), cut_short=True)
    return AnswerList(None)
