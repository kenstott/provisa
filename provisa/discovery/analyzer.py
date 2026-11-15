# Copyright (c) 2026 Kenneth Stott
# Canary: 0751b927-cbd6-452f-ac0e-474804d24c37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Analyze prompt via Claude API and parse relationship candidates."""

# Requirements: REQ-018, REQ-167, REQ-413, REQ-612

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass

from provisa.discovery.collector import DiscoveryInput
from provisa.otel_compat import get_tracer as _get_tracer

_tracer = _get_tracer(__name__)

log = logging.getLogger(__name__)


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

#: The most the model may write in its answer. An answer that reaches it stops where it is.
MAX_ANSWER_TOKENS = 4096


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


def _validate_candidate(raw: dict, discovery_input: DiscoveryInput) -> bool:  # REQ-612
    """Check that referenced columns exist in the metadata."""
    required_keys = {
        "source_table_id",
        "source_column",
        "target_table_id",
        "target_column",
        "cardinality",
        "confidence",
    }
    if not required_keys.issubset(raw.keys()):
        return False

    if raw.get("cardinality") not in ("many-to-one", "one-to-many"):
        return False

    tables_by_id = {t.table_id: t for t in discovery_input.tables}

    src_table = tables_by_id.get(raw["source_table_id"])
    if src_table is None:
        return False
    src_col_names = {c["name"] for c in src_table.columns}
    if raw["source_column"] not in src_col_names:
        return False

    tgt_table = tables_by_id.get(raw["target_table_id"])
    if tgt_table is None:
        return False
    tgt_col_names = {c["name"] for c in tgt_table.columns}
    if raw["target_column"] not in tgt_col_names:
        return False

    return True


class ModelAnswerError(RuntimeError):
    """The model gave no answer discovery can use: an empty one, or one with no list in it (a
    refusal, or prose in place of the list asked for)."""


@dataclass
class Analysis:  # REQ-413
    """What the model's answer held: the candidates kept, and what became of the rest."""

    candidates: list[RelationshipCandidate]
    #: The answer held something list-like that could not be read as a list of relationships.
    unreadable: bool = False
    #: How many relationships the answer proposed, kept or not.
    proposed: int = 0
    #: How many fell below ``min_confidence``.
    below_threshold: int = 0
    #: How many named a table or column that does not exist, or lacked a required field.
    invalid: int = 0
    #: The answer stopped at the model's token limit before its list closed: the candidates are
    #: those of its elements that were complete.
    cut_short: bool = False


def analyze_answer(
    prompt: str,
    api_keys: dict[str, str],
    discovery_input: DiscoveryInput,
    min_confidence: float = 0.7,
    *,
    config: dict | None = None,
) -> Analysis:  # REQ-018, REQ-167, REQ-413, REQ-612
    """Ask the model assigned to relationship inference, and read its answer. A failure of the
    model (its client's error, an empty answer, an answer with no list) is raised, never
    returned as an empty result."""
    with _tracer.start_as_current_span("discovery.analyze"):
        from provisa.llm.client import ProvisaLLMClient

        llm = ProvisaLLMClient("relationship_inference", config=config, api_keys=api_keys)
        answer = llm.complete_sync_detailed(prompt, "You are a data analyst.", MAX_ANSWER_TOKENS)
        response_text = answer.text
        log.warning(
            "LLM raw response (%d chars, cut off at the token limit: %s): %s",
            len(response_text),
            answer.cut_off,
            response_text[:3000],
        )
        if not response_text.strip():
            raise ModelAnswerError(f"{llm.vendor}:{llm.model} gave an empty answer")
        if "[" not in response_text:
            raise ModelAnswerError(
                f"{llm.vendor}:{llm.model} did not answer with a list of relationships: "
                f"{response_text.strip()[:300]}"
            )

        read = read_answer_list(response_text, answer.cut_off)
        candidates_raw = read.items
        if candidates_raw is None:
            log.warning("LLM response holds no readable JSON list. Raw: %s", response_text[:500])
            return Analysis(candidates=[], unreadable=True)
        if read.cut_short:
            log.warning(
                "LLM answer cut off at %d tokens; %d complete candidates kept",
                MAX_ANSWER_TOKENS,
                len(candidates_raw),
            )

        analysis = Analysis(candidates=[], proposed=len(candidates_raw), cut_short=read.cut_short)
        for raw in candidates_raw:
            if not isinstance(raw, dict) or not _validate_candidate(raw, discovery_input):
                log.warning("Invalid candidate filtered: %s", raw)
                analysis.invalid += 1
                continue
            confidence = float(raw["confidence"])
            if confidence < min_confidence:
                log.warning(
                    "Candidate below threshold (%.2f < %.2f): %s", confidence, min_confidence, raw
                )
                analysis.below_threshold += 1
                continue
            analysis.candidates.append(
                RelationshipCandidate(
                    source_table_id=int(raw["source_table_id"]),
                    source_column=str(raw["source_column"]),
                    target_table_id=int(raw["target_table_id"]),
                    target_column=str(raw["target_column"]),
                    cardinality=str(raw["cardinality"]),
                    confidence=confidence,
                    reasoning=str(raw.get("reasoning", "")),
                    suggested_name=str(raw.get("suggested_name", "")),
                )
            )
        return analysis


def analyze(
    prompt: str,
    api_keys: dict[str, str],
    discovery_input: DiscoveryInput,
    min_confidence: float = 0.7,
) -> list[RelationshipCandidate]:  # REQ-018, REQ-167, REQ-612
    """The candidates :func:`analyze_answer` kept."""
    return analyze_answer(prompt, api_keys, discovery_input, min_confidence).candidates
