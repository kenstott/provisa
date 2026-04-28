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

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass

import anthropic

from provisa.discovery.collector import DiscoveryInput

log = logging.getLogger(__name__)


@dataclass
class RelationshipCandidate:
    source_table_id: int
    source_column: str
    target_table_id: int
    target_column: str
    cardinality: str
    confidence: float
    reasoning: str
    suggested_name: str = ""


def _extract_json(text: str) -> str:
    """Extract JSON array from response text, handling markdown code blocks."""
    # Try to find JSON in code blocks first
    match = re.search(r"```(?:json)?\s*(\[.*?])\s*```", text, re.DOTALL)
    if match:
        return match.group(1)
    # Try bare JSON array
    match = re.search(r"\[.*]", text, re.DOTALL)
    if match:
        return match.group(0)
    return text


def _validate_candidate(raw: dict, discovery_input: DiscoveryInput) -> bool:
    """Check that referenced columns exist in the metadata."""
    required_keys = {
        "source_table_id", "source_column", "target_table_id",
        "target_column", "cardinality", "confidence",
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


def analyze(
    prompt: str,
    api_key: str,
    discovery_input: DiscoveryInput,
    min_confidence: float = 0.7,
) -> list[RelationshipCandidate]:
    """Call Claude API with prompt, parse and validate response."""
    client = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )

    response_text = message.content[0].text
    log.warning("LLM raw response (%d chars): %s", len(response_text), response_text[:3000])

    try:
        raw_json = _extract_json(response_text)
        candidates_raw = json.loads(raw_json)
    except (json.JSONDecodeError, TypeError) as e:
        log.warning("Malformed LLM response (%s). Raw text: %s", e, response_text[:500])
        return []

    if not isinstance(candidates_raw, list):
        log.warning("LLM response is not a JSON array. Raw: %s", response_text[:500])
        return []

    results: list[RelationshipCandidate] = []
    for raw in candidates_raw:
        if not isinstance(raw, dict):
            continue
        if not _validate_candidate(raw, discovery_input):
            log.warning("Invalid candidate filtered: %s", raw)
            continue
        confidence = float(raw["confidence"])
        if confidence < min_confidence:
            log.warning("Candidate below threshold (%.2f < %.2f): %s", confidence, min_confidence, raw)
            continue
        results.append(RelationshipCandidate(
            source_table_id=int(raw["source_table_id"]),
            source_column=str(raw["source_column"]),
            target_table_id=int(raw["target_table_id"]),
            target_column=str(raw["target_column"]),
            cardinality=str(raw["cardinality"]),
            confidence=confidence,
            reasoning=str(raw.get("reasoning", "")),
            suggested_name=str(raw.get("suggested_name", "")),
        ))

    return results
