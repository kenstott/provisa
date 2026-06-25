# Copyright (c) 2026 Kenneth Stott
# Canary: b3c4d5e6-f7a8-9012-bcde-345678901234
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""NL-assisted table discovery: fuzzy pre-filter + haiku LLM semantic ranking."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass

log = logging.getLogger(__name__)

_MAX_FUZZY_CANDIDATES = 40
_MIN_FUZZY_SCORE = 30  # rapidfuzz WRatio threshold

# Requirements: REQ-167


@dataclass
class TableCandidate:  # REQ-167
    name: str
    comment: str | None
    columns: list[str]
    schema_name: str


@dataclass
class RankedTable:  # REQ-167
    name: str
    schema_name: str
    comment: str | None
    confidence: float
    reasoning: str


def _tokenize(text: str) -> set[str]:
    """Split snake_case / camelCase / spaces into lowercase tokens."""
    text = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)
    return {t.lower() for t in re.split(r"[\W_]+", text) if len(t) > 1}


def _fuzzy_score(query_tokens: set[str], candidate: TableCandidate) -> int:
    """Score candidate against query tokens — token overlap across name + columns + comment."""
    target_tokens: set[str] = set()
    target_tokens.update(_tokenize(candidate.name))
    for col in candidate.columns:
        target_tokens.update(_tokenize(col))
    if candidate.comment:
        target_tokens.update(_tokenize(candidate.comment))

    if not target_tokens:
        return 0
    matched = query_tokens & target_tokens
    # Overlap ratio relative to query size
    return int(100 * len(matched) / len(query_tokens)) if query_tokens else 0


def fuzzy_filter(  # REQ-167
    query: str,
    candidates: list[TableCandidate],
    max_results: int = _MAX_FUZZY_CANDIDATES,
) -> list[TableCandidate]:
    """Return up to max_results candidates ranked by token overlap, score >= threshold."""
    query_tokens = _tokenize(query)
    if not query_tokens:
        return candidates[:max_results]

    scored = [(c, _fuzzy_score(query_tokens, c)) for c in candidates]
    scored = [(c, s) for c, s in scored if s >= _MIN_FUZZY_SCORE]
    scored.sort(key=lambda x: x[1], reverse=True)
    return [c for c, _ in scored[:max_results]]


def build_search_prompt(query: str, candidates: list[TableCandidate]) -> str:  # REQ-167
    lines = [
        "You are a data catalog assistant. A user is searching for tables using natural language.",
        f'\nUser query: "{query}"\n',
        "Below are candidate tables with their column names and descriptions.",
        "Rank the tables that best match the user's intent. Return ONLY a JSON array.\n",
        "## Candidates\n",
    ]
    for c in candidates:
        cols = ", ".join(c.columns[:20]) if c.columns else "(no columns)"
        desc = f" — {c.comment}" if c.comment else ""
        lines.append(f"- {c.schema_name}.{c.name}{desc}\n  Columns: {cols}")

    lines.append(
        "\n## Output Format\n"
        "Return ONLY a JSON array. No other text. Only include tables that are relevant.\n"
        "```json\n"
        "[\n"
        '  {"schema": "<schema_name>", "table": "<table_name>", '
        '"confidence": <0.0-1.0>, "reasoning": "<one sentence>"}\n'
        "]\n"
        "```"
    )
    return "\n".join(lines)


def parse_llm_response(text: str) -> list[dict]:
    """Extract JSON array from LLM response text."""
    match = re.search(r"```(?:json)?\s*(\[.*?])\s*```", text, re.DOTALL)
    if match:
        raw = match.group(1)
    else:
        match = re.search(r"\[.*]", text, re.DOTALL)
        raw = match.group(0) if match else "[]"
    return json.loads(raw)


async def llm_rank(  # REQ-167
    query: str,
    candidates: list[TableCandidate],
    api_key: str | None = None,
) -> list[RankedTable]:
    """Call LLM to semantically rank candidates. Returns empty list on failure."""
    from provisa.llm.client import ProviasLLMClient

    prompt = build_search_prompt(query, candidates)
    candidate_index = {(c.schema_name, c.name): c for c in candidates}

    client = ProviasLLMClient("table_selection")
    try:
        raw = await client.complete(
            prompt,
            system="You are a data catalog assistant.",
            max_tokens=1024,
        )
    except Exception as exc:
        log.warning("LLM table search failed: %s", exc)
        return []

    try:
        items = parse_llm_response(raw)
    except Exception:
        return []

    results: list[RankedTable] = []
    for item in items:
        schema = item.get("schema", "")
        name = item.get("table", "")
        key = (schema, name)
        orig = candidate_index.get(key)
        results.append(
            RankedTable(
                name=name,
                schema_name=schema,
                comment=orig.comment if orig else None,
                confidence=float(item.get("confidence", 0.0)),
                reasoning=item.get("reasoning", ""),
            )
        )
    results.sort(key=lambda r: r.confidence, reverse=True)
    return results


async def search_tables(  # REQ-167
    query: str,
    candidates: list[TableCandidate],
    api_key: str | None = None,
) -> list[RankedTable]:
    """Two-pass search: fuzzy pre-filter, then optional LLM ranking."""
    filtered = fuzzy_filter(query, candidates)

    if not filtered:
        return []

    ranked = await llm_rank(query, filtered)
    if ranked:
        return ranked

    # Fallback: return fuzzy results as RankedTable with no confidence scores
    query_tokens = _tokenize(query)
    return [
        RankedTable(
            name=c.name,
            schema_name=c.schema_name,
            comment=c.comment,
            confidence=_fuzzy_score(query_tokens, c) / 100.0,
            reasoning="Matched by name/column keyword overlap.",
        )
        for c in filtered
    ]
