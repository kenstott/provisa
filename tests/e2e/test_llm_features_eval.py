# Copyright (c) 2026 Kenneth Stott
# Canary: dd2ad536-c082-4e87-9f3a-69f420320c44
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Non-deterministic evaluation of the LLM-assisted features (REQ-018/167/355/356/612).

These call the REAL Anthropic model (no mocks) and judge inherently non-deterministic output:

- DESCRIPTIONS (generate table / column description): scored by an LLM-AS-JUDGE against a rubric —
  a second model call returns a 0..1 relevance score; we assert it clears a lenient bar. Wording
  varies run to run, so we never string-match; we grade meaning.
- RELATIONSHIPS (LLM relationship inference): graded by GROUND TRUTH — a synthetic schema with a
  KNOWN foreign-key answer set; we assert recall (found the obvious FKs) and precision (didn't
  hallucinate edges that don't correspond to a real column pair) over the inferred edges.

Skipped without ANTHROPIC_API_KEY. Marked e2e (runs in the e2e lane). Thresholds are deliberately
lenient — the point is to catch the model/prompt REGRESSING to nonsense, not to pin exact phrasing.
"""

from __future__ import annotations

import os
import re

import pytest

from provisa.api.admin.schema_helpers import _call_llm
from provisa.discovery.analyzer import analyze
from provisa.discovery.collector import DiscoveryInput, TableMeta
from provisa.discovery.prompt import build_prompt

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(
        not os.environ.get("ANTHROPIC_API_KEY"),
        reason="ANTHROPIC_API_KEY not set — LLM eval needs the live model",
    ),
]


async def _judge(rubric: str) -> float:
    """LLM-as-judge: return a 0..1 score for ``rubric``. Robust to chatty output — we extract the
    first float in [0,1] the judge emits. A malformed judge reply scores 0 (fails closed)."""
    reply = await _call_llm(
        rubric + "\n\nRespond with ONLY a single number between 0 and 1.",
        "table_description",
        max_tokens=16,
    )
    m = re.search(r"(0(?:\.\d+)?|1(?:\.0+)?)", reply or "")
    return float(m.group(1)) if m else 0.0


# ── descriptions: LLM-as-judge ───────────────────────────────────────────────

_ORDERS_PROMPT = (
    "You are a data catalog assistant. Write a concise one-to-two sentence description for a "
    "database table named 'orders' in schema 'public' from source 'shop'. "
    "Columns: id, customer_id, total_amount, status, created_at. "
    "Respond with only the description text, no preamble."
)


@pytest.mark.asyncio
async def test_table_description_is_on_topic():
    desc = await _call_llm(_ORDERS_PROMPT, "table_description", max_tokens=256)
    assert desc and 15 <= len(desc) <= 500, f"implausible description: {desc!r}"
    score = await _judge(
        "On a scale 0..1, how well does this description accurately and specifically describe a "
        "database 'orders' table whose columns are id, customer_id, total_amount, status, "
        f"created_at (an e-commerce order record)?\n\nDescription: {desc}"
    )
    assert score >= 0.6, f"judge scored the description too low ({score}): {desc!r}"


@pytest.mark.asyncio
async def test_column_description_is_on_topic():
    prompt = (
        "You are a data catalog assistant. Write a concise one-sentence description for the column "
        "'customer_id' of a database table named 'orders' (columns: id, customer_id, total_amount, "
        "status, created_at). Respond with only the description text, no preamble."
    )
    desc = await _call_llm(prompt, "column_description", max_tokens=128)
    assert desc and 5 <= len(desc) <= 300, f"implausible column description: {desc!r}"
    score = await _judge(
        "On a scale 0..1, does this one-line description correctly explain that 'customer_id' is a "
        "reference/foreign key identifying the customer who placed the order?\n\n"
        f"Description: {desc}"
    )
    assert score >= 0.5, f"judge scored the column description too low ({score}): {desc!r}"


# ── relationships: ground-truth precision/recall ─────────────────────────────


def _tbl(tid: int, name: str, cols: list[str]) -> TableMeta:
    return TableMeta(
        table_id=tid,
        source_id="shop",
        domain_id="sales",
        schema_name="public",
        table_name=name,
        columns=[{"name": c, "type": "integer" if c.endswith("id") else "text"} for c in cols],
        sample_values=[],
    )


def test_relationship_inference_finds_obvious_fks():
    # Synthetic schema with NO declared constraints; the obvious FKs must be inferred from naming.
    customers = _tbl(1, "customers", ["id", "name", "email"])
    orders = _tbl(2, "orders", ["id", "customer_id", "total_amount", "status"])
    order_items = _tbl(3, "order_items", ["id", "order_id", "product_id", "quantity"])
    di = DiscoveryInput(tables=[customers, orders, order_items], existing_relationships=[], rejected_pairs=[])

    prompt = build_prompt(di)
    cands = analyze(prompt, os.environ["ANTHROPIC_API_KEY"], di, min_confidence=0.5)

    edges = {(c.source_table_id, c.source_column, c.target_table_id, c.target_column) for c in cands}
    # GROUND TRUTH — the two unambiguous FKs (many-to-one child -> parent).
    ground_truth = {
        (2, "customer_id", 1, "id"),  # orders.customer_id -> customers.id
        (3, "order_id", 2, "id"),  # order_items.order_id -> orders.id
    }
    # order_items.product_id has no products table here, so it is NOT ground truth (may or may not
    # be guessed; a candidate that references a non-existent column is already filtered by analyze).

    found = edges & ground_truth
    recall = len(found) / len(ground_truth)
    # Precision over the ground-truthable space: every inferred edge must at least connect two real
    # columns (analyze validates this) and point child->parent on an *_id column.
    id_edges = [e for e in edges if e[1].endswith("_id")]
    precision = (len(found) / len(id_edges)) if id_edges else 0.0

    assert cands, "the model returned no relationship candidates at all"
    assert recall >= 0.5, f"missed the obvious FKs (recall {recall:.2f}); inferred: {sorted(edges)}"
    # Lenient precision floor — tolerate ONE extra plausible guess (e.g. product_id) but not a flood.
    assert precision >= 0.33, f"too many spurious edges (precision {precision:.2f}); inferred: {sorted(edges)}"


def test_relationship_inference_does_not_invent_edges_on_unrelated_tables():
    # Two tables with NO shared/foreign key naming → the model should infer (almost) nothing.
    weather = _tbl(10, "weather", ["id", "city", "temperature", "recorded_at"])
    recipes = _tbl(11, "recipes", ["id", "title", "instructions", "servings"])
    di = DiscoveryInput(tables=[weather, recipes], existing_relationships=[], rejected_pairs=[])

    cands = analyze(build_prompt(di), os.environ["ANTHROPIC_API_KEY"], di, min_confidence=0.7)
    # At a 0.7 confidence bar there is no real FK to find; allow at most one low-signal guess.
    assert len(cands) <= 1, f"hallucinated relationships between unrelated tables: {cands}"
