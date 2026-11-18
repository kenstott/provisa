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

These call the REAL model (Haiku through OpenRouter) (no mocks) and judge inherently non-deterministic output:

- DESCRIPTIONS (generate table / column description): scored by an LLM-AS-JUDGE against a rubric —
  a second model call returns a 0..1 relevance score; we assert it clears a lenient bar. Wording
  varies run to run, so we never string-match; we grade meaning.
- RELATIONSHIPS (LLM relationship inference): graded by GROUND TRUTH — a synthetic schema with a
  KNOWN foreign-key answer set; we assert recall (found the obvious FKs) and precision (didn't
  hallucinate edges that don't correspond to a real column pair) over the inferred edges.

Every operation is routed to OpenRouter through the client's ``ai_endpoints``/``ai_models`` config
(REQ-1790). Skipped without OPENROUTER_API_KEY. Marked e2e (runs in the e2e lane). Thresholds are deliberately
lenient — the point is to catch the model/prompt REGRESSING to nonsense, not to pin exact phrasing.
"""

from __future__ import annotations

import os
import re

import pytest

from provisa.api.admin.schema_helpers import _call_llm
from provisa.discovery.columns import UNKNOWN, ColumnFacts, TableFacts, type_groups

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(
        not os.environ.get("OPENROUTER_API_KEY"),
        reason="OPENROUTER_API_KEY not set — LLM eval needs the live model",
    ),
]


_OPENROUTER_MODEL = "anthropic/claude-haiku-5.5"
_OPERATIONS = ("table_description", "column_description", "relationship_inference")
_OPENROUTER_CONFIG = {
    "ai_endpoints": [
        {
            "id": "openrouter",
            "style": "openai",
            "base_url": "https://openrouter.ai/api/v1",
            "api_key_env": "OPENROUTER_API_KEY",
            "enabled": True,
        }
    ],
    "ai_models": {op: {"vendor": "openrouter", "model": _OPENROUTER_MODEL} for op in _OPERATIONS},
}


@pytest.fixture(autouse=True)
def _openrouter_config(monkeypatch, bind_org):
    """ProvisaLLMClient reads the deployment config; give it the OpenRouter endpoint and models.
    _call_llm reads the acting org's keys, a tenant-data path, so the deployment org is bound."""
    import provisa.api.app as app_mod

    bind_org(app_mod.state.org_id)
    monkeypatch.setattr("provisa.api.admin._config_io.read_config", lambda: _OPENROUTER_CONFIG)


async def _judge(rubric: str) -> float:
    """LLM-as-judge: return a 0..1 score for ``rubric``. Robust to chatty output — we extract the
    first float in [0,1] the judge emits. A malformed judge reply scores 0 (fails closed)."""
    reply = await _call_llm(
        rubric + "\n\nRespond with ONLY a single number between 0 and 1.",
        "table_description",
        max_tokens=1024,
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
    desc = await _call_llm(_ORDERS_PROMPT, "table_description", max_tokens=2048)
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
    desc = await _call_llm(prompt, "column_description", max_tokens=2048)
    assert desc and 5 <= len(desc) <= 300, f"implausible column description: {desc!r}"
    score = await _judge(
        "On a scale 0..1, does this one-line description correctly explain that 'customer_id' is a "
        "reference/foreign key identifying the customer who placed the order?\n\n"
        f"Description: {desc}"
    )
    assert score >= 0.5, f"judge scored the column description too low ({score}): {desc!r}"


# ── relationships: ground-truth precision/recall ─────────────────────────────


def _tbl(tid: int, name: str, cols: list[str]) -> TableFacts:
    """A table as a discovery run sees it (REQ-413), with no sample values."""

    def col(c: str) -> ColumnFacts:
        data_type = "integer" if c.endswith("id") else "varchar"
        return ColumnFacts(
            name=c,
            data_type=data_type,
            description=None,
            tags=(),
            classes=(UNKNOWN,),
            sensitive=False,
            samples=(),
            is_primary_key=c == "id",
            groups=type_groups(data_type, subtype=None, array_depth=0),
        )

    return TableFacts(tid, "sales", "public", name, None, (), None, tuple(col(c) for c in cols))


class _Watcher:
    async def stop_asked(self) -> bool:
        return False

    async def returned(self, tally, proposals) -> None:
        return None


async def _inferred(tables: list[TableFacts], min_confidence: float) -> set[tuple]:
    """The edges a discovery run's requests over ``tables`` (compared with themselves) get from
    the live model at ``min_confidence`` or more: the run's suggestions, and those it left out as
    below its threshold when ``min_confidence`` is lower."""
    from provisa.discovery.run import SYSTEM_PROMPT
    from provisa.discovery.runner import execute
    from provisa.discovery.threshold import DEFAULT
    from provisa.discovery.windows import plan
    from provisa.llm.client import ProvisaLLMClient

    max_tokens = 4_096
    planned = plan(tables, None, 32_000, max_tokens)
    client = ProvisaLLMClient("relationship_inference", config=_OPENROUTER_CONFIG, api_keys={})

    def ask(prompt: str, max_out: int):
        return client.complete_sync_detailed(prompt, SYSTEM_PROMPT, max_out)

    tally, kept = await execute(
        planned, max_tokens, ask, _Watcher(), threshold=DEFAULT, key_values=None
    )
    assert tally.failed == [], f"a request failed: {tally.failed}"
    assert tally.unanswered == [], f"table pairs left unanswered: {tally.unanswered}"
    found = [*kept, *(r.proposal for r in tally.left_out if r.left_out == "below_threshold")]
    return {p.edge for p in found if p is not None and p.confidence >= min_confidence}


@pytest.mark.asyncio
async def test_relationship_inference_finds_obvious_fks():
    # Synthetic schema with NO declared constraints; the obvious FKs must be inferred from naming.
    customers = _tbl(1, "customers", ["id", "name", "email"])
    orders = _tbl(2, "orders", ["id", "customer_id", "total_amount", "status"])
    order_items = _tbl(3, "order_items", ["id", "order_id", "product_id", "quantity"])

    edges = await _inferred([customers, orders, order_items], min_confidence=0.5)

    # GROUND TRUTH — the two unambiguous FKs (many-to-one child -> parent).
    ground_truth = {
        (2, "customer_id", 1, "id"),  # orders.customer_id -> customers.id
        (3, "order_id", 2, "id"),  # order_items.order_id -> orders.id
    }
    # order_items.product_id has no products table here, so it is NOT ground truth (may or may not
    # be guessed; a proposal that names a non-existent column is already left out by the run).

    found = edges & ground_truth
    recall = len(found) / len(ground_truth)
    # Precision over the ground-truthable space: every inferred edge must at least connect two real
    # columns (the run checks this) and point child->parent on an *_id column.
    id_edges = [e for e in edges if e[1].endswith("_id")]
    precision = (len(found) / len(id_edges)) if id_edges else 0.0

    assert edges, "the model returned no relationship candidates at all"
    assert recall >= 0.5, f"missed the obvious FKs (recall {recall:.2f}); inferred: {sorted(edges)}"
    # Lenient precision floor — tolerate ONE extra plausible guess (e.g. product_id) but not a flood.
    assert precision >= 0.33, (
        f"too many spurious edges (precision {precision:.2f}); inferred: {sorted(edges)}"
    )


@pytest.mark.asyncio
async def test_relationship_inference_does_not_invent_edges_on_unrelated_tables():
    # Two tables with NO shared/foreign key naming → the model should infer (almost) nothing.
    weather = _tbl(10, "weather", ["id", "city", "temperature", "recorded_at"])
    recipes = _tbl(11, "recipes", ["id", "title", "instructions", "servings"])

    edges = await _inferred([weather, recipes], min_confidence=0.7)
    # At a 0.7 confidence bar there is no real FK to find; allow at most one low-signal guess.
    assert len(edges) <= 1, f"hallucinated relationships between unrelated tables: {edges}"


# ── discovery over a folder of files: the key the files share ───────────────────────────────────


def _drop_tables():
    """The five tables of the files demo's drop folder as a discovery run reads them (REQ-413):
    the types its files source reports and, for each text column, the sample values a run sent
    for it on a test instance (tests/integration/test_discovery_files_e2e.py)."""
    from provisa.discovery.columns import UNKNOWN, ColumnFacts, TableFacts, type_groups

    def col(name: str, data_type: str, *samples: str) -> ColumnFacts:
        return ColumnFacts(
            name=name,
            data_type=data_type,
            description=None,
            tags=(),
            classes=(UNKNOWN,),
            sensitive=False,
            samples=samples,
            is_primary_key=False,
            groups=type_groups(data_type, subtype=None, array_depth=0),
        )

    def tbl(table_id: int, name: str, *columns: ColumnFacts) -> TableFacts:
        return TableFacts(table_id, "city-files", "drop", name, None, (), None, columns)

    neighborhoods = ("Harbor Point", "Old Town", "Eastgate", "Riverside", "Millbrook")
    return [
        tbl(
            309,
            "budget_2026__budget",
            col("neighborhood", "varchar", "Riverside", "Old Town", "Millbrook", "Harbor Point"),
            col("department", "varchar", "Public Works", "Parks", "Planning", "Sanitation"),
            col("amount", "integer"),
        ),
        tbl(
            310,
            "building_permits",
            col("permit_id", "varchar", "BP-0001", "BP-0002", "BP-0003", "BP-0004", "BP-0005"),
            col("address", "varchar", "149 Bank St", "59 River Rd", "168 Willow Ln"),
            col("neighborhood", "varchar", "Riverside"),
            col("permit_type", "varchar", "Renovation", "Addition", "New building", "Demolition"),
            col("issued_on", "date"),
            col("expires_on", "date"),
            col("status", "varchar", "active", "expired"),
            col("valuation", "bigint"),
        ),
        *(
            tbl(
                table_id,
                name,
                col("complaint_id", "integer"),
                col("opened_at", "timestamp"),
                col("status", "varchar", "open", "closed"),
                col("closed_at", "timestamp"),
                col("category", "varchar", "Blocked sidewalk", "Dust", "Noise", "Trash"),
                col("neighborhood", "varchar", *neighborhoods),
                col("address", "varchar", *addresses),
            )
            for table_id, name, addresses in (
                (311, "complaints_2026_08", ("238 Pier Ave", "33 Mill Ln", "86 Gate St")),
                (312, "complaints_2026_09", ("13 Pier Ave", "1 Willow Ln", "230 Bank St")),
            )
        ),
        tbl(
            313,
            "inspections",
            col("inspection_id", "integer"),
            col("permit_id", "varchar", "BP-0096", "BP-0081", "BP-0016", "BP-0093", "BP-0076"),
            col("inspected_on", "date"),
            col("result", "varchar", "passed", "failed", "corrections needed"),
        ),
    ]


def test_discovery_suggests_the_permit_key_a_folder_of_files_shares(record_property):
    """One call, with a short answer: the table pairs of the five files, as a run sends them.
    inspections.permit_id names building_permits.permit_id, and the model is to say so, under the
    pair of those two tables, at the confidence a suggestion is kept at, by the rubric the call
    states. Everything it answered
    is recorded with the test's result (``discovery_answer``)."""
    import json

    from provisa.discovery.analyzer import read_answer_list
    from provisa.discovery.run import SYSTEM_PROMPT
    from provisa.discovery.runner import Tally, read_answers
    from provisa.discovery.threshold import DEFAULT
    from provisa.discovery.windows import plan
    from provisa.llm.client import ProvisaLLMClient

    max_tokens = 4_096
    planned = plan(_drop_tables(), None, 32_000, max_tokens)
    (call,) = [
        c
        for c in planned.calls
        if any({p.source.table_id, p.target.table_id} == {313, 310} for p in c.pairs)
    ]
    client = ProvisaLLMClient("relationship_inference", config=_OPENROUTER_CONFIG, api_keys={})
    answer = client.complete_sync_detailed(call.prompt(DEFAULT), SYSTEM_PROMPT, max_tokens)
    read = read_answer_list(answer.text, answer.cut_off)
    assert read.items is not None, f"no list in the answer: {answer.text!r}"
    tally = Tally(total=1, threshold=DEFAULT)
    _, found, _, _ = read_answers(call, read.items, tally)
    kept = [p for p in found if p.confidence >= DEFAULT]
    said = [
        {"edge": p.edge, "confidence": p.confidence, "left_out": None, "reasoning": p.reasoning}
        for p in kept
    ] + [
        {
            "edge": None if r.proposal is None else r.proposal.edge,
            "confidence": None if r.proposal is None else r.proposal.confidence,
            "left_out": r.left_out,
            "reasoning": r.answer if r.proposal is None else r.proposal.reasoning,
        }
        for r in tally.left_out
    ]
    record_property("discovery_model", f"{answer.vendor}:{answer.model}")
    record_property("discovery_answer", json.dumps(said))
    permit = [p for p in kept if p.pair == frozenset({(313, "permit_id"), (310, "permit_id")})]
    assert permit, f"the permit key was not kept at {DEFAULT} or more; answered: {said}"
    assert permit[0].start == (313, "permit_id"), f"the wrong way round: {said}"
