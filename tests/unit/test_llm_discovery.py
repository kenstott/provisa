# Copyright (c) 2026 Kenneth Stott
# Canary: 17161644-5263-43f9-8563-bd4612201128
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for LLM relationship discovery as a run asks and reads it (REQ-413).

Source files:
  - provisa/discovery/runner.py  — read_answers(), execute()
  - provisa/discovery/threshold.py — DEFAULT: the threshold of a person who has set none
  - provisa/discovery/context.py — prompt(): the request a run sends
  - provisa/discovery/windows.py — plan(): the calls of a run

This file covers:
  - The proposal read from an answer: its fields and their types
  - Confidence threshold boundary — exactly at the run's threshold is kept, just below left out
  - A model that fails is reported for its request, never swallowed
  - An element with an invalid cardinality, a missing key or an unknown table is left out
  - The call states the JSON shape it wants back and carries the sample values
"""

from __future__ import annotations

import json

import pytest

from provisa.discovery.columns import UNKNOWN, ColumnFacts, TableFacts, type_groups
from provisa.discovery.direction import Proposal
from provisa.discovery import threshold
from provisa.discovery.runner import NOT_NAMED, Tally, execute, read_answers
from provisa.discovery.runner import judged
from provisa.discovery.windows import Call, Plan, plan
from tests.unit.discovery_answers import from_ids, listed_tables
from provisa.llm.client import Completion
from provisa.llm.limits import DEFAULT_LIMITS


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _col(name: str, *samples: str) -> ColumnFacts:
    return ColumnFacts(
        name=name,
        data_type="integer",
        description=None,
        tags=(),
        classes=(UNKNOWN,),
        sensitive=False,
        samples=samples,
        is_primary_key=name == "id",
        groups=type_groups("integer", subtype=None, array_depth=0),
    )


def _make_table(table_id: int, name: str, columns: list[str], *samples: str) -> TableFacts:
    return TableFacts(
        table_id=table_id,
        domain_id="d1",
        schema_name="public",
        table_name=name,
        description=None,
        tags=(),
        role=None,
        columns=tuple(_col(c, *samples) for c in columns),
    )


def _tables(tables: list[TableFacts] | None) -> list[TableFacts]:
    if tables is None:
        tables = [
            _make_table(1, "orders", ["id", "customer_id"]),
            _make_table(2, "customers", ["id", "name"]),
        ]
    return tables


def _plan(tables: list[TableFacts] | None = None) -> Plan:
    """The run over ``tables`` compared with themselves: one call."""
    planned = plan(_tables(tables), None, *DEFAULT_LIMITS)
    assert len(planned.calls) == 1
    return planned


def _call(tables: list[TableFacts] | None = None) -> Call:
    return _plan(tables).calls[0]


def _candidate_json(
    src_table=1,
    src_col="customer_id",
    tgt_table=2,
    tgt_col="id",
    cardinality="many-to-one",
    confidence=0.9,
    reasoning="FK pattern",
) -> dict:
    return {
        "source_table_id": src_table,
        "source_column": src_col,
        "target_table_id": tgt_table,
        "target_column": tgt_col,
        "cardinality": cardinality,
        "confidence": confidence,
        "reasoning": reasoning,
    }


async def _read(
    items: list, tables: list[TableFacts] | None = None
) -> tuple[list[Proposal], Tally]:
    """The proposals an answer's elements make, judged as the run judges them (Key value overlap
    not chosen, held to the threshold). The elements name tables by id, as the model's answer
    is placed under the listed pair of their tables (:func:`from_ids`)."""
    call = _call(tables)
    names = {t.table_id: t.table_name for t in _tables(tables)}
    placed = json.loads(from_ids(call.prompt(threshold.DEFAULT), json.dumps(items), names))
    tally = Tally(total=1, threshold=threshold.DEFAULT)
    _, found, apart, zeros = read_answers(call, placed, tally)
    return await judged(found, apart, None, None, tally, zeros), tally


def _named_left(tally: Tally) -> list[str | None]:
    """Why each element the model wrote was left out: the column pairs it did not name, scored
    zero, aside."""
    return [
        r.left_out
        for r in tally.left_out
        if r.proposal is None or r.proposal.reasoning != NOT_NAMED
    ]


class _Watcher:
    async def stop_asked(self) -> bool:
        return False

    async def returned(self, tally: Tally, proposals: list[Proposal]) -> None:
        return None


async def _direct(fn, *args):
    return fn(*args)


async def _run(ask) -> tuple[Tally, list[Proposal]]:
    return await execute(
        _plan(),
        4_096,
        ask,
        _Watcher(),
        threshold=threshold.DEFAULT,
        key_values=None,
        to_thread=_direct,
    )


# ---------------------------------------------------------------------------
# FK suggestion request/response shape
# ---------------------------------------------------------------------------


class TestFKSuggestionRequestResponseShape:
    """A run reads each element of the model's answer into a proposal with the correct field
    types."""

    async def test_response_fields_are_correct_types(self):
        """Each proposal must have correctly typed fields."""
        kept, _ = await _read([_candidate_json(confidence=0.85)])

        assert len(kept) == 1
        p = kept[0]
        assert isinstance(p, Proposal)
        src_table, src_col, tgt_table, tgt_col = p.edge
        assert isinstance(src_table, int)
        assert isinstance(src_col, str)
        assert isinstance(tgt_table, int)
        assert isinstance(tgt_col, str)
        assert isinstance(p.confidence, float)
        assert isinstance(p.reasoning, str)

    async def test_response_field_values_match_llm_output(self):
        """All proposal fields must map to their LLM response counterparts."""
        raw = _candidate_json(
            src_table=1,
            src_col="customer_id",
            tgt_table=2,
            tgt_col="id",
            cardinality="many-to-one",
            confidence=0.92,
            reasoning="Name pattern match",
        )
        (p,), _ = await _read([raw])

        assert p.edge == (1, "customer_id", 2, "id")
        assert p.written == ((1, "customer_id"), (2, "id"))
        assert p.confidence == pytest.approx(0.92)
        assert p.reasoning == "Name pattern match"


# ---------------------------------------------------------------------------
# Confidence threshold boundary
# ---------------------------------------------------------------------------


class TestConfidenceThresholdBoundary:
    """Proposals exactly at the run's threshold are kept; just below are left out."""

    def test_the_default_threshold_is_0_7(self):
        assert threshold.DEFAULT == 0.7

    async def test_exactly_at_threshold_is_included(self):
        """confidence == the threshold is kept (inclusive lower bound)."""
        kept, _ = await _read([_candidate_json(confidence=0.7)])
        assert len(kept) == 1
        assert kept[0].confidence == pytest.approx(0.7)

    async def test_just_below_threshold_is_excluded(self):
        """confidence < the threshold is left out as below_threshold."""
        kept, tally = await _read([_candidate_json(confidence=0.699)])
        assert kept == []
        assert tally.below_threshold - tally.zero_scored == 1
        assert _named_left(tally) == ["below_threshold"]

    async def test_mixed_confidence_returns_only_passing(self):
        """Three proposals: two pass, one does not."""
        tables = [
            _make_table(1, "orders", ["id", "customer_id", "product_id"]),
            _make_table(2, "customers", ["id", "name"]),
            _make_table(3, "products", ["id", "title"]),
        ]
        payload = [
            _candidate_json(src_col="customer_id", tgt_table=2, confidence=0.95),
            _candidate_json(src_col="product_id", tgt_table=3, confidence=0.5),  # left out
            {
                "source_table_id": 2,
                "source_column": "id",
                "target_table_id": 1,
                "target_column": "customer_id",
                "cardinality": "one-to-many",
                "confidence": 0.8,
                "reasoning": "reverse side",
            },
        ]
        kept, _ = await _read(payload, tables)
        assert len(kept) == 2
        confidences = {p.confidence for p in kept}
        assert 0.5 not in confidences


# ---------------------------------------------------------------------------
# Error when LLM unavailable
# ---------------------------------------------------------------------------


class TestLLMUnavailableError:
    """A model's error is reported for its request: never swallowed."""

    @pytest.mark.parametrize(
        "error",
        [
            ConnectionError("LLM unreachable"),
            PermissionError("Invalid API key"),
            RuntimeError("Rate limit exceeded"),
        ],
    )
    async def test_a_model_error_is_reported_as_a_failed_request(self, error):
        def ask(prompt: str, max_tokens: int) -> Completion:
            raise error

        tally, proposals = await _run(ask)
        assert proposals == []
        assert tally.done == 2  # asked, then its pairs asked again
        failed = tally.failed[0]
        assert failed["code"] == "llm.request_failed"
        assert failed["params"] == {"reason": str(error)}
        assert len(tally.unanswered) == tally.table_pairs

    async def test_an_answer_of_none_for_every_pair_proposes_nothing(self):
        """If the LLM answers every listed pair "none", the run has no proposal and no
        failure."""

        def ask(prompt: str, max_tokens: int) -> Completion:
            return Completion(from_ids(prompt, "[]", {}), "anthropic", "test-model", None, False)

        tally, proposals = await _run(ask)
        assert proposals == []
        assert tally.failed == [] and tally.unanswered == []
        assert tally.proposed == 0


# ---------------------------------------------------------------------------
# Additional validation edge cases
# ---------------------------------------------------------------------------


class TestCandidateValidation:
    """read_items leaves out an element that names no two compared columns of its request."""

    async def test_invalid_cardinality_filtered(self):
        """Only 'many-to-one' and 'one-to-many' are valid cardinalities."""
        kept, tally = await _read([_candidate_json(cardinality="many-to-many")])
        assert kept == []
        assert _named_left(tally) == ["invalid_shape"]

    async def test_missing_required_key_filtered(self):
        """An element missing 'cardinality' is left out."""
        raw = _candidate_json()
        del raw["cardinality"]
        kept, tally = await _read([raw])
        assert kept == []
        assert _named_left(tally) == ["invalid_shape"]

    async def test_source_table_not_in_input_filtered(self):
        """Reference to an unknown source_table_id is left out."""
        kept, tally = await _read([_candidate_json(src_table=99)])
        assert kept == []
        assert _named_left(tally) == ["unknown_column"]

    async def test_target_table_not_in_input_filtered(self):
        """Reference to an unknown target_table_id is left out."""
        kept, tally = await _read([_candidate_json(tgt_table=99)])
        assert kept == []
        assert _named_left(tally) == ["unknown_column"]

    async def test_unknown_column_filtered(self):
        """Reference to a column its table does not have is left out."""
        kept, tally = await _read([_candidate_json(src_col="nonexistent_col")])
        assert kept == []
        assert _named_left(tally) == ["unknown_column"]

    async def test_valid_candidate_passes_validation(self):
        """A well-formed element naming known tables and columns is kept."""
        raw = _candidate_json(
            src_table=1,
            src_col="customer_id",
            tgt_table=2,
            tgt_col="id",
            cardinality="many-to-one",
        )
        kept, tally = await _read([raw])
        assert [p.edge for p in kept] == [(1, "customer_id", 2, "id")]
        assert _named_left(tally) == []

    async def test_one_to_many_cardinality_passes_validation(self):
        """one-to-many is a valid cardinality value, read the other way round."""
        tables = [
            _make_table(1, "customers", ["id"]),
            _make_table(2, "orders", ["id", "customer_id"]),
        ]
        raw = {
            "source_table_id": 1,
            "source_column": "id",
            "target_table_id": 2,
            "target_column": "customer_id",
            "cardinality": "one-to-many",
            "confidence": 0.9,
            "reasoning": "PK to FK",
        }
        kept, _ = await _read([raw], tables)
        assert [p.edge for p in kept] == [(2, "customer_id", 1, "id")]


# ---------------------------------------------------------------------------
# Prompt output-format section
# ---------------------------------------------------------------------------


class TestPromptOutputFormat:
    """The call a run sends states the JSON shape of its answer."""

    def _prompt(self, tables: list[TableFacts] | None = None) -> str:
        return _call(tables).prompt(threshold.DEFAULT)

    def test_output_format_section_present(self):
        assert "Return ONLY a JSON array, no other text, one element per pair:" in self._prompt()

    def test_prompt_requests_json_array(self):
        assert "JSON array" in self._prompt()

    def test_prompt_lists_required_keys(self):
        prompt = self._prompt()
        for key in (
            "pair",
            "column_pairs",
            "source_table",
            "source_column",
            "target_table",
            "target_column",
            "cardinality",
            "confidence",
        ):
            assert key in prompt

    def test_prompt_specifies_cardinality_values(self):
        prompt = self._prompt()
        assert "many-to-one" in prompt
        assert "one-to-many" in prompt

    def test_prompt_specifies_confidence_range(self):
        assert "0.0-1.0" in self._prompt()

    def test_sample_values_included_in_prompt(self):
        """Columns with sample values send them in the call."""
        tables = [
            _make_table(1, "orders", ["id", "customer_id"], "42"),
            _make_table(2, "customers", ["id"], "42"),
        ]
        prompt = self._prompt(tables)
        assert '"sample_values":["42"]' in prompt

    def test_empty_sample_values_skipped(self):
        """Columns with no sample values send an empty list and no value."""
        prompt = self._prompt()
        assert '"sample_values":[]' in prompt
        tables = listed_tables(prompt)
        assert all(c["sample_values"] == [] for t in tables for c in t["comparable_columns"])
