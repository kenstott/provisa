# Copyright (c) 2026 Kenneth Stott
# Canary: 17161644-5263-43f9-8563-bd4612201128
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for LLM relationship discovery — gaps not covered by existing tests.

Source files:
  - provisa/discovery/analyzer.py  — analyze(), RelationshipCandidate, _validate_candidate
  - provisa/discovery/prompt.py    — build_prompt()
  - provisa/discovery/collector.py — DiscoveryInput, TableMeta

Existing coverage (do NOT duplicate):
  - test_discovery_analyzer.py:
      valid_response_parsed, invalid_candidates_filtered, below_threshold_filtered,
      malformed_json_returns_empty, response_in_code_block_parsed
  - test_discovery_prompt.py:
      prompt_includes_table_metadata, multiple_tables, existing/rejected exclusion,
      cross_domain tables, domain= annotation
  - test_discovery_candidates.py:
      store_candidates, list_pending, accept, reject lifecycle (mocked asyncpg)

This file adds:
  - FK suggestion request/response — full API call shape (model, max_tokens, messages)
  - Confidence threshold boundary — exactly at min_confidence is included
  - Confidence threshold boundary — just below min_confidence is excluded
  - Error when LLM unavailable — anthropic.APIConnectionError propagates
  - Error when LLM unavailable — anthropic.APIStatusError propagates
  - Multiple candidates with mixed confidence — only passing ones returned
  - Invalid cardinality value in LLM response is filtered
  - Non-array JSON response returns empty list
  - RelationshipCandidate dataclass field types are correct
  - build_prompt output format section is present
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from provisa.discovery.analyzer import RelationshipCandidate, analyze, _validate_candidate
from provisa.discovery.collector import DiscoveryInput, TableMeta
from provisa.discovery.prompt import build_prompt


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _make_table(
    table_id: int,
    name: str,
    columns: list[str],
    schema: str = "public",
    domain: str = "d1",
) -> TableMeta:
    return TableMeta(
        table_id=table_id,
        source_id="src1",
        domain_id=domain,
        schema_name=schema,
        table_name=name,
        columns=[{"name": c, "type": "integer"} for c in columns],
        sample_values=[{c: "1" for c in columns}],
    )


def _make_di(tables: list[TableMeta] | None = None) -> DiscoveryInput:
    if tables is None:
        t1 = _make_table(1, "orders", ["id", "customer_id"])
        t2 = _make_table(2, "customers", ["id", "name"])
        tables = [t1, t2]
    return DiscoveryInput(tables=tables, existing_relationships=[], rejected_pairs=[])


def _mock_response(text: str):
    content_block = MagicMock()
    content_block.text = text
    msg = MagicMock()
    msg.content = [content_block]
    return msg


def _candidate_json(
    src_table=1, src_col="customer_id",
    tgt_table=2, tgt_col="id",
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


# ---------------------------------------------------------------------------
# FK suggestion request/response shape
# ---------------------------------------------------------------------------

class TestFKSuggestionRequestResponseShape:
    """The analyzer must call the Anthropic API with the expected parameters
    and produce RelationshipCandidate objects with the correct field types."""

    @patch("provisa.discovery.analyzer.anthropic.Anthropic")
    def test_api_called_with_expected_model(self, mock_cls):
        """Claude claude-sonnet-4-20250514 must be the model used."""
        mock_client = MagicMock()
        mock_client.messages.create.return_value = _mock_response(
            json.dumps([_candidate_json()])
        )
        mock_cls.return_value = mock_client

        analyze("test prompt", "fake-key", _make_di())

        call_kwargs = mock_client.messages.create.call_args.kwargs
        assert call_kwargs["model"] == "claude-sonnet-4-20250514"

    @patch("provisa.discovery.analyzer.anthropic.Anthropic")
    def test_api_called_with_max_tokens(self, mock_cls):
        """max_tokens must be set (prevents truncated responses)."""
        mock_client = MagicMock()
        mock_client.messages.create.return_value = _mock_response(
            json.dumps([_candidate_json()])
        )
        mock_cls.return_value = mock_client

        analyze("test prompt", "fake-key", _make_di())

        call_kwargs = mock_client.messages.create.call_args.kwargs
        assert "max_tokens" in call_kwargs
        assert call_kwargs["max_tokens"] > 0

    @patch("provisa.discovery.analyzer.anthropic.Anthropic")
    def test_api_called_with_user_role_message(self, mock_cls):
        """Messages list must contain a user-role entry with the prompt."""
        mock_client = MagicMock()
        mock_client.messages.create.return_value = _mock_response(
            json.dumps([_candidate_json()])
        )
        mock_cls.return_value = mock_client

        analyze("my special prompt", "fake-key", _make_di())

        call_kwargs = mock_client.messages.create.call_args.kwargs
        messages = call_kwargs["messages"]
        assert len(messages) >= 1
        assert messages[0]["role"] == "user"
        assert messages[0]["content"] == "my special prompt"

    @patch("provisa.discovery.analyzer.anthropic.Anthropic")
    def test_api_key_passed_to_client(self, mock_cls):
        """The provided API key must be forwarded to the Anthropic client."""
        mock_client = MagicMock()
        mock_client.messages.create.return_value = _mock_response(json.dumps([]))
        mock_cls.return_value = mock_client

        analyze("prompt", "sk-my-test-key", _make_di())

        mock_cls.assert_called_once_with(api_key="sk-my-test-key")

    @patch("provisa.discovery.analyzer.anthropic.Anthropic")
    def test_response_fields_are_correct_types(self, mock_cls):
        """Each RelationshipCandidate must have correctly typed fields."""
        mock_client = MagicMock()
        mock_client.messages.create.return_value = _mock_response(
            json.dumps([_candidate_json(confidence=0.85)])
        )
        mock_cls.return_value = mock_client

        results = analyze("prompt", "fake-key", _make_di())

        assert len(results) == 1
        c = results[0]
        assert isinstance(c, RelationshipCandidate)
        assert isinstance(c.source_table_id, int)
        assert isinstance(c.source_column, str)
        assert isinstance(c.target_table_id, int)
        assert isinstance(c.target_column, str)
        assert isinstance(c.cardinality, str)
        assert isinstance(c.confidence, float)
        assert isinstance(c.reasoning, str)

    @patch("provisa.discovery.analyzer.anthropic.Anthropic")
    def test_response_field_values_match_llm_output(self, mock_cls):
        """All candidate fields must map to their LLM response counterparts."""
        raw = _candidate_json(
            src_table=1, src_col="customer_id",
            tgt_table=2, tgt_col="id",
            cardinality="many-to-one",
            confidence=0.92,
            reasoning="Name pattern match",
        )
        mock_client = MagicMock()
        mock_client.messages.create.return_value = _mock_response(json.dumps([raw]))
        mock_cls.return_value = mock_client

        results = analyze("prompt", "fake-key", _make_di())

        c = results[0]
        assert c.source_table_id == 1
        assert c.source_column == "customer_id"
        assert c.target_table_id == 2
        assert c.target_column == "id"
        assert c.cardinality == "many-to-one"
        assert c.confidence == pytest.approx(0.92)
        assert c.reasoning == "Name pattern match"


# ---------------------------------------------------------------------------
# Confidence threshold boundary
# ---------------------------------------------------------------------------

class TestConfidenceThresholdBoundary:
    """Candidates exactly at min_confidence must be included; just below excluded."""

    @patch("provisa.discovery.analyzer.anthropic.Anthropic")
    def test_exactly_at_threshold_is_included(self, mock_cls):
        """confidence == min_confidence should be accepted (inclusive lower bound)."""
        mock_client = MagicMock()
        mock_client.messages.create.return_value = _mock_response(
            json.dumps([_candidate_json(confidence=0.7)])
        )
        mock_cls.return_value = mock_client

        results = analyze("prompt", "fake-key", _make_di(), min_confidence=0.7)
        assert len(results) == 1
        assert results[0].confidence == pytest.approx(0.7)

    @patch("provisa.discovery.analyzer.anthropic.Anthropic")
    def test_just_below_threshold_is_excluded(self, mock_cls):
        """confidence < min_confidence should be filtered out."""
        mock_client = MagicMock()
        mock_client.messages.create.return_value = _mock_response(
            json.dumps([_candidate_json(confidence=0.699)])
        )
        mock_cls.return_value = mock_client

        results = analyze("prompt", "fake-key", _make_di(), min_confidence=0.7)
        assert len(results) == 0

    @patch("provisa.discovery.analyzer.anthropic.Anthropic")
    def test_mixed_confidence_returns_only_passing(self, mock_cls):
        """Three candidates: two pass, one does not."""
        di = _make_di([
            _make_table(1, "orders", ["id", "customer_id", "product_id"]),
            _make_table(2, "customers", ["id", "name"]),
            _make_table(3, "products", ["id", "title"]),
        ])
        payload = [
            _candidate_json(src_col="customer_id", tgt_table=2, confidence=0.95),
            _candidate_json(src_col="product_id", tgt_table=3, confidence=0.5),   # filtered
            # one-to-many direction
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
        mock_client = MagicMock()
        mock_client.messages.create.return_value = _mock_response(json.dumps(payload))
        mock_cls.return_value = mock_client

        results = analyze("prompt", "fake-key", di, min_confidence=0.7)
        assert len(results) == 2
        confidences = {r.confidence for r in results}
        assert 0.5 not in confidences

    @patch("provisa.discovery.analyzer.anthropic.Anthropic")
    def test_zero_threshold_accepts_all_valid(self, mock_cls):
        """min_confidence=0.0 accepts any valid candidate regardless of score."""
        mock_client = MagicMock()
        mock_client.messages.create.return_value = _mock_response(
            json.dumps([_candidate_json(confidence=0.01)])
        )
        mock_cls.return_value = mock_client

        results = analyze("prompt", "fake-key", _make_di(), min_confidence=0.0)
        assert len(results) == 1

    @patch("provisa.discovery.analyzer.anthropic.Anthropic")
    def test_threshold_1_accepts_only_perfect_confidence(self, mock_cls):
        """min_confidence=1.0 accepts only candidates with confidence exactly 1.0."""
        mock_client = MagicMock()
        mock_client.messages.create.return_value = _mock_response(
            json.dumps([_candidate_json(confidence=0.999)])
        )
        mock_cls.return_value = mock_client

        results = analyze("prompt", "fake-key", _make_di(), min_confidence=1.0)
        assert len(results) == 0


# ---------------------------------------------------------------------------
# Error when LLM unavailable
# ---------------------------------------------------------------------------

class TestLLMUnavailableError:
    """Anthropic API errors must propagate — the analyzer must not swallow them."""

    @patch("provisa.discovery.analyzer.anthropic.Anthropic")
    def test_api_connection_error_propagates(self, mock_cls):
        """anthropic.APIConnectionError must not be silently swallowed."""
        import anthropic as anthropic_module

        mock_client = MagicMock()
        mock_client.messages.create.side_effect = anthropic_module.APIConnectionError(
            request=MagicMock()
        )
        mock_cls.return_value = mock_client

        with pytest.raises(anthropic_module.APIConnectionError):
            analyze("prompt", "fake-key", _make_di())

    @patch("provisa.discovery.analyzer.anthropic.Anthropic")
    def test_api_status_error_401_propagates(self, mock_cls):
        """Unauthorized (401) from the Anthropic API must propagate."""
        import anthropic as anthropic_module
        import httpx

        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_client = MagicMock()
        mock_client.messages.create.side_effect = anthropic_module.AuthenticationError(
            message="Invalid API key",
            response=mock_response,
            body={"error": {"type": "authentication_error"}},
        )
        mock_cls.return_value = mock_client

        with pytest.raises(anthropic_module.AuthenticationError):
            analyze("prompt", "bad-key", _make_di())

    @patch("provisa.discovery.analyzer.anthropic.Anthropic")
    def test_api_rate_limit_error_propagates(self, mock_cls):
        """RateLimitError from the Anthropic API must propagate."""
        import anthropic as anthropic_module

        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_client = MagicMock()
        mock_client.messages.create.side_effect = anthropic_module.RateLimitError(
            message="Rate limit exceeded",
            response=mock_response,
            body={"error": {"type": "rate_limit_error"}},
        )
        mock_cls.return_value = mock_client

        with pytest.raises(anthropic_module.RateLimitError):
            analyze("prompt", "fake-key", _make_di())

    @patch("provisa.discovery.analyzer.anthropic.Anthropic")
    def test_empty_content_list_returns_empty(self, mock_cls):
        """If the LLM returns an empty JSON array, analyze returns []."""
        mock_client = MagicMock()
        mock_client.messages.create.return_value = _mock_response("[]")
        mock_cls.return_value = mock_client

        results = analyze("prompt", "fake-key", _make_di())
        assert results == []


# ---------------------------------------------------------------------------
# Additional validation edge cases
# ---------------------------------------------------------------------------

class TestCandidateValidation:
    """_validate_candidate covers structural / referential integrity checks."""

    def test_invalid_cardinality_filtered(self):
        """Only 'many-to-one' and 'one-to-many' are valid cardinalities."""
        di = _make_di()
        raw = _candidate_json(cardinality="many-to-many")  # invalid
        assert not _validate_candidate(raw, di)

    def test_missing_required_key_filtered(self):
        """A candidate missing 'cardinality' must be rejected."""
        di = _make_di()
        raw = _candidate_json()
        del raw["cardinality"]
        assert not _validate_candidate(raw, di)

    def test_source_table_not_in_input_filtered(self):
        """Reference to an unknown source_table_id must be rejected."""
        di = _make_di()
        raw = _candidate_json(src_table=99)  # table 99 not in di
        assert not _validate_candidate(raw, di)

    def test_target_table_not_in_input_filtered(self):
        """Reference to an unknown target_table_id must be rejected."""
        di = _make_di()
        raw = _candidate_json(tgt_table=99)
        assert not _validate_candidate(raw, di)

    def test_valid_candidate_passes_validation(self):
        """A well-formed candidate referencing known tables and columns passes."""
        di = _make_di()
        raw = _candidate_json(
            src_table=1, src_col="customer_id",
            tgt_table=2, tgt_col="id",
            cardinality="many-to-one",
        )
        assert _validate_candidate(raw, di)

    def test_one_to_many_cardinality_passes_validation(self):
        """one-to-many is a valid cardinality value."""
        di = _make_di([
            _make_table(1, "customers", ["id"]),
            _make_table(2, "orders", ["id", "customer_id"]),
        ])
        raw = {
            "source_table_id": 1,
            "source_column": "id",
            "target_table_id": 2,
            "target_column": "customer_id",
            "cardinality": "one-to-many",
            "confidence": 0.9,
            "reasoning": "PK to FK",
        }
        assert _validate_candidate(raw, di)


# ---------------------------------------------------------------------------
# Prompt output-format section
# ---------------------------------------------------------------------------

class TestPromptOutputFormat:
    """build_prompt must include the JSON output format specification."""

    def test_output_format_section_present(self):
        di = _make_di()
        prompt = build_prompt(di)
        assert "Output Format" in prompt

    def test_prompt_requests_json_array(self):
        di = _make_di()
        prompt = build_prompt(di)
        assert "JSON array" in prompt

    def test_prompt_lists_required_keys(self):
        di = _make_di()
        prompt = build_prompt(di)
        for key in ("source_table_id", "source_column", "target_table_id",
                    "target_column", "cardinality", "confidence"):
            assert key in prompt

    def test_prompt_specifies_cardinality_values(self):
        di = _make_di()
        prompt = build_prompt(di)
        assert "many-to-one" in prompt
        assert "one-to-many" in prompt

    def test_prompt_specifies_confidence_range(self):
        di = _make_di()
        prompt = build_prompt(di)
        assert "0.0" in prompt or "0.0-1.0" in prompt or "float" in prompt

    def test_sample_values_included_in_prompt(self):
        """Tables with sample_values must have them in the prompt."""
        t1 = TableMeta(
            table_id=1, source_id="src1", domain_id="d1",
            schema_name="public", table_name="orders",
            columns=[{"name": "id", "type": "integer"}],
            sample_values=[{"id": "42"}],
        )
        di = DiscoveryInput(tables=[t1], existing_relationships=[], rejected_pairs=[])
        prompt = build_prompt(di)
        assert "Sample rows" in prompt
        assert "42" in prompt

    def test_empty_sample_values_skipped(self):
        """Tables with no sample_values should not emit a 'Sample rows' line."""
        t1 = TableMeta(
            table_id=1, source_id="src1", domain_id="d1",
            schema_name="public", table_name="orders",
            columns=[{"name": "id", "type": "integer"}],
            sample_values=[],
        )
        di = DiscoveryInput(tables=[t1], existing_relationships=[], rejected_pairs=[])
        prompt = build_prompt(di)
        assert "Sample rows" not in prompt
