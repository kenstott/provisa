# Copyright (c) 2026 Kenneth Stott
# Canary: bfc90b91-8697-4865-8727-ca823b8942af
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for discovery analyzer — mocks the anthropic client."""

import json
from unittest.mock import MagicMock, patch

from provisa.discovery.analyzer import RelationshipCandidate, analyze
from provisa.discovery.collector import DiscoveryInput, TableMeta


def _make_discovery_input():
    t1 = TableMeta(
        table_id=1, source_id="src1", domain_id="d1",
        schema_name="public", table_name="orders",
        columns=[{"name": "id", "type": "integer"}, {"name": "customer_id", "type": "integer"}],
        sample_values=[],
    )
    t2 = TableMeta(
        table_id=2, source_id="src1", domain_id="d1",
        schema_name="public", table_name="customers",
        columns=[{"name": "id", "type": "integer"}, {"name": "name", "type": "varchar"}],
        sample_values=[],
    )
    return DiscoveryInput(tables=[t1, t2], existing_relationships=[], rejected_pairs=[])


def _mock_response(text: str):
    content_block = MagicMock()
    content_block.text = text
    msg = MagicMock()
    msg.content = [content_block]
    return msg


@patch("provisa.discovery.analyzer.anthropic.Anthropic")
def test_valid_response_parsed(mock_cls):
    candidates_json = json.dumps([{
        "source_table_id": 1,
        "source_column": "customer_id",
        "target_table_id": 2,
        "target_column": "id",
        "cardinality": "many-to-one",
        "confidence": 0.95,
        "reasoning": "FK pattern",
    }])
    mock_client = MagicMock()
    mock_client.messages.create.return_value = _mock_response(candidates_json)
    mock_cls.return_value = mock_client

    di = _make_discovery_input()
    results = analyze("test prompt", "fake-key", di)
    assert len(results) == 1
    assert results[0].source_column == "customer_id"
    assert results[0].confidence == 0.95


@patch("provisa.discovery.analyzer.anthropic.Anthropic")
def test_invalid_candidates_filtered(mock_cls):
    """Candidate references nonexistent column — should be filtered."""
    candidates_json = json.dumps([{
        "source_table_id": 1,
        "source_column": "nonexistent_col",
        "target_table_id": 2,
        "target_column": "id",
        "cardinality": "many-to-one",
        "confidence": 0.9,
        "reasoning": "guess",
    }])
    mock_client = MagicMock()
    mock_client.messages.create.return_value = _mock_response(candidates_json)
    mock_cls.return_value = mock_client

    di = _make_discovery_input()
    results = analyze("test prompt", "fake-key", di)
    assert len(results) == 0


@patch("provisa.discovery.analyzer.anthropic.Anthropic")
def test_below_threshold_filtered(mock_cls):
    candidates_json = json.dumps([{
        "source_table_id": 1,
        "source_column": "customer_id",
        "target_table_id": 2,
        "target_column": "id",
        "cardinality": "many-to-one",
        "confidence": 0.3,
        "reasoning": "weak match",
    }])
    mock_client = MagicMock()
    mock_client.messages.create.return_value = _mock_response(candidates_json)
    mock_cls.return_value = mock_client

    di = _make_discovery_input()
    results = analyze("test prompt", "fake-key", di, min_confidence=0.7)
    assert len(results) == 0


@patch("provisa.discovery.analyzer.anthropic.Anthropic")
def test_malformed_json_returns_empty(mock_cls):
    mock_client = MagicMock()
    mock_client.messages.create.return_value = _mock_response("This is not JSON at all {{{")
    mock_cls.return_value = mock_client

    di = _make_discovery_input()
    results = analyze("test prompt", "fake-key", di)
    assert results == []


@patch("provisa.discovery.analyzer.anthropic.Anthropic")
def test_response_in_code_block_parsed(mock_cls):
    candidates_json = json.dumps([{
        "source_table_id": 1,
        "source_column": "customer_id",
        "target_table_id": 2,
        "target_column": "id",
        "cardinality": "many-to-one",
        "confidence": 0.85,
        "reasoning": "FK pattern",
    }])
    text = f"Here are the results:\n```json\n{candidates_json}\n```"
    mock_client = MagicMock()
    mock_client.messages.create.return_value = _mock_response(text)
    mock_cls.return_value = mock_client

    di = _make_discovery_input()
    results = analyze("test prompt", "fake-key", di)
    assert len(results) == 1
