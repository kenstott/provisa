# Copyright (c) 2025 Kenneth Stott
# Canary: 6dd46827-be36-4a91-abc6-e48579fc845b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E tests for LLM relationship discovery flow.

Tests the full pipeline: collect metadata → build prompt → analyze → store
candidates → accept/reject lifecycle. Uses mocked Claude API and mocked DB.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.discovery.analyzer import RelationshipCandidate, analyze
from provisa.discovery.candidates import store_candidates
from provisa.discovery.collector import DiscoveryInput, TableMeta
from provisa.discovery.prompt import build_prompt


def _orders_table():
    return TableMeta(
        table_id=1, source_id="sales-pg", domain_id="sales-analytics",
        schema_name="public", table_name="orders",
        columns=[
            {"name": "id", "type": "integer"},
            {"name": "customer_id", "type": "integer"},
            {"name": "amount", "type": "decimal"},
        ],
        sample_values=[
            {"id": "1", "customer_id": "10", "amount": "99.99"},
            {"id": "2", "customer_id": "20", "amount": "49.99"},
        ],
    )


def _customers_table():
    return TableMeta(
        table_id=2, source_id="sales-pg", domain_id="sales-analytics",
        schema_name="public", table_name="customers",
        columns=[
            {"name": "id", "type": "integer"},
            {"name": "name", "type": "varchar"},
            {"name": "email", "type": "varchar"},
        ],
        sample_values=[
            {"id": "10", "name": "Alice", "email": "alice@test.com"},
            {"id": "20", "name": "Bob", "email": "bob@test.com"},
        ],
    )


def _products_table():
    return TableMeta(
        table_id=3, source_id="catalog-pg", domain_id="product-catalog",
        schema_name="public", table_name="products",
        columns=[
            {"name": "id", "type": "integer"},
            {"name": "name", "type": "varchar"},
            {"name": "price", "type": "decimal"},
        ],
        sample_values=[],
    )


def _mock_claude_response(candidates: list[dict]) -> MagicMock:
    content_block = MagicMock()
    content_block.text = json.dumps(candidates)
    msg = MagicMock()
    msg.content = [content_block]
    return msg


class TestTableScopeDiscovery:
    """Trigger table-scope discovery → candidates returned."""

    @patch("provisa.discovery.analyzer.anthropic.Anthropic")
    def test_table_scope_finds_relationship(self, mock_cls):
        orders = _orders_table()
        customers = _customers_table()
        di = DiscoveryInput(
            tables=[orders, customers],
            existing_relationships=[],
            rejected_pairs=[],
        )

        mock_client = MagicMock()
        mock_client.messages.create.return_value = _mock_claude_response([{
            "source_table_id": 1,
            "source_column": "customer_id",
            "target_table_id": 2,
            "target_column": "id",
            "cardinality": "many-to-one",
            "confidence": 0.95,
            "reasoning": "orders.customer_id references customers.id",
        }])
        mock_cls.return_value = mock_client

        prompt = build_prompt(di)
        results = analyze(prompt, "fake-key", di)

        assert len(results) == 1
        assert results[0].source_table_id == 1
        assert results[0].source_column == "customer_id"
        assert results[0].target_table_id == 2
        assert results[0].target_column == "id"
        assert results[0].confidence == 0.95


class TestDomainScopeDiscovery:
    """Trigger domain-scope discovery → candidates across domain tables."""

    @patch("provisa.discovery.analyzer.anthropic.Anthropic")
    def test_domain_scope_prompt_includes_all_domain_tables(self, mock_cls):
        orders = _orders_table()
        customers = _customers_table()
        di = DiscoveryInput(
            tables=[orders, customers],
            existing_relationships=[],
            rejected_pairs=[],
        )

        prompt = build_prompt(di)
        assert "public.orders" in prompt
        assert "public.customers" in prompt
        assert "domain=sales-analytics" in prompt


class TestCrossDomainDiscovery:
    """Cross-domain discovery finds relationships across sources."""

    @patch("provisa.discovery.analyzer.anthropic.Anthropic")
    def test_cross_domain_includes_tables_from_different_domains(self, mock_cls):
        orders = _orders_table()
        products = _products_table()
        di = DiscoveryInput(
            tables=[orders, products],
            existing_relationships=[],
            rejected_pairs=[],
        )

        prompt = build_prompt(di)
        assert "domain=sales-analytics" in prompt
        assert "domain=product-catalog" in prompt

    @patch("provisa.discovery.analyzer.anthropic.Anthropic")
    def test_cross_domain_finds_cross_source_relationship(self, mock_cls):
        orders = _orders_table()
        # Add product_id to orders
        orders.columns.append({"name": "product_id", "type": "integer"})
        products = _products_table()
        di = DiscoveryInput(
            tables=[orders, products],
            existing_relationships=[],
            rejected_pairs=[],
        )

        mock_client = MagicMock()
        mock_client.messages.create.return_value = _mock_claude_response([{
            "source_table_id": 1,
            "source_column": "product_id",
            "target_table_id": 3,
            "target_column": "id",
            "cardinality": "many-to-one",
            "confidence": 0.88,
            "reasoning": "orders.product_id → products.id cross-source FK",
        }])
        mock_cls.return_value = mock_client

        prompt = build_prompt(di)
        results = analyze(prompt, "fake-key", di)
        assert len(results) == 1
        assert results[0].source_column == "product_id"


class TestAcceptCandidateCreatesRelationship:
    """Accept candidate → relationship appears in registration model."""

    @pytest.mark.asyncio
    async def test_store_and_accept_flow(self):
        conn = AsyncMock()
        candidates = [RelationshipCandidate(
            source_table_id=1, source_column="customer_id",
            target_table_id=2, target_column="id",
            cardinality="many-to-one", confidence=0.95,
            reasoning="FK pattern",
        )]

        # Store
        conn.fetchval.return_value = 42
        ids = await store_candidates(conn, candidates, "table")
        assert ids == [42]

        # Accept — needs fetchrow to return the candidate
        from provisa.discovery.candidates import accept

        class MockRecord(dict):
            pass

        conn.fetchrow.return_value = MockRecord({
            "id": 42,
            "source_table_id": 1, "target_table_id": 2,
            "source_column": "customer_id", "target_column": "id",
            "cardinality": "many-to-one", "confidence": 0.95,
        })
        result = await accept(conn, 42)
        assert result["source_column"] == "customer_id"
        assert "relationship_id" in result

        # Verify INSERT INTO relationships was called
        insert_call = conn.execute.call_args
        assert "INSERT INTO relationships" in insert_call[0][0]


class TestRejectPreventsReSuggestion:
    """Reject candidate → excluded from future discovery prompts (REQ-095)."""

    def test_rejected_pairs_excluded_from_prompt(self):
        orders = _orders_table()
        customers = _customers_table()
        rejected = [{
            "source_table_id": 1,
            "source_column": "customer_id",
            "target_table_id": 2,
            "target_column": "id",
        }]
        di = DiscoveryInput(
            tables=[orders, customers],
            existing_relationships=[],
            rejected_pairs=rejected,
        )
        prompt = build_prompt(di)
        assert "Previously Rejected" in prompt
        assert "source_column=customer_id" in prompt

    @patch("provisa.discovery.analyzer.anthropic.Anthropic")
    def test_existing_relationship_excluded_from_prompt(self, mock_cls):
        orders = _orders_table()
        customers = _customers_table()
        existing = [{
            "source_table_id": 1,
            "source_column": "customer_id",
            "target_table_id": 2,
            "target_column": "id",
            "cardinality": "many-to-one",
        }]
        di = DiscoveryInput(
            tables=[orders, customers],
            existing_relationships=existing,
            rejected_pairs=[],
        )
        prompt = build_prompt(di)
        assert "Already Existing Relationships" in prompt
        assert "source_column=customer_id" in prompt


class TestConfidenceThreshold:
    """Low-confidence candidates filtered out."""

    @patch("provisa.discovery.analyzer.anthropic.Anthropic")
    def test_low_confidence_filtered(self, mock_cls):
        orders = _orders_table()
        customers = _customers_table()
        di = DiscoveryInput(tables=[orders, customers], existing_relationships=[], rejected_pairs=[])

        mock_client = MagicMock()
        mock_client.messages.create.return_value = _mock_claude_response([
            {
                "source_table_id": 1, "source_column": "customer_id",
                "target_table_id": 2, "target_column": "id",
                "cardinality": "many-to-one", "confidence": 0.3,
                "reasoning": "weak match",
            },
            {
                "source_table_id": 1, "source_column": "customer_id",
                "target_table_id": 2, "target_column": "name",
                "cardinality": "many-to-one", "confidence": 0.85,
                "reasoning": "strong match",
            },
        ])
        mock_cls.return_value = mock_client

        results = analyze(build_prompt(di), "fake-key", di, min_confidence=0.7)
        assert len(results) == 1
        assert results[0].confidence == 0.85


class TestSampleDataInPrompt:
    """Sample data included in prompt for LLM analysis (REQ-094)."""

    def test_sample_values_in_prompt(self):
        orders = _orders_table()
        di = DiscoveryInput(tables=[orders], existing_relationships=[], rejected_pairs=[])
        prompt = build_prompt(di)
        assert "Sample rows" in prompt
        assert "99.99" in prompt
