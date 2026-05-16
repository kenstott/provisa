# Copyright (c) 2026 Kenneth Stott
# Canary: d5e6f7a8-b9c0-1234-def0-567890123456
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the COPYRIGHT holder.

"""Unit tests for NL-assisted table search helpers (REQ-464)."""

from __future__ import annotations

import json

import pytest

from provisa.discovery.table_search import (
    TableCandidate,
    _tokenize,
    build_search_prompt,
    fuzzy_filter,
    parse_llm_response,
    search_tables,
)


def _make(
    name: str, columns: list[str], comment: str | None = None, schema: str = "public"
) -> TableCandidate:
    return TableCandidate(name=name, comment=comment, columns=columns, schema_name=schema)


# ---------------------------------------------------------------------------
# Tokenizer
# ---------------------------------------------------------------------------


class TestTokenize:
    def test_snake_case(self):
        assert "orders" in _tokenize("sales_orders")
        assert "sales" in _tokenize("sales_orders")

    def test_camel_case(self):
        assert "customer" in _tokenize("customerName")
        assert "name" in _tokenize("customerName")

    def test_single_char_dropped(self):
        assert "a" not in _tokenize("a_b_c")

    def test_lowercase(self):
        tokens = _tokenize("SalesORDER")
        assert all(t == t.lower() for t in tokens)


# ---------------------------------------------------------------------------
# Fuzzy filter
# ---------------------------------------------------------------------------


class TestFuzzyFilter:
    def test_exact_table_name_match(self):
        candidates = [
            _make("orders", ["id", "customer_id", "total"]),
            _make("products", ["id", "sku", "price"]),
        ]
        results = fuzzy_filter("orders", candidates)
        assert results[0].name == "orders"

    def test_column_match(self):
        # "invoice" not in table name but in column
        candidates = [
            _make("acct_doc", ["invoice_number", "amount"]),
            _make("products", ["id", "price"]),
        ]
        results = fuzzy_filter("invoice", candidates)
        assert len(results) >= 1
        assert results[0].name == "acct_doc"

    def test_comment_match(self):
        candidates = [
            _make("zsd_vbak", [], comment="Sales order header"),
            _make("products", ["id"]),
        ]
        results = fuzzy_filter("sales order", candidates)
        assert results[0].name == "zsd_vbak"

    def test_no_match_returns_empty(self):
        candidates = [_make("products", ["id", "price"])]
        results = fuzzy_filter("xyzzy_nonexistent_term", candidates)
        assert results == []

    def test_max_results_respected(self):
        candidates = [_make(f"orders_{i}", ["order_id", "amount"]) for i in range(50)]
        results = fuzzy_filter("orders amount", candidates, max_results=10)
        assert len(results) <= 10

    def test_empty_query_returns_candidates(self):
        candidates = [_make("orders", ["id"]), _make("products", ["id"])]
        results = fuzzy_filter("", candidates, max_results=5)
        assert len(results) == 2


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------


class TestBuildSearchPrompt:
    def test_contains_query(self):
        c = _make("orders", ["id", "amount"])
        prompt = build_search_prompt("sales orders", [c])
        assert "sales orders" in prompt

    def test_contains_table_name(self):
        c = _make("zsd_vbak", ["mandt", "vbeln"], comment="Sales header")
        prompt = build_search_prompt("invoice", [c])
        assert "zsd_vbak" in prompt

    def test_contains_columns(self):
        c = _make("orders", ["customer_id", "total_amount"])
        prompt = build_search_prompt("total", [c])
        assert "customer_id" in prompt or "total_amount" in prompt

    def test_output_format_instructions(self):
        prompt = build_search_prompt("x", [_make("t", [])])
        assert "confidence" in prompt
        assert "reasoning" in prompt


# ---------------------------------------------------------------------------
# LLM response parser
# ---------------------------------------------------------------------------


class TestParseLlmResponse:
    def test_bare_json_array(self):
        text = '[{"schema": "public", "table": "orders", "confidence": 0.9, "reasoning": "Direct match"}]'
        result = parse_llm_response(text)
        assert len(result) == 1
        assert result[0]["table"] == "orders"

    def test_markdown_code_block(self):
        text = (
            "Here are the results:\n"
            "```json\n"
            '[{"schema": "public", "table": "orders", "confidence": 0.85, "reasoning": "Match"}]\n'
            "```"
        )
        result = parse_llm_response(text)
        assert result[0]["confidence"] == 0.85

    def test_empty_array(self):
        assert parse_llm_response("[]") == []

    def test_multiple_results(self):
        data = [
            {"schema": "sales", "table": "orders", "confidence": 0.9, "reasoning": "a"},
            {"schema": "sales", "table": "invoices", "confidence": 0.7, "reasoning": "b"},
        ]
        result = parse_llm_response(json.dumps(data))
        assert len(result) == 2

    def test_malformed_falls_back_empty(self):
        # No JSON brackets — fallback to "[]" → empty list
        assert parse_llm_response("not json at all, no brackets") == []


# ---------------------------------------------------------------------------
# search_tables — fuzzy-only path (no API key)
# ---------------------------------------------------------------------------


class TestSearchTablesFuzzyOnly:
    @pytest.mark.asyncio
    async def test_returns_ranked_results(self):
        candidates = [
            _make("sales_orders", ["id", "customer_id", "amount"]),
            _make("products", ["id", "sku", "price"]),
        ]
        results = await search_tables("sales orders", candidates, api_key=None)
        assert len(results) >= 1
        assert results[0].name == "sales_orders"

    @pytest.mark.asyncio
    async def test_confidence_between_zero_and_one(self):
        candidates = [_make("orders", ["id", "amount"])]
        results = await search_tables("orders", candidates, api_key=None)
        assert all(0.0 <= r.confidence <= 1.0 for r in results)

    @pytest.mark.asyncio
    async def test_no_match_empty(self):
        candidates = [_make("products", ["id", "price"])]
        results = await search_tables("zzznonexistent", candidates, api_key=None)
        assert results == []

    @pytest.mark.asyncio
    async def test_sorted_by_confidence_descending(self):
        candidates = [
            _make("sales_orders", ["order_id", "customer_id", "order_amount"]),
            _make("order_items", ["order_id", "product_id"]),
        ]
        results = await search_tables("order amount", candidates, api_key=None)
        if len(results) > 1:
            for i in range(len(results) - 1):
                assert results[i].confidence >= results[i + 1].confidence
