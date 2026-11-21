# Copyright (c) 2026 Kenneth Stott
# Canary: 8972bebd-0c6e-4000-a582-4e967700ff34
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa.jev.decisions — optional Jev-assisted confidence scoring."""

from unittest.mock import AsyncMock

from provisa.jev.decisions import refine_glossary_edges

API_KEY = "ts_test_key"


async def test_refine_glossary_edges_drops_low_confidence(monkeypatch):
    mock = AsyncMock(return_value={"answers": {"0": {"noul": 0.9}, "1": {"noul": 0.2}}})
    monkeypatch.setattr("provisa.jev.client.evaluate", mock)
    proposals = [
        {"from": "Order", "to": "Customer", "rel_type": "RELATED_TO"},
        {"from": "Order", "to": "Widget", "rel_type": "RELATED_TO"},
    ]
    terms_by_name = {
        "Order": {"definition": "A purchase order"},
        "Customer": {"definition": "A buyer"},
        "Widget": {"definition": "A product"},
    }
    out = await refine_glossary_edges(proposals, terms_by_name, API_KEY, min_confidence=0.6)
    assert out == [proposals[0]]


async def test_refine_glossary_edges_keeps_unscored_on_jev_failure(monkeypatch):
    async def _boom(*a, **k):
        raise RuntimeError("boom")

    monkeypatch.setattr("provisa.jev.client.evaluate", _boom)
    proposals = [{"from": "A", "to": "B", "rel_type": "RELATED_TO"}]
    out = await refine_glossary_edges(proposals, {}, API_KEY, min_confidence=0.6)
    assert out == proposals


async def test_refine_glossary_edges_empty_input_skips_call(monkeypatch):
    mock = AsyncMock()
    monkeypatch.setattr("provisa.jev.client.evaluate", mock)
    assert await refine_glossary_edges([], {}, API_KEY, min_confidence=0.6) == []
    mock.assert_not_awaited()
