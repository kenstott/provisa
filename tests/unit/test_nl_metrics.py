# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1d9e2f-7a48-4b36-9d02-8e4f6a1c3b57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1319: metrics join the NL matcher vocabulary and resolve metric-shaped questions to
the semantic SQL form; REQ-1320: the prompt shows the star shape (fact/dimension roles)."""

from __future__ import annotations

import pytest

from provisa.core.models import Metric
from provisa.nl.prompt import format_entities
from provisa.nl.schema_matcher import SchemaEntity, SchemaMatcher, metric_entities

_asyncio = pytest.mark.asyncio(loop_scope="session")

_SDL = """
type orders { id: Int amount: Float region_id: Int }
type regions { id: Int region: String }
type Query { orders: orders regions: regions }
"""

_METRICS = [
    Metric(
        name="total_revenue",
        expression="SUM(orders.amount)",
        description="Gross order revenue",
        ai_context="Sum of order amounts before refunds.",
    ),
]

_ROLES = {"orders": "fact", "regions": "dimension"}


# ---------------------------------------------------------------------------
# metric_entities — vocabulary construction
# ---------------------------------------------------------------------------


def test_metric_entities_embed_name_description_ai_context():
    ents = metric_entities(_METRICS)
    assert len(ents) == 1
    e = ents[0]
    assert e.exact_name == "total_revenue"
    assert e.kind == "metric"
    assert e.parent is None
    assert "metric total_revenue" in e.embed_text
    assert "Gross order revenue" in e.embed_text
    assert "before refunds" in e.embed_text
    assert e.description == "Gross order revenue"


@_asyncio
async def test_build_includes_metric_entities_and_roles():
    matcher = await SchemaMatcher.build(_SDL, None, metrics=_METRICS, table_roles=_ROLES)
    kinds = {(e.kind, e.exact_name) for e in matcher.entities}
    assert ("metric", "total_revenue") in kinds
    assert ("table", "orders") in kinds
    assert matcher.table_roles == _ROLES


# ---------------------------------------------------------------------------
# resolve_metric — metric-shaped question → semantic SQL form
# ---------------------------------------------------------------------------


@_asyncio
async def test_resolve_metric_emits_semantic_sql_with_dimension():
    matcher = await SchemaMatcher.build(_SDL, None, metrics=_METRICS, table_roles=_ROLES)
    sql = matcher.resolve_metric("What is the total revenue by region?")
    assert sql == "SELECT region, value FROM metrics.total_revenue GROUP BY region"


@_asyncio
async def test_resolve_metric_no_dimension_mentioned():
    matcher = await SchemaMatcher.build(_SDL, None, metrics=_METRICS, table_roles=_ROLES)
    sql = matcher.resolve_metric("What is the total revenue?")
    assert sql == "SELECT value FROM metrics.total_revenue"


@_asyncio
async def test_resolve_metric_ignores_fact_table_columns():
    # "amount" is a column of the fact table — never a dimension in the semantic form.
    matcher = await SchemaMatcher.build(_SDL, None, metrics=_METRICS, table_roles=_ROLES)
    sql = matcher.resolve_metric("total revenue by region and amount")
    assert sql == "SELECT region, value FROM metrics.total_revenue GROUP BY region"


@_asyncio
async def test_resolve_metric_none_for_non_metric_question():
    matcher = await SchemaMatcher.build(_SDL, None, metrics=_METRICS, table_roles=_ROLES)
    assert matcher.resolve_metric("list all orders in the east region") is None


@_asyncio
async def test_resolve_metric_none_when_no_metrics():
    matcher = await SchemaMatcher.build(_SDL, None, metrics=[], table_roles=_ROLES)
    assert matcher.resolve_metric("total revenue by region") is None


# ---------------------------------------------------------------------------
# format_entities — METRICS section + star roles in the prompt
# ---------------------------------------------------------------------------


def _entities():
    return [
        SchemaEntity(exact_name="orders", kind="table", parent=None, embed_text="table orders"),
        SchemaEntity(exact_name="amount", kind="field", parent="orders", embed_text="f"),
        SchemaEntity(exact_name="regions", kind="table", parent=None, embed_text="table regions"),
        SchemaEntity(exact_name="region", kind="field", parent="regions", embed_text="f"),
        SchemaEntity(
            exact_name="total_revenue",
            kind="metric",
            parent=None,
            embed_text="metric total_revenue",
            description="Gross order revenue",
        ),
    ]


def test_format_entities_tags_star_roles():
    block = format_entities(_entities(), table_roles=_ROLES)
    assert "table: orders [fact]  fields: amount" in block
    assert "table: regions [dimension]  fields: region" in block


def test_format_entities_metrics_section_with_addressing_form():
    block = format_entities(_entities(), table_roles=_ROLES)
    assert "METRICS" in block
    assert "SELECT <dimensions>, value FROM metrics.<name> GROUP BY <dimensions>" in block
    assert "metric: total_revenue — Gross order revenue" in block


def test_format_entities_no_metrics_no_section():
    ents = [e for e in _entities() if e.kind != "metric"]
    block = format_entities(ents, table_roles=_ROLES)
    assert "METRICS" not in block


def test_format_entities_without_roles_unchanged_shape():
    ents = [e for e in _entities() if e.kind != "metric"]
    block = format_entities(ents)
    assert "table: orders  fields: amount" in block
    assert "[fact]" not in block
