# Copyright (c) 2026 Kenneth Stott
# Canary: 8c0b8c93-2878-4d8a-8edc-4b3de4f1d1e9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1320: the Flight/MCP catalog builder must surface the same
"[fact]"/"[dimension, scd2]" description suffix as GraphQL introspection and
pg_description, so MCP tool callers see the star shape too.
"""

from __future__ import annotations

from types import SimpleNamespace

from provisa.api.flight.catalog import build_catalog_tables_from_context


def _state_with_table(**table_info_kwargs):
    tinfo = SimpleNamespace(
        domain_id="sales", description="Order records", columns=[], **table_info_kwargs
    )
    ctx = SimpleNamespace(table_map={"orders": tinfo})
    return SimpleNamespace(contexts={"analyst": ctx})


def test_fact_role_appended_to_description():
    state = _state_with_table(modeling_role="fact", modeling_history=None)
    tables = build_catalog_tables_from_context(state)
    assert tables[0].description == "Order records [fact]"


def test_dimension_role_with_history_appended():
    state = _state_with_table(modeling_role="dimension", modeling_history="scd2")
    tables = build_catalog_tables_from_context(state)
    assert tables[0].description == "Order records [dimension, scd2]"


def test_no_role_leaves_description_unchanged():
    state = _state_with_table(modeling_role=None, modeling_history=None)
    tables = build_catalog_tables_from_context(state)
    assert tables[0].description == "Order records"
