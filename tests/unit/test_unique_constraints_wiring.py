# Copyright (c) 2026 Kenneth Stott
"""REQ-1093: unique-constraint wiring — model mapper, ctx population, MCP exposure."""

from __future__ import annotations

from types import SimpleNamespace

from provisa.api.mcp.tools import _unique_constraints


def test_mcp_unique_constraints_role_scoped_from_ctx():
    tmeta = SimpleNamespace(table_id=7, domain_id="sales", table_name="users", field_name="users")
    ctx = SimpleNamespace(
        tables={"users": tmeta},
        unique_constraints={7: [("users_email_key", ["email"])]},
    )
    state = SimpleNamespace(contexts={"analyst": ctx})
    got = _unique_constraints(state, "analyst", "sales", "users")
    assert got == [{"name": "users_email_key", "columns": ["email"]}]


def test_mcp_unique_constraints_unknown_table_returns_empty():
    ctx = SimpleNamespace(tables={}, unique_constraints={})
    state = SimpleNamespace(contexts={"analyst": ctx})
    assert _unique_constraints(state, "analyst", "sales", "missing") == []


def test_table_model_from_input_maps_unique_constraints():
    from provisa.api.admin._live_mappers import table_model_from_input

    uc = SimpleNamespace(name="uq", columns=["a", "b"])
    inp = SimpleNamespace(
        source_id="s",
        domain_id="d",
        schema_name="public",
        table_name="t",
        description=None,
        watermark_column=None,
        change_signal=None,
        probe_query=None,
        probe_type=None,
        view_sql=None,
        materialize=False,
        mv_refresh_interval=300,
        mv_debounce_quiet=0.0,
        mv_debounce_max_delay=5.0,
        mv_consistency="shared",
        data_product=False,
        enable_aggregates=False,
        enable_group_by=False,
        live=None,
        unique_constraints=[uc],
    )
    model = table_model_from_input(inp, columns=[], presets=[], alias="t")
    assert len(model.unique_constraints) == 1
    assert model.unique_constraints[0].name == "uq"
    assert model.unique_constraints[0].columns == ["a", "b"]
