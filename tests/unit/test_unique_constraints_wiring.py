# Copyright (c) 2026 Kenneth Stott
# Canary: 45888e52-d861-4c52-b60a-9985df3de191
"""REQ-1093: unique-constraint wiring — model mapper, ctx population, MCP exposure."""

from __future__ import annotations

from types import SimpleNamespace

from provisa.api.mcp.tools import _unique_constraints
from tests.role_state import RoleState


def test_mcp_unique_constraints_role_scoped_from_ctx():
    # display_name mirrors the real TableMeta: semantic_table_name() reads it (None → field_name).
    tmeta = SimpleNamespace(
        table_id=7, domain_id="sales", table_name="users", field_name="users", display_name=None
    )
    ctx = SimpleNamespace(
        tables={"users": tmeta},
        unique_constraints={7: [("users_email_key", ["email"])]},
    )
    state = RoleState(contexts={"analyst": ctx})
    got = _unique_constraints(state, "analyst", "sales", "users")
    assert got == [{"name": "users_email_key", "columns": ["email"]}]


def test_mcp_unique_constraints_unknown_table_returns_empty():
    ctx = SimpleNamespace(tables={}, unique_constraints={})
    state = RoleState(contexts={"analyst": ctx})
    assert _unique_constraints(state, "analyst", "sales", "missing") == []


def _input(**over):
    """A TableInput as the admin form sends it, every field set; ``over`` replaces some."""
    fields = dict(
        source_id="s",
        domain_id="d",
        schema_name="public",
        table_name="t",
        description=None,
        watermark_column=None,
        change_signal=None,
        probe_query=None,
        probe_type=None,
        load_protected=None,  # REQ-1141
        off_peak_window=None,  # REQ-1141
        off_peak_tz=None,  # REQ-1141
        arguments=None,  # REQ-2061
        landing_filter=None,  # REQ-2062
        view_sql=None,
        dq_contract=None,  # REQ-1443
        profiler_source_id=None,  # REQ-1934
        materialize=False,
        mv_refresh_interval=300,
        mv_debounce_quiet=0.0,
        mv_debounce_max_delay=5.0,
        push_debounce_quiet=0.0,
        push_debounce_max_delay=5.0,
        mv_consistency="shared",
        mv_preprocess=None,  # REQ-957
        business_key=[],  # REQ-2109
        valid_time=None,  # REQ-2109
        recorded_time=None,  # REQ-2109
        mv_persist="replace",  # REQ-965
        mv_primary_key=[],  # REQ-970
        mv_incremental=False,  # REQ-969
        mv_calendar=None,  # REQ-962
        mv_grain=None,  # REQ-962/1168
        mv_allowed_lateness=0.0,  # REQ-961
        mv_expected_events=None,  # REQ-961
        mv_business_day_grain=False,  # REQ-962
        product_id=None,
        enable_aggregates=False,
        enable_group_by=False,
        modeling_role=None,  # REQ-1320
        modeling_history=None,  # REQ-1320
        live=None,
        unique_constraints=[],
    )
    fields.update(over)
    return SimpleNamespace(**fields)


def test_table_model_from_input_maps_unique_constraints():
    from provisa.api.admin._live_mappers import table_model_from_input

    uc = SimpleNamespace(name="uq", columns=["a", "b"])
    inp = _input(unique_constraints=[uc])
    model = table_model_from_input(inp, columns=[], alias="t")
    assert len(model.unique_constraints) == 1
    assert model.unique_constraints[0].name == "uq"
    assert model.unique_constraints[0].columns == ["a", "b"]


def test_table_model_from_input_maps_the_landing_scope():  # REQ-2061, REQ-2062
    from provisa.api.admin._live_mappers import table_model_from_input

    unset = table_model_from_input(_input(), columns=[], alias="t")
    assert (unset.arguments, unset.landing_filter) == ({}, None)
    inp = _input(arguments={"org": "acme"}, landing_filter="")
    model = table_model_from_input(inp, columns=[], alias="t")
    assert (model.arguments, model.landing_filter) == ({"org": "acme"}, None)
