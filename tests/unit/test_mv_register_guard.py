# Copyright (c) 2026 Kenneth Stott
# Canary: 3a7f9c21-6b48-4e0d-9c15-2f8e6a1d47b3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-964 (Phase 0): MV registration rejects non-deterministic SQL.

_sync_view_mv is the single register/update hook for a materialized user view.
It must raise before registering an MV whose SQL is non-deterministic, so
recompute-to-current and replay stay sound.
"""

from __future__ import annotations

import types

import pytest

from provisa.api.admin import schema_common


@pytest.fixture
def fake_state(monkeypatch):
    """Minimal app-state stand-in with a spy MV registry and no engine dialect."""
    registered: list = []
    registry = types.SimpleNamespace(
        get=lambda _: None,
        register=lambda mv: registered.append(mv),
        unregister=lambda _: None,
    )
    fed = types.SimpleNamespace(
        materialize_store_target=lambda _: ("postgresql", "mv_cache"),
    )
    state = types.SimpleNamespace(
        mv_registry=registry, org_id="test", engine=None, federation_engine=fed
    )
    fake_app = types.ModuleType("provisa.api.app")
    fake_app.state = state  # type: ignore[attr-defined]
    monkeypatch.setitem(__import__("sys").modules, "provisa.api.app", fake_app)
    state.registered = registered
    return state


def test_deterministic_mv_registers(fake_state):
    schema_common._sync_view_mv("sales", "SELECT region, sum(amt) AS t FROM o GROUP BY region", 300)
    assert len(fake_state.registered) == 1
    assert fake_state.registered[0].id == "view-sales"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT now() AS ts, region FROM o",
        "SELECT random() AS r, region FROM o",
        "SELECT a FROM o LIMIT 10",  # unordered LIMIT — arbitrary rows
    ],
)
def test_non_deterministic_mv_rejected(fake_state, sql):
    with pytest.raises(ValueError, match="non-deterministic MV"):
        schema_common._sync_view_mv("bad", sql, 300)
    assert fake_state.registered == []


# REQ-879: consistency tier flows into the MVDefinition and is a closed set.
_DET_SQL = "SELECT region, sum(amt) AS t FROM o GROUP BY region"


@pytest.mark.parametrize("tier", ["shared", "distributed"])
def test_consistency_tier_sets_mvdefinition(fake_state, tier):
    schema_common._sync_view_mv("sales", _DET_SQL, 300, consistency=tier)
    assert fake_state.registered[0].consistency == tier


def test_consistency_defaults_to_shared(fake_state):
    schema_common._sync_view_mv("sales", _DET_SQL, 300)
    assert fake_state.registered[0].consistency == "shared"


def test_invalid_consistency_rejected(fake_state):
    with pytest.raises(ValueError, match="invalid MV consistency"):
        schema_common._sync_view_mv("sales", _DET_SQL, 300, consistency="bogus")
    assert fake_state.registered == []


def test_table_input_maps_consistency():
    """The GraphQL TableInput → core model mapper carries mv_consistency (REQ-879)."""
    import types as _t

    from provisa.api.admin._live_mappers import table_model_from_input

    inp = _t.SimpleNamespace(
        source_id="s",
        domain_id="d",
        schema_name="public",
        table_name="v",
        description=None,
        watermark_column=None,
        change_signal=None,
        probe_query=None,
        probe_type=None,
        load_protected=None,  # REQ-1141
        off_peak_window=None,  # REQ-1141
        off_peak_tz=None,  # REQ-1141
        view_sql="SELECT 1",
        materialize=True,
        mv_refresh_interval=300,
        mv_debounce_quiet=0.0,
        mv_debounce_max_delay=5.0,
        mv_consistency="distributed",
        mv_preprocess=None,  # REQ-957
        mv_bitemporal_mode=None,  # REQ-1162
        mv_bitemporal_key=[],  # REQ-1162
        mv_persist="replace",  # REQ-965
        mv_primary_key=[],  # REQ-970
        mv_incremental=False,  # REQ-969
        data_product=False,
        enable_aggregates=False,
        enable_group_by=False,
        live=None,
    )
    model = table_model_from_input(inp, [], [], None)
    assert model.mv_consistency == "distributed"
