# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1e9a47-3b2d-4f86-a0c7-8d4e2f61b9a3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1907: a table's role_ttl list, its lookup (defaulting to cache_ttl) and the effective TTL
max(cache_ttl, role_ttl(role)) — the operator's cache_ttl is always the floor."""

# Requirements: REQ-1907

from __future__ import annotations

from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from provisa.core.models import Table
from provisa.federation.role_ttl import (
    declared_cache_ttl,
    effective_ttl,
    floor_ttl,
    max_accepted_ttl,
    role_ttl,
)

pytestmark = pytest.mark.unit


def _table(cache_ttl=None, role_ttl=None):
    return SimpleNamespace(cache_ttl=cache_ttl, role_ttl=role_ttl or {})


def _source(cache_ttl=None):
    return SimpleNamespace(cache_ttl=cache_ttl)


def test_floor_is_the_tables_own_cache_ttl_else_its_sources_else_0():
    assert floor_ttl(_table(cache_ttl=60), _source(cache_ttl=900)) == 60
    assert floor_ttl(_table(), _source(cache_ttl=900)) == 900
    assert floor_ttl(_table(), _source()) == 0
    assert declared_cache_ttl(_table(), _source()) is None


def test_an_unlisted_role_uses_cache_ttl():
    t = _table(cache_ttl=60, role_ttl={"analyst": 360})
    assert role_ttl(t, _source(), "trader") == 60
    assert role_ttl(t, _source(), None) == 60


def test_a_listed_role_uses_its_entry():
    t = _table(cache_ttl=60, role_ttl={"analyst": 360, "trader": 0})
    assert role_ttl(t, _source(), "analyst") == 360
    assert role_ttl(t, _source(), "trader") == 0


def test_effective_ttl_is_always_the_max_of_cache_ttl_and_role_ttl():
    t = _table(cache_ttl=60, role_ttl={"analyst": 360, "trader": 0})
    assert effective_ttl(t, _source(), "analyst") == 360
    assert effective_ttl(t, _source(), "trader") == 60  # an entry below the floor has no effect
    assert effective_ttl(t, _source(), "other") == 60


def test_effective_ttl_inherits_the_sources_floor():
    t = _table(role_ttl={"trader": 0, "analyst": 3600})
    assert effective_ttl(t, _source(cache_ttl=120), "trader") == 120
    assert effective_ttl(t, _source(cache_ttl=120), "analyst") == 3600


def test_no_cache_ttl_takes_a_0_floor_so_the_role_entry_is_the_effective_ttl():
    """REQ-1907 (amended): with no cache_ttl the floor is 0 — a listed role's entry holds the
    table back, an unlisted role gets 0 and its freshness check alone decides."""
    t = _table(role_ttl={"analyst": 360})
    assert effective_ttl(t, _source(), "analyst") == 360
    assert effective_ttl(t, _source(), "trader") == 0


def test_max_accepted_ttl_is_the_longest_any_reader_accepts():
    assert max_accepted_ttl(_table(cache_ttl=60, role_ttl={"a": 360, "b": 0}), _source()) == 360
    assert max_accepted_ttl(_table(cache_ttl=60), _source()) == 60
    assert max_accepted_ttl(_table(role_ttl={"a": 5}), _source(cache_ttl=30)) == 30
    assert max_accepted_ttl(_table(role_ttl={"a": 5}), _source()) == 5


def _cfg_table(**kw):
    return dict(
        source_id="s", domain_id="d", table="t", columns=[{"name": "id", "visible_to": []}], **kw
    )


def test_the_table_model_parses_role_ttl_as_a_role_keyed_map():
    t = Table.model_validate(_cfg_table(cache_ttl=60, role_ttl={"trader": 0, "analyst": 360}))
    assert t.role_ttl == {"trader": 0, "analyst": 360}
    assert Table.model_validate(_cfg_table()).role_ttl == {}


def test_a_negative_role_ttl_is_rejected():
    with pytest.raises(ValidationError, match="role_ttl"):
        Table.model_validate(_cfg_table(role_ttl={"trader": -1}))


def test_config_rejects_a_role_ttl_naming_an_unknown_role():
    from provisa.core.config_loader import _validate_role_ttl

    cfg = SimpleNamespace(
        roles=[SimpleNamespace(id="analyst")],
        tables=[SimpleNamespace(table_name="orders", role_ttl={"analyst": 360, "ghost": 0})],
    )
    with pytest.raises(ValueError, match="ghost"):
        _validate_role_ttl(cfg)


def test_config_accepts_known_and_system_roles():
    from provisa.core.config_loader import _validate_role_ttl
    from provisa.security.rights import SYSTEM_ROLE_IDS

    system_role = next(iter(SYSTEM_ROLE_IDS))
    cfg = SimpleNamespace(
        roles=[SimpleNamespace(id="analyst")],
        tables=[SimpleNamespace(table_name="orders", role_ttl={"analyst": 360, system_role: 0})],
    )
    _validate_role_ttl(cfg)


# --- REQ-1907 (amended 2026-09-30): ttl / ttl_probe needs a table or source cache_ttl --------


def _landing_cfg(table_kw, source_kw):
    src = dict(
        id="s",
        change_signal="kafka",
        cache_ttl=None,
        prefer_materialized=False,
        load_protected=False,
        freshness_gate=False,
    )
    src.update(source_kw)
    tbl = dict(
        table_name="orders",
        source_id="s",
        cache_ttl=None,
        change_signal=None,
        prefer_materialized=None,
        load_protected=None,
        materialize=False,
        row_materialize=False,
    )
    tbl.update(table_kw)
    return SimpleNamespace(sources=[SimpleNamespace(**src)], tables=[SimpleNamespace(**tbl)])


_LANDING_FLAGS = [
    ({"materialize": True}, {}),
    ({"row_materialize": True}, {}),
    ({"prefer_materialized": True}, {}),
    ({}, {"prefer_materialized": True}),
    ({"load_protected": True}, {}),
    ({}, {"load_protected": True}),
]


@pytest.mark.parametrize("signal", ["ttl", "ttl_probe"])
@pytest.mark.parametrize(("table_flag", "source_flag"), _LANDING_FLAGS)
def test_config_rejects_a_landed_ttl_signal_with_no_table_or_source_cache_ttl(
    signal, table_flag, source_flag
):
    """REQ-1907 (amended 2026-09-30, option B): config alone guarantees the table lands."""
    from provisa.core.config_loader import _validate_landing_ttl

    with pytest.raises(ValueError, match=r"orders.*add a cache_ttl"):
        _validate_landing_ttl(_landing_cfg({"change_signal": signal, **table_flag}, source_flag))
    with pytest.raises(ValueError, match=r"orders.*add a cache_ttl"):
        _validate_landing_ttl(_landing_cfg(table_flag, {"change_signal": signal, **source_flag}))


@pytest.mark.parametrize("signal", ["ttl", "ttl_probe"])
def test_config_accepts_a_ttl_table_config_does_not_force_to_land(signal):
    """Whether it lands depends on the engine's reach -- the read path judges it."""
    from provisa.core.config_loader import _validate_landing_ttl

    _validate_landing_ttl(_landing_cfg({"change_signal": signal}, {}))
    # a table override of False beats a landing source default
    _validate_landing_ttl(
        _landing_cfg(
            {"change_signal": signal, "prefer_materialized": False, "load_protected": False},
            {"prefer_materialized": True, "load_protected": True},
        )
    )


@pytest.mark.parametrize(("table_flag", "source_flag"), _LANDING_FLAGS)
def test_config_accepts_a_landed_ttl_table_with_a_table_or_source_cache_ttl(
    table_flag, source_flag
):
    from provisa.core.config_loader import _validate_landing_ttl

    _validate_landing_ttl(
        _landing_cfg({"change_signal": "ttl", "cache_ttl": 60, **table_flag}, source_flag)
    )
    _validate_landing_ttl(
        _landing_cfg(table_flag, {"change_signal": "ttl_probe", "cache_ttl": 60, **source_flag})
    )


@pytest.mark.parametrize("signal", ["probe", "native", "debezium", "kafka", "signal"])
@pytest.mark.parametrize(("table_flag", "source_flag"), _LANDING_FLAGS)
def test_config_accepts_the_no_ttl_freshness_signals_on_a_landed_table(
    signal, table_flag, source_flag
):
    from provisa.core.config_loader import _validate_landing_ttl

    _validate_landing_ttl(_landing_cfg({"change_signal": signal, **table_flag}, source_flag))
    _validate_landing_ttl(
        _landing_cfg(table_flag, {"change_signal": signal, "freshness_gate": True, **source_flag})
    )
