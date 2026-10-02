# Copyright (c) 2026 Kenneth Stott
# Canary: 9b3d7f20-1a6c-4e85-b2f4-0c8e5d1a7f36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A replication setting that would not be enforced is refused, not stored (REQ-826, REQ-030).

Routing and replication read a source the configuration file declares FROM that file
(``federation.registry_view.registered_sources``): the control-plane row of such a source is not
consulted. The admin setters wrote that row and answered "success", so an operator who turned
replication on for a config-declared source saw it accepted while every read went on reaching the
source live. The setters now refuse such a source and say why."""

# Requirements: REQ-826, REQ-030

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from provisa.api.admin import schema_mutation
from provisa.api.admin.schema_mutation import Mutation


# The stored row the setters read before judging the save: a change-fed table of a source the
# control plane owns, so neither the replication-clock rule nor the load_protected rule refuses.
_STORED = SimpleNamespace(
    source_id="from-api",
    schema_name="public",
    table_name="customers",
    change_signal="kafka",
    cache_ttl=None,
    materialize=False,
    row_materialize=False,
    load_protected=False,
)


class _Pool:
    """Stands in for the control plane: records every statement, reports one row updated and
    answers a row read with ``_STORED``."""

    def __init__(self) -> None:
        self.statements: list = []

    def acquire(self):
        pool = self

        class _Conn:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def execute_core(self, statement):
                pool.statements.append(statement)
                return SimpleNamespace(rowcount=1, fetchone=lambda: _STORED)

        return _Conn()


@pytest.fixture
def admin(monkeypatch):
    import provisa.api.app as app_mod

    pool = _Pool()
    rebuild = AsyncMock()
    state = SimpleNamespace(
        config=SimpleNamespace(sources=[SimpleNamespace(id="from-config")]),
        tables=[
            {"id": 7, "source_id": "from-config", "table_name": "orders"},
            {"id": 8, "source_id": "from-api", "table_name": "customers"},
        ],
    )
    monkeypatch.setattr(app_mod, "state", state, raising=False)
    monkeypatch.setattr(schema_mutation, "_get_pool", AsyncMock(return_value=pool))
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", rebuild)
    # The save-time refusals (REQ-1907, REQ-826) are tested against a real control plane in
    # test_landing_ttl_admin.py; here every save is one they allow.
    monkeypatch.setattr(schema_mutation, "landing_ttl_refusal", AsyncMock(return_value=None))
    return SimpleNamespace(pool=pool, rebuild=rebuild)


def _updates(pool: _Pool) -> list[str]:
    """The tables the recorded statements UPDATE."""
    return [st.table.name for st in pool.statements if getattr(st, "is_update", False)]


def _info(monkeypatch):
    import provisa.api.app as app_mod
    from tests.unit.gate_identity import grant

    return grant(monkeypatch, "source_registration", "table_registration", state=app_mod.state)[0]


async def test_the_source_setter_refuses_a_config_declared_source(admin, monkeypatch):
    result = await Mutation().update_source_replicate(
        _info(monkeypatch), source_id="from-config", replicate=0
    )
    assert result.success is False
    assert result.code == "schema.source_setting_config_declared"
    assert "'from-config'" in result.message and "configuration file" in result.message
    assert "not be enforced" in result.message
    assert admin.pool.statements == [], "a setting that is not enforced was stored"
    admin.rebuild.assert_not_awaited()


async def test_the_source_setter_still_sets_a_source_the_control_plane_owns(admin, monkeypatch):
    result = await Mutation().update_source_replicate(
        _info(monkeypatch), source_id="from-api", replicate=0
    )
    assert result.success is True and result.code == "schema.source_replicate_set"
    assert _updates(admin.pool) == ["sources"]
    admin.rebuild.assert_awaited_once()


async def test_the_table_setter_refuses_a_table_of_a_config_declared_source(admin, monkeypatch):
    result = await Mutation().update_table_replicate(_info(monkeypatch), table_id=7, replicate=0)
    assert result.success is False
    assert result.code == "schema.source_setting_config_declared"
    assert "'from-config'" in result.message and "configuration file" in result.message
    assert admin.pool.statements == []


async def test_the_table_setter_still_sets_a_table_of_a_control_plane_source(admin, monkeypatch):
    result = await Mutation().update_table_replicate(_info(monkeypatch), table_id=8, replicate=0)
    assert result.success is True and result.code == "schema.table_replicate_set"
    assert _updates(admin.pool) == ["registered_tables"]
    # a saved value must route: the floored tables are published by the schema build
    admin.rebuild.assert_awaited_once()
