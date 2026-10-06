# Copyright (c) 2026 Kenneth Stott
# Canary: 9b3d7f20-1a6c-4e85-b2f4-0c8e5d1a7f36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A replication setting is stored for every source, a seeded one included (REQ-826, REQ-030,
REQ-1919).

After the seed the model store alone owns the model: routing and replication read a source from
its control-plane row (``federation.registry_view.registered_sources``), never from the
configuration file. A source a configuration seeded is the store's like any other, so the admin
setters store its replication setting, and the setting governs."""

# Requirements: REQ-826, REQ-030, REQ-1919

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


async def test_the_source_setter_sets_a_source_a_configuration_seeded(admin, monkeypatch):
    result = await Mutation().update_source_replicate(
        _info(monkeypatch), source_id="from-config", replicate=0
    )
    assert result.success is True and result.code == "schema.source_replicate_set"
    assert _updates(admin.pool) == ["sources"]
    admin.rebuild.assert_awaited_once()


async def test_the_source_setter_still_sets_a_source_the_control_plane_owns(admin, monkeypatch):
    result = await Mutation().update_source_replicate(
        _info(monkeypatch), source_id="from-api", replicate=0
    )
    assert result.success is True and result.code == "schema.source_replicate_set"
    assert _updates(admin.pool) == ["sources"]
    admin.rebuild.assert_awaited_once()


async def test_the_table_setter_sets_a_table_of_a_seeded_source(admin, monkeypatch):
    result = await Mutation().update_table_replicate(_info(monkeypatch), table_id=7, replicate=0)
    assert result.success is True and result.code == "schema.table_replicate_set"
    assert _updates(admin.pool) == ["registered_tables"]
    admin.rebuild.assert_awaited_once()


async def test_the_table_setter_still_sets_a_table_of_a_control_plane_source(admin, monkeypatch):
    result = await Mutation().update_table_replicate(_info(monkeypatch), table_id=8, replicate=0)
    assert result.success is True and result.code == "schema.table_replicate_set"
    assert _updates(admin.pool) == ["registered_tables"]
    # a saved value must route: the floored tables are published by the schema build
    admin.rebuild.assert_awaited_once()
