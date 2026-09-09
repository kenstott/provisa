# Copyright (c) 2026 Kenneth Stott
# Canary: b9e09e13-6709-4dbd-9b97-f500bd53b760
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1674: landing paths read the registry (control plane + config overlay), not the config file."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.core.models import Source, SourceType, Table
from provisa.federation import registry_view


class _Acquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


def _state(config_sources, config_tables, rows, registered):
    conn = object()
    db = SimpleNamespace(acquire=lambda: _Acquire(conn))
    return (
        SimpleNamespace(
            config=SimpleNamespace(sources=config_sources, tables=config_tables), tenant_db=db
        ),
        rows,
        registered,
    )


@pytest.mark.asyncio
async def test_ui_created_source_joins_config_sources_and_builtins_stay_out(monkeypatch):
    cfg_src = Source(
        id="cfg_pg", type=SourceType.postgresql, host="h", port=5432, password="${env:PW}"
    )
    rows = [
        {"id": "cfg_pg", "type": "postgresql", "host": "other", "port": 1, "bound": True},
        {
            "id": "ui_mongo",
            "type": "mongodb",
            "host": "localhost",
            "port": 37117,
            "database": "provisa",
            "org_id": "e2e",
            "mapping": {},
        },
        {"id": "provisa-admin", "type": "duckdb"},
    ]
    state, rows, _ = _state([cfg_src], [], rows, [])

    async def _list_all(conn):
        return rows

    monkeypatch.setattr("provisa.core.repositories.source.list_all", _list_all)
    out = {s.id: s for s in await registry_view.registered_sources(state)}
    assert set(out) == {"cfg_pg", "ui_mongo"}
    assert (
        out["cfg_pg"].password == "${env:PW}"
    )  # the config's Source wins: it carries the secret ref
    assert out["ui_mongo"].type is SourceType.mongodb and out["ui_mongo"].database == "provisa"


@pytest.mark.asyncio
async def test_registered_tables_carry_config_settings_only_where_declared(monkeypatch):
    cfg_tbl = Table(
        source_id="cfg_pg",
        domain_id="d",
        schema="public",
        table="orders",
        columns=[],
        change_signal="ttl",
        cache_ttl=30,
    )
    registered = [
        {
            "source_id": "cfg_pg",
            "schema_name": "public",
            "table_name": "orders",
            "dq_contract": None,
            "columns": [
                {
                    "column_name": "id",
                    "data_type": "integer",
                    "is_primary_key": True,
                    "native_filter_type": None,
                }
            ],
        },
        {
            "source_id": "ui_mongo",
            "schema_name": "pet_store",
            "table_name": "product_reviews",
            "dq_contract": None,
            "columns": [
                {
                    "column_name": "rating",
                    "data_type": "bigint",
                    "is_primary_key": False,
                    "native_filter_type": None,
                }
            ],
        },
    ]
    state, _, registered = _state([], [cfg_tbl], [], registered)

    async def _fetch_tables(conn):
        return registered

    monkeypatch.setattr("provisa.api.admin.db_queries.fetch_tables", _fetch_tables)
    tables = {t.table_name: t for t in await registry_view.registered_tables(state)}
    assert tables["orders"].change_signal == "ttl" and tables["orders"].cache_ttl == 30
    assert tables["orders"].columns[0].is_primary_key is True
    assert tables["product_reviews"].change_signal is None
    assert tables["product_reviews"].columns[0].data_type == "bigint"
