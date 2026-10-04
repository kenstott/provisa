# Copyright (c) 2026 Kenneth Stott
# Canary: eef1dca5-19c7-4d63-87ea-9cebdd83adbd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1922: the hot-tables and replica-status resolvers carry each row's home region, resolved from
the table's stored region (no client-side join), so the admin lists filter by region like the others."""

# Requirements: REQ-1922

from types import SimpleNamespace

import pytest

from tests.unit.gate_identity import grant


class _Conn:
    pass


class _AcquireCM:
    async def __aenter__(self):
        return _Conn()

    async def __aexit__(self, *_a):
        return False


class _TenantDb:
    def acquire(self):
        return _AcquireCM()


@pytest.mark.asyncio
async def test_table_region_maps_builds_id_and_key_maps(monkeypatch):
    from provisa.api.admin import schema_query as sq

    async def _fetch(_conn):
        return [
            {
                "id": 1,
                "source_id": "src",
                "schema_name": "public",
                "table_name": "t",
                "region": "eu",
            },
            {
                "id": 2,
                "source_id": "src2",
                "schema_name": "public",
                "table_name": "r",
                "region": None,
            },
        ]

    monkeypatch.setattr("provisa.api.admin.db_queries.fetch_tables", _fetch)
    by_id, by_key = await sq._table_region_maps(SimpleNamespace(tenant_db=_TenantDb()))
    assert by_id == {1: "eu", 2: None}
    assert by_key == {("src", "public", "t"): "eu", ("src2", "public", "r"): None}


@pytest.mark.asyncio
async def test_table_region_maps_empty_without_a_control_plane():
    from provisa.api.admin import schema_query as sq

    by_id, by_key = await sq._table_region_maps(SimpleNamespace(tenant_db=None))
    assert by_id == {} and by_key == {}


@pytest.mark.asyncio
async def test_hot_tables_stamps_each_row_with_its_tables_region(monkeypatch):
    from provisa.api import app as app_module
    from provisa.api.admin import schema_query as sq

    # A hot-tier entry carries table_id; a busy-replica entry carries (source_id, schema, table) as
    # catalog/schema/table_name. Each row's region comes from the matching map.
    class _Hot:
        def snapshot(self):
            return [
                {
                    "table_id": 1,
                    "table_name": "t",
                    "catalog": "src",
                    "schema": "public",
                    "row_count": 5,
                    "loaded": True,
                }
            ]

    async def _busy(_state):
        return [
            {
                "table_name": "r",
                "catalog": "src2",
                "schema": "public",
                "row_count": 3,
                "serving": True,
            }
        ]

    async def _maps(_state):
        return ({1: "eu"}, {("src2", "public", "r"): "us"})

    monkeypatch.setattr(app_module.state, "hot_manager", _Hot(), raising=False)
    monkeypatch.setattr("provisa.federation.replica_hot.busy_replicas", _busy)
    monkeypatch.setattr(sq, "_table_region_maps", _maps)
    info, _ = grant(monkeypatch, "observability")

    rows = await sq.Query().hot_tables(info)
    by_name = {r.table_name: r.region for r in rows}
    assert by_name == {"t": "eu", "r": "us"}
