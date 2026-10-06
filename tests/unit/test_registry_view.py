# Copyright (c) 2026 Kenneth Stott
# Canary: b9e09e13-6709-4dbd-9b97-f500bd53b760
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1674, REQ-1919: landing paths read the registry — the control plane's rows — never the
config file. After the seed the model store alone owns the model: a source or table the file
declares is what its row says, and one the file declares that the store does not hold does not
exist."""

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
            config=SimpleNamespace(sources=config_sources, tables=config_tables),
            model_db=db,
            tenant_db=db,
        ),
        rows,
        registered,
    )


@pytest.mark.asyncio
async def test_every_source_is_its_row_and_builtins_stay_out(monkeypatch):
    cfg_src = Source(
        id="cfg_pg", type=SourceType.postgresql, host="h", port=5432, password="${env:PW}"
    )
    rows = [
        {
            "id": "cfg_pg",
            "type": "postgresql",
            "host": "other",
            "port": 1,
            "bound": True,
            "password_ref": "",
        },
        {
            "id": "ui_mongo",
            "type": "mongodb",
            "host": "localhost",
            "port": 37117,
            "database": "provisa",
            "org_id": "e2e",
            "mapping": {},
            # REQ-1695: a source registered through the Sources form keeps its password as a
            # reference into the org vault, in the row's own column.
            "password_ref": "${secret:source_ui_mongo_password}",
        },
        {"id": "provisa-admin", "type": "duckdb", "password_ref": ""},
    ]
    only_in_file = Source(id="file_only", type=SourceType.postgresql, host="h", port=5432)
    state, rows, _ = _state([cfg_src, only_in_file], [], rows, [])

    async def _list_all(conn):
        return rows

    monkeypatch.setattr("provisa.core.repositories.source.list_all", _list_all)
    out = {s.id: s for s in await registry_view.registered_sources(state)}
    assert set(out) == {"cfg_pg", "ui_mongo"}  # the file-only source does not exist
    # The row wins over the file's declaration of the same id: an admin's edit governs.
    assert (out["cfg_pg"].host, out["cfg_pg"].port, out["cfg_pg"].password) == ("other", 1, "")
    assert out["ui_mongo"].type is SourceType.mongodb and out["ui_mongo"].database == "provisa"
    # REQ-1695: the control-plane row's password_ref IS the Source's password — a UI-created
    # source authenticates like a config-declared one instead of reaching its connector empty.
    assert out["ui_mongo"].password == "${secret:source_ui_mongo_password}"


@pytest.mark.asyncio
async def test_registered_tables_carry_the_settings_their_rows_hold(monkeypatch):
    # The file says otherwise; the row is what the table is (REQ-1919).
    cfg_tbl = Table(
        source_id="cfg_pg",
        domain_id="d",
        schema="public",
        table="orders",
        columns=[],
        change_signal="probe",
        cache_ttl=999,
        watermark_column="file_wm",
    )
    registered = [
        {
            "id": 1,
            "source_id": "cfg_pg",
            "schema_name": "public",
            "table_name": "orders",
            "dq_contract": None,
            "role_ttl": {},  # REQ-1907
            "pagination": None,  # REQ-318: the table sets no paging
            "replicate": None,
            "load_protected": None,
            "change_signal": "ttl",  # REQ-929: saved on the row by the seed
            "region": None,  # REQ-1921: it names no region
            "cache_ttl": 30,
            "live": None,
            "watermark_column": "updated_at",
            "probe_type": None,
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
            "id": 2,
            "source_id": "ui_mongo",
            "schema_name": "pet_store",
            "table_name": "product_reviews",
            "dq_contract": None,
            "role_ttl": {},  # REQ-1907
            "pagination": None,  # REQ-318: the table sets no paging
            "replicate": None,
            "load_protected": None,
            "change_signal": None,  # REQ-929: the table sets none
            "region": None,  # REQ-1921: it names no region
            "cache_ttl": None,
            "live": None,
            "watermark_column": None,
            "probe_type": None,
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
    assert tables["orders"].watermark_column == "updated_at"
    assert tables["orders"].columns[0].is_primary_key is True
    assert tables["product_reviews"].change_signal is None
    assert tables["product_reviews"].columns[0].data_type == "bigint"
