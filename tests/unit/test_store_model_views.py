# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8d2c61-9b47-4e0a-8c15-7a2e6d9b4f10
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1318, REQ-1919: the model store writes out every view as a configuration states it.

Registration stores a view declared from metrics with both its declaration (``view_metrics``) and
the SQL that declaration compiles to (``view_sql``), which every reader of a view's definition
reads. A configuration states one definition form, so the store writes the declaration alone:
applying what it writes to an empty store builds the same model.
"""

# Requirements: REQ-1318, REQ-1919

from __future__ import annotations

import contextlib
from typing import Any

import pytest
from sqlalchemy import insert

from provisa.core.models import DERIVED_SOURCE_ID
from provisa.core.schema_org import sources

pytestmark = pytest.mark.asyncio

_COLUMNS = [
    {"name": "id", "data_type": "integer", "is_primary_key": True, "visible_to": ["seller"]},
    {"name": "region", "data_type": "varchar", "visible_to": ["seller"]},
    {"name": "amount", "data_type": "numeric", "visible_to": ["seller"]},
]


def _raw() -> dict[str, Any]:
    """A table, a view declared from a metric over it, and a free-hand SQL view over it."""
    return {
        "sources": [
            {"id": "cfg", "type": "postgresql", "host": "h", "port": 5432, "database": "d"}
        ],
        "domains": [{"id": "sales"}],
        "roles": [
            {
                "id": "seller",
                "description": "Test role.",
                "capabilities": [],
                "domain_access": ["sales"],
            }
        ],
        "tables": [
            {
                "source_id": "cfg",
                "domain_id": "sales",
                "schema": "public",
                "table": "orders",
                "columns": _COLUMNS,
            },
            {
                "source_id": DERIVED_SOURCE_ID,
                "domain_id": "sales",
                "schema": "views",
                "table": "revenue_by_region",
                "columns": [
                    {"name": "region", "data_type": "varchar", "visible_to": ["seller"]},
                    {"name": "revenue", "data_type": "numeric", "visible_to": ["seller"]},
                ],
                "view_metrics": {"metrics": ["revenue"], "dimensions": ["region"]},
            },
            {
                "source_id": DERIVED_SOURCE_ID,
                "domain_id": "sales",
                "schema": "views",
                "table": "big_orders",
                "columns": _COLUMNS,
                "view_sql": "SELECT id, region, amount FROM orders WHERE amount > 100",
            },
        ],
        "metrics": [
            {"name": "revenue", "expression": "SUM(orders.amount)", "visible_to": ["seller"]}
        ],
    }


@contextlib.asynccontextmanager
async def _store(control_plane, monkeypatch):
    from provisa.core import secrets_providers, secrets_store

    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)

    @contextlib.asynccontextmanager
    async def _unbound():
        yield

    monkeypatch.setattr(secrets_store, "bound_to_request_org", _unbound)
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id=DERIVED_SOURCE_ID, type="postgresql"))
        yield conn


def _tables(model: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {t["table"]: t for t in model["tables"]}


async def test_a_view_declared_from_metrics_is_written_as_declared(control_plane, monkeypatch):
    from provisa.core.config_loader import apply_config, parse_config_dict
    from provisa.core.repositories import table as table_repo
    from provisa.core.store_config import store_model

    async with _store(control_plane, monkeypatch) as conn:
        await apply_config(parse_config_dict(_raw()), conn)
        # Registration holds both: the declaration, and the SQL it compiles to.
        row = await table_repo.find_by_table_name(conn, "revenue_by_region")
        assert row is not None
        assert row["view_metrics"] == {
            "metrics": ["revenue"],
            "dimensions": ["region"],
            "filters": [],
        }
        assert "SUM" in row["view_sql"].upper()
        tables = _tables(await store_model(conn))
    assert tables["revenue_by_region"]["view_metrics"] == {
        "metrics": ["revenue"],
        "dimensions": ["region"],
    }
    assert "view_sql" not in tables["revenue_by_region"]
    assert tables["big_orders"]["view_sql"] == (
        "SELECT id, region, amount FROM orders WHERE amount > 100"
    )
    assert "view_metrics" not in tables["big_orders"]
    assert "view_sql" not in tables["orders"] and "view_metrics" not in tables["orders"]


async def test_what_the_store_writes_builds_the_same_model(
    control_plane, control_plane_template, monkeypatch
):
    """Save, load into an empty store, save again: a metrics view, a SQL view and a table."""
    from provisa.core.config_loader import apply_config, parse_config_dict
    from provisa.core.repositories import table as table_repo
    from provisa.core.store_config import store_model, with_store_model
    from tests.pg_control_plane import clone

    async with _store(control_plane, monkeypatch) as conn:
        await apply_config(parse_config_dict(_raw()), conn)
        saved = await store_model(conn)
        held = await table_repo.find_by_table_name(conn, "revenue_by_region")
    empty = clone(control_plane.pg, control_plane_template)
    try:
        async with _store(empty, monkeypatch) as conn:
            await apply_config(parse_config_dict(with_store_model({}, saved)), conn)
            loaded = await store_model(conn)
            reheld = await table_repo.find_by_table_name(conn, "revenue_by_region")
    finally:
        empty.close()
    assert loaded == saved
    assert set(_tables(saved)) >= {"orders", "revenue_by_region", "big_orders"}
    assert held is not None and reheld is not None
    # The SQL a loaded store reads the view through is the one the original compiled.
    assert (reheld["view_sql"], reheld["view_metrics"]) == (held["view_sql"], held["view_metrics"])


async def test_an_environment_carries_a_metrics_view_whole(control_plane, monkeypatch):
    """A commit's projection and a deploy's rows hold the view as registration stored it."""
    from provisa.core.config_loader import apply_config, parse_config_dict
    from provisa.core.env_deploy import _decompose, _existing_ids, _roles_after
    from provisa.core.env_project import project
    from provisa.core.repositories import table as table_repo

    async with _store(control_plane, monkeypatch) as conn:
        await apply_config(parse_config_dict(_raw()), conn)
        held = await table_repo.find_by_table_name(conn, "revenue_by_region")
        tree = await project(conn, "org_default")
        rows = _decompose(
            tree,
            await _existing_ids(conn, "org_default"),
            await _roles_after(conn, "org_default", tree),
        )
    assert held is not None
    [row] = [r for r in rows["registered_tables"] if r["table_name"] == "revenue_by_region"]
    assert (row["view_sql"], row["view_metrics"]) == (held["view_sql"], held["view_metrics"])
