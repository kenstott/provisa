# Copyright (c) 2026 Kenneth Stott
# Canary: 7a3f2e91-4c8b-4d0f-a1e5-6b9c3d7f2140
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""product_memberships scoping and seeded-member handling (REQ-1634).

product_memberships resolves member names against config.tables. Seeded tables (e.g. graphql-
demo's assignments) exist in the store but are not part of the export model (config.tables), so
a name in config.seeded_table_names may be absent; any other unresolvable name raises by name.

Root Cause #2: product_memberships(config) validates ALL products; a bad reference in
workforce_ops must not raise when only pet_health's memberships are needed.
"""

# Requirements: REQ-1634

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from provisa.api.metadata_export.builder import product_memberships
from provisa.core.models import (
    Column,
    DataProduct,
    Domain,
    ProvisaConfig,
    Source,
    SourceType,
    Table,
)


def _table(
    *,
    source_id: str,
    schema: str,
    name: str,
    domain_id: str = "sales",
    alias: str | None = None,
) -> Table:
    return Table(
        source_id=source_id,
        schema_name=schema,
        table_name=name,
        domain_id=domain_id,
        alias=alias,
        columns=[Column(name="id", data_type="integer", visible_to=["admin"])],
    )


def _config(
    tables: list[Table],
    products: list[DataProduct],
    source_ids: list[str] | None = None,
) -> ProvisaConfig:
    ids = source_ids or sorted({t.source_id for t in tables})
    return ProvisaConfig(
        sources=[Source(id=sid, type=SourceType.postgresql, description=sid) for sid in ids],
        domains=[Domain(id="sales", description="Sales", steward="admin")],
        tables=tables,
        data_products=products,
        roles=[],
    )


# ── sanity: basic resolution works ───────────────────────────────────────────


def test_product_memberships_resolves_member_by_virtual_name():
    pets = _table(source_id="wh", schema="public", name="pets")
    cfg = _config(
        tables=[pets],
        products=[
            DataProduct(id="pet_health", domain_id="sales", name="Pet Health", tables=["pets"])
        ],
    )
    result = product_memberships(cfg)
    assert ("wh", "public", "pets") in result
    assert result[("wh", "public", "pets")] == ("pet_health",)


def test_product_memberships_resolves_member_by_fully_qualified_name():
    pets = _table(source_id="wh", schema="public", name="pets")
    cfg = _config(
        tables=[pets],
        products=[
            DataProduct(
                id="pet_health", domain_id="sales", name="Pet Health", tables=["wh.public.pets"]
            )
        ],
    )
    result = product_memberships(cfg)
    assert ("wh", "public", "pets") in result


def test_product_memberships_raises_for_unknown_member():
    cfg = _config(
        tables=[],
        products=[
            DataProduct(
                id="workforce_ops", domain_id="sales", name="Workforce Ops", tables=["assignments"]
            )
        ],
    )
    with pytest.raises(ValueError, match="data product 'workforce_ops' lists table 'assignments'"):
        product_memberships(cfg)


# ── seeded members ────────────────────────────────────────────────────────────


@pytest.mark.parametrize("member", ["assignments", "graphql-demo.graphql.assignments"])
def test_product_memberships_tolerates_a_seeded_member(member):
    """A seeded table stays out of config.tables, by either name a product gives it; it records
    no membership and the product's other members still resolve."""
    pets = _table(source_id="wh", schema="public", name="pets")
    cfg = _config(
        tables=[pets],
        products=[
            DataProduct(
                id="workforce_ops", domain_id="sales", name="Workforce Ops", tables=[member, "pets"]
            )
        ],
    )
    cfg._seeded_table_names = frozenset({"assignments", "graphql-demo.graphql.assignments"})
    assert product_memberships(cfg) == {("wh", "public", "pets"): ("workforce_ops",)}
    assert product_memberships(cfg, product_id="workforce_ops") == {
        ("wh", "public", "pets"): ("workforce_ops",)
    }


def test_product_memberships_raises_for_an_unknown_member_beside_a_seeded_one():
    """Knowing the seeded names excuses those names only."""
    cfg = _config(
        tables=[],
        products=[
            DataProduct(
                id="workforce_ops",
                domain_id="sales",
                name="Workforce Ops",
                tables=["assignments", "typo_table"],
            )
        ],
    )
    cfg._seeded_table_names = frozenset({"assignments"})
    with pytest.raises(ValueError, match="workforce_ops.*typo_table.*no table"):
        product_memberships(cfg, product_id="workforce_ops")


def _row(source_id: str, table_name: str, domain_id: str, alias: str | None = None) -> dict:
    return {
        "source_id": source_id,
        "schema_name": "s",
        "table_name": table_name,
        "domain_id": domain_id,
        "alias": alias,
    }


_STORE_ROWS = [
    _row("graphql-demo", "assignments", "shelter", alias="shifts"),
    _row("pg", "catalog_view", "meta"),
    _row("pg", "pets", "sales", alias="animals"),
]


async def test_seeded_table_names_are_the_stores_seeded_tables_by_each_name(monkeypatch):
    from provisa.core.repositories import table as table_repo
    from provisa.core.store_config import seeded_table_names

    async def list_all(conn, *, parts_of=None):
        return _STORE_ROWS

    monkeypatch.setattr(table_repo, "list_all", list_all)
    assert await seeded_table_names(MagicMock()) == {
        "shifts",
        "graphql-demo.s.assignments",
        "catalog_view",
        "pg.s.catalog_view",
    }


async def test_the_export_model_carries_the_stores_seeded_names(monkeypatch):
    """The wiring product_memberships relies on: the model a publish or report builds from knows
    the store's seeded names, so a seeded member passes and an unknown one still raises."""
    from provisa.api.admin import config_export, schema_helpers
    from provisa.api.app import state
    from provisa.api.metadata_export.publishing import _model_for_export
    from provisa.core.repositories import source as source_repo
    from provisa.core.repositories import table as table_repo

    def live_config(members: list[str]) -> dict:
        return {
            "sources": [],
            "domains": [{"id": "sales", "description": "Sales", "steward": "admin"}],
            "tables": [],
            "roles": [],
            "data_products": [
                {"id": "workforce_ops", "domain_id": "sales", "name": "W", "tables": members}
            ],
        }

    members = ["shifts"]

    async def build_live_config():
        return live_config(members)

    @asynccontextmanager
    async def acquire():
        yield object()

    async def get_pool():
        return SimpleNamespace(acquire=acquire)

    async def no_sources(conn):
        return []

    async def list_all(conn, *, parts_of=None):
        return _STORE_ROWS

    monkeypatch.setattr(config_export, "build_live_config", build_live_config)
    monkeypatch.setattr(schema_helpers, "_get_pool", get_pool)
    monkeypatch.setattr(source_repo, "list_all", no_sources)
    monkeypatch.setattr(table_repo, "list_all", list_all)
    monkeypatch.setattr(state, "config", None)

    assert product_memberships(await _model_for_export()) == {}
    members = ["shifts", "typo_table"]
    with pytest.raises(ValueError, match="workforce_ops.*typo_table.*no table"):
        product_memberships(await _model_for_export())


# ── Root Cause #2: scoping ────────────────────────────────────────────────────


def test_product_memberships_scoped_to_product_id_ignores_other_products():
    """Root Cause #2: product_id=... limits validation to that one product's members.
    A bad reference in workforce_ops must not raise when only pet_health is requested."""
    pets = _table(source_id="wh", schema="public", name="pets", domain_id="sales")
    # workforce_ops lists "assignments" which is NOT in config.tables — broken product
    cfg = _config(
        tables=[pets],
        products=[
            DataProduct(id="pet_health", domain_id="sales", name="Pet Health", tables=["pets"]),
            DataProduct(
                id="workforce_ops", domain_id="sales", name="Workforce Ops", tables=["assignments"]
            ),  # broken: "assignments" not in config.tables
        ],
    )

    # Unscoped call raises because workforce_ops has a bad reference
    with pytest.raises(ValueError, match="workforce_ops.*assignments"):
        product_memberships(cfg)

    # Scoped to pet_health: must NOT raise, and must resolve pet_health's member
    result = product_memberships(cfg, product_id="pet_health")
    assert ("wh", "public", "pets") in result
    assert result[("wh", "public", "pets")] == ("pet_health",)
    # workforce_ops's broken member is not in the result
    assert all("assignments" not in str(k) for k in result)


def test_product_memberships_scoped_still_raises_for_requested_products_own_bad_member():
    """Scoping to a product with a bad reference must still raise — no silent drops."""
    cfg = _config(
        tables=[],
        products=[
            DataProduct(
                id="workforce_ops", domain_id="sales", name="Workforce Ops", tables=["assignments"]
            ),  # broken
        ],
    )
    with pytest.raises(ValueError, match="workforce_ops.*assignments.*no table"):
        product_memberships(cfg, product_id="workforce_ops")


def test_product_memberships_scoped_to_absent_product_returns_empty():
    """Scoping to a product_id that is not in the config returns an empty mapping."""
    cfg = _config(
        tables=[_table(source_id="wh", schema="public", name="pets")],
        products=[
            DataProduct(id="pet_health", domain_id="sales", name="Pet Health", tables=["pets"])
        ],
    )
    result = product_memberships(cfg, product_id="no_such_product")
    assert result == {}
