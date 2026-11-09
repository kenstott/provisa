# Copyright (c) 2026 Kenneth Stott
# Canary: 7a3f2e91-4c8b-4d0f-a1e5-6b9c3d7f2140
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""product_memberships scoping: a product_id parameter limits validation to one product's
members, so a bad reference in another product does not break the requested one.

Root Cause #1: a data product listing a table from a seeded source (graphql-demo) must
resolve — the store model must include demo-seeded tables in config.tables.

Root Cause #2: product_memberships(config) validates ALL products; a bad reference in
workforce_ops must not raise when only pet_health's memberships are needed.
"""

# Requirements: REQ-1634

from __future__ import annotations

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
