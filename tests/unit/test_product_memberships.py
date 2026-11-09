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
demo's assignments) exist in the store but are not part of the export model (config.tables).
Rather than raising, product_memberships silently skips any member name it cannot find in
config.tables — validation of member names against the store is done by set_product_members at
load time (config_loader), not by this runtime export helper.

Root Cause #2: product_memberships(config) iterates ALL products; product_id=... limits output
to that one product, so an unresolvable reference in another product does not interfere.
"""

# Requirements: REQ-1634

from __future__ import annotations

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


def test_product_memberships_skips_member_absent_from_config_tables():
    """A member name no config table answers is silently skipped: it names a seeded table that
    exists in the store but is not in the export model. Store-level validation (set_product_members)
    catches genuinely bad references at load time, so the runtime helper returns a partial map."""
    cfg = _config(
        tables=[],
        products=[
            DataProduct(
                id="workforce_ops", domain_id="sales", name="Workforce Ops", tables=["assignments"]
            )
        ],
    )
    result = product_memberships(cfg)
    assert result == {}  # "assignments" not in config.tables → skipped, no raise


# ── product_id scoping ────────────────────────────────────────────────────────


def test_product_memberships_scoped_to_product_id_ignores_other_products():
    """product_id=... limits output to that one product's members.
    An unresolvable member in workforce_ops is silently skipped regardless of scope; scoping to
    pet_health further avoids even processing workforce_ops's entries."""
    pets = _table(source_id="wh", schema="public", name="pets", domain_id="sales")
    # workforce_ops lists "assignments" which is NOT in config.tables (seeded member)
    cfg = _config(
        tables=[pets],
        products=[
            DataProduct(id="pet_health", domain_id="sales", name="Pet Health", tables=["pets"]),
            DataProduct(
                id="workforce_ops", domain_id="sales", name="Workforce Ops", tables=["assignments"]
            ),  # seeded member: not in config.tables, silently skipped
        ],
    )

    # Unscoped: "assignments" skipped, only pet_health's resolvable member appears
    result_all = product_memberships(cfg)
    assert ("wh", "public", "pets") in result_all
    assert result_all[("wh", "public", "pets")] == ("pet_health",)
    assert all("assignments" not in str(k) for k in result_all)

    # Scoped to pet_health: same result, only pet_health's members processed
    result = product_memberships(cfg, product_id="pet_health")
    assert ("wh", "public", "pets") in result
    assert result[("wh", "public", "pets")] == ("pet_health",)
    assert all("assignments" not in str(k) for k in result)


def test_product_memberships_scoped_skips_own_unresolvable_member():
    """A member of the requested product that is not in config.tables is silently skipped.
    This covers seeded members (graphql-demo tables) that live in the store but not the export
    model; the returned mapping is partial (empty when every member is seeded)."""
    cfg = _config(
        tables=[],
        products=[
            DataProduct(
                id="workforce_ops", domain_id="sales", name="Workforce Ops", tables=["assignments"]
            ),
        ],
    )
    result = product_memberships(cfg, product_id="workforce_ops")
    assert result == {}  # "assignments" not in config.tables → skipped, no raise


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
