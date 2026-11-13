# Copyright (c) 2026 Kenneth Stott
# Canary: 52162881-5362-4245-9988-8529adb46fc2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1921, A DRAFT IS SERVED TO ORG_ADMIN (decided 2026-11-12): a draft is in no export. A catalog
publish -- the tables, the data products and the contracts derived from them -- leaves out every
draft, a member of a product included, until it is released. The configuration export is the
model itself, and keeps a draft as one (``draft: true``), so applying it back keeps it a draft.
The model report, a steward's review of everything registered, lists it."""

# Requirements: REQ-1921

from __future__ import annotations

from provisa.api.metadata_export.builder import build_snapshot
from provisa.core.models import (
    Column,
    DataProduct,
    Domain,
    ProvisaConfig,
    Source,
    SourceType,
    Table,
)


def _config() -> ProvisaConfig:
    def table(name: str, *, draft: bool) -> Table:
        return Table(
            source_id="wh",
            domain_id="sales",
            schema_name="public",
            table_name=name,
            draft=draft,
            columns=[Column(name="id", data_type="integer", visible_to=["admin"])],
        )

    return ProvisaConfig(
        sources=[Source(id="wh", type=SourceType.postgresql)],
        domains=[Domain(id="sales", description="Sales", steward="data-steward")],
        tables=[table("customers", draft=False), table("refunds", draft=True)],
        data_products=[
            DataProduct(id="c360", domain_id="sales", name="C360", tables=["customers", "refunds"]),
            DataProduct(id="returns", domain_id="sales", name="Returns", tables=["refunds"]),
        ],
        roles=[],
    )


def test_a_catalog_publish_leaves_out_a_draft_and_the_product_it_alone_makes():
    snapshot = build_snapshot(_config(), org_id="acme", dialect="postgres", contexts={})
    assert [t.name for t in snapshot.tables] == ["customers"]
    members = {p.id: [m.fqn() for m in p.members] for p in snapshot.data_products}
    # A product whose only member is a draft has nothing published behind it.
    assert members == {"c360": ["wh.public.customers"]}


def test_the_model_report_lists_a_draft_for_its_steward():
    snapshot = build_snapshot(
        _config(), org_id="acme", dialect="postgres", contexts={}, data_products_only=False
    )
    assert sorted(t.name for t in snapshot.tables) == ["customers", "refunds"]


def test_the_configuration_export_keeps_a_draft_as_one():
    """The export writes the store's model as it holds it: the stored row's draft flag with it."""
    from provisa.core.store_config import MODEL_SECTIONS, with_store_model

    stored = [
        {"source_id": "wh", "domain_id": "sales", "table_name": "customers", "draft": False},
        {"source_id": "wh", "domain_id": "sales", "table_name": "refunds", "draft": True},
    ]
    model = {**{section: [] for section in MODEL_SECTIONS}, "naming_rules": [], "tables": stored}
    written = with_store_model({}, model)
    assert {t["table_name"]: t["draft"] for t in written["tables"]} == {
        "customers": False,
        "refunds": True,
    }
