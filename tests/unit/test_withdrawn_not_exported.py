# Copyright (c) 2026 Kenneth Stott
# Canary: 52162881-5362-4245-9988-8529adb46fc2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1921: a withdrawn table is in no catalog export. A catalog publish -- the tables, the data
products and the contracts derived from them -- leaves out every withdrawn table, itself or
through its source, a member of a product included, until it is put back. The configuration
export is the model itself, and carries a withdrawn table or source as withdrawn
(``withdrawn: true``), so applying it back -- a promotion -- carries a move in progress. The model
report, a steward's review of everything registered, lists it."""

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


def _config(*, source_withdrawn: bool = False) -> ProvisaConfig:
    def table(name: str, *, withdrawn: bool, source_id: str = "wh") -> Table:
        return Table(
            source_id=source_id,
            domain_id="sales",
            schema_name="public",
            table_name=name,
            withdrawn=withdrawn,
            columns=[Column(name="id", data_type="integer", visible_to=["admin"])],
        )

    return ProvisaConfig(
        sources=[
            Source(id="wh", type=SourceType.postgresql),
            Source(id="crm", type=SourceType.postgresql, withdrawn=source_withdrawn),
        ],
        domains=[Domain(id="sales", description="Sales")],
        tables=[
            table("customers", withdrawn=False),
            table("refunds", withdrawn=True),
            table("leads", withdrawn=False, source_id="crm"),
        ],
        data_products=[
            DataProduct(id="c360", domain_id="sales", name="C360", tables=["customers", "refunds"]),
            DataProduct(id="returns", domain_id="sales", name="Returns", tables=["refunds"]),
            DataProduct(id="pipeline", domain_id="sales", name="Pipeline", tables=["leads"]),
        ],
        roles=[],
    )


def _published(config: ProvisaConfig) -> tuple[list[str], dict[str, list[str]]]:
    snapshot = build_snapshot(config, org_id="acme", dialect="postgres", contexts={})
    members = {p.id: [m.fqn() for m in p.members] for p in snapshot.data_products}
    return sorted(t.name for t in snapshot.tables), members


def test_a_catalog_publish_leaves_out_a_withdrawn_table_and_the_product_it_alone_makes():
    tables, members = _published(_config())
    assert tables == ["customers", "leads"]
    # A product whose only member is withdrawn has nothing published behind it.
    assert members == {"c360": ["wh.public.customers"], "pipeline": ["crm.public.leads"]}


def test_a_catalog_publish_leaves_out_the_tables_of_a_withdrawn_source():
    tables, members = _published(_config(source_withdrawn=True))
    assert tables == ["customers"]
    assert members == {"c360": ["wh.public.customers"]}


def test_the_model_report_lists_a_withdrawn_table_for_its_steward():
    snapshot = build_snapshot(
        _config(source_withdrawn=True),
        org_id="acme",
        dialect="postgres",
        contexts={},
        data_products_only=False,
    )
    assert sorted(t.name for t in snapshot.tables) == ["customers", "leads", "refunds"]


def test_the_configuration_export_carries_a_withdrawn_table_and_source_as_withdrawn():
    """The export writes the store's model as it holds it: the stored row's withdrawn flag with
    it, and only where it is set (a default is left out)."""
    from provisa.core.store_config import _written

    assert _written(Source(id="crm", type=SourceType.postgresql, withdrawn=True))["withdrawn"]
    assert "withdrawn" not in _written(Source(id="wh", type=SourceType.postgresql))
    table = _config().tables
    assert {t.table_name: _written(t).get("withdrawn", False) for t in table} == {
        "customers": False,
        "refunds": True,
        "leads": False,
    }
