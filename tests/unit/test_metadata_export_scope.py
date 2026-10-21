# Copyright (c) 2026 Kenneth Stott
# Canary: 0c8f4e26-91a7-4b3d-8e15-7d2a6f9b3c40
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2122: a catalog is published its domains' tables and data products, and the glossary.

Every other fact goes with the assets it is about, so nothing a catalog receives points at an
asset it was not given.
"""

from __future__ import annotations

from provisa.api.metadata_export.model import (
    AssetKind,
    AssetRef,
    ColumnAsset,
    DataProductAsset,
    DomainAsset,
    GlossaryTermAsset,
    GlossaryTermEdge,
    LineageEdge,
    MetadataSnapshot,
    ModelTag,
    RelationshipEdge,
    SourceAsset,
    TableAsset,
)
from provisa.api.metadata_export.scope import for_domains


def _table(source: str, name: str, domain: str) -> TableAsset:
    ref = AssetRef(kind=AssetKind.TABLE, parts=(source, name))
    column = ColumnAsset(
        ref=AssetRef(kind=AssetKind.COLUMN, parts=(source, name, "id")),
        name="id",
        data_type="integer",
        description="",
    )
    return TableAsset(
        ref=ref, name=name, source_id=source, domain_id=domain, description="", columns=[column]
    )


def _snapshot() -> MetadataSnapshot:
    orders = _table("pg", "orders", "sales")
    ledger = _table("wh", "ledger", "finance")
    rel = RelationshipEdge(
        id="orders-ledger",
        source=orders.ref,
        target=ledger.ref,
        source_column="id",
        target_column="id",
        cardinality="many-to-one",
        alias=None,
        owner=None,
        version=1,
        needs_review=False,
    )
    return MetadataSnapshot(
        org_id="acme",
        sources=[
            SourceAsset(
                ref=AssetRef(kind=AssetKind.SOURCE, parts=("pg",)),
                id="pg",
                source_type="postgresql",
                description="",
            ),
            SourceAsset(
                ref=AssetRef(kind=AssetKind.SOURCE, parts=("wh",)),
                id="wh",
                source_type="snowflake",
                description="",
            ),
        ],
        domains=[
            DomainAsset(id="sales", description="", steward=None, pending=True),
            DomainAsset(id="finance", description="", steward=None, pending=True),
        ],
        tables=[orders, ledger],
        data_products=[
            DataProductAsset(
                ref=AssetRef(kind=AssetKind.DATA_PRODUCT, parts=("sales_360",)),
                id="sales_360",
                name="Sales 360",
                domain_id="sales",
                owner=None,
                description="",
                members=(orders.ref,),
            ),
            DataProductAsset(
                ref=AssetRef(kind=AssetKind.DATA_PRODUCT, parts=("books",)),
                id="books",
                name="Books",
                domain_id="finance",
                owner=None,
                description="",
                members=(ledger.ref,),
            ),
        ],
        relationships=[rel],
        lineage=[
            LineageEdge(upstream=orders.columns[0].ref, downstream=ledger.columns[0].ref),
            LineageEdge(upstream=orders.ref, downstream=orders.ref),
        ],
        model_tags=[
            ModelTag(tag_id="pii", is_system=False, asset=orders.ref),
            ModelTag(tag_id="gl", is_system=False, asset=ledger.ref),
            ModelTag(tag_id="joined", is_system=False, relationship_id="orders-ledger"),
        ],
        glossary_terms=[
            GlossaryTermAsset(
                term_id=1,
                name="order id",
                definition=None,
                is_abstract=False,
                deprecated=False,
                refs=(orders.columns[0].ref, ledger.columns[0].ref),
            ),
            GlossaryTermAsset(
                term_id=2, name="revenue", definition="x", is_abstract=True, deprecated=False
            ),
        ],
        glossary_edges=[GlossaryTermEdge(from_term_id=2, to_term_id=1, rel_type="RELATED_TO")],
    )


def test_a_catalog_gets_its_domains_tables_and_products_alone():
    sales = for_domains(_snapshot(), ["sales"])

    assert [t.name for t in sales.tables] == ["orders"]
    assert [p.id for p in sales.data_products] == ["sales_360"]
    assert [d.id for d in sales.domains] == ["sales"]
    assert [s.id for s in sales.sources] == ["pg"]


def test_every_other_fact_goes_only_with_the_assets_it_is_about():
    sales = for_domains(_snapshot(), ["sales"])

    # The relationship reaches a finance table, so a sales-only catalog is not given it, nor the
    # tag on it, nor the lineage edge that crosses into finance.
    assert sales.relationships == []
    assert [m.tag_id for m in sales.model_tags] == ["pii"]
    assert [(e.upstream.parts, e.downstream.parts) for e in sales.lineage] == [
        (("pg", "orders"), ("pg", "orders"))
    ]


def test_the_glossary_goes_to_every_catalog_linked_to_its_assets_alone():
    sales = for_domains(_snapshot(), ["sales"])

    assert [t.name for t in sales.glossary_terms] == ["order id", "revenue"]
    assert [r.parts for r in sales.glossary_terms[0].refs] == [("pg", "orders", "id")]
    assert len(sales.glossary_edges) == 1


def test_a_catalog_serving_both_domains_gets_everything():
    snapshot = _snapshot()
    both = for_domains(snapshot, ["sales", "finance"])

    assert both.tables == snapshot.tables
    assert both.relationships == snapshot.relationships
    assert both.model_tags == snapshot.model_tags
    assert both.lineage == snapshot.lineage
