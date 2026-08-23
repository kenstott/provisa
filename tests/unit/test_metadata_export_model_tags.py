# Copyright (c) 2026 Kenneth Stott
# Canary: b36098e6-5ace-4abb-80c5-1a5364714963
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Registry tags in the published snapshot (REQ-1375, REQ-1377, REQ-1378).

The 'technical' system tag classifies tables and columns out of the Data Product; every
other tag on a published asset ships with it; tags on withheld assets are withheld too.
"""

# Requirements: REQ-1375, REQ-1377, REQ-1378

from __future__ import annotations

from provisa.api.metadata_export.atlan import AtlanExport
from provisa.api.metadata_export.atlas import classification_defs, to_entities
from provisa.api.metadata_export.builder import build_snapshot
from provisa.api.metadata_export.datahub import to_proposals
from provisa.core.models import (
    Cardinality,
    Column,
    Domain,
    ProvisaConfig,
    Relationship,
    Source,
    SourceType,
    Table,
    Tag,
    TagAssignment,
)


def _table(table_name: str, *, data_product: bool = True, columns: list[Column] | None = None):
    return Table(
        source_id="wh",
        domain_id="sales",
        schema_name="public",
        table_name=table_name,
        data_product=data_product,
        columns=columns
        if columns is not None
        else [Column(name="id", data_type="integer", visible_to=["admin"])],
    )


def _config(**kwargs) -> ProvisaConfig:
    base = {
        "sources": [Source(id="wh", type=SourceType.postgresql, description="warehouse")],
        "domains": [Domain(id="sales", description="Sales", steward="data-steward")],
        "tables": [_table("orders")],
        "roles": [],
        "tags": [
            Tag(id="technical", applies_to=["column"], is_system=True),
            Tag(
                id="deprecated",
                applies_to=["source", "table", "column", "relationship"],
                is_system=True,
            ),
            Tag(id="gold", applies_to=["table"], description="Curated"),
        ],
    }
    base.update(kwargs)
    return ProvisaConfig(**base)


def test_technical_is_column_only_a_table_assignment_never_excludes():
    # At table level the Data Product flag IS the export control — a (stale/invalid)
    # technical@table assignment must not silently exclude a table the flag publishes.
    config = _config(
        tables=[_table("orders"), _table("etl_audit")],
        tag_assignments=[
            TagAssignment(tag_id="technical", object_type="table", table_ref="wh.public.etl_audit")
        ],
    )
    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")
    assert sorted(t.name for t in snapshot.tables) == ["etl_audit", "orders"]


def test_technical_column_is_dropped_from_its_published_table():
    config = _config(
        tables=[
            _table(
                "orders",
                columns=[
                    Column(name="id", data_type="integer", visible_to=["admin"]),
                    Column(name="_loaded_at", data_type="timestamp", visible_to=["admin"]),
                ],
            )
        ],
        tag_assignments=[
            TagAssignment(
                tag_id="technical",
                object_type="column",
                table_ref="wh.public.orders",
                column_name="_loaded_at",
            )
        ],
    )
    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")
    assert [c.name for c in snapshot.columns()] == ["id"]
    assert snapshot.model_tags == []


def test_sources_publish_only_when_one_of_their_tables_does():
    # The Data Product filter gates sources too — internal stores and sources with nothing
    # published must not appear in the catalog at all, tags included.
    config = _config(
        sources=[
            Source(id="wh", type=SourceType.postgresql, description="warehouse"),
            Source(id="internal-admin", type=SourceType.postgresql, description="plumbing"),
        ],
        tables=[_table("orders"), _table("drafts", data_product=False)],
        tag_assignments=[
            TagAssignment(
                tag_id="deprecated",
                object_type="source",
                source_id="internal-admin",
                reason="internal",
            ),
        ],
    )
    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")
    assert [s.id for s in snapshot.sources] == ["wh"]
    assert snapshot.model_tags == []


def test_tags_on_published_assets_ship_and_withheld_assets_keep_theirs_back():
    config = _config(
        tables=[_table("orders"), _table("drafts", data_product=False)],
        tag_assignments=[
            TagAssignment(tag_id="gold", object_type="table", table_ref="wh.public.orders"),
            TagAssignment(tag_id="gold", object_type="table", table_ref="wh.public.drafts"),
            TagAssignment(tag_id="deprecated", object_type="source", source_id="wh"),
        ],
    )
    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")
    published = {(t.tag_id, t.asset.fqn()) for t in snapshot.model_tags if t.asset}
    assert published == {("gold", "wh.public.orders"), ("deprecated", "wh")}
    system_flags = {t.tag_id: t.is_system for t in snapshot.model_tags}
    assert system_flags == {"gold": False, "deprecated": True}


def test_relationship_tags_survive_only_with_their_edge():
    rel = Relationship(
        id="rel-1",
        source_table_id="orders",
        target_table_id="customers",
        source_column="customer_id",
        target_column="id",
        cardinality=Cardinality.many_to_one,
    )
    config = _config(
        tables=[_table("orders"), _table("customers")],
        relationships=[rel],
        tag_assignments=[
            TagAssignment(tag_id="deprecated", object_type="relationship", relationship_id="rel-1"),
            TagAssignment(tag_id="deprecated", object_type="relationship", relationship_id="ghost"),
        ],
    )
    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")
    assert [t.relationship_id for t in snapshot.model_tags] == ["rel-1"]


def test_atlas_registry_tags_become_prefixed_classifications_with_typedefs():
    config = _config(
        tag_assignments=[
            TagAssignment(tag_id="gold", object_type="table", table_ref="wh.public.orders"),
            TagAssignment(tag_id="deprecated", object_type="source", source_id="wh"),
        ],
    )
    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")
    defs = {d["name"] for d in classification_defs(snapshot)}
    assert {"provisa_gold", "provisa_deprecated"} <= defs
    entities = to_entities(snapshot)
    table_entity = next(e for e in entities if e.kind == "table")
    assert {"typeName": "provisa_gold", "attributes": {"system": False}} in (
        table_entity.classifications
    )
    instance_entity = next(e for e in entities if e.kind == "instance")
    assert {"typeName": "provisa_deprecated", "attributes": {"system": True}} in (
        instance_entity.classifications
    )


def test_atlas_relationship_tags_ride_the_governance_document():
    rel = Relationship(
        id="rel-1",
        source_table_id="orders",
        target_table_id="customers",
        source_column="customer_id",
        target_column="id",
        cardinality=Cardinality.many_to_one,
    )
    config = _config(
        tables=[_table("orders"), _table("customers")],
        relationships=[rel],
        tag_assignments=[
            TagAssignment(tag_id="deprecated", object_type="relationship", relationship_id="rel-1"),
        ],
    )
    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")
    import json

    table_entity = next(
        e for e in to_entities(snapshot) if e.kind == "table" and e.attributes["name"] == "orders"
    )
    doc = json.loads(table_entity.attributes["userDescription"])
    assert doc["approvedRelationships"][0]["tags"] == ["deprecated"]


def test_datahub_maps_deprecated_to_the_native_deprecation_aspect():
    config = _config(
        tag_assignments=[
            TagAssignment(tag_id="deprecated", object_type="table", table_ref="wh.public.orders"),
            TagAssignment(tag_id="gold", object_type="table", table_ref="wh.public.orders"),
        ],
    )
    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")
    proposals = to_proposals(snapshot)
    deprecation = [p for p in proposals if p.aspect_name == "deprecation"]
    assert len(deprecation) == 1 and deprecation[0].aspect["deprecated"] is True
    tag_props = {p.aspect["name"] for p in proposals if p.aspect_name == "tagProperties"}
    assert {"provisa_gold", "provisa_deprecated"} <= tag_props
    global_tags = next(p for p in proposals if p.aspect_name == "globalTags")
    assert {"tag": "urn:li:tag:provisa_gold"} in global_tags.aspect["tags"]


def test_reason_and_removal_date_ride_every_deprecation_construct():
    config = _config(
        tag_assignments=[
            TagAssignment(
                tag_id="deprecated",
                object_type="table",
                table_ref="wh.public.orders",
                reason="Replaced by order_facts",
                expires_on="2026-12-01",
            ),
        ],
    )
    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")
    tag = snapshot.model_tags[0]
    assert (tag.reason, tag.expires_on) == ("Replaced by order_facts", "2026-12-01")
    # Atlas: classification attributes carry both, typed.
    table_entity = next(e for e in to_entities(snapshot) if e.kind == "table")
    dep = next(c for c in table_entity.classifications if c["typeName"] == "provisa_deprecated")
    assert dep["attributes"]["reason"] == "Replaced by order_facts"
    assert dep["attributes"]["expiresOn"] == "2026-12-01"
    # DataHub: the native deprecation aspect carries the reason as its note and the
    # removal date as decommissionTime (epoch millis).
    proposals = to_proposals(snapshot)
    aspect = next(p for p in proposals if p.aspect_name == "deprecation").aspect
    assert aspect["note"] == "Replaced by order_facts"
    assert aspect["decommissionTime"] == 1_796_083_200_000  # 2026-12-01T00:00:00Z
    # Atlan: reason + removal date in the certificate status message.
    exporter = AtlanExport.__new__(AtlanExport)
    atlan_table = next(e for e in exporter._atlan_entities(snapshot) if e.type_name == "Table")
    assert atlan_table.attributes["certificateStatusMessage"] == (
        "Replaced by order_facts (removal: 2026-12-01)"
    )


def test_atlan_maps_deprecated_to_certificate_status():
    config = _config(
        tag_assignments=[
            TagAssignment(tag_id="deprecated", object_type="table", table_ref="wh.public.orders"),
        ],
    )
    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")
    exporter = AtlanExport.__new__(AtlanExport)
    entities = exporter._atlan_entities(snapshot)
    table_entity = next(e for e in entities if e.type_name == "Table")
    assert table_entity.attributes["certificateStatus"] == "DEPRECATED"
    untouched = next(e for e in entities if e.type_name == "Database")
    assert "certificateStatus" not in untouched.attributes
