# Copyright (c) 2026 Kenneth Stott
# Canary: 45a7e969-3023-4d95-95e3-0eac91c1db1b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Semantic URIs on exported assets (REQ-1385).

Kind-in-path grammar: unmarked domain nesting, ``tables/`` at the leaf transition,
``sources/`` for the source registry, typed fragments ``#field:``/``#rel:``. Addressing is
business identity (alias over physical name); the physical triple stays in ``ref``.
"""

# Requirements: REQ-1385

from __future__ import annotations

import json

from provisa.api.metadata_export.atlas import to_entities
from provisa.api.metadata_export.builder import build_snapshot
from provisa.api.metadata_export.datahub import to_proposals
from provisa.api.metadata_export.openlineage import to_events
from provisa.core.models import (
    Cardinality,
    Column,
    Domain,
    ProvisaConfig,
    Relationship,
    Source,
    SourceType,
    Table,
)


def _config(**kwargs) -> ProvisaConfig:
    base = {
        "sources": [Source(id="wh", type=SourceType.postgresql, description="warehouse")],
        "domains": [Domain(id="sales", description="Sales", steward="data-steward")],
        "tables": [
            Table(
                source_id="wh",
                domain_id="sales",
                schema_name="public",
                table_name="orders",
                data_product=True,
                alias="Order",
                columns=[
                    Column(name="id", data_type="integer", visible_to=["admin"]),
                    Column(
                        name="amount",
                        data_type="numeric",
                        visible_to=["admin"],
                        alias="total",
                    ),
                ],
            )
        ],
        "roles": [],
    }
    base.update(kwargs)
    return ProvisaConfig(**base)


def test_uri_grammar_addresses_business_identity():
    snapshot = build_snapshot(_config(), org_id="acme", dialect="postgres")
    # Alias wins over the physical table name; source/schema never appear in the path.
    assert snapshot.tables[0].semantic_uri == "provisa://acme/sales/tables/Order"
    by_name = {c.name: c.semantic_uri for c in snapshot.columns()}
    assert by_name["id"] == "provisa://acme/sales/tables/Order#field:id"
    # Column alias wins for the fragment's business name.
    assert by_name["amount"] == "provisa://acme/sales/tables/Order#field:total"
    assert snapshot.sources[0].semantic_uri == "provisa://acme/sources/wh"
    assert snapshot.domains[0].semantic_uri == "provisa://acme/sales"


def test_relationships_resolve_tables_by_alias_the_config_vocabulary():
    # The config vocabulary is the VIRTUAL name — alias when set (config_export id_to_name,
    # loader resolver). A relationship naming the alias must resolve, not refuse (#97).
    rel = Relationship(
        id="rel-a",
        source_table_id="Order",  # the alias, not table_name "orders"
        target_table_id="",
        source_column="customer_id",
        target_column="",
        cardinality=Cardinality.many_to_one,
        target_function_name="lookup",
    )
    snapshot = build_snapshot(_config(relationships=[rel]), org_id="acme", dialect="postgres")
    assert snapshot.relationships[0].source.fqn() == "wh.public.orders"


def test_relationship_uri_anchors_at_source_table():
    rel = Relationship(
        id="rel-1",
        source_table_id="orders",
        target_table_id="customers",
        source_column="customer_id",
        target_column="id",
        cardinality=Cardinality.many_to_one,
        alias="customer",
    )
    config = _config(
        tables=_config().tables
        + [
            Table(
                source_id="wh",
                domain_id="sales",
                schema_name="public",
                table_name="customers",
                data_product=True,
                columns=[Column(name="id", data_type="integer", visible_to=["admin"])],
            )
        ],
        relationships=[rel],
    )
    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")
    assert snapshot.relationships[0].semantic_uri == (
        "provisa://acme/sales/tables/Order#rel:customer"
    )


def test_unaliased_relationship_uri_uses_registry_id():
    rel = Relationship(
        id="rel-9",
        source_table_id="orders",
        target_table_id="",
        source_column="customer_id",
        target_column="",
        cardinality=Cardinality.many_to_one,
        target_function_name="lookup",
    )
    snapshot = build_snapshot(_config(relationships=[rel]), org_id="acme", dialect="postgres")
    assert snapshot.relationships[0].semantic_uri.endswith("#rel:rel-9")


def test_uri_segments_are_percent_encoded():
    config = _config(
        domains=[Domain(id="sales ops", description="Sales Ops", steward="s")],
        tables=[
            Table(
                source_id="wh",
                domain_id="sales ops",
                schema_name="public",
                table_name="order lines",
                data_product=True,
                columns=[Column(name="id", data_type="integer", visible_to=["admin"])],
            )
        ],
    )
    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")
    assert snapshot.tables[0].semantic_uri == "provisa://acme/sales%20ops/tables/order%20lines"


def test_atlas_carries_uri_as_entity_attribute():
    snapshot = build_snapshot(_config(), org_id="acme", dialect="postgres")
    entities = to_entities(snapshot)
    instance = next(e for e in entities if e.kind == "instance")
    assert instance.attributes["provisaUri"] == "provisa://acme/sources/wh"
    table = next(e for e in entities if e.kind == "table")
    assert table.attributes["provisaUri"] == "provisa://acme/sales/tables/Order"
    column = next(e for e in entities if e.kind == "column")
    assert column.attributes["provisaUri"].startswith("provisa://acme/sales/tables/Order#field:")


def test_datahub_carries_uri_as_external_url_and_custom_property():
    snapshot = build_snapshot(_config(), org_id="acme", dialect="postgres")
    props = next(p for p in to_proposals(snapshot) if p.aspect_name == "datasetProperties")
    assert props.aspect["externalUrl"] == "provisa://acme/sales/tables/Order"
    assert props.aspect["customProperties"]["provisaUri"] == "provisa://acme/sales/tables/Order"


def test_openlineage_carries_uri_facet():
    from datetime import datetime, timezone

    snapshot = build_snapshot(_config(), org_id="acme", dialect="postgres")
    events = to_events(snapshot, event_time=datetime(2026, 8, 5, tzinfo=timezone.utc))
    payload = json.dumps([e.payload for e in events], default=str)
    assert "provisa_uri" in payload and "provisa://acme/sales/tables/Order" in payload
