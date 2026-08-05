# Copyright (c) 2026 Kenneth Stott
# Canary: 5a71c3e8-2b94-4f10-8d6c-e3097f4b21da
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1069: the four vendor adapters map the same snapshot into four native shapes.

These assert the mapping, not the transport — the Atlas adapter's transport is exercised
against a live server in ``tests/integration/test_metadata_export_atlas_e2e.py``. The snapshot
is the shared governed fixture the e2e tests publish, so a mapping proven here is a mapping of
the same config that reaches a real catalog.
"""

# Requirements: REQ-1068, REQ-1069, REQ-1070, REQ-1071, REQ-1073

from __future__ import annotations

import json

import httpx
import pytest

from provisa.api.metadata_export import metadata_export
from provisa.api.metadata_export.atlan import CONNECTOR_NAME, TYPE_MAP, AtlanExport
from provisa.api.metadata_export.atlas import (
    CLUSTER,
    GOVERNANCE_ATTRIBUTE,
    AtlasExport,
    classification_defs,
    to_entities,
)
from provisa.api.metadata_export.collibra import (
    COLUMN_TO_TABLE_RELATION,
    GOVERNANCE_ATTRIBUTE as COLLIBRA_GOVERNANCE,
    LINEAGE_ATTRIBUTE,
    RELATIONSHIP_ATTRIBUTE,
    STEWARD_ATTRIBUTE,
    TABLE_TO_DATABASE_RELATION,
    CollibraExport,
    to_rows,
)
from provisa.api.metadata_export.datahub import DataHubExport, to_proposals
from provisa.core.models import MetadataExportConfig
from tests.integration.metadata_export_fixture import (
    MASK_PATTERN,
    RLS_FILTER,
    governed_snapshot,
)

ORDERS = "wh.public.orders"
TOTALS = "wh.public.order_totals"


@pytest.fixture
def snapshot():
    return governed_snapshot()


def _no_rule_body(document: str) -> None:
    """REQ-1071: a signal is published, never the rule that produces it.

    A mask pattern or an RLS predicate in an external catalog is an instruction for evading the
    control it describes, which is why every adapter is checked for it rather than only the ones
    whose mapping happens to carry free text.
    """
    assert MASK_PATTERN not in document
    assert RLS_FILTER not in document
    assert "current_setting" not in document
    assert "XXX-XX-" not in document


# --- Apache Atlas / Microsoft Purview -------------------------------------------------------


def test_atlas_publishes_parents_before_the_children_that_reference_them(snapshot):
    entities = to_entities(snapshot)
    kinds = [entity.kind for entity in entities]
    assert kinds[:2] == ["instance", "database"]
    # Every table's guid is emitted before any entity referencing it, which is the ordering
    # Atlas resolves a bulk request under.
    seen: set[str] = set()
    for entity in entities:
        for reference in entity.relationships.values():
            assert reference["guid"] in seen
        seen.add(entity.guid)


def test_atlas_types_and_qualified_names_are_the_rdbms_model(snapshot):
    entities = {entity.attributes["qualifiedName"]: entity for entity in to_entities(snapshot)}
    assert entities[f"wh@{CLUSTER}"].type_name == "rdbms_instance"
    assert entities[f"wh.default@{CLUSTER}"].type_name == "rdbms_db"
    assert entities[f"{ORDERS}@{CLUSTER}"].type_name == "rdbms_table"
    assert entities[f"{ORDERS}.ssn@{CLUSTER}"].type_name == "rdbms_column"
    assert entities[f"{ORDERS}.ssn@{CLUSTER}"].attributes["data_type"] == "text"


def test_atlas_governance_signals_become_classifications(snapshot):
    entities = {entity.attributes["qualifiedName"]: entity for entity in to_entities(snapshot)}
    ssn = entities[f"{ORDERS}.ssn@{CLUSTER}"].classifications
    assert [c["typeName"] for c in ssn] == ["provisa_masked"]
    assert ssn[0]["attributes"]["exemptRoles"] == ["admin"]
    assert [c["typeName"] for c in entities[f"{ORDERS}.margin@{CLUSTER}"].classifications] == [
        "provisa_visibility_restricted"
    ]
    assert [c["typeName"] for c in entities[f"{ORDERS}@{CLUSTER}"].classifications] == [
        "provisa_rls_restricted"
    ]
    assert entities[f"{ORDERS}.id@{CLUSTER}"].classifications == []


def test_atlas_registers_a_typedef_for_every_signal_the_snapshot_carries(snapshot):
    defs = classification_defs(snapshot)
    assert [d["name"] for d in defs] == [
        "provisa_masked",
        "provisa_rls_restricted",
        "provisa_visibility_restricted",
    ]
    attribute_names = {a["name"] for a in defs[0]["attributeDefs"]}
    assert attribute_names == {"ruleId", "restrictedRoles", "exemptRoles"}


def test_atlas_carries_the_domain_and_approved_relationships_atlas_has_no_type_for(snapshot):
    entities = {entity.attributes["qualifiedName"]: entity for entity in to_entities(snapshot)}
    orders = entities[f"{ORDERS}@{CLUSTER}"]
    assert orders.attributes["owner"] == "data-steward"
    document = json.loads(orders.attributes[GOVERNANCE_ATTRIBUTE])
    assert document["domain"] == {
        "id": "sales",
        "description": "Sales domain",
        "steward": "data-steward",
        "pending": False,
    }
    approved = document["approvedRelationships"]
    assert [edge["id"] for edge in approved] == ["orders-to-customers"]
    assert approved[0]["target"] == "wh.public.customers"
    assert approved[0]["owner"] == "join-steward"


def test_atlas_lineage_references_its_tables_by_the_guids_in_the_same_batch(snapshot):
    entities = to_entities(snapshot)
    guids = {e.attributes["qualifiedName"]: e.guid for e in entities}
    process = next(entity for entity in entities if entity.kind == "lineage")
    assert process.type_name == "Process"
    assert process.attributes["inputs"] == [{"guid": guids[f"{ORDERS}@{CLUSTER}"]}]
    assert process.attributes["outputs"] == [{"guid": guids[f"{TOTALS}@{CLUSTER}"]}]
    transforms = {
        edge["to"]: edge["transforms"] for edge in json.loads(process.attributes["description"])
    }
    assert transforms[f"{TOTALS}.net"] == ["orders.amount * 2"]


def test_atlas_never_hands_the_catalog_a_route_to_the_source(snapshot):
    """A connection string in the catalog is an ungoverned path around Provisa."""
    instance = next(entity for entity in to_entities(snapshot) if entity.kind == "instance")
    assert instance.attributes["platform"] == "provisa"
    assert instance.attributes["protocol"] == "provisa"
    assert "hostname" not in instance.attributes and "url" not in instance.attributes


def test_atlas_payload_carries_no_rule_body(snapshot):
    _no_rule_body(json.dumps([entity.payload() for entity in to_entities(snapshot)]))


async def test_atlas_basic_mode_sends_the_credential_as_the_http_pair_atlas_requires():
    """Stock Apache Atlas answers a bearer token with 401; ``basic`` is its own default."""
    export = metadata_export(
        MetadataExportConfig(
            enabled=True,
            provider="atlas",
            endpoint="http://atlas:21000",
            auth_mode="basic",
            username="admin",
            token="secret",
        )
    )
    async with httpx.AsyncClient() as client:
        headers = await export._headers(client)
    assert headers["Authorization"] == "Basic YWRtaW46c2VjcmV0"


# --- Atlan ----------------------------------------------------------------------------------


def test_atlan_retypes_every_entity_into_atlans_own_type_set(snapshot):
    export = metadata_export(
        MetadataExportConfig(
            enabled=True, provider="atlan", endpoint="https://tenant.atlan.com", token="t"
        )
    )
    entities = export._atlan_entities(snapshot)
    assert {entity.type_name for entity in entities} <= set(TYPE_MAP.values())
    by_kind = {entity.kind: entity for entity in entities}
    assert by_kind["instance"].type_name == "Connection"
    assert by_kind["database"].type_name == "Database"
    assert by_kind["table"].type_name == "Table"
    assert by_kind["column"].type_name == "Column"


def test_atlan_roots_every_asset_at_the_orgs_own_connection(snapshot):
    export = metadata_export(
        MetadataExportConfig(
            enabled=True, provider="atlan", endpoint="https://tenant.atlan.com", token="t"
        )
    )
    entities = export._atlan_entities(snapshot)
    expected = f"default/{CONNECTOR_NAME}/{snapshot.org_id}"
    for entity in entities:
        assert entity.attributes["connectorName"] == CONNECTOR_NAME
        if entity.kind == "instance":
            # The connection is the root; naming itself as its own parent is what Atlan rejects.
            assert "connectionQualifiedName" not in entity.attributes
        else:
            assert entity.attributes["connectionQualifiedName"] == expected


# --- DataHub --------------------------------------------------------------------------------


def _urn(fqn: str) -> str:
    """The dataset URN DataHub addresses a Provisa table by, spelled out rather than built with
    the adapter's own helper — a mapping test that reuses the mapping proves nothing."""
    return f"urn:li:dataset:(urn:li:dataPlatform:provisa,{fqn},PROD)"


def test_datahub_defines_every_tag_before_the_dataset_that_references_it(snapshot):
    proposals = to_proposals(snapshot)
    tag_names = [p.aspect["name"] for p in proposals if p.aspect_name == "tagProperties"]
    assert tag_names == [
        "provisa_masked",
        "provisa_rls_restricted",
        "provisa_visibility_restricted",
    ]
    first_dataset = next(i for i, p in enumerate(proposals) if p.entity_type == "dataset")
    last_tag = max(i for i, p in enumerate(proposals) if p.aspect_name == "tagProperties")
    assert last_tag < first_dataset


def test_datahub_column_tags_ride_in_the_schema_aspect(snapshot):
    proposals = to_proposals(snapshot)
    schema = next(
        p for p in proposals if p.aspect_name == "schemaMetadata" and p.urn == _urn(ORDERS)
    )
    fields = {f["fieldPath"]: f for f in schema.aspect["fields"]}
    assert fields["ssn"]["globalTags"]["tags"] == [{"tag": "urn:li:tag:provisa_masked"}]
    assert fields["margin"]["globalTags"]["tags"] == [
        {"tag": "urn:li:tag:provisa_visibility_restricted"}
    ]
    assert fields["id"]["globalTags"]["tags"] == []
    # The federated type is exact and DataHub's type union is closed, so the native type is what
    # a reader is given rather than an invented correspondence.
    assert fields["amount"]["nativeDataType"] == "numeric"


def test_datahub_owns_only_the_aspects_it_publishes(snapshot):
    """An aspect Provisa does not map is never proposed, so curation in DataHub survives."""
    assert {p.aspect_name for p in to_proposals(snapshot)} == {
        "tagProperties",
        "datasetProperties",
        "schemaMetadata",
        "globalTags",
        "ownership",
        "upstreamLineage",
    }


def test_datahub_publishes_column_level_lineage_alongside_the_table_edge(snapshot):
    lineage = next(p for p in to_proposals(snapshot) if p.aspect_name == "upstreamLineage")
    assert lineage.urn == _urn(TOTALS)
    assert [u["dataset"] for u in lineage.aspect["upstreams"]] == [_urn(ORDERS)]
    fine = {
        edge["downstreams"][0].rsplit(",", 1)[-1].rstrip(")"): edge
        for edge in lineage.aspect["fineGrainedLineages"]
    }
    assert fine["net"]["transformOperation"] == "orders.amount * 2"
    assert fine["net"]["upstreams"] == [f"urn:li:schemaField:({_urn(ORDERS)},amount)"]


def test_datahub_leaves_an_unstewarded_domains_tables_unowned(snapshot):
    """REQ-609: an owner DataHub shows has to be one somebody actually holds."""
    ownership = [p for p in to_proposals(snapshot) if p.aspect_name == "ownership"]
    owners = {o["owner"] for p in ownership for o in p.aspect["owners"]}
    assert owners == {"urn:li:corpuser:data-steward"}


def test_datahub_payload_carries_no_rule_body(snapshot):
    _no_rule_body(json.dumps([p.payload() for p in to_proposals(snapshot)]))


# --- Collibra -------------------------------------------------------------------------------


@pytest.fixture
def rows(snapshot):
    return to_rows(snapshot, "Provisa", "Provisa Governed Assets")


def test_collibra_addresses_every_asset_by_name_inside_its_domain(rows):
    for row in rows:
        assert row["resourceType"] == "Asset"
        assert row["identifier"]["domain"] == {
            "name": "Provisa Governed Assets",
            "community": {"name": "Provisa"},
        }
    by_name = {row["name"]: row for row in rows}
    assert by_name[ORDERS]["type"] == {"name": "Table"}
    assert by_name[f"{ORDERS}.ssn"]["type"] == {"name": "Column"}
    assert by_name["wh"]["type"] == {"name": "Database"}


def test_collibra_relates_each_asset_to_its_parent(rows):
    by_name = {row["name"]: row for row in rows}
    assert by_name[f"{ORDERS}.ssn"]["relations"][f"{COLUMN_TO_TABLE_RELATION}:TARGET"][0][
        "name"
    ] == ORDERS
    assert by_name[ORDERS]["relations"][f"{TABLE_TO_DATABASE_RELATION}:TARGET"][0]["name"] == "wh"


def test_collibra_governance_lands_in_attributes_not_in_collibras_own_taxonomy(rows):
    """Asserting into Collibra's classification would overwrite a data office's vocabulary."""
    by_name = {row["name"]: row for row in rows}
    ssn = json.loads(by_name[f"{ORDERS}.ssn"]["attributes"][COLLIBRA_GOVERNANCE][0]["value"])
    assert [entry["signal"] for entry in ssn] == ["masked"]
    assert ssn[0]["exemptRoles"] == ["admin"]
    orders = by_name[ORDERS]["attributes"]
    assert json.loads(orders[COLLIBRA_GOVERNANCE][0]["value"])[0]["signal"] == "rls_restricted"
    assert orders[STEWARD_ATTRIBUTE] == [{"value": "data-steward"}]
    approved = json.loads(orders[RELATIONSHIP_ATTRIBUTE][0]["value"])
    assert [edge["id"] for edge in approved] == ["orders-to-customers"]


def test_collibra_lineage_names_the_upstream_columns_and_the_transform(rows):
    by_name = {row["name"]: row for row in rows}
    net = json.loads(by_name[f"{TOTALS}.net"]["attributes"][LINEAGE_ATTRIBUTE][0]["value"])
    assert net == [{"from": f"{ORDERS}.amount", "transforms": ["orders.amount * 2"]}]
    assert LINEAGE_ATTRIBUTE not in by_name[f"{ORDERS}.amount"]["attributes"]


def test_collibra_payload_carries_no_rule_body(rows):
    _no_rule_body(json.dumps(rows))


# --- Registration ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("atlas", AtlasExport),
        ("atlan", AtlanExport),
        ("datahub", DataHubExport),
        ("collibra", CollibraExport),
    ],
)
def test_each_vendor_name_resolves_to_its_adapter(name, expected):
    export = metadata_export(
        MetadataExportConfig(enabled=True, provider=name, endpoint="https://catalog.example")
    )
    assert isinstance(export, expected)
    assert export.provider_name == name
