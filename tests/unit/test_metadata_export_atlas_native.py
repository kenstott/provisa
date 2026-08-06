# Copyright (c) 2026 Kenneth Stott
# Canary: 3e3b7a4f-262a-4a06-88d7-fabaae24d31c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa-native Atlas types and the enrichment-preserving publish (REQ-1388, REQ-1389).

Apache Atlas gets provisa_source/provisa_table/provisa_column entities — no rdbms
masquerade, business names presented, governance in a typed Provisa-owned attribute and
NEVER in userDescription (the human field). Atlan keeps the mapped built-in types.
"""

# Requirements: REQ-1388, REQ-1389

from __future__ import annotations

from provisa.api.metadata_export.atlan import AtlanExport
from provisa.api.metadata_export.atlas import (
    PROVISA_COLUMN_TYPE,
    PROVISA_SOURCE_TYPE,
    PROVISA_TABLE_TYPE,
    entity_type_defs,
    relationship_type_defs,
    to_native_entities,
)
from provisa.api.metadata_export.builder import build_snapshot
from provisa.core.models import Column, Domain, ProvisaConfig, Source, SourceType, Table


def _config(**kwargs) -> ProvisaConfig:
    base = {
        "sources": [Source(id="petstore-api", type=SourceType.openapi, description="Pet API")],
        "domains": [Domain(id="pet-store", description="Pets", steward="steward-1")],
        "tables": [
            Table(
                source_id="petstore-api",
                domain_id="pet-store",
                schema_name="default",
                table_name="find_pets_by_status",
                alias="pets",
                data_product=True,
                columns=[
                    Column(
                        name="pet_id",
                        data_type="integer",
                        visible_to=["admin"],
                        alias="petId",
                    )
                ],
            )
        ],
        "roles": [],
    }
    base.update(kwargs)
    return ProvisaConfig(**base)


def _snapshot():
    return build_snapshot(_config(), org_id="acme", dialect="postgres")


def test_native_entities_state_what_the_model_holds():
    entities = to_native_entities(_snapshot())
    by_type = {e.type_name: e for e in entities}
    # No rdbms masquerade, no instance/db split — source, table, column.
    assert set(by_type) == {PROVISA_SOURCE_TYPE, PROVISA_TABLE_TYPE, PROVISA_COLUMN_TYPE}
    source = by_type[PROVISA_SOURCE_TYPE]
    assert source.attributes["sourceType"] == "openapi"
    assert source.attributes["name"] == "petstore-api"
    table = by_type[PROVISA_TABLE_TYPE]
    # Business name presented; physical identity stays in qualifiedName (the binding).
    assert table.attributes["name"] == "pets"
    assert "find_pets_by_status" in table.attributes["qualifiedName"]
    assert table.attributes["provisaDomain"] == "pet-store"
    assert table.attributes["owner"] == "steward-1"  # REQ-609
    assert table.relationships == {"source": {"guid": source.guid}}
    column = by_type[PROVISA_COLUMN_TYPE]
    assert column.attributes["name"] == "petId"
    assert column.attributes["dataType"] == "integer"
    assert column.relationships == {"table": {"guid": table.guid}}


def test_user_description_is_never_written_it_belongs_to_humans():
    # REQ-1389: userDescription is the field the Atlas UI edits — writing it clobbers
    # steward-authored descriptions on every publish. Governance rides its own attribute.
    for entity in to_native_entities(_snapshot()):
        assert "userDescription" not in entity.attributes


def test_governance_document_rides_the_typed_attribute():
    import json

    from provisa.core.models import Cardinality, Relationship

    config = _config(
        tables=_config().tables
        + [
            Table(
                source_id="petstore-api",
                domain_id="pet-store",
                schema_name="default",
                table_name="orders",
                data_product=True,
                columns=[Column(name="id", data_type="integer", visible_to=["admin"])],
            )
        ],
        relationships=[
            Relationship(
                id="rel-1",
                source_table_id="pets",
                target_table_id="orders",
                source_column="pet_id",
                target_column="id",
                cardinality=Cardinality.many_to_one,
            )
        ],
    )
    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")
    table = next(
        e for e in to_native_entities(snapshot) if e.attributes.get("name") == "pets"
    )
    doc = json.loads(table.attributes["provisaGovernance"])
    assert doc["approvedRelationships"][0]["id"] == "rel-1"
    assert "userDescription" not in table.attributes


def test_typedefs_cover_the_types_and_containments():
    entity_names = {d["name"] for d in entity_type_defs()}
    assert entity_names == {PROVISA_SOURCE_TYPE, PROVISA_TABLE_TYPE, PROVISA_COLUMN_TYPE}
    # Tables/columns extend DataSet so built-in Process lineage keeps working.
    supers = {d["name"]: d["superTypes"] for d in entity_type_defs()}
    assert supers[PROVISA_TABLE_TYPE] == ["DataSet"]
    assert supers[PROVISA_COLUMN_TYPE] == ["DataSet"]
    rels = {d["name"]: d for d in relationship_type_defs()}
    assert rels["provisa_source_tables"]["endDef1"]["isContainer"] is True
    assert rels["provisa_table_columns"]["endDef2"]["type"] == PROVISA_COLUMN_TYPE


def test_urn_rebind_updates_in_place_when_the_physical_address_moved():
    from provisa.api.metadata_export.atlas import rebind_to_live_identities

    entities = to_native_entities(_snapshot())
    table = next(e for e in entities if e.type_name == PROVISA_TABLE_TYPE)
    column = next(e for e in entities if e.type_name == PROVISA_COLUMN_TYPE)
    old_placeholder = table.guid
    # The catalog holds the same URN under a DIFFERENT physical address (re-platformed).
    live = {table.attributes["provisaUri"]: ("live-guid-1", "old-source.old_schema.old_name@provisa")}
    rebound = rebind_to_live_identities(entities, live)
    assert rebound == 1
    assert table.guid == "live-guid-1"  # update in place, no duplicate
    # Containment references follow the rebind.
    assert column.relationships["table"]["guid"] == "live-guid-1"
    assert old_placeholder != "live-guid-1"


def test_urn_rebind_leaves_unmoved_entities_alone():
    from provisa.api.metadata_export.atlas import rebind_to_live_identities

    entities = to_native_entities(_snapshot())
    table = next(e for e in entities if e.type_name == PROVISA_TABLE_TYPE)
    placeholder = table.guid
    live = {table.attributes["provisaUri"]: ("live-guid-1", table.attributes["qualifiedName"])}
    assert rebind_to_live_identities(entities, live) == 0
    assert table.guid == placeholder


def test_atlan_keeps_builtin_types_and_no_classification_merge():
    # REQ-1388 caveat: Atlan's layered type system gives custom types reduced UI treatment,
    # so its adapter maps to built-ins; the per-guid classification sync stays off there
    # until those routes are verified.
    assert AtlanExport.classification_merge is False
    exporter = AtlanExport.__new__(AtlanExport)
    entities = exporter._atlan_entities(_snapshot())
    assert {e.type_name for e in entities} <= {"Connection", "Database", "Table", "Column", "Process"}
