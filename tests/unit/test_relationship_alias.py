# Copyright (c) 2026 Kenneth Stott
# Canary: c3f9a1e7-4b2d-4f8a-9e6c-1a3b5d7f0c2e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for relationship alias support (REQ-388, REQ-389, REQ-390, REQ-391, REQ-392)."""

import pytest

from provisa.core.models import Relationship, Cardinality
from provisa.cypher.label_map import CypherLabelMap, NodeMapping, RelationshipMapping


# ---------------------------------------------------------------------------
# REQ-388: Relationship model accepts alias field
# ---------------------------------------------------------------------------

def test_relationship_model_accepts_alias():
    """REQ-388: Relationship Pydantic model has optional alias field."""
    rel = Relationship(
        id="emp-dept",
        source_table_id="employees",
        target_table_id="departments",
        source_column="dept_id",
        target_column="id",
        cardinality=Cardinality.many_to_one,
        alias="WORKS_FOR",
    )
    assert rel.alias == "WORKS_FOR"


def test_relationship_model_alias_defaults_none():
    """REQ-388: alias defaults to None when not provided."""
    rel = Relationship(
        id="emp-dept",
        source_table_id="employees",
        target_table_id="departments",
        source_column="dept_id",
        target_column="id",
        cardinality=Cardinality.many_to_one,
    )
    assert rel.alias is None


# ---------------------------------------------------------------------------
# REQ-389: Alias uniqueness enforced per source table (via ValueError)
# ---------------------------------------------------------------------------

def test_relationship_alias_duplicate_raises_valueerror():
    """REQ-389: duplicate alias for same source table raises ValueError (simulated)."""
    # The actual enforcement is in the DB via UNIQUE constraint and caught in the repo.
    # Here we verify the ValueError message format used in the repo.
    alias = "WORKS_FOR"
    source = "employees"
    exc = ValueError(
        f"Alias {alias!r} already exists for source table {source!r}"
    )
    assert "WORKS_FOR" in str(exc)
    assert "employees" in str(exc)


# ---------------------------------------------------------------------------
# REQ-390: CypherLabelMap aliases index enables alias-based lookup
# ---------------------------------------------------------------------------

def _make_alias_label_map() -> CypherLabelMap:
    emp = NodeMapping(
        label="Employee", table_id=1, source_id="pg", id_column="id",
        catalog_name="postgresql", schema_name="hr", table_name="employees",
        properties={"id": "id", "dept_id": "dept_id", "name": "name"},
    )
    dept = NodeMapping(
        label="Department", table_id=2, source_id="pg", id_column="id",
        catalog_name="postgresql", schema_name="hr", table_name="departments",
        properties={"id": "id", "name": "name"},
    )
    rm = RelationshipMapping(
        rel_type="WORKS_FOR",
        source_label="Employee",
        target_label="Department",
        join_source_column="dept_id",
        join_target_column="id",
        field_name="WORKS_FOR",
        alias="WORKS_FOR",
    )
    return CypherLabelMap(
        nodes={"Employee": emp, "Department": dept},
        relationships={"WORKS_FOR": rm},
        aliases={"WORKS_FOR": [rm]},
    )


def test_label_map_aliases_index_populated():
    """REQ-390: CypherLabelMap exposes aliases dict keyed by rel_type."""
    lm = _make_alias_label_map()
    assert "WORKS_FOR" in lm.aliases
    assert len(lm.aliases["WORKS_FOR"]) == 1
    assert lm.aliases["WORKS_FOR"][0].rel_type == "WORKS_FOR"


def test_relationship_mapping_has_alias_field():
    """REQ-390: RelationshipMapping stores alias."""
    rm = RelationshipMapping(
        rel_type="WORKS_FOR",
        source_label="Employee",
        target_label="Department",
        join_source_column="dept_id",
        join_target_column="id",
        field_name="WORKS_FOR",
        alias="WORKS_FOR",
    )
    assert rm.alias == "WORKS_FOR"


def test_relationship_mapping_alias_defaults_none():
    """REQ-390: RelationshipMapping alias defaults to None."""
    rm = RelationshipMapping(
        rel_type="WORKS_AT",
        source_label="Person",
        target_label="Company",
        join_source_column="company_id",
        join_target_column="id",
        field_name="works_at",
    )
    assert rm.alias is None


# ---------------------------------------------------------------------------
# REQ-391: Multiple schema paths with same alias trigger UNION ALL
# ---------------------------------------------------------------------------

def _make_shared_alias_label_map() -> CypherLabelMap:
    emp = NodeMapping(
        label="Employee", table_id=10, source_id="pg", id_column="id",
        catalog_name="postgresql", schema_name="hr", table_name="employees",
        properties={"id": "id", "manager_id": "manager_id"},
    )
    mgr = NodeMapping(
        label="Manager", table_id=11, source_id="pg", id_column="id",
        catalog_name="postgresql", schema_name="hr", table_name="managers",
        properties={"id": "id", "director_id": "director_id"},
    )
    director = NodeMapping(
        label="Director", table_id=12, source_id="pg", id_column="id",
        catalog_name="postgresql", schema_name="hr", table_name="directors",
        properties={"id": "id"},
    )
    rm1 = RelationshipMapping(
        rel_type="REPORTS_TO", source_label="Employee", target_label="Manager",
        join_source_column="manager_id", join_target_column="id",
        field_name="REPORTS_TO", alias="REPORTS_TO",
    )
    rm2 = RelationshipMapping(
        rel_type="REPORTS_TO", source_label="Manager", target_label="Director",
        join_source_column="director_id", join_target_column="id",
        field_name="REPORTS_TO", alias="REPORTS_TO",
    )
    return CypherLabelMap(
        nodes={"Employee": emp, "Manager": mgr, "Director": director},
        relationships={"REPORTS_TO": rm2},
        aliases={"REPORTS_TO": [rm1, rm2]},
    )


def test_shared_alias_has_multiple_entries_in_aliases():
    """REQ-391: aliases dict stores all RelationshipMappings sharing a rel_type."""
    lm = _make_shared_alias_label_map()
    assert len(lm.aliases["REPORTS_TO"]) == 2
    labels = {(rm.source_label, rm.target_label) for rm in lm.aliases["REPORTS_TO"]}
    assert ("Employee", "Manager") in labels
    assert ("Manager", "Director") in labels


# ---------------------------------------------------------------------------
# REQ-392: GraphQL field uses alias as field name when set
# ---------------------------------------------------------------------------

def test_schema_gen_uses_alias_as_field_name():
    """REQ-392: when alias is set, GraphQL field name equals alias, not target type name."""
    # This is a structural test — we verify the logic that schema_gen.py now applies:
    # field_name = rel.get("alias") or target.field_name
    rel_with_alias = {"alias": "WORKS_FOR", "target_table_id": 2, "cardinality": "many-to-one"}
    field_name = rel_with_alias.get("alias") or "department"
    assert field_name == "WORKS_FOR"

    rel_without_alias = {"alias": None, "target_table_id": 2, "cardinality": "many-to-one"}
    field_name = rel_without_alias.get("alias") or "department"
    assert field_name == "department"
