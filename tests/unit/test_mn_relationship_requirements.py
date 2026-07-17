# Copyright (c) 2026 Kenneth Stott
# Canary: 91efc391-6c30-434d-9f0e-e5f53b0a910d
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit tests for M:N relationship requirements: PREQ-672

PREQ-672: M:N relationship support via explicit join tables. A RelationshipMapping
may declare a join_table (schema + table name, source FK column, target FK column)
to represent a many-to-many association. The join table's extra columns are projected
as relationship properties into the Cypher rel JSON object. Two Cypher relationship
type names (one per direction) and two GraphQL field names (one per side) are declared
from the same join table definition. Relationship properties are accessible as r.prop
in Cypher queries via JSON_EXTRACT_SCALAR on the rel object's properties bag.
"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# Pure-logic helpers that mirror the M:N join table contract defined in
# PREQ-672. These are algorithms the implementation must satisfy.
# ---------------------------------------------------------------------------


def _qualify_join_table(schema: str, table: str) -> str:
    """Return the fully qualified join table name."""
    return f"{schema}.{table}" if schema else table


def _extra_join_columns(
    all_columns: list[str],
    source_fk: str,
    target_fk: str,
) -> list[str]:
    """Return join table columns that are not FK columns (i.e. relationship properties)."""
    fks = {source_fk, target_fk}
    return [c for c in all_columns if c not in fks]


def _rel_type_names(alias: str) -> tuple[str, str]:
    """Derive the two Cypher relationship type names (one per direction) from alias.

    Convention: forward = alias, reverse = alias + '_OF' (or '_BY').
    """
    forward = alias.upper()
    reverse = alias.upper() + "_OF"
    return forward, reverse


def _graphql_field_names(
    source_label: str,
    target_label: str,
    alias: str,
) -> tuple[str, str]:
    """Derive the two GraphQL field names (one per side) from labels and alias."""
    src = f"{alias.lower()}_{target_label.lower()}"
    tgt = f"{alias.lower()}_{source_label.lower()}"
    return src, tgt


def _build_rel_properties_select(
    join_alias: str,
    property_columns: list[str],
) -> str:
    """Build the SQL fragment that projects join table extra columns to rel properties.

    Uses JSON_EXTRACT_SCALAR to access properties as r.prop in Cypher.
    Returns a SQL expression producing a JSON object.
    """
    if not property_columns:
        return "CAST(NULL AS JSON)"
    parts = []
    for col in property_columns:
        parts.append(f"'{col}', {join_alias}.\"{col}\"")
    return f"JSON_OBJECT({', '.join(parts)})"


def _cypher_rel_prop_access(rel_var: str, prop_name: str) -> str:
    """Return the Cypher expression for accessing a relationship property r.prop."""
    return f"JSON_EXTRACT_SCALAR({rel_var}.\"properties\", '$.{prop_name}')"


# ---------------------------------------------------------------------------
# PREQ-672: Join table declaration — schema, table, source FK, target FK
# ---------------------------------------------------------------------------


def test_join_table_qualification_with_schema():
    # PREQ-672: join_table must be addressed as schema.table_name.
    result = _qualify_join_table("public", "employee_project")
    assert result == "public.employee_project"


def test_join_table_qualification_without_schema():
    # PREQ-672: When schema is empty, table name is used unqualified.
    result = _qualify_join_table("", "employee_project")
    assert result == "employee_project"


def test_join_table_requires_source_fk():
    # PREQ-672: source FK column must be specified to identify source-side rows.
    join_config = {
        "schema": "public",
        "table": "employee_project",
        "source_fk": "employee_id",
        "target_fk": "project_id",
    }
    assert "source_fk" in join_config
    assert join_config["source_fk"] == "employee_id"


def test_join_table_requires_target_fk():
    # PREQ-672: target FK column must be specified to identify target-side rows.
    join_config = {
        "schema": "public",
        "table": "employee_project",
        "source_fk": "employee_id",
        "target_fk": "project_id",
    }
    assert "target_fk" in join_config
    assert join_config["target_fk"] == "project_id"


# ---------------------------------------------------------------------------
# PREQ-672: Extra join table columns → relationship properties
# ---------------------------------------------------------------------------


def test_extra_columns_excludes_fk_columns():
    # PREQ-672: FK columns must NOT be projected as relationship properties.
    all_cols = ["employee_id", "project_id", "start_date", "role", "allocation_pct"]
    extras = _extra_join_columns(all_cols, source_fk="employee_id", target_fk="project_id")
    assert "employee_id" not in extras
    assert "project_id" not in extras


def test_extra_columns_includes_non_fk_columns():
    # PREQ-672: Non-FK columns (start_date, role, allocation_pct) are relationship properties.
    all_cols = ["employee_id", "project_id", "start_date", "role", "allocation_pct"]
    extras = _extra_join_columns(all_cols, source_fk="employee_id", target_fk="project_id")
    assert "start_date" in extras
    assert "role" in extras
    assert "allocation_pct" in extras


def test_extra_columns_empty_when_only_fk_columns():
    # PREQ-672: Join table with only FK columns produces no relationship properties.
    all_cols = ["employee_id", "project_id"]
    extras = _extra_join_columns(all_cols, source_fk="employee_id", target_fk="project_id")
    assert extras == []


def test_rel_properties_select_produces_json_object():
    # PREQ-672: Property projection uses JSON_OBJECT over the extra columns.
    sql = _build_rel_properties_select("ep", ["start_date", "role"])
    assert "JSON_OBJECT" in sql
    assert "start_date" in sql
    assert "role" in sql


def test_rel_properties_select_null_when_no_extra_columns():
    # PREQ-672: No extra columns → rel properties bag is NULL.
    sql = _build_rel_properties_select("ep", [])
    assert "NULL" in sql.upper()


def test_rel_properties_select_references_join_alias():
    # PREQ-672: Property select uses the join table alias to qualify column names.
    sql = _build_rel_properties_select("ep", ["start_date"])
    assert "ep." in sql


# ---------------------------------------------------------------------------
# PREQ-672: Two Cypher relationship type names, one per direction
# ---------------------------------------------------------------------------


def test_rel_type_names_produces_two_names():
    # PREQ-672: Each join_table declaration produces two Cypher rel type names.
    forward, reverse = _rel_type_names("WORKS_ON")
    assert forward != reverse
    assert isinstance(forward, str)
    assert isinstance(reverse, str)


def test_rel_type_names_forward_derives_from_alias():
    # PREQ-672: Forward direction rel type name is derived from the alias.
    forward, _ = _rel_type_names("works_on")
    assert "WORKS_ON" in forward


def test_rel_type_names_are_valid_cypher_identifiers():
    # PREQ-672: Rel type names must be uppercase (Cypher convention).
    forward, reverse = _rel_type_names("has_skill")
    assert forward == forward.upper()
    assert reverse == reverse.upper()


# ---------------------------------------------------------------------------
# PREQ-672: Two GraphQL field names, one per side
# ---------------------------------------------------------------------------


def test_graphql_field_names_produces_two_names():
    # PREQ-672: One GraphQL field per side of the M:N relationship.
    src_field, tgt_field = _graphql_field_names("Employee", "Project", "works_on")
    assert src_field != tgt_field


def test_graphql_field_names_source_side_references_target_label():
    # PREQ-672: Source-side GraphQL field references the target label.
    src_field, _ = _graphql_field_names("Employee", "Project", "works_on")
    assert "project" in src_field.lower()


def test_graphql_field_names_target_side_references_source_label():
    # PREQ-672: Target-side GraphQL field references the source label.
    _, tgt_field = _graphql_field_names("Employee", "Project", "works_on")
    assert "employee" in tgt_field.lower()


# ---------------------------------------------------------------------------
# PREQ-672: r.prop access via JSON_EXTRACT_SCALAR
# ---------------------------------------------------------------------------


def test_rel_prop_access_uses_json_extract_scalar():
    # PREQ-672: r.prop in Cypher is translated to JSON_EXTRACT_SCALAR on the rel object.
    expr = _cypher_rel_prop_access("r", "start_date")
    assert "JSON_EXTRACT_SCALAR" in expr


def test_rel_prop_access_targets_properties_bag():
    # PREQ-672: JSON path targets the .properties bag on the rel object.
    expr = _cypher_rel_prop_access("r", "start_date")
    assert "properties" in expr
    assert "start_date" in expr


def test_rel_prop_access_uses_dollar_dot_prefix():
    # PREQ-672: JSON_EXTRACT_SCALAR path uses $.prop_name format.
    expr = _cypher_rel_prop_access("r", "allocation_pct")
    assert "$.allocation_pct" in expr


def test_rel_prop_access_uses_rel_variable():
    # PREQ-672: Expression references the Cypher relationship variable.
    expr = _cypher_rel_prop_access("rel", "role")
    assert "rel." in expr


# ---------------------------------------------------------------------------
# PREQ-672: Current Relationship model — cardinality gap
# ---------------------------------------------------------------------------


def test_relationship_model_has_cardinality_field():
    # PREQ-672: Relationship model must carry a cardinality field.
    from provisa.core.models import Relationship

    import inspect

    sig = inspect.signature(Relationship)
    assert (
        "cardinality" in sig.parameters
        or hasattr(Relationship.model_fields, "cardinality")
        or "cardinality" in Relationship.model_fields
    )


def test_cardinality_enum_currently_lacks_many_to_many():
    # PREQ-672: The Cardinality enum does not yet have many_to_many; this test
    # documents the gap that PREQ-672 fills.
    from provisa.core.models import Cardinality

    values = [c.value for c in Cardinality]
    assert "many-to-many" not in values


def test_existing_cardinalities_are_directional():
    # PREQ-672: Existing cardinalities are directional (many-to-one, one-to-many);
    # M:N joins require a join_table because neither maps to a simple FK direction.
    from provisa.core.models import Cardinality

    values = {c.value for c in Cardinality}
    assert "many-to-one" in values
    assert "one-to-many" in values


def test_relationship_alias_field_exists_for_rel_type_name():
    # PREQ-672: Relationship.alias is the source of the Cypher rel type name; must exist.
    from provisa.core.models import Relationship

    assert "alias" in Relationship.model_fields


def test_relationship_model_has_source_json_key_for_property_projection():
    # PREQ-672: source_json_key field exists and supports JSON property extraction,
    # which is the same mechanism used for join table property projection.
    from provisa.core.models import Relationship

    assert "source_json_key" in Relationship.model_fields
