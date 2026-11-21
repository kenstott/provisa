# Copyright (c) 2026 Kenneth Stott
# Canary: 12216321-1abd-4c4a-8d2d-9b47b49484ac
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-313/REQ-399: a detected relationship names its FK/PK columns as they are registered.

_detect_relationships infers the FK/PK columns from the GraphQL field names (breedName -> breed.name),
but _upsert_tables_to_semantic_layer registers columns under apply_sql_name (breed_name). rel_repo.upsert
marks is_foreign_key / is_primary_key by matching source_column/target_column against the registered
column name, so a relationship carrying the raw camelCase column marks no row. The mapper now emits the
registered column names, so the FK flag lands on the registered column."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy.sql.dml import Update

from provisa.core.models import Cardinality, Relationship
from provisa.core.repositories import relationship as rel_mod
from provisa.graphql_remote.mapper import map_schema


# --- minimal __schema builders (camelCase FK) ---
def _scalar(name):
    return {"kind": "SCALAR", "name": name, "ofType": None}


def _obj(name):
    return {"kind": "OBJECT", "name": name, "ofType": None}


def _list_of(name):
    return {"kind": "LIST", "name": None, "ofType": _obj(name)}


def _schema():
    breed = {
        "kind": "OBJECT",
        "name": "AnimalBreed",
        "fields": [{"name": "name", "type": _scalar("String")}],
    }
    assignment = {
        "kind": "OBJECT",
        "name": "Assignment",
        "fields": [
            {"name": "breedName", "type": _scalar("String")},
            {"name": "breed", "type": _obj("AnimalBreed")},
        ],
    }
    query = {
        "kind": "OBJECT",
        "name": "Query",
        "fields": [
            {"name": "animalBreeds", "type": _list_of("AnimalBreed"), "args": []},
            {"name": "assignmentsByEmployee", "type": _list_of("Assignment"), "args": []},
        ],
    }
    return {
        "queryType": {"name": "Query"},
        "mutationType": None,
        "types": [query, breed, assignment],
    }


def _detected_fk_relationship():
    _tables, _functions, rels = map_schema(_schema(), "", "src1")
    return next(r for r in rels if r["cardinality"] == "many-to-one")


def test_mapper_emits_the_registered_column_names_for_a_camelcase_fk():
    rel = _detected_fk_relationship()
    assert rel["source_table_id"] == "assignments_by_employee"
    assert rel["target_table_id"] == "animal_breeds"
    # breedName -> breed_name, the name _upsert_tables_to_semantic_layer registers the column under.
    assert rel["source_column"] == "breed_name"
    assert rel["target_column"] == "name"


class _Conn:
    """Records the UPDATE statements rel_repo.upsert issues; every other call is a benign stub."""

    def __init__(self):
        self.updates: list[str] = []

    async def upsert(self, *a, **k):
        return None

    async def execute_core(self, stmt):
        if isinstance(stmt, Update):
            self.updates.append(str(stmt.compile(compile_kwargs={"literal_binds": True})))
        res = MagicMock()
        res.scalar.return_value = 0  # no conflicting PK on the target
        res.fetchone.return_value = None
        res.fetchall.return_value = []
        return res


@pytest.mark.asyncio
async def test_upsert_marks_is_foreign_key_on_the_registered_column(monkeypatch):
    rel = _detected_fk_relationship()

    async def _find(conn, name):
        # The registered_tables row as find_by_table_name returns it: domain_id is NOT NULL, and
        # the change is named at the source table's model URI (domain + alias or table name).
        return {
            "id": 1 if name == "assignments_by_employee" else 2,
            "domain_id": "src1",
            "table_name": name,
            "alias": None,
        }

    monkeypatch.setattr(rel_mod.table_repo, "find_by_table_name", _find)
    monkeypatch.setattr(rel_mod.model_change, "name", lambda *a, **k: None)

    conn = _Conn()
    await rel_mod.upsert(
        conn,
        Relationship(
            id=rel["id"],
            source_table_id=rel["source_table_id"],
            target_table_id=rel["target_table_id"],
            source_column=rel["source_column"],
            target_column=rel["target_column"],
            cardinality=Cardinality(rel["cardinality"]),
        ),
    )

    fk_updates = [u for u in conn.updates if "is_foreign_key" in u]
    assert fk_updates, "no is_foreign_key UPDATE issued"
    # The FK flag must be set WHERE column_name = the registered column (breed_name), not breedName.
    assert any("breed_name" in u for u in fk_updates), fk_updates
    assert not any("breedName" in u for u in conn.updates), conn.updates


def _schema_without_a_key_scalar():
    """Schedule carries its employee as a nested object only (no employeeId), and Employee lists
    its schedules: neither field names a column a join could use."""
    employee = {
        "kind": "OBJECT",
        "name": "Employee",
        "fields": [
            {"name": "id", "type": _scalar("Int")},
            {"name": "schedules", "type": _list_of("Schedule")},
        ],
    }
    schedule = {
        "kind": "OBJECT",
        "name": "Schedule",
        "fields": [
            {"name": "id", "type": _scalar("Int")},
            {"name": "employee", "type": _obj("Employee")},
        ],
    }
    query = {
        "kind": "OBJECT",
        "name": "Query",
        "fields": [
            {"name": "employees", "type": _list_of("Employee"), "args": []},
            {"name": "schedules", "type": _list_of("Schedule"), "args": []},
        ],
    }
    return {
        "queryType": {"name": "Query"},
        "mutationType": None,
        "types": [query, employee, schedule],
    }


def test_a_field_naming_no_join_column_is_not_offered_as_a_relationship():
    """REQ-313: a relationship joins on columns. A nested object with no key scalar beside it
    (Schedule.employee) and a list held on the other side (Employee.schedules) name none, so the
    mapper offers no relationship for them rather than one with empty columns."""
    _tables, _functions, rels = map_schema(_schema_without_a_key_scalar(), "", "src1")
    assert rels == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source_column", "target_column", "missing"),
    [("", "id", "source"), ("employee_id", "", "target")],
)
async def test_a_relationship_without_its_columns_is_refused_by_name(
    monkeypatch, source_column, target_column, missing
):
    async def _find(conn, name):
        return {"id": 1, "domain_id": "src1", "table_name": name, "alias": None}

    monkeypatch.setattr(rel_mod.table_repo, "find_by_table_name", _find)
    monkeypatch.setattr(rel_mod.model_change, "name", lambda *a, **k: None)
    with pytest.raises(rel_mod.RelationshipWithoutColumns) as refused:
        await rel_mod.upsert(
            _Conn(),
            Relationship(
                id="schedules-to-employees",
                source_table_id="schedules",
                target_table_id="employees",
                source_column=source_column,
                target_column=target_column,
                cardinality=Cardinality.many_to_one,
            ),
        )
    assert (refused.value.rel_id, refused.value.end) == ("schedules-to-employees", missing)
    assert "schedules-to-employees" in str(refused.value)


@pytest.mark.asyncio
async def test_a_relationship_to_a_function_needs_no_target_column(monkeypatch):
    async def _find(conn, name):
        return {"id": 1, "domain_id": "src1", "table_name": name, "alias": None}

    monkeypatch.setattr(rel_mod.table_repo, "find_by_table_name", _find)
    monkeypatch.setattr(rel_mod.model_change, "name", lambda *a, **k: None)
    await rel_mod.upsert(
        _Conn(),
        Relationship(
            id="schedule-lookup",
            source_table_id="schedules",
            source_column="employee_id",
            target_function_name="lookup",
            cardinality=Cardinality.many_to_one,
        ),
    )


def test_a_nested_object_offered_as_a_relationship_is_not_also_a_column():
    """REQ-1929: Assignment.breed is offered as the relationship breed_name -> animal_breeds.name,
    so it is not registered as a column too: a relationship never hides a column."""
    tables, _functions, rels = map_schema(_schema(), "", "src1")
    assert any(r["cardinality"] == "many-to-one" for r in rels)
    assignments = next(t for t in tables if t["name"] == "assignmentsByEmployee")
    assert [c["name"] for c in assignments["columns"]] == ["breedName"]
