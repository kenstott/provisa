# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-313: a relationship the GraphQL remote mapper detects names each end by the table's
registered name, the one _upsert_tables_to_semantic_layer lands it under (apply_sql_name), so the
relationship upsert finds both tables. Naming them by the GraphQL field ("assignmentsByEmployee")
left every multi-word table unregistered as a relationship end."""

from __future__ import annotations

from provisa.compiler.naming import apply_sql_name
from provisa.graphql_remote.mapper import map_schema


def _list_of(type_name: str) -> dict:
    return {"kind": "LIST", "name": None, "ofType": {"kind": "OBJECT", "name": type_name}}


def _scalar(name: str) -> dict:
    return {"kind": "SCALAR", "name": name, "ofType": None}


def _object(name: str, fields: list[tuple[str, dict]]) -> dict:
    return {
        "name": name,
        "kind": "OBJECT",
        "fields": [{"name": f, "type": t, "args": []} for f, t in fields],
        "interfaces": [],
        "enumValues": None,
        "inputFields": None,
    }


_SCHEMA = {
    "types": [
        _object(
            "Query",
            [
                ("animalBreeds", _list_of("AnimalBreed")),
                ("assignmentsByEmployee", _list_of("Assignment")),
            ],
        ),
        _object("AnimalBreed", [("id", _scalar("ID")), ("name", _scalar("String"))]),
        _object(
            "Assignment",
            [
                ("id", _scalar("ID")),
                ("breedId", _scalar("ID")),
                ("breed", {"kind": "OBJECT", "name": "AnimalBreed", "ofType": None}),
            ],
        ),
    ],
    "queryType": {"name": "Query"},
    "mutationType": None,
}


def test_each_relationship_end_is_a_registered_table_name():
    tables, _functions, relationships = map_schema(_SCHEMA, namespace="", source_id="demo")
    registered = {apply_sql_name(t["name"]) for t in tables}
    assert relationships, "the mapper detected no relationship to check"
    for rel in relationships:
        assert rel["source_table_id"] in registered, rel
        assert rel["target_table_id"] in registered, rel
    ends = {(r["source_table_id"], r["target_table_id"]) for r in relationships}
    assert ("assignments_by_employee", "animal_breeds") in ends
