# Copyright (c) 2026 Kenneth Stott
# Canary: 8efe6036-fa56-4a15-9ab7-69dc1adb57c6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A profile reads only the columns its table's relation holds.

A remote GraphQL table registers a field pointing at ONE object of another registered type
(``employee``, ``breed``) as a column beside the scalars landed from it (``employee_id``,
``breed_name``). The relation a statement reads -- the rows inlined into it, or the cache table
they land in -- is built without those columns. A profile statement that selected them failed in
the engine: the profile of the table itself in its first read, and the profile of any table whose
parent it is in the dependence statements.
"""

from __future__ import annotations

from types import SimpleNamespace

import duckdb
import sqlglot

from provisa.compiler.sql_types import JoinMeta, TableMeta
from provisa.graphql_remote.relation_columns import columns_outside_relation, reference_columns
from provisa.profiler import dependence
from provisa.profiler.run import PROFILE_ROLE, parents_of, resolve_target
from provisa.profiler.statement import Sample, profile_sql

PETS, VISITS = 1, 2
_PET_COLUMNS = [
    {"name": "id", "type": "integer"},
    {"name": "name", "type": "text"},
    {"name": "employee_id", "type": "integer", "gql_selection": "employee_id: employee { id }"},
    {"name": "breed_name", "type": "text", "gql_selection": "breed_name: breed { name }"},
    {"name": "employee", "type": "jsonb", "gql_object_type": "Employee", "gql_is_list": False},
    {"name": "breed", "type": "jsonb", "gql_object_type": "Breed", "gql_is_list": False},
    # A list of objects is data the relation holds (landed as JSON), unlike a single reference.
    {"name": "tags", "type": "jsonb", "gql_object_type": "Breed", "gql_is_list": True},
]
_SOURCES = {
    "graphql-demo": {
        "source_id": "graphql-demo",
        "tables": [
            {"sql_name": "pets", "gql_type_name": "Pet", "columns": _PET_COLUMNS},
            {"sql_name": "employees", "gql_type_name": "Employee", "columns": []},
            {"sql_name": "breeds", "gql_type_name": "Breed", "columns": []},
        ],
    }
}


def _col(name: str, data_type: str) -> SimpleNamespace:
    return SimpleNamespace(column_name=name, data_type=data_type, subtype=None)


def _state() -> SimpleNamespace:
    pets = TableMeta(PETS, "pets", "Pet", "graphql-demo", "graphql_demo", "public", "pets", "d")
    visits = TableMeta(VISITS, "visits", "Visit", "pg", "pg", "public", "visits", "d")
    column_types = {
        PETS: [
            _col("id", "integer"),
            _col("name", "varchar"),
            _col("employee_id", "integer"),
            _col("breed_name", "varchar"),
            _col("employee", "jsonb"),
            _col("breed", "jsonb"),
            _col("tags", "jsonb"),
        ],
        VISITS: [_col("id", "integer"), _col("pet_id", "integer"), _col("reason", "varchar")],
    }
    ctx = SimpleNamespace(
        tables={"pets": pets, "visits": visits},
        # The org admin sees every registered column, the object fields among them.
        physical_to_sql={
            (tid, c.column_name): c.column_name for tid, cs in column_types.items() for c in cs
        },
        joins={
            ("Visit", "pet"): JoinMeta("pet_id", "id", "integer", "integer", pets, "many-to-one")
        },
        pk_columns={PETS: ["id"], VISITS: ["id"]},
        unique_constraints={},
    )
    return SimpleNamespace(
        contexts={PROFILE_ROLE: ctx},
        schema_build_cache={"column_types": column_types},
        graphql_remote_sources=_SOURCES,
    )


def _engine() -> duckdb.DuckDBPyConnection:
    """The relations as a statement reads them: pets holds what the remote rows carry."""
    con = duckdb.connect()
    con.execute("CREATE SCHEMA d")
    con.execute(
        "CREATE TABLE d.pets AS SELECT range AS id, 'pet' || range::VARCHAR AS name, "
        "range % 3 AS employee_id, CASE WHEN range % 2 = 0 THEN 'Siamese' ELSE 'Maine Coon' END "
        "AS breed_name, '[]'::JSON AS tags FROM range(20)"
    )
    con.execute(
        "CREATE TABLE d.visits AS SELECT range AS id, range % 20 AS pet_id, "
        "CASE WHEN range % 2 = 0 THEN 'checkup' ELSE 'injury' END AS reason FROM range(200)"
    )
    return con


def _run(con: duckdb.DuckDBPyConnection, sql: str) -> list[tuple]:
    return con.execute(sqlglot.transpile(sql, read="postgres", write="duckdb")[0]).fetchall()


def test_the_rule_names_single_references_to_registered_types_only():
    pets = _SOURCES["graphql-demo"]["tables"][0]
    assert reference_columns(pets, {"Pet", "Employee", "Breed"}) == {"employee", "breed"}
    # A type that is not a registered table is landed as JSON: the relation holds it.
    assert reference_columns(pets, {"Pet", "Employee"}) == {"employee"}
    assert columns_outside_relation(_SOURCES, "graphql-demo", "pets") == {"employee", "breed"}
    assert columns_outside_relation(_SOURCES, "pg", "pets") == set()
    assert columns_outside_relation(_SOURCES, "graphql-demo", "visits") == set()


def test_a_parent_reached_from_the_profiled_table_is_read_without_its_object_fields():
    state = _state()
    target = resolve_target(state, VISITS, "visits", {})
    (parent,) = target.parents
    assert [c.name for c in parent.columns] == ["id", "name", "employee_id", "breed_name", "tags"]
    assert [c.name for c in parents_of(state, VISITS)[0].columns] == [
        c.name for c in parent.columns
    ]

    # The dependence statement that failed in the engine ("... does not have a column named
    # "employee"") runs over the relation as it is.
    cols = dependence.choose_columns(
        [(c, 2, 1.0) for c in target.columns],
        [(parent, {"name": (20, 1.0), "breed_name": (2, 1.0)})],
        8,
        50,
        8,
    )
    assert {c.name for c in cols} >= {"reason", "pet.breed_name"}
    sql = dependence.pairs_sql(
        target.pgwire_name, target.columns, target.parents, cols, Sample("whole")
    )
    assert _run(_engine(), sql)


def test_the_remote_table_itself_is_profiled_without_its_object_fields():
    target = resolve_target(_state(), PETS, "pets", {})
    assert [c.name for c in target.columns] == ["id", "name", "employee_id", "breed_name", "tags"]
    sql = profile_sql(
        target.pgwire_name, target.columns, target.fanouts, Sample("whole"), 20, target.keys, []
    )
    assert _run(_engine(), sql)
