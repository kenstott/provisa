# Copyright (c) 2026 Kenneth Stott
# Canary: 8b1e6c02-4d97-4a35-9e6b-7c05f3ad12ee
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1405/REQ-1408: the NL-generated JSON:API ``?include=`` must be the list the JSON:API
handler accepts — end to end, with no stub in between.

``?groupBy=`` names PHYSICAL columns and ``?include=`` names the columns as the GraphQL schema
EXPOSES them, so the two halves of one generated URL use different vocabularies. The unit tests
around ``_resolve_aggregation_plan`` feed it a stub context, which can agree with a schema that
was never built; that is how ``include=pet.breed_name`` reached a handler whose ``pet`` type
carries ``breedName`` and answered ``Unknown field 'breed_name' on relationship 'pet'``.

Here the context and the schema both come from one ``SchemaInput`` through the real
``build_context``/``generate_schema``, under the default apollo (camelCase) convention where a
multi-word column is the only thing that can tell the two namings apart.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.jsonapi.generator import _build_group_by_node_selection
from provisa.compiler import naming as _naming
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.nl.runner import _generate_jsonapi_query, _resolve_aggregation_plan

# "count of inquiries by user, with the pet's details" — the shape that produced the reported
# failure: a single-table group-by whose joined-dimension columns ride along as include dot-paths.
_SQL = (
    "SELECT i.user_id, COUNT(i.id) AS c, "
    "json_agg(json_build_object('id', p.id, 'name', p.name, 'breed_name', p.breed_name)) AS pets "
    "FROM inquiries i JOIN pets p ON p.id = i.pet_id GROUP BY i.user_id"
)


def _col(name: str, data_type: str = "varchar(100)") -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=True)


@pytest.fixture()
def petstore():
    """inquiries -> pets, modeled on config/provisa-install.yaml's pet-store domain."""
    _naming.configure(gql="apollo_graphql", sql="snake")
    tables = [
        {
            "id": 1,
            "source_id": "pet-store-sqlite",
            "domain_id": "pet-store",
            "schema_name": "main",
            "table_name": "inquiries",
            "enable_aggregates": True,
            "enable_group_by": True,
            "columns": [
                {"column_name": "id", "visible_to": []},
                {"column_name": "user_id", "visible_to": []},
                {"column_name": "pet_id", "visible_to": []},
            ],
        },
        {
            "id": 2,
            "source_id": "pet-store-sqlite",
            "domain_id": "pet-store",
            "schema_name": "main",
            "table_name": "pets",
            "enable_aggregates": True,
            "enable_group_by": True,
            "columns": [
                {"column_name": "id", "visible_to": []},
                {"column_name": "name", "visible_to": []},
                {"column_name": "breed_name", "visible_to": []},
            ],
        },
    ]
    si = SchemaInput(
        tables=tables,
        relationships=[
            {
                "id": "inquiries-to-pets",
                "alias": "HAS_PETS",
                "cardinality": "many-to-one",
                "source_table_id": 1,
                "source_column": "pet_id",
                "target_table_id": 2,
                "target_column": "id",
            }
        ],
        column_types={
            1: [_col("id", "integer"), _col("user_id", "integer"), _col("pet_id", "integer")],
            2: [_col("id", "integer"), _col("name"), _col("breed_name")],
        },
        naming_rules=[],
        role={"id": "org_admin", "capabilities": [], "domain_access": ["*"]},
        domains=[{"id": "pet-store", "description": "Pet store"}],
    )
    app_state = SimpleNamespace(
        tables=[
            {
                "table_name": t["table_name"],
                "domain_id": t["domain_id"],
                "enable_aggregates": True,
                "enable_group_by": True,
            }
            for t in tables
        ]
    )
    yield generate_schema(si), build_context(si), app_state
    _naming.configure()


def test_the_schema_renames_the_multi_word_column(petstore):
    """The premise: under apollo, ``breed_name`` is exposed as ``breedName``. Without this the
    rest of the file would pass on a schema that never renamed anything."""
    schema, ctx, _ = petstore
    assert ctx.exposed_to_physical[(2, "breedName")] == "breed_name"
    pet_type = schema.query_type.fields["pets"].type
    while hasattr(pet_type, "of_type"):
        pet_type = pet_type.of_type
    assert "breedName" in pet_type.fields
    assert "breed_name" not in pet_type.fields


def test_generated_include_names_columns_as_the_schema_exposes_them(petstore):
    _schema, ctx, app_state = petstore
    plan = _resolve_aggregation_plan(ctx, app_state, _SQL)
    assert plan is not None
    assert plan.dim_paths_gql == ["pet.id", "pet.name", "pet.breedName"]
    # dim_paths stays physical — it is what gRPC's include= takes.
    assert plan.dim_paths == ["pet.id", "pet.name", "pet.breed_name"]


def test_generated_url_is_accepted_by_the_jsonapi_handler(petstore):
    schema, ctx, app_state = petstore
    plan = _resolve_aggregation_plan(ctx, app_state, _SQL)
    assert plan is not None
    url, err = _generate_jsonapi_query(plan, set(), {})
    assert err is None
    assert url is not None
    include_param = url.split("&include=", 1)[1]
    selection, detail = _build_group_by_node_selection(schema, "inquiries", ["userId"], include_param)
    assert detail is None, detail
    assert "pet { id name breedName }" in selection


def test_the_physical_naming_is_the_one_the_handler_rejects(petstore):
    """The failure that was reported, asserted as the handler's own behavior — so a regression
    that reverts the include list to physical names fails here instead of in a browser."""
    schema, _ctx, _app_state = petstore
    _selection, detail = _build_group_by_node_selection(
        schema, "inquiries", ["userId"], "pet.breed_name"
    )
    assert detail == "Unknown field 'breed_name' on relationship 'pet'"
