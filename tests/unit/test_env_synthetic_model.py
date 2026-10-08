# Copyright (c) 2026 Kenneth Stott
# Canary: c142b8f2-17b8-4289-9616-ed3215f24945
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Test (synthetic) environment calls no source API (REQ-1942): an API table that needs a
required parameter is not available without a declared profile, the commands of a generated API
source are not defined, and Generate answers a warning that is confirmed by its digest."""

# Requirements: REQ-1942

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.errors import ApiError
from provisa.federation import replica_routing
from provisa.synthetic import env_model
from provisa.synthetic.datasets import TableNotAvailable, refuse_unavailable


def _table(table_id: int, name: str, source: str, *filters: str | None) -> dict:
    return {
        "id": table_id,
        "table_name": name,
        "source_id": source,
        "schema_name": "public",
        "columns": [{"column_name": "id", "native_filter_type": None}]
        + [
            {"column_name": f"_nf_{i}", "native_filter_type": kind}
            for i, kind in enumerate(filters)
        ],
    }


@pytest.mark.parametrize(
    ("source_type", "kind", "required"),
    [
        ("openapi", "path_param", True),
        ("openapi", "query_param", False),  # an OpenAPI query parameter is optional
        ("graphql_remote", "query_param", True),
        ("grpc_remote", "grpc_input", True),
        ("postgresql", "path_param", False),
    ],
)
def test_a_required_parameter_is_told_by_its_sources_type(source_type, kind, required):
    found = env_model.required_parameters(_table(1, "pet_by_id", "petstore", kind), source_type)
    assert found == (["_nf_0"] if required else [])


def _plan() -> dict:
    run = {"runId": "r1", "env": "prod", "origin": "measured", "rowCount": 40}
    return {
        "tables": [
            {
                "tableId": 1,
                "tableName": "breeds",
                "source": "petstore",
                "api": True,
                "runs": [run],
            }
        ],
        "sources": [{"id": "petstore", "type": "openapi", "becomes": "postgresql"}],
        "unavailable": [{"tableId": 2, "tableName": "pet_by_id", "reason": "why"}],
        "commandsNotDefined": [{"source": "petstore", "commands": ["add_pet"]}],
        "lastGeneration": None,
    }


def test_the_warning_names_what_is_generated_lost_and_discarded():
    shown = env_model.warning(
        _plan(), {1: ("prod", "r1")}, seed=3, scale=2.5, kept_mutations={1: 4}
    )
    assert shown["title"] == "Limitations of Synthetic Data"
    assert {item["key"] for item in shown["limitations"]} == {
        "no_source_api",
        "required_parameter_tables",
        "source_commands",
        "manual_fix_up",
    }
    (table,) = shown["tables"]
    assert table["profile"] == {"runId": "r1", "env": "prod", "origin": "measured"}
    assert table["estimatedRows"] == 100 and table["scale"] == 2.5
    assert shown["sources"] == [{"id": "petstore", "type": "openapi", "becomes": "postgresql"}]
    assert shown["unavailable"][0]["tableName"] == "pet_by_id"
    assert shown["commandsNotDefined"] == [{"source": "petstore", "commands": ["add_pet"]}]
    assert shown["keptMutationsDiscarded"] == {"1": 4}
    # No generation has finished in the environment yet: rows are estimated, time is not.
    assert shown["estimatedRows"] == 100 and shown["estimatedSeconds"] is None
    timed = {**_plan(), "lastGeneration": (50, 4.0)}
    again = env_model.warning(timed, {1: ("prod", "r1")}, seed=3, scale=2.5, kept_mutations={})
    assert again["estimatedSeconds"] == pytest.approx(8.0)


def test_the_digest_names_exactly_what_was_shown():
    runs = {1: ("prod", "r1")}
    first = env_model.warning(_plan(), runs, seed=3, scale=1.0, kept_mutations={})
    again = env_model.warning(_plan(), runs, seed=3, scale=1.0, kept_mutations={})
    assert first["digest"] == again["digest"]
    changed = _plan()
    changed["commandsNotDefined"][0]["commands"].append("delete_pet")
    for other in (
        env_model.warning(changed, runs, seed=3, scale=1.0, kept_mutations={}),
        env_model.warning(_plan(), runs, seed=3, scale=2.0, kept_mutations={}),
        env_model.warning(_plan(), runs, seed=3, scale=1.0, kept_mutations={1: 1}),
    ):
        assert other["digest"] != first["digest"]


def _registry(synthetic: dict, bound: frozenset[str]) -> replica_routing._Registry:
    return replica_routing._Registry(
        [
            _table(1, "breeds", "petstore"),
            _table(2, "pet_by_id", "petstore", "path_param"),
            _table(3, "orders", "pg"),
        ],
        {
            # Bound to the synthetic store, each source's type is the store's.
            "petstore": SimpleNamespace(id="petstore", type="postgresql"),
            "pg": SimpleNamespace(id="pg", type="postgresql"),
        },
        serving=frozenset(),
        promoted=frozenset(),
        synthetic=synthetic,
        bound=bound,
    )


def test_a_table_of_a_synthetic_bound_source_with_no_generated_copy_is_unavailable():
    schema = "org_a_env_dev_syn__model"
    generated = {1: (env_model.DATASET_ID, schema), 3: (env_model.DATASET_ID, schema)}
    unavailable = replica_routing._unavailable(_registry(generated, frozenset({"petstore", "pg"})))
    assert set(unavailable) == {2}
    assert "required parameter(s) _nf_0" in unavailable[2]
    assert "Declare a profile of it" in unavailable[2]
    routes = SimpleNamespace(unavailable=unavailable)
    refuse_unavailable(routes, [1, 3])
    with pytest.raises(TableNotAvailable, match="'pet_by_id' is not available in a Test"):
        refuse_unavailable(routes, [3, 2])


def test_nothing_is_unavailable_where_no_source_is_bound_to_a_synthetic_store():
    assert replica_routing._unavailable(_registry({}, frozenset())) == {}
    # A dataset of the operator's own (REQ-1939) binds no source.
    assert replica_routing._unavailable(_registry({3: ("mine", "s")}, frozenset())) == {}


async def test_a_command_of_a_generated_api_source_is_refused_saying_why():
    from provisa.api.data.action_exec import invoke_command

    reason = env_model.undefined_reason("add_pet", "petstore")
    state = SimpleNamespace(
        tracked_functions={}, tracked_webhooks={}, undefined_commands={"add_pet": reason}
    )
    with pytest.raises(ApiError) as refused:
        await invoke_command("add_pet", {}, state, "dev")
    assert refused.value.status_code == 409
    assert refused.value.code == "functions.not_defined_synthetic"
    assert (
        "bound to a synthetic store" in refused.value.detail
        and "'petstore'" in refused.value.detail
    )
    with pytest.raises(ApiError) as unknown:
        await invoke_command("nope", {}, state, "dev")
    assert unknown.value.code == "functions.unknown_command"


def test_a_generated_table_is_read_as_an_ordinary_table_its_required_parameter_a_column():
    """REQ-1942: bound to a synthetic store, a generated API table's required parameter is a
    column of it, typed as it was generated; any other argument of the API is no column of it;
    a table with no generated copy, and a table of any other source, are read as the model has
    them."""
    from provisa.synthetic.datasets import as_generated

    def api_table(name: str) -> dict:
        return {
            "source_id": "petstore",
            "schema_name": "default",
            "table_name": name,
            "columns": [
                {"column_name": "id", "native_filter_type": None, "data_type": "integer"},
                {"column_name": "petId", "native_filter_type": "path_param", "data_type": None},
                {"column_name": "limit", "native_filter_type": "query_param", "data_type": None},
            ],
        }

    generated, missing = api_table("get_pet_by_id"), api_table("get_owner_by_id")
    other = {**api_table("orders"), "source_id": "pg"}
    bound = {
        "petstore": {
            "schema": "s",
            "tables": [["default", "get_pet_by_id"]],
            "parameters": {"default.get_pet_by_id": {"petId": "bigint"}},
            "model_type": "openapi",
        }
    }
    read, unread, untouched = as_generated([generated, missing, other], bound)
    assert [
        (c["column_name"], c["native_filter_type"], c["data_type"]) for c in read["columns"]
    ] == [
        ("id", None, "integer"),
        ("petId", None, "bigint"),
    ]
    assert unread == missing and untouched == other
    # The model's own rows are not changed by how the environment reads them.
    assert generated["columns"][1]["native_filter_type"] == "path_param"


def test_each_reason_a_table_has_no_generated_rows_is_said_as_itself():
    """REQ-1942: a table of a synthetic-bound source with no generated copy is refused for one
    of two reasons, each said as itself: it is read from its API only by a required parameter and
    has no declared profile; or it was registered after the model was generated."""
    parameterised = env_model.unavailable_reason("pet_by_id", ["petId"])
    assert "only by the required parameter(s) petId" in parameterised
    assert "Declare a profile of it and generate again" in parameterised
    late = env_model.unavailable_reason("returns", [])
    assert "was registered after the model was generated" in late
    assert late.endswith("Generate again to generate it.")
    assert "required parameter" not in late and "Declare a profile" not in late

    registry = replica_routing._Registry(
        [
            _table(1, "orders", "pg"),
            _table(2, "returns", "pg"),
            _table(3, "pet_by_id", "pg", "path_param"),
        ],
        {"pg": SimpleNamespace(id="pg", type="duckdb")},
        serving=frozenset(),
        promoted=frozenset(),
        synthetic={1: (env_model.DATASET_ID, "s")},
        bound=frozenset({"pg"}),
    )
    unavailable = replica_routing._unavailable(registry)
    assert "was registered after the model was generated" in unavailable[2]
    assert "only by the required parameter(s) _nf_0" in unavailable[3]
