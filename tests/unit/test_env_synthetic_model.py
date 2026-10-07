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
        "unavailable": [{"tableId": 2, "tableName": "pet_by_id", "reason": "why"}],
        "commandsNotDefined": [{"source": "petstore", "commands": ["add_pet"]}],
    }


def test_the_warning_names_what_is_generated_lost_and_discarded():
    shown = env_model.warning(
        _plan(), {1: ("prod", "r1")}, seed=3, scale=2.5, kept_mutations={1: 4}
    )
    assert shown["title"] == "Limitations of Synthetic Data"
    assert {item["key"] for item in shown["limitations"]} == {
        "no_source_api",
        "required_parameter_tables",
        "api_commands",
        "manual_fix_up",
    }
    (table,) = shown["tables"]
    assert table["profile"] == {"runId": "r1", "env": "prod", "origin": "measured"}
    assert table["estimatedRows"] == 100 and table["scale"] == 2.5
    assert shown["sources"] == ["petstore"]
    assert shown["unavailable"][0]["tableName"] == "pet_by_id"
    assert shown["commandsNotDefined"] == [{"source": "petstore", "commands": ["add_pet"]}]
    assert shown["keptMutationsDiscarded"] == {"1": 4}


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


def _registry(synthetic: dict) -> replica_routing._Registry:
    return replica_routing._Registry(
        [
            _table(1, "breeds", "petstore"),
            _table(2, "pet_by_id", "petstore", "path_param"),
            _table(3, "orders", "pg"),
        ],
        {
            "petstore": SimpleNamespace(id="petstore", type="openapi"),
            "pg": SimpleNamespace(id="pg", type="postgresql"),
        },
        serving=frozenset(),
        promoted=frozenset(),
        synthetic=synthetic,
    )


def test_an_api_table_with_no_generated_copy_is_unavailable_once_the_model_is_generated():
    schema = "org_a_env_dev_syn__model"
    generated = {1: (env_model.DATASET_ID, schema), 3: (env_model.DATASET_ID, schema)}
    unavailable = replica_routing._unavailable(_registry(generated))
    assert set(unavailable) == {2}
    assert "required parameter(s) _nf_0" in unavailable[2]
    assert "Declare a profile of it" in unavailable[2]
    routes = SimpleNamespace(unavailable=unavailable)
    refuse_unavailable(routes, [1, 3])
    with pytest.raises(TableNotAvailable, match="'pet_by_id' is not available in a Test"):
        refuse_unavailable(routes, [3, 2])


def test_nothing_is_unavailable_before_the_whole_model_is_generated():
    assert replica_routing._unavailable(_registry({})) == {}
    # A dataset of the operator's own (REQ-1939) is not the environment's whole model.
    assert replica_routing._unavailable(_registry({3: ("mine", "s")})) == {}


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
    assert "calls no source API" in refused.value.detail and "'petstore'" in refused.value.detail
    with pytest.raises(ApiError) as unknown:
        await invoke_command("nope", {}, state, "dev")
    assert unknown.value.code == "functions.unknown_command"
