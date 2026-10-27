# Copyright (c) 2026 Kenneth Stott
# Canary: 3da7a50b-4e04-4aba-8316-2d67b9d2bc7f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A query command that runs a MongoDB pipeline (REQ-1988): its native query is checked when it
is registered, a call binds its typed arguments as BSON values, and the documents come back as
JSON, each with its own shape."""

# Requirements: REQ-1988

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pydantic
import pytest
from bson import ObjectId

from provisa.core.models import Function, FunctionArgument

_BINDING = {
    "inputCollection": "movies",
    "pipeline": [
        {
            "$searchMeta": {
                "index": "plot_text",
                "text": {"query": "{{q}}", "path": "plot"},
                "facet": {"facets": {"genre": {"type": "string", "path": "genre"}}},
            }
        },
        {"$limit": "{{n}}"},
    ],
}


def _command(**kw) -> Function:
    fields = {
        "name": "search_facets",
        "source_id": "atlas",
        "function_name": "search_facets",
        "returns": "",
        "kind": "query",
        "impl_kind": "source_query",
        "binding": _BINDING,
        "domain_id": "films",
        "arguments": [
            FunctionArgument(name="q", type="text"),
            FunctionArgument(name="n", type="integer"),
        ],
    }
    fields.update(kw)
    return Function(**fields)


def test_a_pipeline_command_registers_with_its_typed_arguments():
    assert _command().impl_kind == "source_query"


@pytest.mark.parametrize(
    ("kw", "reason"),
    [
        ({"kind": "mutation"}, "a source_query command is a query"),
        ({"arguments": [FunctionArgument(name="q", type="text")]}, r"\['n'\], which the table"),
        (
            {
                "binding": {"inputCollection": "movies", "pipeline": [{"$out": "copy"}]},
                "arguments": [],
            },
            "only reads",
        ),
    ],
)
def test_a_pipeline_command_that_cannot_be_run_is_refused(kw, reason):
    with pytest.raises(pydantic.ValidationError, match=reason):
        _command(**kw)


async def test_a_call_binds_its_arguments_and_returns_the_documents_as_json():
    from provisa.executor.function_dispatch import dispatch_function

    seen: dict = {}

    def batches(conn, database, collection, pipeline, batch_rows):
        seen.update(database=database, collection=collection, pipeline=pipeline)
        yield [{"_id": ObjectId("65f0c0ffee0000000000abcd"), "count": {"lowerBound": 2}}]

    source = SimpleNamespace(id="atlas", type=SimpleNamespace(value="mongodb"), database="films")
    fn = _command().model_dump()
    state = SimpleNamespace(udf_trace_sink=None, minted_sessions=None)
    with (
        patch(
            "provisa.federation.registry_view.registered_sources", AsyncMock(return_value=[source])
        ),
        patch("provisa.mongodb.fetch.MongoConnection.from_source", return_value=object()),
        patch("provisa.mongodb.fetch.aggregate_row_batches", side_effect=batches),
    ):
        # The SQL surfaces pass a call's arguments by position.
        rows = await dispatch_function(fn, {"a0": '{"$ne": null}', "a1": "5"}, state, "analyst")
    assert (seen["database"], seen["collection"]) == ("films", "movies")
    search = seen["pipeline"][0]["$searchMeta"]
    assert search["text"]["query"] == '{"$ne": null}'  # operator syntax stays a string
    assert seen["pipeline"][1] == {"$limit": 5}
    assert rows == [{"_id": {"$oid": "65f0c0ffee0000000000abcd"}, "count": {"lowerBound": 2}}]


async def test_an_argument_not_of_its_type_runs_nothing():
    from provisa.core.parameter_values import ParameterValueRefused
    from provisa.executor.function_dispatch import dispatch_function

    state = SimpleNamespace(udf_trace_sink=None, minted_sessions=None)
    with (
        patch("provisa.mongodb.fetch.aggregate_row_batches") as ran,
        pytest.raises(ParameterValueRefused, match="'n' takes a value of type integer"),
    ):
        await dispatch_function(_command().model_dump(), {"q": "old", "n": "five"}, state, None)
    ran.assert_not_called()


def test_graphql_returns_a_pipeline_commands_documents_as_json():
    from graphql import get_named_type

    from provisa.compiler.actions_schema import _build_action_fields
    from provisa.compiler.schema_types import SchemaInput
    from provisa.compiler.type_map import JSONScalar

    si = SchemaInput(
        tables=[],
        relationships=[],
        column_types={},
        naming_rules=[],
        role={"id": "dev", "capabilities": [], "domain_access": ["*"]},
        domains=[],
        functions=[_command(visible_to=["dev"]).model_dump()],
    )
    query_fields, _ = _build_action_fields(si, {}, [])
    (field,) = query_fields.values()
    assert get_named_type(field.type) is JSONScalar
    assert set(field.args) >= {"q", "n"}
