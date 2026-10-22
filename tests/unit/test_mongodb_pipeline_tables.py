# Copyright (c) 2026 Kenneth Stott
# Canary: 583b97c3-9762-48d7-b39a-86c0b6b64bba
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A MongoDB table registered from an aggregation pipeline (REQ-1980): its native query is
checked at registration, its arguments are converted to their declared types and bound into the
pipeline as BSON values -- never written into its text -- and every document it returns is
checked against the table's output columns."""

# Requirements: REQ-1980

from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.federation.replica_address import ReplicaRoutes
from bson import Binary, Decimal128, Int64, ObjectId

from provisa.core.models import Column, Table
from provisa.core.parameter_values import ParameterValueRefused, convert
from provisa.mongodb.native_query import NativeQueryRefused, bind, check, check_table
from provisa.mongodb.pipeline_rows import (
    OutputColumn,
    PipelineOutputMismatch,
    infer_columns,
    project,
)

_VECTOR_SEARCH = json.dumps(
    {
        "inputCollection": "movies",
        "pipeline": [
            {
                "$vectorSearch": {
                    "index": "plot_vec",
                    "path": "plot_embedding",
                    "queryVector": "{{query_vector}}",
                    "numCandidates": 200,
                    "limit": "{{k}}",
                }
            },
            {"$match": {"genre": "{{genre}}"}},
            {"$project": {"_id": 0, "title": 1, "score": {"$meta": "vectorSearchScore"}}},
        ],
    }
)


# --- arguments: converted strictly to their declared types ---


@pytest.mark.parametrize(
    ("value", "data_type", "expected"),
    [
        ("42", "integer", 42),
        (7, "bigint", 7),
        ("2.5", "double", 2.5),
        ("19.99", "numeric", Decimal("19.99")),
        ("true", "boolean", True),
        ("2025-03-02", "date", date(2025, 3, 2)),
        ("2025-03-02T10:00:00", "timestamp", datetime(2025, 3, 2, 10, tzinfo=timezone.utc)),
        ("Drama", "text", "Drama"),
        ('{"$oid": "65f0c0ffee0000000000abcd"}', "json", ObjectId("65f0c0ffee0000000000abcd")),
        ("[0.1, 0.2, 0.3]", "array(double)", [0.1, 0.2, 0.3]),
        ([1, 2], "array(double)", [1.0, 2.0]),
        (None, "integer", None),
    ],
)
def test_an_argument_is_converted_to_its_declared_type(value, data_type, expected):
    assert convert(value, data_type, table="t", parameter="p") == expected


@pytest.mark.parametrize(
    ("value", "data_type"),
    [
        ("forty-two", "integer"),
        (True, "integer"),
        ("2.5", "integer"),
        ("yes", "boolean"),
        (42, "text"),
        ('{"$gt": ""}', "array(double)"),
        ('[0.1, "x"]', "array(double)"),
        ("2025-03-02T10:00:00", "date"),
    ],
)
def test_a_value_not_of_the_declared_type_is_refused_naming_the_parameter(value, data_type):
    with pytest.raises(ParameterValueRefused, match=r"Table 'movies': parameter 'p' takes"):
        convert(value, data_type, table="movies", parameter="p")


# --- the native query: checked at registration ---


def test_a_native_query_names_its_arguments_and_uses_each():
    query = check("similar", _VECTOR_SEARCH, {"query_vector", "k", "genre"})
    assert query.input_collection == "movies"
    with pytest.raises(NativeQueryRefused, match=r"\['genre'\], which the table declares"):
        check("similar", _VECTOR_SEARCH, {"query_vector", "k"})
    with pytest.raises(NativeQueryRefused, match=r"\['extra'\] appear nowhere"):
        check("similar", _VECTOR_SEARCH, {"query_vector", "k", "genre", "extra"})


def test_a_placeholder_inside_a_longer_string_is_refused():
    text = json.dumps(
        {"inputCollection": "movies", "pipeline": [{"$match": {"title": "Star {{q}}"}}]}
    )
    with pytest.raises(NativeQueryRefused, match="inside a longer string"):
        check("t", text, {"q"})


@pytest.mark.parametrize(
    "stage",
    [
        {"$out": "copy"},
        {"$merge": {"into": "copy"}},
        {"$facet": {"a": [{"$match": {}}, {"$out": "copy"}]}},
        {"$lookup": {"from": "x", "pipeline": [{"$merge": {"into": "y"}}], "as": "z"}},
    ],
)
def test_a_pipeline_that_writes_is_refused(stage):
    text = json.dumps({"inputCollection": "movies", "pipeline": [{"$match": {}}, stage]})
    with pytest.raises(NativeQueryRefused, match="only reads"):
        check("t", text, set())


@pytest.mark.parametrize(
    ("text", "reason"),
    [
        ("not json", "not Extended JSON"),
        ('{"pipeline": []}', "names its inputCollection"),
        ('{"inputCollection": "m", "pipeline": [{"$match": {}, "$limit": 1}]}', "each of one"),
        ('{"inputCollection": "m", "pipeline": [], "let": {}}', "inputCollection and pipeline"),
    ],
)
def test_a_malformed_native_query_is_refused(text, reason):
    with pytest.raises(NativeQueryRefused, match=reason):
        check("t", text, set())


# --- binding: a value is a BSON value, never text ---


def test_arguments_are_bound_as_bson_values_of_their_types():
    query = check("similar", _VECTOR_SEARCH, {"query_vector", "k", "genre"})
    pipeline = bind(
        query,
        {
            "query_vector": ([0.25, 0.5], "array(double)"),
            "k": (10, "bigint"),
            "genre": (None, "text"),
        },
    )
    search = pipeline[0]["$vectorSearch"]
    assert search["queryVector"] == [0.25, 0.5]
    assert all(isinstance(v, float) for v in search["queryVector"])
    assert isinstance(search["limit"], Int64) and search["limit"] == 10
    assert pipeline[1] == {"$match": {"genre": None}}  # an argument with no value binds null
    # The stored pipeline is untouched: each read binds its own copy.
    assert query.pipeline[0]["$vectorSearch"]["queryVector"] == "{{query_vector}}"


def test_a_value_holding_operator_syntax_stays_a_string():
    query = check("similar", _VECTOR_SEARCH, {"query_vector", "k", "genre"})
    pipeline = bind(
        query,
        {
            "query_vector": ([1.0], "array(double)"),
            "k": (1, "integer"),
            "genre": ('{"$gt": ""}', "text"),
        },
    )
    assert pipeline[1] == {"$match": {"genre": '{"$gt": ""}'}}


def test_decimal_uuid_and_date_bind_as_their_bson_types():
    text = json.dumps(
        {
            "inputCollection": "c",
            "pipeline": [{"$match": {"price": "{{p}}", "ref": "{{r}}", "on": "{{d}}"}}],
        }
    )
    ref = uuid.uuid4()
    match = bind(
        check("t", text, {"p", "r", "d"}),
        {"p": (Decimal("9.5"), "numeric"), "r": (ref, "uuid"), "d": (date(2025, 1, 2), "date")},
    )[0]["$match"]
    assert match["price"] == Decimal128("9.5")
    assert isinstance(match["ref"], Binary) and match["ref"].subtype == 4
    assert match["on"] == datetime(2025, 1, 2, tzinfo=timezone.utc)


# --- the table as registered ---


def _pipeline_table(**kw) -> Table:
    return Table(
        source_id="atlas",
        domain_id="films",
        schema_name="sample_mflix",
        table_name="similar",
        query_template=_VECTOR_SEARCH,
        columns=[
            Column(name="title", visible_to=["analyst"], data_type="varchar"),
            Column(name="score", visible_to=["analyst"], data_type="double"),
            Column(
                name="_nf_query_vector",
                visible_to=[],
                data_type="array(double)",
                native_filter_type="query_arg",
                native_filter_required=False,
                required_by_steward=True,
            ),
            Column(
                name="_nf_k",
                visible_to=[],
                data_type="integer",
                native_filter_type="query_arg",
                native_filter_required=False,
            ),
            Column(
                name="_nf_genre",
                visible_to=[],
                data_type="varchar",
                native_filter_type="query_arg",
                native_filter_required=False,
            ),
        ],
        **kw,
    )


def test_a_pipeline_table_registers_with_its_arguments_as_parameters():
    assert check_table(_pipeline_table()).input_collection == "movies"


def test_a_pipeline_table_has_no_change_stream():
    with pytest.raises(NativeQueryRefused, match="change stream watches a collection"):
        check_table(_pipeline_table(change_signal="native"))


def test_a_pipeline_table_has_no_rows_fetched_by_key():
    # A table's own validation already asks a keyed fetch for a key; a pipeline has none at all.
    keyed = _pipeline_table().model_copy(update={"row_materialize": True})
    with pytest.raises(NativeQueryRefused, match="no key to be fetched by"):
        check_table(keyed)


# --- the rows: checked against the output columns ---

_OUT = [
    OutputColumn("title", "varchar"),
    OutputColumn("score", "double"),
    OutputColumn("year", "integer", "released.year"),
    OutputColumn("cast", "json"),
    OutputColumn("id", "text", "_id"),
]


def test_a_document_is_projected_onto_the_output_columns():
    oid = ObjectId()
    row = project(
        "similar",
        {
            "_id": oid,
            "title": "Up",
            "score": 0.91,
            "released": {"year": 2009},
            "cast": ["Ed"],
            "x": 1,
        },
        _OUT,
    )
    assert row == {"title": "Up", "score": 0.91, "year": 2009, "cast": ["Ed"], "id": str(oid)}


def test_a_field_the_document_lacks_is_null():
    assert project("similar", {"title": "Up"}, _OUT)["score"] is None


def test_a_field_of_another_type_fails_the_read_naming_the_column():
    with pytest.raises(PipelineOutputMismatch, match=r"returned a str for column 'score'"):
        project("similar", {"title": "Up", "score": "high"}, _OUT)


def test_a_preview_suggests_the_columns_its_documents_hold():
    docs = [
        {"title": "Up", "score": 0.9, "released": {"year": 2009}, "tags": ["a"], "n": None},
        {"title": "Cars", "score": 1, "released": {"year": Int64(2006)}, "tags": [], "n": None},
    ]
    by = {c.name: c for c in infer_columns(docs)}
    assert by["title"].data_type == "text"
    assert by["score"].data_type == "json"  # a double and an integer: typed differently
    assert (by["released_year"].data_type, by["released_year"].path) == ("json", "released.year")
    assert by["tags"].data_type == "json"
    assert by["n"].data_type == "text"


# --- a read: the materialization step runs the pipeline and lands its rows ---

TABLE = 11


def _state_table(required_by_steward: bool = True) -> dict:
    def param(name: str, data_type: str, steward: bool = False) -> dict:
        return {
            "column_name": f"_nf_{name}",
            "data_type": data_type,
            "native_filter_type": "query_arg",
            "native_filter_required": False,
            "required_by_steward": steward,
            "path": None,
        }

    return {
        "id": TABLE,
        "source_id": "atlas",
        "schema_name": "sample_mflix",
        "table_name": "similar",
        "query_template": _VECTOR_SEARCH,
        "cache_ttl": 120,
        "columns": [
            {
                "column_name": "title",
                "data_type": "varchar",
                "native_filter_type": None,
                "path": None,
            },
            {
                "column_name": "score",
                "data_type": "double",
                "native_filter_type": None,
                "path": None,
            },
            param("query_vector", "array(double)", required_by_steward),
            param("k", "integer"),
            param("genre", "varchar"),
        ],
    }


def _state(table: dict) -> SimpleNamespace:
    engine = MagicMock()
    engine.land_source_table = AsyncMock()
    return SimpleNamespace(
        hot_manager=None,
        api_endpoints={},
        graphql_remote_sources={},
        source_types={"atlas": "mongodb"},
        replica_routes=ReplicaRoutes(),
        tables=[table],
        federation_engine=engine,
        source_cache={},
        response_cache_default_ttl=300,
    )


async def test_a_read_runs_the_pipeline_with_its_arguments_and_lands_the_rows():
    from provisa.api.data.materialization import _materialize_api_to_engine_cache
    from provisa.api_source.engine_cache import CacheLocation

    state = _state(_state_table())
    seen: dict = {}

    def batches(conn, database, collection, pipeline, batch_rows):
        seen.update(database=database, collection=collection, pipeline=pipeline)
        yield [{"title": "Up", "score": 0.91}, {"title": "Cars", "score": 0.5}]

    source = SimpleNamespace(id="atlas", database="sample_mflix")
    loc = CacheLocation("cat", "sch", "relational")
    with (
        patch(
            "provisa.federation.registry_view.registered_sources", AsyncMock(return_value=[source])
        ),
        patch("provisa.mongodb.fetch.MongoConnection.from_source", return_value=object()),
        patch("provisa.mongodb.fetch.aggregate_row_batches", side_effect=batches),
        patch("provisa.api_source.engine_cache.cache_location", return_value=loc),
        patch("provisa.api_source.engine_cache.table_known_live", return_value=False),
        patch("provisa.api_source.engine_cache.ensure_cache_schema"),
        patch("provisa.api_source.engine_cache.analyze_cache_table", AsyncMock()),
        patch("provisa.api_source.engine_cache.schedule_drop") as drop,
        patch("provisa.api_source.engine_cache.cache_table_name", return_value="p_1") as named,
    ):
        rewrites, _, dropped = await _materialize_api_to_engine_cache(
            "SELECT * FROM similar",
            state,
            nf_args={"query_vector": "[0.25, 0.5]", "k": 3},
            table_ids=[TABLE],
        )
    assert dropped == {} and rewrites == {"similar": (loc, "p_1")}
    assert (seen["database"], seen["collection"]) == ("sample_mflix", "movies")
    assert seen["pipeline"][0]["$vectorSearch"]["queryVector"] == [0.25, 0.5]
    assert seen["pipeline"][0]["$vectorSearch"]["limit"] == 3
    # Kept under the arguments given: another vector is another answer.
    assert named.call_args.args == (
        "atlas",
        "pipeline:similar",
        {"query_vector": "[0.25, 0.5]", "k": "3"},
    )
    landed = state.federation_engine.land_source_table.await_args.kwargs
    assert landed["columns"] == [("title", "text"), ("score", "double")]
    assert landed["rows"] == [{"title": "Up", "score": 0.91}, {"title": "Cars", "score": 0.5}]
    assert drop.call_args.args[3] == 120  # the table's TTL


async def test_a_read_without_a_required_argument_runs_nothing():
    from provisa.api.data.materialization import _materialize_api_to_engine_cache

    state = _state(_state_table())
    with patch("provisa.mongodb.pipeline_table.read", AsyncMock()) as ran:
        with pytest.raises(ValueError, match=r"requires the parameter\(s\) \['query_vector'\]"):
            await _materialize_api_to_engine_cache(
                "SELECT * FROM similar", state, nf_args={"k": 3}, table_ids=[TABLE]
            )
    ran.assert_not_awaited()


async def test_an_argument_of_the_wrong_type_runs_nothing():
    from provisa.api.data.materialization import _materialize_api_to_engine_cache
    from provisa.api_source.engine_cache import CacheLocation

    state = _state(_state_table())
    with (
        patch(
            "provisa.api_source.engine_cache.cache_location",
            return_value=CacheLocation("c", "s", "relational"),
        ),
        patch("provisa.mongodb.fetch.aggregate_row_batches") as ran,
        pytest.raises(ParameterValueRefused, match="'query_vector' takes a value of type array"),
    ):
        await _materialize_api_to_engine_cache(
            "SELECT * FROM similar",
            state,
            nf_args={"query_vector": '{"$gt": ""}'},
            table_ids=[TABLE],
        )
    ran.assert_not_called()


def test_a_pipeline_table_is_always_materialized():
    from provisa.api.data.materialization import would_materialize_optimize

    assert would_materialize_optimize(
        "SELECT * FROM similar", _state(_state_table()), table_ids=[TABLE]
    )


def test_a_pipeline_with_arguments_has_no_whole_table_to_replicate():
    from provisa.mongodb.pipeline_table import whole_batches

    table = SimpleNamespace(
        table_name="similar",
        query_template=_VECTOR_SEARCH,
        columns=[
            SimpleNamespace(
                name="_nf_k", data_type="integer", path=None, native_filter_type="query_arg"
            )
        ],
    )
    with pytest.raises(ValueError, match="function of its arguments"):
        next(whole_batches(object(), "db", table, 10))


# --- registration: config and the admin API refuse a pipeline table that cannot be read ---


def test_config_refuses_a_pipeline_table_naming_an_undeclared_argument():
    from provisa.core.config_loader import _validate_neo4j_sources

    table = _pipeline_table()
    table.columns = [c for c in table.columns if c.name != "_nf_genre"]
    config = SimpleNamespace(
        sources=[SimpleNamespace(id="atlas", type=SimpleNamespace(value="mongodb"))],
        tables=[table],
    )
    with pytest.raises(NativeQueryRefused, match=r"\['genre'\], which the table declares"):
        _validate_neo4j_sources(config)  # pyright: ignore[reportArgumentType]


def test_config_accepts_a_mongodb_table_with_or_without_a_native_query():
    from provisa.core.config_loader import _validate_neo4j_sources

    collection = Table(
        source_id="atlas",
        domain_id="films",
        schema_name="sample_mflix",
        table_name="movies",
        columns=[Column(name="title", visible_to=[], data_type="varchar")],
    )
    config = SimpleNamespace(
        sources=[SimpleNamespace(id="atlas", type=SimpleNamespace(value="mongodb"))],
        tables=[_pipeline_table(), collection],
    )
    _validate_neo4j_sources(config)  # pyright: ignore[reportArgumentType]


async def test_register_table_refuses_a_pipeline_that_writes_naming_the_table():
    from provisa.api.admin import _query_api_registration as registration

    table = _pipeline_table()
    table.query_template = json.dumps({"inputCollection": "movies", "pipeline": [{"$out": "copy"}]})
    table.columns = [c for c in table.columns if not c.native_filter_type]
    with patch.object(registration, "source_row", AsyncMock(return_value={"type": "mongodb"})):
        refused = await registration.pipeline_table_refusal(MagicMock(), table)
    assert refused is not None and refused.success is False
    assert refused.code == "schema.mongodb_native_query_invalid"
    assert "Table 'similar'" in refused.message and "$out" in refused.message


async def test_a_preview_runs_the_pipeline_with_sample_arguments():
    from provisa.api.admin import _query_api_registration as registration

    seen: dict = {}

    def batches(conn, database, collection, pipeline, batch_rows):
        seen["pipeline"] = pipeline
        yield [{"title": "Up", "score": 0.9, "_id": ObjectId("65f0c0ffee0000000000abcd")}]

    source = SimpleNamespace(id="atlas", database="sample_mflix")
    with (
        patch.object(registration, "source_row", AsyncMock(return_value={"type": "mongodb"})),
        patch(
            "provisa.federation.registry_view.registered_sources", AsyncMock(return_value=[source])
        ),
        patch("provisa.mongodb.fetch.MongoConnection.from_source", return_value=object()),
        patch("provisa.mongodb.fetch.aggregate_row_batches", side_effect=batches),
    ):
        preview = await registration.preview_pipeline(
            MagicMock(),
            "atlas",
            _VECTOR_SEARCH,
            [
                {"name": "query_vector", "dataType": "array(double)", "value": "[0.1, 0.2]"},
                {"name": "k", "dataType": "integer", "value": 5},
                {"name": "genre", "dataType": "varchar", "value": "Drama"},
            ],
        )
    assert preview.error is None
    assert seen["pipeline"][-1] == {"$limit": 5}
    assert preview.rows[0]["_id"] == {"$oid": "65f0c0ffee0000000000abcd"}
    assert {c.name: c.data_type for c in preview.columns} == {
        "title": "text",
        "score": "double",
        "_id": "text",
    }


async def test_a_preview_reports_a_half_written_pipeline_as_its_error():
    from provisa.api.admin import _query_api_registration as registration

    with patch.object(registration, "source_row", AsyncMock(return_value={"type": "mongodb"})):
        preview = await registration.preview_pipeline(
            MagicMock(), "atlas", '{"inputCollection":', []
        )
    assert preview.rows == [] and "not Extended JSON" in (preview.error or "")


def test_sql_gives_a_query_vector_as_an_array_literal():
    from provisa.compiler.nf_extractor import extract_nf_args

    sql, params, args = extract_nf_args(
        "SELECT title FROM similar WHERE similar._nf_query_vector = ARRAY[0.5, -0.25, 1] "
        "AND _nf_k = -3 AND genre = 'Drama'",
        [],
        {"_nf_query_vector", "_nf_k"},
    )
    assert args == {"query_vector": [0.5, -0.25, 1], "k": -3}
    assert sql == "SELECT title FROM similar WHERE genre = 'Drama'" and params == []
    assert convert(args["query_vector"], "array(double)", table="t", parameter="v") == [
        0.5,
        -0.25,
        1.0,
    ]
