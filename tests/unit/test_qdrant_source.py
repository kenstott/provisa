# Copyright (c) 2026 Kenneth Stott
# Canary: 05c334bc-ff45-42ef-afab-68c7b7876cd9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Qdrant as a source (REQ-1983): its connection, its collection and search tables, a search's
arguments placed into the request as JSON values (never into its text), a text query embedded by
the search's registered model, and the points checked on the way out. The client is stood in
for: no instance is reached."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.federation.replica_address import ReplicaRoutes

from provisa.core.models import Column, Table
from provisa.qdrant.fetch import QdrantConnection, QdrantSourceInvalid
from provisa.qdrant.table import QdrantTableRefused, check_table, plan

_URL = "https://qdrant.example.com:6333"


def _source(**kw) -> SimpleNamespace:
    fields = {"id": "vec", "host": _URL, "password": "k3y", "federation_hints": {}}
    fields.update(kw)
    return SimpleNamespace(**fields)


def _point(pid, payload, score=None, vector=None) -> SimpleNamespace:
    return SimpleNamespace(id=pid, payload=payload, score=score, vector=vector)


class _Client:
    """Qdrant's client as the connector calls it: scroll pages, retrieve, query_points."""

    def __init__(self, points: list | None = None, page: int = 2) -> None:
        self.points = points or []
        self.page = page
        self.calls: list[tuple[str, dict]] = []

    def scroll(self, **kw):
        self.calls.append(("scroll", kw))
        start = kw.get("offset") or 0
        chunk = self.points[start : start + kw["limit"]]
        nxt = start + kw["limit"]
        return chunk, (nxt if nxt < len(self.points) else None)

    def retrieve(self, **kw):
        self.calls.append(("retrieve", kw))
        return [p for p in self.points if p.id in kw["ids"]]

    def query_points(self, **kw):
        self.calls.append(("query_points", kw))
        return SimpleNamespace(points=self.points[: kw["limit"]])


# --- the connection ---


def test_the_url_and_api_key_are_resolved_at_use(monkeypatch):
    monkeypatch.setenv("QDRANT_KEY", "from-env")
    conn = QdrantConnection.from_source(
        _source(password="${env:QDRANT_KEY}", federation_hints={"prefer_grpc": "true"})
    )
    assert (conn.url, conn.api_key, conn.prefer_grpc) == (_URL, "from-env", True)


def test_an_instance_without_an_api_key_is_reached_with_none():
    assert QdrantConnection.from_source(_source(password="")).api_key is None


def test_a_host_that_is_not_a_url_is_refused_naming_the_source():
    with pytest.raises(QdrantSourceInvalid, match=r"'vec': the host is the instance's URL"):
        QdrantConnection.from_source(_source(host="qdrant.example.com"))


# --- tables as registered ---

_SEARCH = json.dumps(
    {
        "collection": "docs",
        "query": "@q",
        "using": "text",
        "filter": {"must": [{"key": "lang", "match": {"value": "@lang"}}]},
        "limit": "@k",
    }
)


def _param(name: str, data_type: str = "text", required: bool = False) -> Column:
    return Column(
        name=f"_nf_{name}",
        visible_to=[],
        data_type=data_type,
        native_filter_type="query_arg",
        native_filter_required=required,
    )


def _q() -> Column:
    return _param("q", "array(double)", required=True)


def _table(search: str | None = _SEARCH, *params: Column) -> Table:
    return Table(
        source_id="vec",
        domain_id="kb",
        schema_name="default",
        table_name="docs_search" if search else "docs",
        query_template=search,
        columns=[
            Column(name="id", visible_to=["analyst"], data_type="text", path="@id"),
            Column(name="title", visible_to=["analyst"], data_type="text"),
            *params,
        ],
    )


def test_a_search_declares_each_of_its_arguments():
    search = check_table(_table(_SEARCH, _q(), _param("lang"), _param("k", "integer")))
    assert search is not None and search.collection == "docs"


def test_a_collection_table_has_no_search_and_no_arguments():
    assert check_table(_table(None)) is None
    with pytest.raises(QdrantTableRefused, match="a collection table has no search"):
        check_table(_table(None, _param("lang")))


@pytest.mark.parametrize(
    ("params", "reason"),
    [
        ((_q(), _param("lang")), r"names \['k'\], which the table declares as no argument"),
        (
            (_q(), _param("lang"), _param("k", "integer"), _param("extra")),
            r"\['extra'\] appear nowhere",
        ),
        (
            (_param("q", "text", required=True), _param("lang"), _param("k", "integer")),
            r"'q' is a vector, array\(double\)",
        ),
        (
            (_param("q", "array(double)"), _param("lang"), _param("k", "integer")),
            "'q' is required: Qdrant requires a query",
        ),
    ],
)
def test_a_search_that_cannot_be_read_is_refused(params, reason):
    with pytest.raises(QdrantTableRefused, match=reason):
        check_table(_table(_SEARCH, *params))


@pytest.mark.parametrize(
    ("search", "reason"),
    [
        ("not json", "the search is not JSON"),
        (json.dumps({"collection": "docs", "query": "@q", "top": 3}), r"not \['top'\]"),
        (json.dumps({"query": "@q"}), "names its collection"),
        (json.dumps({"collection": "docs", "query": [0.1]}), 'query is an argument, "@name"'),
        (json.dumps({"collection": "docs", "query": "@q", "limit": "ten"}), "limit is a number"),
        (json.dumps({"collection": "docs", "query": "@q", "model": "@m"}), "model names"),
    ],
)
def test_a_search_template_is_checked(search, reason):
    with pytest.raises(QdrantTableRefused, match=reason):
        check_table(_table(search, _q()))


def _state_table(search: str | None = _SEARCH, *params: tuple[str, str, bool]) -> dict:
    return {
        "id": 4,
        "source_id": "vec",
        "schema_name": "default",
        "table_name": "docs_search" if search else "docs",
        "query_template": search,
        "change_signal": None,
        "columns": [
            {"column_name": "id", "data_type": "text", "native_filter_type": None, "path": "@id"},
            {
                "column_name": "score",
                "data_type": "double",
                "native_filter_type": None,
                "path": "@score",
            },
            {"column_name": "title", "data_type": "text", "native_filter_type": None, "path": None},
            {
                "column_name": "published",
                "data_type": "timestamp",
                "native_filter_type": None,
                "path": "meta.published",
            },
            *(
                {
                    "column_name": f"_nf_{n}",
                    "data_type": t,
                    "native_filter_type": "query_arg",
                    "native_filter_required": req,
                    "path": None,
                }
                for n, t, req in params
            ),
        ],
    }


_ARGS = (("q", "array(double)", True), ("lang", "text", False), ("k", "integer", False))


async def test_a_search_places_its_arguments_into_the_request_as_values():
    planned = await plan(
        SimpleNamespace(),
        _state_table(_SEARCH, *_ARGS),
        {
            "q": "[0.1, 0.2]",
            "lang": "en",
            "k": 3,
        },
    )
    assert planned == {
        "collection": "docs",
        "query": [0.1, 0.2],
        "using": "text",
        "filter": {"must": [{"key": "lang", "match": {"value": "en"}}]},
        "limit": 3,
        "score_threshold": None,
    }


async def test_an_optional_argument_not_given_filters_nothing_and_the_limit_is_qdrants():
    planned = await plan(SimpleNamespace(), _state_table(_SEARCH, *_ARGS), {"q": [1, 0]})
    assert (planned["filter"], planned["limit"]) == (None, 10)


async def test_an_argument_is_converted_strictly_to_its_type():
    from provisa.core.parameter_values import ParameterValueRefused

    with pytest.raises(ParameterValueRefused):
        await plan(SimpleNamespace(), _state_table(_SEARCH, *_ARGS), {"q": [1, 0], "k": "three"})


async def test_a_text_query_needs_the_searchs_model():
    with pytest.raises(QdrantTableRefused, match="a text query needs the search's embedding model"):
        await plan(SimpleNamespace(), _state_table(_SEARCH, *_ARGS), {"q": "brown dogs"})


_MODEL_SEARCH = json.dumps({"collection": "docs", "query": "@q", "model": "mini"})


async def test_a_text_query_is_embedded_by_the_searchs_registered_model():
    from provisa.vector.registry import VectorModel

    model = VectorModel(id="mini", provider="openai", dimensions=2)
    provider = MagicMock()
    provider.embed = AsyncMock(return_value=[[0.5, 0.25]])
    table = _state_table(_MODEL_SEARCH, ("q", "array(double)", True))
    with (
        patch("provisa.qdrant.table.registered_model", AsyncMock(return_value=model)),
        patch("provisa.vector.providers.get_provider", return_value=provider),
    ):
        planned = await plan(SimpleNamespace(), table, {"q": "brown dogs"})
    provider.embed.assert_awaited_once_with(["brown dogs"], model)
    assert planned["query"] == [0.5, 0.25]


async def test_a_vector_of_another_size_than_the_models_is_refused():
    from provisa.vector.registry import VectorModel, VectorModelError

    model = VectorModel(id="mini", provider="openai", dimensions=3)
    table = _state_table(_MODEL_SEARCH, ("q", "array(double)", True))
    with patch("provisa.qdrant.table.registered_model", AsyncMock(return_value=model)):
        with pytest.raises(VectorModelError, match="2 dimensions; model 'mini' expects 3"):
            await plan(SimpleNamespace(), table, {"q": [0.1, 0.2]})


async def test_a_model_the_org_has_not_registered_is_refused_by_name():
    from provisa.qdrant.table import registered_model
    from provisa.vector.registry import VectorModelError

    cfg = {"vector_models": [{"id": "other", "provider": "openai", "dimensions": 2}]}
    with patch("provisa.api.mcp.tools.effective_config", AsyncMock(return_value=cfg)):
        with pytest.raises(VectorModelError, match="'mini' is not registered"):
            await registered_model(SimpleNamespace(), "mini")


async def test_a_search_reads_the_nearest_points_with_their_scores_and_checks_them():
    from provisa.qdrant.table import read

    client = _Client(
        [
            _point("a1", {"title": "Dogs", "meta": {"published": "2026-01-02T03:04:05Z"}}, 0.91),
            _point("a2", {"title": "Cats"}, 0.72),
        ]
    )
    with (
        patch(
            "provisa.federation.registry_view.registered_sources",
            AsyncMock(return_value=[_source()]),
        ),
        patch.object(QdrantConnection, "client", lambda self: client),
    ):
        answer = await read(SimpleNamespace(), _state_table(_SEARCH, *_ARGS), {"q": [1, 0]})
    ((kind, sent),) = client.calls
    assert kind == "query_points"
    assert (sent["collection_name"], sent["query"], sent["using"], sent["limit"]) == (
        "docs",
        [1.0, 0.0],
        "text",
        10,
    )
    assert sent["query_filter"] is None and sent["with_vectors"] is False
    assert answer.rows == [
        {
            "id": "a1",
            "score": 0.91,
            "title": "Dogs",
            "published": datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        },
        {"id": "a2", "score": 0.72, "title": "Cats", "published": None},
    ]
    assert answer.columns == [
        ("id", "text"),
        ("score", "double"),
        ("title", "text"),
        ("published", "timestamp"),
    ]


async def test_a_point_whose_payload_is_not_its_columns_type_is_refused_naming_it():
    from provisa.mongodb.pipeline_rows import PipelineOutputMismatch
    from provisa.qdrant.table import read

    client = _Client([_point("a1", {"title": 7}, 0.9)])
    with (
        patch(
            "provisa.federation.registry_view.registered_sources",
            AsyncMock(return_value=[_source()]),
        ),
        patch.object(QdrantConnection, "client", lambda self: client),
    ):
        with pytest.raises(PipelineOutputMismatch, match="column 'title', which is text"):
            await read(SimpleNamespace(), _state_table(_SEARCH, *_ARGS), {"q": [1, 0]})


async def test_a_search_with_a_ttl_of_its_own_is_landed_under_its_arguments():
    """A search is parameterized: its rows are kept only when the table sets a TTL of its own,
    and then under the arguments that read them (REQ-1986)."""
    from provisa.api.data.materialization import _materialize_api_to_engine_cache
    from provisa.api_source.engine_cache import CacheLocation
    from provisa.mongodb.pipeline_table import PipelineRead

    table = {**_state_table(_SEARCH, *_ARGS), "cache_ttl": 300}
    engine = MagicMock()
    engine.land_source_table = AsyncMock()
    state = SimpleNamespace(
        hot_manager=None,
        api_endpoints={},
        graphql_remote_sources={},
        source_types={"vec": "qdrant"},
        replica_routes=ReplicaRoutes(),
        tables=[table],
        federation_engine=engine,
        source_cache={},
        response_cache_default_ttl=300,
    )
    answer = PipelineRead(rows=[{"id": "a1"}], columns=[("id", "text")])
    loc = CacheLocation("cat", "sch", "relational")
    with (
        patch("provisa.qdrant.table.read", AsyncMock(return_value=answer)) as ran,
        patch("provisa.api_source.engine_cache.cache_location", return_value=loc),
        patch("provisa.api_source.engine_cache.table_known_live", return_value=False),
        patch("provisa.api_source.engine_cache.ensure_cache_schema"),
        patch("provisa.api_source.engine_cache.analyze_cache_table", AsyncMock()),
        patch("provisa.api_source.engine_cache.schedule_drop"),
        patch("provisa.api_source.engine_cache.cache_table_name", return_value="c_1") as named,
    ):
        rewrites, _, _ = await _materialize_api_to_engine_cache(
            "SELECT * FROM docs_search", state, nf_args={"q": "[1, 0]"}, table_ids=[4]
        )
    assert rewrites == {"docs_search": (loc, "c_1")}
    ran.assert_awaited_once()
    assert named.call_args.args == ("vec", "pipeline:docs_search", {"q": "[1, 0]"})


async def test_a_search_with_no_ttl_of_its_own_is_read_live_for_the_statement_alone():
    """REQ-1986: whatever its source or the organisation keeps, a search with no TTL of its own
    is read for each statement and handed to it inline: nothing is landed, kept or reused."""
    from provisa.api.data.materialization import _materialize_api_to_engine_cache
    from provisa.mongodb.pipeline_table import PipelineRead

    table = _state_table(_SEARCH, *_ARGS)
    engine = MagicMock()
    engine.land_source_table = AsyncMock()
    state = SimpleNamespace(
        hot_manager=None,
        api_endpoints={},
        graphql_remote_sources={},
        source_types={"vec": "qdrant"},
        replica_routes=ReplicaRoutes(),
        tables=[table],
        federation_engine=engine,
        source_cache={"vec": {"cache_ttl": 600}},
        response_cache_default_ttl=300,
    )
    answer = PipelineRead(rows=[{"id": "a1"}], columns=[("id", "text")])
    with patch("provisa.qdrant.table.read", AsyncMock(return_value=answer)) as ran:
        for _ in range(2):
            rewrites, inline, _ = await _materialize_api_to_engine_cache(
                "SELECT * FROM docs_search", state, nf_args={"q": "[1, 0]"}, table_ids=[4]
            )
            assert rewrites == {} and inline["docs_search"].rows == [{"id": "a1"}]
    assert ran.await_count == 2  # one read per statement
    engine.land_source_table.assert_not_awaited()


async def test_a_search_left_without_its_query_is_refused_before_any_read():
    from provisa.api.data.materialization import _materialize_api_to_engine_cache

    state = SimpleNamespace(
        hot_manager=None,
        api_endpoints={},
        graphql_remote_sources={},
        source_types={"vec": "qdrant"},
        replica_routes=ReplicaRoutes(),
        tables=[_state_table(_SEARCH, *_ARGS)],
    )
    with patch("provisa.qdrant.table.read", AsyncMock()) as ran:
        with pytest.raises(ValueError, match=r"'docs_search' requires the parameter\(s\) \['q'\]"):
            await _materialize_api_to_engine_cache(
                "SELECT * FROM docs_search", state, nf_args={}, table_ids=[4]
            )
    ran.assert_not_awaited()


# --- live-only (REQ-2000) ---


def test_qdrant_is_live_only_and_owns_no_replica():
    from provisa.core.source_registry import is_live_only
    from provisa.federation.replica_routing import owns_replica, reads_replica

    source = SimpleNamespace(type="qdrant", replicate=0, load_protected=None)
    table = SimpleNamespace(replicate=0, load_protected=None)
    assert is_live_only("qdrant") and not is_live_only("postgresql")
    assert not owns_replica(source)
    assert not reads_replica(source, table, engine=None, promoted=True)


def test_a_qdrant_table_has_no_loader_and_no_keyed_fetch():
    import importlib.util

    from provisa.events.source_loader import _ADAPTER_FETCH_ONLY

    assert "qdrant" not in _ADAPTER_FETCH_ONLY
    assert importlib.util.find_spec("provisa.qdrant.loader") is None


def _ttl_table(**kw) -> Any:
    from provisa.api.admin._landing_ttl import TableTtl

    fields = dict(
        schema_name="qdrant",
        table_name="docs",
        change_signal=None,
        cache_ttl=None,
        materialize=False,
        row_materialize=False,
        replicate=None,
        load_protected=None,
    )
    return TableTtl(**{**fields, **kw})


@pytest.mark.parametrize(
    "source_replicate, table, refused",
    [
        (None, {}, None),
        (0, {}, "Source 'vec'"),
        (None, {"replicate": 500}, "Table 'docs'"),
        (None, {"row_materialize": True}, "Table 'docs'"),
    ],
)
def test_a_live_only_source_refuses_any_replication_setting(source_replicate, table, refused):
    from provisa.api.admin._landing_ttl import SourceTtl, _live_only_refusal

    source = SourceTtl("ttl", None, source_replicate, False, False)
    result = _live_only_refusal("vec", "qdrant", source, [_ttl_table(**table)])
    if refused is None:
        assert result is None
    else:
        assert result is not None and not result.success
        assert result.message == (
            f"{refused}: qdrant is live-only: its tables are read live, never replicated"
        )
        assert result.code == "schema.live_only_replicated"


# --- what Register Table offers ---


async def test_a_collections_columns_are_its_id_its_payload_and_its_vectors():
    from provisa.qdrant import introspect
    from provisa.qdrant.fetch import CollectionShape

    client = _Client(
        [
            _point(1, {"title": "a", "lang": "en", "meta": {"pages": 3}}),
            _point(2, {"title": "b", "lang": "de", "meta": {"pages": 5}}),
        ]
    )
    shape = CollectionShape(
        vectors={"": (4, "Cosine"), "image": (8, "Dot")},
        indexed={"lang": "keyword", "published": "datetime"},
    )
    with (
        patch(
            "provisa.federation.registry_view.registered_sources",
            AsyncMock(return_value=[_source()]),
        ),
        patch.object(QdrantConnection, "client", lambda self: client),
        patch("provisa.qdrant.fetch.collection_shape", return_value=shape),
    ):
        cols = await introspect.columns(SimpleNamespace(), "vec", "docs")
        vectors = await introspect.vectors(SimpleNamespace(), "vec", "docs")
    assert [(c["name"], c["data_type"], c["path"], c["is_primary_key"]) for c in cols] == [
        ("id", "bigint", "@id", True),
        ("title", "text", None, False),
        ("lang", "text", None, False),
        ("meta_pages", "integer", "meta.pages", False),
        ("published", "timestamp", None, False),
        ("vector", "json", "@vector", False),
        ("image_vector", "json", "@vector.image", False),
    ]
    assert vectors == [
        {"name": "", "size": 4, "distance": "Cosine"},
        {"name": "image", "size": 8, "distance": "Dot"},
    ]


def test_qdrant_has_a_direct_driver_and_is_never_landed():
    """REQ-2000: a statement on one Qdrant source runs there, through its direct driver; the
    engine never reads it from a landed replica."""
    from provisa.executor.drivers.registry import has_native_driver
    from provisa.federation.strategy import _MATERIALIZE_ONLY
    from provisa.transpiler.router import VIRTUAL_SOURCES

    assert "qdrant" not in _MATERIALIZE_ONLY
    assert "qdrant" not in VIRTUAL_SOURCES
    assert has_native_driver("qdrant")


def test_a_search_in_a_config_is_checked_at_load():
    from provisa.core.config_loader import _validate_neo4j_sources

    config = SimpleNamespace(
        sources=[SimpleNamespace(id="vec", type=SimpleNamespace(value="qdrant"))],
        tables=[_table(_SEARCH, _q())],
    )
    with pytest.raises(QdrantTableRefused, match=r"names \['k', 'lang'\]"):
        _validate_neo4j_sources(config)  # type: ignore[arg-type]
