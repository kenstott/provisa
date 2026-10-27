# Copyright (c) 2026 Kenneth Stott
# Canary: f3a2b32f-cf5a-453a-95f6-47a7db066a04
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: MongoDB tables registered from aggregation pipelines, on Atlas Search and Atlas Vector
Search (REQ-1980), reached through a connection string (REQ-1985), with their arguments as table
parameters (REQ-1986).

Everything here is the ``test`` instance: a ``mongodb/mongodb-atlas-local`` server this module
starts on a leased port and removes (it runs mongot, so $search, $searchMeta and $vectorSearch
are served), and an isolated server on DuckDB and on Trino. The source is given by a mongodb:// connection
string. Tables:

* ``similar_movies``: ``$vectorSearch`` over ``movies.plot_embedding``, its query vector a
  required argument (array(double)) and its genre filter optional;
* ``movies_matching``: ``$search`` over the plots, its text a required argument;
* ``movie_hits``: ``$searchMeta``, the count of plots matching its text;
* ``all_genres``: a pipeline with no arguments, which reads as a view;
* ``reviews``: the plain ``reviews`` collection, joined to the pipeline tables."""

# Requirements: REQ-1980, REQ-1985, REQ-1986

from __future__ import annotations

import json
import os
import subprocess
import time

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.requires_trino, pytest.mark.integration]

_ROLE = "org_admin"
_ANALYST = "itest_analyst"
_IMAGE = "mongodb/mongodb-atlas-local:8.0"
# The code a search index command fails with while mongot is not yet answering.
_SEARCH_NOT_READY = 125
_DB = "films"

_MOVIES = [
    {
        "_id": 1,
        "title": "Up",
        "genre": "Animation",
        "plot": "an old man flies his house with balloons",
        "plot_embedding": [1.0, 0.0, 0.0],
    },
    {
        "_id": 2,
        "title": "Cars",
        "genre": "Animation",
        "plot": "a race car learns about friendship",
        "plot_embedding": [0.9, 0.1, 0.0],
    },
    {
        "_id": 3,
        "title": "Heat",
        "genre": "Crime",
        "plot": "a detective chases a crew of bank robbers",
        "plot_embedding": [0.0, 1.0, 0.0],
    },
    {
        "_id": 4,
        "title": "Fargo",
        "genre": "Crime",
        "plot": "a car salesman hires two criminals",
        "plot_embedding": [0.1, 0.9, 0.0],
    },
    {
        "_id": 5,
        "title": "Amour",
        "genre": "Drama",
        "plot": "an old couple faces illness",
        "plot_embedding": [0.0, 0.0, 1.0],
    },
]
_REVIEWS = [
    {"_id": 10, "movie_id": 1, "stars": 5},
    {"_id": 11, "movie_id": 1, "stars": 4},
    {"_id": 12, "movie_id": 2, "stars": 3},
    {"_id": 13, "movie_id": 3, "stars": 5},
]


def _native(pipeline: list[dict], collection: str = "movies") -> str:
    return json.dumps({"inputCollection": collection, "pipeline": pipeline})


def _arg(name: str, data_type: str, *, steward: bool = False) -> dict:
    return {
        "name": f"_nf_{name}",
        "data_type": data_type,
        "native_filter_type": "query_arg",
        "native_filter_required": False,
        "required_by_steward": steward,
        "visible_to": [],
    }


def _out(name: str, data_type: str, roles: list[str]) -> dict:
    return {"name": name, "data_type": data_type, "visible_to": roles}


class _Atlas:
    """A MongoDB Atlas local deployment (mongod and mongot) on a leased port."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.port,) = lease_ports(1)
        self._name = f"provisa-itest-atlas-{os.getpid()}"
        # One address for both readers: this process, and Trino in the test stack's container,
        # which reads ``reviews`` itself and cannot dial the host's loopback. The default bridge's
        # gateway is a host address, and the one the stack's host.docker.internal names
        # (docker-compose.e2e-mounts.yml).
        self.host = subprocess.run(
            ["docker", "network", "inspect", "bridge"]
            + ["--format", "{{(index .IPAM.Config 0).Gateway}}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    @property
    def uri(self) -> str:
        return f"mongodb://{self.host}:{self.port}/?directConnection=true"

    def client(self):
        import pymongo

        return pymongo.MongoClient(self.uri, serverSelectionTimeoutMS=120_000)

    def start(self) -> None:
        from pymongo.errors import OperationFailure
        from pymongo.operations import SearchIndexModel

        subprocess.run(
            ["docker", "run", "-d", "--rm", "--memory", "2g", "--name", self._name]
            + ["-p", f"{self.host}:{self.port}:27017", _IMAGE],
            check=True,
            capture_output=True,
        )
        # The image's entrypoint starts mongod for its own setup and then shuts it down to start
        # the deployment's mongod and mongot: a client that connects as soon as mongod answers is
        # talking to the setup server, and its commands fail "interrupted at shutdown" (code
        # 11600, CI 2026-10-13). The image's healthcheck is healthy only once the deployment
        # itself is up.
        deadline = time.monotonic() + 300
        while True:
            health = subprocess.run(
                ["docker", "inspect", "--format", "{{.State.Health.Status}}", self._name],
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
            if health == "healthy":
                break
            if time.monotonic() > deadline:
                raise TimeoutError(f"{_IMAGE} was not healthy after 300 s: {health}")
            time.sleep(2)
        with self.client() as c:
            db = c[_DB]
            db.movies.insert_many([dict(m) for m in _MOVIES])
            db.reviews.insert_many([dict(r) for r in _REVIEWS])
            # mongod answers before mongot, the search process beside it, is up: until it is, a
            # search index command fails with code 125 ("Error connecting to Search Index
            # Management service"). Wait for mongot itself before creating the indexes.
            deadline = time.monotonic() + 300
            while True:
                try:
                    list(db.movies.list_search_indexes())
                    break
                except OperationFailure as exc:
                    if exc.code != _SEARCH_NOT_READY or time.monotonic() > deadline:
                        raise
                    time.sleep(2)
            db.movies.create_search_index(
                SearchIndexModel(
                    definition={
                        "mappings": {"dynamic": False, "fields": {"plot": {"type": "string"}}}
                    },
                    name="plot_text",
                )
            )
            db.movies.create_search_index(
                SearchIndexModel(
                    definition={
                        "fields": [
                            {
                                "type": "vector",
                                "path": "plot_embedding",
                                "numDimensions": 3,
                                "similarity": "cosine",
                            },
                            {"type": "filter", "path": "genre"},
                        ]
                    },
                    name="plot_vec",
                    type="vectorSearch",
                )
            )
            # A search index serves queries once mongot has built it.
            deadline = time.monotonic() + 300
            while (
                not all(ix.get("queryable") for ix in db.movies.list_search_indexes())
                or len(list(db.movies.list_search_indexes())) < 2
            ):
                if time.monotonic() > deadline:
                    raise TimeoutError(list(db.movies.list_search_indexes()))
                time.sleep(2)

    def stop(self) -> None:
        subprocess.run(["docker", "rm", "-f", self._name], capture_output=True)


@pytest.fixture(scope="module")
def atlas():
    a = _Atlas()
    try:
        a.start()
        yield a
    finally:
        a.stop()


def _config(atlas: _Atlas, engine: str) -> dict:
    vector = _native(
        [
            {
                "$vectorSearch": {
                    "index": "plot_vec",
                    "path": "plot_embedding",
                    "queryVector": "{{query_vector}}",
                    "numCandidates": 20,
                    "limit": 3,
                }
            },
            {
                "$project": {
                    "movie_id": "$_id",
                    "_id": 0,
                    "title": 1,
                    "genre": 1,
                    "score": {"$meta": "vectorSearchScore"},
                }
            },
        ]
    )
    search = _native(
        [
            {"$search": {"index": "plot_text", "text": {"query": "{{q}}", "path": "plot"}}},
            {"$project": {"movie_id": "$_id", "_id": 0, "title": 1}},
        ]
    )
    meta = _native(
        [
            {
                "$searchMeta": {
                    "index": "plot_text",
                    "text": {"query": "{{q}}", "path": "plot"},
                    "count": {"type": "total"},
                }
            },
            {"$project": {"hits": "$count.total"}},
        ]
    )
    genres = _native(
        [
            {"$group": {"_id": "$genre", "movies": {"$sum": 1}}},
            {"$project": {"genre": "$_id", "_id": 0, "movies": 1}},
        ]
    )
    both = [_ROLE, _ANALYST]
    return {
        "federation_engine": engine,
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "films", "description": "pipeline tables e2e"}],
        "roles": [
            {
                "id": _ANALYST,
                "parents": ["analyst"],
                "description": "Test role.",
                "capabilities": ["query_development"],
                "domain_access": ["films"],
            }
        ],
        "sources": [
            # REQ-1985: the source is given by a connection string, with no port of its own.
            {"id": "atlas", "type": "mongodb", "host": atlas.uri, "port": 0, "database": _DB}
        ],
        "tables": [
            {
                "source_id": "atlas",
                "domain_id": "films",
                "schema": _DB,
                "table": "similar_movies",
                "cache_ttl": 60,
                "query_template": vector,
                "columns": [
                    _out("movie_id", "integer", both),
                    _out("title", "varchar", both),
                    _out("genre", "varchar", both),
                    _out("score", "double", [_ROLE]),  # the analyst is not granted the score
                    _arg("query_vector", "array(double)", steward=True),
                ],
            },
            {
                "source_id": "atlas",
                "domain_id": "films",
                "schema": _DB,
                "table": "movies_matching",
                "cache_ttl": 60,
                "query_template": search,
                "columns": [
                    _out("movie_id", "integer", both),
                    _out("title", "varchar", both),
                    _arg("q", "varchar", steward=True),
                ],
            },
            {
                "source_id": "atlas",
                "domain_id": "films",
                "schema": _DB,
                "table": "movie_hits",
                "cache_ttl": 60,
                "query_template": meta,
                "columns": [_out("hits", "bigint", both), _arg("q", "varchar", steward=True)],
            },
            {
                "source_id": "atlas",
                "domain_id": "films",
                "schema": _DB,
                "table": "all_genres",
                "cache_ttl": 60,
                "query_template": genres,
                "columns": [_out("genre", "varchar", both), _out("movies", "integer", both)],
            },
            {
                "source_id": "atlas",
                "domain_id": "films",
                "schema": _DB,
                "table": "reviews",
                "cache_ttl": 60,
                "columns": [
                    _out("_id", "integer", both),
                    _out("movie_id", "integer", both),
                    _out("stars", "integer", both),
                ],
            },
        ],
        # The join the tests make is a registered relationship: a join outside the registered
        # ones is refused to a role that may not ignore them (REQ-603).
        "relationships": [
            {
                "id": "similar-movie-reviews",
                "source_table_id": "similar_movies",
                "target_table_id": "reviews",
                "source_column": "movie_id",
                "target_column": "movie_id",
                "cardinality": "one-to-many",
            }
        ],
    }


@pytest.fixture(scope="module", params=["duckdb", "trino"])
def server(atlas, tmp_path_factory, request):
    """An isolated server on each engine: a pipeline is run by Provisa's own reader on every
    engine, and the engine reads its landed rows (REQ-1980)."""
    from tests.integration.isolated_server import IsolatedServer

    engine = request.param
    work = tmp_path_factory.mktemp(f"pipeline_tables_{engine}")
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(_config(atlas, engine)))
    if engine == "duckdb":
        srv = IsolatedServer(
            "mongodb_pipeline_tables",
            engine="duckdb",
            config=str(path),
            control_plane="own",
            materialize_store_url=f"duckdb:///{work / 'store.duckdb'}",
        )
    else:
        # Trino from the test stack, with its Postgres control plane.
        srv = IsolatedServer("mongodb_pipeline_tables_trino", engine="trino", config=str(path))
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


def _sql(server, sql: str, role: str = _ROLE) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}/data/sql",
        json={"sql": sql},
        headers={"X-Provisa-Role": role},
        timeout=server.request_timeout + 30,
    )


def _rows(resp: httpx.Response) -> list[dict]:
    assert resp.status_code == 200, resp.text
    return resp.json()["data"]["sql"]


_UP = "[1.0, 0.0, 0.0]"


def test_a_vector_search_table_reads_the_nearest_movies(server):
    rows = _rows(
        _sql(
            server,
            f"SELECT movie_id, title FROM similar_movies WHERE _nf_query_vector = '{_UP}' "
            "ORDER BY score DESC",
        )
    )
    assert [r["title"] for r in rows][:2] == ["Up", "Cars"]
    assert len(rows) == 3


def test_a_vector_search_table_joins_another_table(server):
    rows = _rows(
        _sql(
            server,
            "SELECT s.title, r.stars FROM similar_movies s JOIN reviews r ON r.movie_id = s.movie_id "
            f"WHERE s._nf_query_vector = '{_UP}' ORDER BY s.title, r.stars",
        )
    )
    assert rows == [
        {"title": "Cars", "stars": 3},
        {"title": "Up", "stars": 4},
        {"title": "Up", "stars": 5},
    ]


def test_a_text_search_table_and_its_count(server):
    rows = _rows(
        _sql(server, "SELECT title FROM movies_matching WHERE _nf_q = 'car' ORDER BY title")
    )
    assert [r["title"] for r in rows] == ["Cars", "Fargo"]
    hits = _rows(_sql(server, "SELECT hits FROM movie_hits WHERE _nf_q = 'old'"))
    assert hits == [{"hits": 2}]


def test_a_pipeline_with_no_arguments_reads_as_a_view(server):
    rows = _rows(_sql(server, "SELECT genre, movies FROM all_genres ORDER BY genre"))
    assert rows == [
        {"genre": "Animation", "movies": 2},
        {"genre": "Crime", "movies": 2},
        {"genre": "Drama", "movies": 1},
    ]


def test_a_read_without_a_required_argument_is_refused_naming_it(server):
    resp = _sql(server, "SELECT title FROM similar_movies")
    assert resp.status_code != 200
    assert "similar_movies" in resp.text and "query_vector" in resp.text


def test_an_argument_holding_operator_syntax_is_refused_as_its_type(server):
    resp = _sql(server, 'SELECT title FROM similar_movies WHERE _nf_query_vector = \'{"$gt": ""}\'')
    assert resp.status_code != 200
    assert "query_vector" in resp.text and "array(double)" in resp.text


def test_a_text_argument_holding_operator_syntax_is_only_text(server):
    rows = _rows(_sql(server, "SELECT title FROM movies_matching WHERE _nf_q = '{\"$ne\": null}'"))
    assert rows == []


def test_a_column_the_role_is_not_granted_is_not_served(server):
    resp = _sql(
        server, f"SELECT score FROM similar_movies WHERE _nf_query_vector = '{_UP}'", role=_ANALYST
    )
    assert resp.status_code != 200 and "score" in resp.text
    rows = _rows(
        _sql(
            server,
            f"SELECT title FROM similar_movies WHERE _nf_query_vector = '{_UP}' ORDER BY title",
            role=_ANALYST,
        )
    )
    assert {r["title"] for r in rows} == {"Up", "Cars", "Amour"} or len(rows) == 3


def _graphql(server, query: str, variables: dict | None = None) -> dict:
    resp = httpx.post(
        f"{server.base_url}/data/graphql",
        json={"query": query, "variables": variables or {}},
        headers={"X-Provisa-Role": _ROLE},
        timeout=server.request_timeout + 30,
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def test_graphql_reads_a_pipeline_table_with_its_arguments(server):
    fields = _graphql(
        server, "{ __schema { queryType { fields { name args { name type { kind } } } } } }"
    )
    field = next(
        f
        for f in fields["data"]["__schema"]["queryType"]["fields"]
        if f["name"].lower().replace("_", "") == "similarmovies"
    )
    arg = next(
        a for a in field["args"] if "query_vector" in a["name"] or "queryVector" in a["name"]
    )
    assert arg["type"]["kind"] == "NON_NULL"  # required by a steward
    answer = _graphql(
        server,
        f"{{ {field['name']}({arg['name']}: [1.0, 0.0, 0.0]) {{ title }} }}",
    )
    assert "errors" not in answer, answer
    assert answer["data"][field["name"]][0]["title"] == "Up"
