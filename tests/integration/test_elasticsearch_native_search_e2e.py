# Copyright (c) 2026 Kenneth Stott
# Canary: 23978417-e49f-46ff-bda2-a01302acbccb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: Elasticsearch tables registered from native searches (REQ-2017), with their arguments as
table parameters (REQ-1986).

Everything here is the ``test`` instance: an Elasticsearch node this module starts on a leased
port and removes, and an isolated server on DuckDB and on Trino. Tables:

* ``reviews_matching``: a match_phrase search with a highlight, its phrase a required argument;
* ``ratings_by_product``: a terms aggregation with an average under it, a row per bucket, its
  minimum rating an optional argument;
* ``low_ratings``: a search with no arguments, which reads as a view;
* ``products``: the plain ``products`` index, joined to the search tables."""

# Requirements: REQ-2017, REQ-1986

from __future__ import annotations

import json
import os
import subprocess
import sys
import time

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration]

_ROLE = "org_admin"
_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch:8.13.0"
_PRODUCTS = [
    {"product_id": 1, "model": "Volt X"},
    {"product_id": 2, "model": "Spark 2"},
    {"product_id": 3, "model": "Glow"},
]
_REVIEWS = [
    {"review_id": "r1", "product_id": 1, "rating": 2, "text": "the battery dies after an hour"},
    {"review_id": "r2", "product_id": 1, "rating": 1, "text": "battery dies overnight, sad"},
    {"review_id": "r3", "product_id": 2, "rating": 5, "text": "great screen and a solid battery"},
    {"review_id": "r4", "product_id": 2, "rating": 4, "text": "the battery dies only when cold"},
    {"review_id": "r5", "product_id": 3, "rating": 3, "text": "fine for the price"},
]


class _Elastic:
    """A single Elasticsearch node, security off, on a leased port."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.port,) = lease_ports(1)
        self._name = f"provisa-itest-es-native-{os.getpid()}"
        # This process reads the node on the host's loopback. Trino, in the test stack's
        # container, reads the plain index through its Elasticsearch connector and cannot dial
        # the host's loopback: it reads the default bridge's gateway, a host address on Linux
        # (CI). Docker Desktop's gateway is inside its VM, so it is bound only where it is one.
        self.gateway = subprocess.run(
            ["docker", "network", "inspect", "bridge"]
            + ["--format", "{{(index .IPAM.Config 0).Gateway}}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        self.base = f"http://127.0.0.1:{self.port}"

    def start(self) -> None:
        subprocess.run(
            ["docker", "run", "-d", "--rm", "--memory", "1g", "--name", self._name]
            + ["-p", f"127.0.0.1:{self.port}:9200"]
            + ([] if sys.platform == "darwin" else ["-p", f"{self.gateway}:{self.port}:9200"])
            # Trino's connector reads metadata from the configured host but scans each shard at its
            # node's publish address -- by default the container's bridge address and port 9200,
            # which Trino's network does not reach. The node publishes the address Trino dials.
            + (
                []
                if sys.platform == "darwin"
                else [
                    "-e",
                    f"http.publish_host={self.gateway}",
                    "-e",
                    f"http.publish_port={self.port}",
                ]
            )
            + ["-e", "discovery.type=single-node", "-e", "xpack.security.enabled=false"]
            + ["-e", "ES_JAVA_OPTS=-Xms512m -Xmx512m", _IMAGE],
            check=True,
            capture_output=True,
        )
        deadline = time.monotonic() + 300
        while True:
            try:
                health = httpx.get(f"{self.base}/_cluster/health", timeout=5)
                if health.status_code == 200 and health.json()["status"] in ("green", "yellow"):
                    break
            except httpx.HTTPError:
                pass
            if time.monotonic() > deadline:
                raise TimeoutError(f"{_IMAGE} did not answer within 300 s")
            time.sleep(2)
        with httpx.Client(base_url=self.base, timeout=60) as c:
            for index, mapping, docs in (
                (
                    "products",
                    {"product_id": {"type": "integer"}, "model": {"type": "keyword"}},
                    _PRODUCTS,
                ),
                (
                    "reviews",
                    {
                        "review_id": {"type": "keyword"},
                        "product_id": {"type": "integer"},
                        "rating": {"type": "integer"},
                        "text": {"type": "text"},
                    },
                    _REVIEWS,
                ),
            ):
                created = c.put(f"/{index}", json={"mappings": {"properties": mapping}})
                assert created.status_code == 200, created.text
                lines = []
                for doc in docs:
                    lines.append(json.dumps({"index": {}}))
                    lines.append(json.dumps(doc))
                bulk = c.post(
                    f"/{index}/_bulk",
                    params={"refresh": "wait_for"},
                    content="\n".join(lines) + "\n",
                    headers={"Content-Type": "application/x-ndjson"},
                )
                assert bulk.status_code == 200 and not bulk.json()["errors"], bulk.text

    def stop(self) -> None:
        subprocess.run(["docker", "rm", "-f", self._name], capture_output=True)


@pytest.fixture(scope="module")
def elastic():
    node = _Elastic()
    try:
        node.start()
        yield node
    finally:
        node.stop()


def _arg(name: str, data_type: str, *, steward: bool = False) -> dict:
    return {
        "name": f"_nf_{name}",
        "data_type": data_type,
        "native_filter_type": "query_arg",
        "native_filter_required": False,
        "required_by_steward": steward,
        "visible_to": [],
    }


def _out(name: str, data_type: str, path: str | None = None) -> dict:
    col = {"name": name, "data_type": data_type, "visible_to": [_ROLE]}
    if path:
        col["path"] = path
    return col


def _config(elastic: _Elastic, engine: str) -> dict:
    matching = {
        "index": "reviews",
        "query": {"match_phrase": {"text": "{{phrase}}"}},
        "highlight": {"fields": {"text": {}}},
    }
    by_product = {
        "index": "reviews",
        "query": {"range": {"rating": {"gte": "{{min_rating}}"}}},
        "aggs": {
            "product_id": {
                "terms": {"field": "product_id"},
                "aggs": {"avg_rating": {"avg": {"field": "rating"}}},
            }
        },
    }
    low = {"index": "reviews", "query": {"range": {"rating": {"lte": 2}}}, "sort": ["review_id"]}
    return {
        "federation_engine": engine,
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "store", "description": "native search e2e"}],
        "roles": [],
        "sources": [
            {
                "id": "es",
                "type": "elasticsearch",
                "host": "127.0.0.1" if engine == "duckdb" else elastic.gateway,
                "port": elastic.port,
            }
        ],
        "tables": [
            {
                "source_id": "es",
                "domain_id": "store",
                "schema": "default",
                "table": "reviews_matching",
                "cache_ttl": 60,
                "query_template": json.dumps(matching),
                "columns": [
                    _out("hit_id", "varchar", "_id"),
                    _out("hit_score", "double", "_score"),
                    _out("review_id", "varchar"),
                    _out("product_id", "integer"),
                    _out("text", "varchar"),
                    _out("highlight_text", "json"),
                    _arg("phrase", "varchar", steward=True),
                ],
            },
            {
                "source_id": "es",
                "domain_id": "store",
                "schema": "default",
                "table": "ratings_by_product",
                "cache_ttl": 60,
                "query_template": json.dumps(by_product),
                "columns": [
                    _out("product_id", "bigint"),
                    _out("product_id_doc_count", "bigint"),
                    _out("avg_rating", "double"),
                    _arg("min_rating", "integer"),
                ],
            },
            {
                "source_id": "es",
                "domain_id": "store",
                "schema": "default",
                "table": "low_ratings",
                "cache_ttl": 60,
                "query_template": json.dumps(low),
                "columns": [
                    _out("review_id", "varchar"),
                    _out("product_id", "integer"),
                    _out("rating", "integer"),
                ],
            },
            {
                "source_id": "es",
                "domain_id": "store",
                "schema": "default",
                "table": "products",
                "cache_ttl": 60,
                "columns": [_out("product_id", "integer"), _out("model", "varchar")],
            },
        ],
        "relationships": [
            {
                "id": "matching-product",
                "source_table_id": "reviews_matching",
                "target_table_id": "products",
                "source_column": "product_id",
                "target_column": "product_id",
                "cardinality": "many-to-one",
            }
        ],
    }


@pytest.fixture(
    scope="module",
    params=["duckdb", pytest.param("trino", marks=pytest.mark.requires_trino)],
)
def server(elastic, tmp_path_factory, request):
    """An isolated server on each engine: a native search is run by Provisa's own reader on every
    engine, and the engine reads its landed rows (REQ-2017); on Trino the plain index is read by
    Trino's own Elasticsearch connector."""
    from tests.integration.isolated_server import IsolatedServer

    engine = request.param
    work = tmp_path_factory.mktemp(f"es_native_search_{engine}")
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(_config(elastic, engine)))
    if engine == "duckdb":
        srv = IsolatedServer(
            "es_native_search",
            engine="duckdb",
            config=str(path),
            control_plane="own",
            materialize_store_url=f"duckdb:///{work / 'store.duckdb'}",
        )
    else:
        # Trino from the test stack, with its Postgres control plane.
        srv = IsolatedServer("es_native_search_trino", engine="trino", config=str(path))
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


def _sql(server, sql: str) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}/data/sql",
        json={"sql": sql},
        headers={"X-Provisa-Role": _ROLE},
        timeout=server.request_timeout + 30,
    )


def _rows(resp: httpx.Response) -> list[dict]:
    assert resp.status_code == 200, resp.text
    return resp.json()["data"]["sql"]


def test_a_phrase_search_answers_its_hits_with_highlights(server):
    rows = _rows(
        _sql(
            server,
            "SELECT review_id, product_id, highlight_text FROM reviews_matching "
            "WHERE _nf_phrase = 'battery dies' ORDER BY review_id",
        )
    )
    assert [r["review_id"] for r in rows] == ["r1", "r2", "r4"]
    assert all("<em>battery" in json.dumps(r["highlight_text"]) for r in rows), rows


def test_a_search_joins_another_table(server):
    rows = _rows(
        _sql(
            server,
            "SELECT p.model, count(*) AS complaints FROM reviews_matching m "
            "JOIN products p ON p.product_id = m.product_id "
            "WHERE m._nf_phrase = 'battery dies' GROUP BY p.model ORDER BY p.model",
        )
    )
    assert rows == [{"model": "Spark 2", "complaints": 1}, {"model": "Volt X", "complaints": 2}]


def test_an_aggregation_answers_a_row_per_bucket(server):
    rows = _rows(
        _sql(
            server,
            "SELECT product_id, product_id_doc_count, avg_rating FROM ratings_by_product "
            "WHERE _nf_min_rating = 2 ORDER BY product_id",
        )
    )
    assert rows == [
        {"product_id": 1, "product_id_doc_count": 1, "avg_rating": 2.0},
        {"product_id": 2, "product_id_doc_count": 2, "avg_rating": 4.5},
        {"product_id": 3, "product_id_doc_count": 1, "avg_rating": 3.0},
    ]


def test_a_search_with_no_arguments_reads_as_a_view(server):
    rows = _rows(_sql(server, "SELECT review_id, rating FROM low_ratings ORDER BY review_id"))
    assert rows == [{"review_id": "r1", "rating": 2}, {"review_id": "r2", "rating": 1}]


def test_a_read_without_a_required_argument_is_refused_naming_it(server):
    resp = _sql(server, "SELECT review_id FROM reviews_matching")
    assert resp.status_code != 200
    assert "reviews_matching" in resp.text and "phrase" in resp.text


def test_an_argument_holding_query_syntax_is_only_text(server):
    hostile = '"}}, "match_all": {"'
    rows = _rows(
        _sql(server, f"SELECT review_id FROM reviews_matching WHERE _nf_phrase = '{hostile}'")
    )
    assert rows == []
