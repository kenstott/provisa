# Copyright (c) 2026 Kenneth Stott
# Canary: 9c4e1f7b-2d8a-4b6c-8e3f-5a7d9b1c0e42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Neo4j sources persist (REQ-1668): config validation, column typing, the endpoint shape, and the
loader's round trip of the query-API columns."""

from __future__ import annotations

import json
from typing import Any

import pytest

from provisa.api_source.models import ApiColumnType, ApiSourceType
from provisa.core.config_loader import _validate_neo4j_sources, parse_config_dict
from provisa.core.models import Column
from provisa.neo4j.persist import api_column_type, api_columns_from_config
from provisa.neo4j.source import Neo4jSourceConfig, build_api_source, build_endpoint

_VIS = ["org_admin"]


def _config(*, tables: list[dict], sources: list[dict] | None = None) -> dict:
    return {
        "sources": sources
        or [
            {
                "id": "graph",
                "type": "neo4j",
                "host": "localhost",
                "port": 7474,
                "database": "neo4j",
            }
        ],
        "domains": [{"id": "d"}],
        "roles": [],
        "naming": {"domain_prefix": False, "rules": []},
        "tables": tables,
    }


def _table(**over: Any) -> dict:
    base = {
        "source_id": "graph",
        "domain_id": "d",
        "schema": "neo4j",
        "table": "adopter",
        "query_template": "MATCH (a:Adopter) RETURN a.id AS id",
        "columns": [{"name": "id", "data_type": "integer", "visible_to": _VIS}],
    }
    base.update(over)
    return base


class TestValidateNeo4jSources:
    def test_valid_source_and_table_pass(self):
        _validate_neo4j_sources(parse_config_dict(_config(tables=[_table()])))

    def test_table_under_neo4j_source_requires_query_template(self):
        cfg = parse_config_dict(_config(tables=[_table(query_template=None)]))
        with pytest.raises(ValueError, match="requires query_template"):
            _validate_neo4j_sources(cfg)

    def test_query_template_is_refused_under_another_source_type(self):
        cfg = parse_config_dict(
            _config(
                sources=[{"id": "graph", "type": "sqlite", "path": "x.db"}],
                tables=[_table(schema="main")],
            )
        )
        with pytest.raises(ValueError, match="only valid under a neo4j source"):
            _validate_neo4j_sources(cfg)

    @pytest.mark.parametrize("missing", ["host", "port", "database"])
    def test_neo4j_source_names_host_port_database(self, missing: str):
        src = {"id": "graph", "type": "neo4j", "host": "h", "port": 7474, "database": "neo4j"}
        del src[missing]
        cfg = parse_config_dict(_config(sources=[src], tables=[_table()]))
        with pytest.raises(ValueError, match=missing):
            _validate_neo4j_sources(cfg)


class TestApiColumnType:
    @pytest.mark.parametrize(
        ("data_type", "expected"),
        [
            ("varchar", ApiColumnType.string),
            ("VARCHAR(120)", ApiColumnType.string),
            ("text", ApiColumnType.string),
            ("integer", ApiColumnType.integer),
            ("bigint", ApiColumnType.integer),
            ("double", ApiColumnType.number),
            ("decimal(10,2)", ApiColumnType.number),
            ("boolean", ApiColumnType.boolean),
            ("json", ApiColumnType.jsonb),
        ],
    )
    def test_known_types_map(self, data_type: str, expected: ApiColumnType):
        assert api_column_type(data_type) is expected

    def test_unknown_type_is_refused(self):
        with pytest.raises(ValueError, match="no API column type"):
            api_column_type("geography")

    def test_columns_from_config_carry_name_and_type(self):
        cols = api_columns_from_config(
            [
                Column(name="id", data_type="integer", visible_to=_VIS),
                Column(name="name", data_type="varchar", visible_to=_VIS),
            ]
        )
        assert [(c.name, c.type) for c in cols] == [
            ("id", ApiColumnType.integer),
            ("name", ApiColumnType.string),
        ]

    def test_untyped_column_is_refused(self):
        with pytest.raises(ValueError, match="requires data_type"):
            api_columns_from_config([Column(name="id", visible_to=_VIS)])


class TestEndpointShape:
    def test_source_type_is_neo4j(self):
        src = build_api_source(Neo4jSourceConfig(source_id="g", host="h", port=7474))
        assert src.type is ApiSourceType.neo4j

    def test_endpoint_posts_the_cypher_to_the_transaction_api(self):
        """Path, body and normalizer agree with ``preview_query`` and ``call_api``: the
        ``{"statements": [{"statement": cypher}]}`` envelope POSTed to ``/db/<db>/tx/commit``,
        flattened by ``neo4j_tabular``."""
        cfg = Neo4jSourceConfig(source_id="g", host="h", port=7474, database="movies")
        ep = build_endpoint(cfg, "actor", "MATCH (a:Actor) RETURN a.name AS name", [])
        assert ep.path == "/db/movies/tx/commit"
        assert ep.method == "POST"
        assert ep.body_encoding == "neo4j_tx"
        assert ep.query_template == "MATCH (a:Actor) RETURN a.name AS name"
        assert ep.response_normalizer == "neo4j_tabular"

    @pytest.mark.asyncio
    async def test_call_api_posts_the_transaction_envelope(self, monkeypatch):
        """``call_api`` sends ``{"statements": [{"statement": cypher}]}`` for ``neo4j_tx``."""
        import httpx
        import respx

        from provisa.api_source.caller import call_api

        cfg = Neo4jSourceConfig(source_id="g", host="h", port=7474)
        ep = build_endpoint(cfg, "actor", "MATCH (a) RETURN a.name AS name", [])
        seen: dict = {}

        def _capture(request: httpx.Request) -> httpx.Response:
            seen["body"] = json.loads(request.content)
            return httpx.Response(
                200,
                json={"results": [{"columns": ["name"], "data": [{"row": ["x"]}]}], "errors": []},
            )

        with respx.mock:
            respx.post("http://h:7474/db/neo4j/tx/commit").mock(side_effect=_capture)
            pages = await call_api(ep, {}, base_url="http://h:7474")
        assert seen["body"] == {"statements": [{"statement": "MATCH (a) RETURN a.name AS name"}]}
        assert pages[0]["results"][0]["data"][0]["row"] == ["x"]

    def test_tabular_normalizer_refuses_a_cypher_error(self):
        from provisa.api_source.normalizers import neo4j_tabular

        with pytest.raises(ValueError, match="neo4j query failed"):
            neo4j_tabular(
                {"results": [], "errors": [{"code": "Neo.ClientError.Statement.SyntaxError"}]}
            )


class _Conn:
    """The two queries ``load_api_sources`` runs, answered from canned rows."""

    def __init__(self, src_rows: list[dict], ep_rows: list[dict]):
        self._src_rows, self._ep_rows = src_rows, ep_rows

    async def fetch(self, sql: str, *args: Any) -> list[dict]:
        return self._src_rows if "FROM api_sources" in sql else self._ep_rows


@pytest.mark.asyncio
async def test_loader_restores_query_api_columns():
    """A persisted neo4j endpoint comes back with its body encoding, query and normalizer — without
    them the hydrated endpoint would GET a query URL with no body."""
    from provisa.api_source.loader import load_api_sources

    conn = _Conn(
        [{"id": "g", "type": "neo4j", "base_url": "http://h:7474", "spec_url": None, "auth": None}],
        [
            {
                "id": 1,
                "source_id": "g",
                "path": "/db/neo4j/tx/commit",
                "method": "POST",
                "table_name": "adopter",
                "columns": json.dumps([{"name": "id", "type": "integer"}]),
                "ttl": 60,
                "response_root": None,
                "error_path": None,
                "pk_column": None,
                "pagination": None,
                "max_concurrency": None,
                "default_params": None,
                "promotions": "[]",
                "body_encoding": "neo4j_tx",
                "query_template": "MATCH (a) RETURN a.id AS id",
                "response_normalizer": "neo4j_tabular",
            }
        ],
    )
    endpoints, sources = await load_api_sources(conn, [], {}, [], {})  # type: ignore[arg-type]
    assert sources["g"].type is ApiSourceType.neo4j
    ep = endpoints["adopter"]
    assert (ep.method, ep.body_encoding, ep.response_normalizer) == (
        "POST",
        "neo4j_tx",
        "neo4j_tabular",
    )
    assert ep.query_template == "MATCH (a) RETURN a.id AS id"
