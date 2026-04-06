# Copyright (c) 2026 Kenneth Stott
# Canary: a2b4c6d8-e0f2-4816-9a2b-c4d6e8f0a2b4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for SPARQL source builder (Phase AO, REQ-295–299).

Pure-Python tests cover extract_variables, _probe_limit, build_api_source,
build_endpoint, and infer_columns. Live-service tests (pytest.mark.requires_sparql)
verify actual query execution against a Fuseki/Blazegraph instance.

To run live tests:
    docker compose up fuseki   # adds Apache Jena Fuseki to the compose stack
    pytest tests/integration/test_sparql_exec.py -m requires_sparql
"""

from __future__ import annotations

import pytest

from provisa.api_source.models import ApiColumn, ApiColumnType, ApiSourceType
from provisa.sparql.source import (
    SparqlSourceConfig,
    _probe_limit,
    build_api_source,
    build_endpoint,
    extract_variables,
    infer_columns,
)

pytestmark = [pytest.mark.integration]


# ---------------------------------------------------------------------------
# extract_variables — pure Python
# ---------------------------------------------------------------------------

class TestExtractVariables:
    def test_single_variable(self):
        q = "SELECT ?name WHERE { ?x rdfs:label ?name }"
        assert extract_variables(q) == ["name"]

    def test_multiple_variables(self):
        q = "SELECT ?id ?name ?email WHERE { ?p foaf:name ?name ; foaf:mbox ?email . BIND(str(?p) AS ?id) }"
        result = extract_variables(q)
        assert "id" in result
        assert "name" in result
        assert "email" in result

    def test_star_returns_empty(self):
        q = "SELECT * WHERE { ?s ?p ?o }"
        assert extract_variables(q) == []

    def test_distinct_modifier(self):
        q = "SELECT DISTINCT ?city WHERE { ?x ex:city ?city }"
        assert extract_variables(q) == ["city"]

    def test_reduced_modifier(self):
        q = "SELECT REDUCED ?name WHERE { ?x rdfs:label ?name }"
        assert extract_variables(q) == ["name"]

    def test_no_select_returns_empty(self):
        q = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"
        assert extract_variables(q) == []

    def test_case_insensitive_select(self):
        q = "select ?x WHERE { ?x a ex:Thing }"
        assert extract_variables(q) == ["x"]

    def test_multiline_select(self):
        q = """
        SELECT
          ?subject
          ?predicate
        WHERE {
          ?subject ?predicate ?object
        }
        """
        result = extract_variables(q)
        assert "subject" in result
        assert "predicate" in result


# ---------------------------------------------------------------------------
# _probe_limit — pure Python
# ---------------------------------------------------------------------------

class TestProbeLimit:
    def test_appends_limit(self):
        q = "SELECT ?x WHERE { ?x a ex:Thing }"
        probed = _probe_limit(q, limit=5)
        assert "LIMIT 5" in probed.upper()

    def test_does_not_duplicate_existing_limit(self):
        q = "SELECT ?x WHERE { ?x a ex:Thing } LIMIT 100"
        probed = _probe_limit(q, limit=5)
        # Should not add another LIMIT
        assert probed.upper().count("LIMIT") == 1

    def test_strips_trailing_semicolon(self):
        q = "SELECT ?x WHERE { ?x a ex:Thing };"
        probed = _probe_limit(q, limit=3)
        assert "LIMIT 3" in probed
        assert probed.index("LIMIT") > probed.index("WHERE")

    def test_custom_limit_value(self):
        q = "SELECT ?x WHERE { ?x a ex:Thing }"
        probed = _probe_limit(q, limit=10)
        assert "LIMIT 10" in probed.upper()

    def test_default_limit_is_five(self):
        q = "SELECT ?x WHERE { ?x a ex:Thing }"
        probed = _probe_limit(q)
        assert "LIMIT 5" in probed.upper()


# ---------------------------------------------------------------------------
# build_api_source — no live service required
# ---------------------------------------------------------------------------

class TestBuildApiSource:
    def test_base_url_extracted_from_endpoint(self):
        cfg = SparqlSourceConfig(
            source_id="fuseki-1",
            endpoint_url="http://localhost:3030/ds/sparql",
        )
        source = build_api_source(cfg)
        assert source.base_url == "http://localhost:3030"

    def test_source_id_set(self):
        cfg = SparqlSourceConfig(
            source_id="sparql-test",
            endpoint_url="http://host:3030/sparql",
        )
        source = build_api_source(cfg)
        assert source.id == "sparql-test"

    def test_source_type_is_openapi(self):
        cfg = SparqlSourceConfig(source_id="s", endpoint_url="http://host/sparql")
        source = build_api_source(cfg)
        assert source.type == ApiSourceType.openapi


# ---------------------------------------------------------------------------
# build_endpoint — no live service required
# ---------------------------------------------------------------------------

class TestBuildEndpoint:
    def _cfg(self, endpoint_url="http://localhost:3030/ds/sparql"):
        return SparqlSourceConfig(source_id="fuseki-1", endpoint_url=endpoint_url)

    def test_builds_endpoint(self):
        cfg = self._cfg()
        ep = build_endpoint(
            cfg, "people", "SELECT ?name WHERE { ?x foaf:name ?name }",
            [ApiColumn(name="name", type=ApiColumnType.string)],
        )
        from provisa.api_source.models import ApiEndpoint
        assert isinstance(ep, ApiEndpoint)

    def test_path_extracted_from_endpoint_url(self):
        cfg = self._cfg("http://localhost:3030/ds/sparql")
        ep = build_endpoint(cfg, "t", "SELECT ?x WHERE { ?x a ex:T }",
                            [ApiColumn(name="x", type=ApiColumnType.string)])
        assert ep.path == "/ds/sparql"

    def test_method_is_post(self):
        cfg = self._cfg()
        ep = build_endpoint(cfg, "t", "SELECT ?x WHERE { ?x a ex:T }",
                            [ApiColumn(name="x", type=ApiColumnType.string)])
        assert ep.method == "POST"

    def test_body_encoding_is_form(self):
        cfg = self._cfg()
        ep = build_endpoint(cfg, "t", "SELECT ?x WHERE { ?x a ex:T }",
                            [ApiColumn(name="x", type=ApiColumnType.string)])
        assert ep.body_encoding == "form"

    def test_response_normalizer_is_sparql_bindings(self):
        cfg = self._cfg()
        ep = build_endpoint(cfg, "t", "SELECT ?x WHERE { ?x a ex:T }",
                            [ApiColumn(name="x", type=ApiColumnType.string)])
        assert ep.response_normalizer == "sparql_bindings"

    def test_query_template_stored(self):
        q = "SELECT ?name WHERE { ?x foaf:name ?name }"
        cfg = self._cfg()
        ep = build_endpoint(cfg, "t", q, [ApiColumn(name="name", type=ApiColumnType.string)])
        assert ep.query_template == q

    def test_ttl_default(self):
        cfg = self._cfg()
        ep = build_endpoint(cfg, "t", "SELECT ?x WHERE { ?x a ex:T }",
                            [ApiColumn(name="x", type=ApiColumnType.string)])
        assert ep.ttl == 300

    def test_root_path_when_no_path(self):
        """Endpoint URL with no path should yield '/'."""
        cfg = SparqlSourceConfig(source_id="s", endpoint_url="http://host:3030")
        ep = build_endpoint(cfg, "t", "SELECT ?x WHERE { ?x a ex:T }",
                            [ApiColumn(name="x", type=ApiColumnType.string)])
        assert ep.path in ("/", "")


# ---------------------------------------------------------------------------
# infer_columns — pure Python
# ---------------------------------------------------------------------------

class TestInferColumns:
    def test_empty_returns_empty(self):
        assert infer_columns([]) == []

    def test_all_columns_are_strings(self):
        """SPARQL variable bindings are strings (typed by the application layer)."""
        rows = [{"name": "Alice", "city": "NY"}, {"name": "Bob", "city": "LA"}]
        cols = infer_columns(rows)
        assert all(c.type == ApiColumnType.string for c in cols)

    def test_column_names_from_first_row(self):
        rows = [{"s": "x", "p": "y", "o": "z"}]
        cols = infer_columns(rows)
        assert [c.name for c in cols] == ["s", "p", "o"]

    def test_single_row_single_var(self):
        cols = infer_columns([{"subject": "http://example.com/1"}])
        assert len(cols) == 1
        assert cols[0].name == "subject"


# ---------------------------------------------------------------------------
# Live-service tests (requires running Fuseki / Blazegraph)
# ---------------------------------------------------------------------------

@pytest.mark.requires_sparql
class TestLiveSparqlExecution:
    """Require Docker Compose fuseki service:
        docker compose up fuseki
    """

    ENDPOINT_URL = "http://localhost:3030/provisa"  # Fuseki in-memory dataset

    @pytest.fixture
    def cfg(self):
        return SparqlSourceConfig(
            source_id="fuseki-live",
            endpoint_url=f"{self.ENDPOINT_URL}/sparql",
        )

    async def test_probe_endpoint_returns_rows(self, cfg):
        """probe_endpoint executes LIMIT 5 and returns flat row dicts."""
        from provisa.sparql.source import probe_endpoint

        q = """
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        SELECT ?n WHERE { VALUES ?n { "Alice"^^xsd:string "Bob"^^xsd:string "Carol"^^xsd:string } }
        """
        rows = await probe_endpoint(cfg, q)
        assert len(rows) <= 5
        assert all("n" in row for row in rows)

    async def test_sparql_bindings_normalizer_shape(self, cfg):
        """sparql_bindings produces flat row dicts from a SPARQL JSON result."""
        import httpx
        from provisa.api_source.normalizers import sparql_bindings

        q = "SELECT ?x WHERE { VALUES ?x { 1 2 3 } }"
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                self.ENDPOINT_URL,
                data={"query": q},
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/sparql-results+json",
                },
                timeout=10.0,
            )
            resp.raise_for_status()

        rows = sparql_bindings(resp.json())
        assert len(rows) > 0
        assert "x" in rows[0]

    async def test_inferred_columns_from_probe(self, cfg):
        """infer_columns produces one string column per SELECT variable."""
        from provisa.sparql.source import probe_endpoint

        q = "SELECT ?subject ?predicate WHERE { ?subject ?predicate [] } LIMIT 3"
        rows = await probe_endpoint(cfg, q)
        if rows:
            cols = infer_columns(rows)
            names = [c.name for c in cols]
            assert "subject" in names
            assert "predicate" in names
