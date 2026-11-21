# Copyright (c) 2026 Kenneth Stott
# Canary: 5d0e2c71-9a4f-4f8e-b3a6-2e7c1d9f4a10
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The bulk RDF export (REQ-2165): a role that reads Customer with email masked exports class
Customer as Turtle; the file declares Customer an owl:Class with its properties' domains and
ranges, holds every instance the role reads with each email masked, and an export at the same
as_recorded yields the same triples. Each CONSTRUCT query is compiled as /data/sparql compiles it,
governed (masks applied) and run on DuckDB; the route's own wiring is run with its pipeline seam
(answer_rows) answered by that model."""

# Requirements: REQ-2165

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import duckdb
import httpx
import pytest
import sqlglot
from rdflib import Graph, Literal, URIRef
from starlette.requests import Request

from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_rewrite import lower_to_catalog_physical, make_semantic_sql
from provisa.compiler.stage2 import apply_governance, build_governance_context
from provisa.gql.assembler import assemble_rows, take_cut
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.mv.bitemporal import request_as_of
from provisa.security.masking import MaskingRule, MaskType
from provisa.sparql_query.compiler import compile_sparql
from provisa.sparql_query.rdf_export import (
    ScopeRefused,
    batches,
    load,
    ntriples,
    plan_export,
    turtle,
)
from provisa.sparql_query.vocabulary import (
    OWL,
    RDF_TYPE,
    RDFS,
    XSD,
    Grounding,
    build_vocabulary,
    column_iri,
    column_types_of,
    table_class_iri,
)

BASE = "https://data.example.com"
ROLE = {"id": "analyst", "capabilities": ["query_development"], "domain_access": ["*"]}
CUSTOMERS, ORDERS = 1, 2
MASK = "***"
EMAILS = ("ada@example.com", "bob@example.com", "cy@example.com")
RELATIONSHIPS = [
    {
        "id": 1,
        "source_table_id": ORDERS,
        "target_table_id": CUSTOMERS,
        "source_column": "customer_id",
        "target_column": "id",
        "cardinality": "many-to-one",
        "alias": "customer",
    },
]


@pytest.fixture(autouse=True)
def public_url(monkeypatch):
    monkeypatch.setenv("PROVISA_PUBLIC_URL", BASE)


def _table(table_id: int, name: str, columns: list[str]) -> dict:
    return {
        "id": table_id,
        "source_id": "pg",
        "domain_id": "shop",
        "schema_name": "public",
        "table_name": name,
        "columns": [
            {"column_name": c, "visible_to": ["analyst"], "is_primary_key": c == "id"}
            for c in columns
        ],
        "write_ops": [],
        "write_returns_rows": False,
        "write_refused_forms": [],
    }


def _meta(*names: tuple[str, str]) -> list[ColumnMetadata]:
    return [ColumnMetadata(column_name=n, data_type=t, is_nullable=True) for n, t in names]


class Model:
    """The analyst's view of customers (email masked) and orders, governed and run on DuckDB as
    the one pipeline runs a SPARQL statement."""

    def __init__(self) -> None:
        self.tables = [
            _table(CUSTOMERS, "customers", ["id", "name", "email"]),
            _table(ORDERS, "orders", ["id", "customer_id", "total"]),
        ]
        si = SchemaInput(
            tables=self.tables,
            relationships=RELATIONSHIPS,
            column_types={
                CUSTOMERS: _meta(("id", "integer"), ("name", "varchar"), ("email", "varchar")),
                ORDERS: _meta(("id", "integer"), ("customer_id", "integer"), ("total", "integer")),
            },
            naming_rules=[],
            role=ROLE,
            domains=[{"id": "shop", "description": "Shop"}],
        )
        generate_schema(si)
        self.ctx = build_context(si)
        self.gov = build_governance_context(
            "analyst",
            RLSContext(rules={}),
            {
                (CUSTOMERS, "analyst"): {
                    "email": (MaskingRule(mask_type=MaskType.constant, value=MASK), "varchar")
                }
            },
            self.ctx,
            self.tables,
            ROLE,
            RELATIONSHIPS,
        )
        self.graph = CypherLabelMap.from_schema(
            self.ctx, domain_access=["*"], all_tables=self.tables
        )
        self.vocab = build_vocabulary(
            self.graph, BASE, column_types_of(self.graph, self.ctx), Grounding()
        )
        self.con = duckdb.connect()
        self.con.execute("ATTACH ':memory:' AS pg")
        self.con.execute("CREATE SCHEMA pg.public")
        self.con.execute(
            "CREATE TABLE pg.public.customers (id INTEGER, name VARCHAR, email VARCHAR)"
        )
        self.con.execute(
            "CREATE TABLE pg.public.orders (id INTEGER, customer_id INTEGER, total INTEGER)"
        )
        for i, email in enumerate(EMAILS):
            self.con.execute(
                f"INSERT INTO pg.public.customers VALUES ({7001 + i}, 'name{i}', '{email}')"
            )
        self.con.execute("INSERT INTO pg.public.orders VALUES (8001, 7001, 90)")

    def lowered(self, query: str) -> tuple[str, dict]:
        compiled = compile_sparql(query, self.vocab, self.graph)
        assert compiled.ast is not None
        sql, _, graph_vars = cypher_to_sql(compiled.ast, self.graph, {})
        sql = apply_graph_rewrites(sql, graph_vars, self.graph)
        return make_semantic_sql(sql.sql(dialect="postgres"), self.ctx), graph_vars

    def rows_of(self, lowered: tuple[str, dict]) -> list[dict]:
        sql, graph_vars = lowered
        physical = lower_to_catalog_physical(apply_governance(sql, self.gov), self.ctx)
        cur = self.con.execute(sqlglot.transpile(physical, read="postgres", write="duckdb")[0])
        names = [d[0] for d in cur.description]
        rows = [dict(zip(names, r)) for r in cur.fetchall()]
        return assemble_rows(take_cut(rows, []), graph_vars)

    async def run(self, query: str) -> list[dict]:
        return self.rows_of(self.lowered(query))

    def domains(self) -> dict[str, str | None]:
        return {label: nm.domain_id for label, nm in self.graph.nodes.items()}


CUSTOMER = table_class_iri(BASE, "shop", "customers")
ORDER = table_class_iri(BASE, "shop", "orders")


def _email() -> URIRef:
    return URIRef(column_iri(BASE, "shop", "customers", "email"))


def _instance_plan(model: Model, scope: str, value: str | None):
    """The plan without the ontology queries, which read ontology._axioms (a control-plane table
    this DuckDB model does not hold)."""
    plan = plan_export(model.vocab, model.domains(), scope, value)  # type: ignore[arg-type]
    return type(plan)([q for q in plan.queries if "/vocab/" in q], plan.declarations)


def _customer_export(model: Model) -> Graph:
    plan = _instance_plan(model, "class", CUSTOMER)
    doc = asyncio.run(turtle(plan, model.run, BASE, model.vocab.prefixes))
    return Graph().parse(data=doc, format="turtle")


def test_the_export_declares_customer_an_owl_class_with_domains_and_ranges():
    g = _customer_export(Model())
    customer = URIRef(CUSTOMER)
    assert (customer, URIRef(RDF_TYPE), URIRef(f"{OWL}Class")) in g
    for column, datatype in (("id", "integer"), ("name", "string"), ("email", "string")):
        prop = URIRef(column_iri(BASE, "shop", "customers", column))
        assert (prop, URIRef(RDF_TYPE), URIRef(f"{OWL}DatatypeProperty")) in g
        assert (prop, URIRef(f"{RDFS}domain"), customer) in g
        assert (prop, URIRef(f"{RDFS}range"), URIRef(f"{XSD}{datatype}")) in g


def test_every_instance_the_role_reads_is_in_the_file_with_each_email_masked():
    g = _customer_export(Model())
    instances = set(g.subjects(URIRef(RDF_TYPE), URIRef(CUSTOMER)))
    assert len(instances) == len(EMAILS)
    emails = {o for s in instances for o in g.objects(s, _email())}
    assert emails == {Literal(MASK, datatype=URIRef(f"{XSD}string"))} or emails == {Literal(MASK)}
    text = g.serialize(format="turtle")
    assert not any(email in text for email in EMAILS)


def test_an_edge_out_of_the_class_is_exported_and_declared_an_object_property():
    model = Model()
    plan = _instance_plan(model, "class", ORDER)
    g = Graph().parse(
        data=asyncio.run(turtle(plan, model.run, BASE, model.vocab.prefixes)), format="turtle"
    )
    edges = [
        (s, p, o) for s, p, o in g if (p, URIRef(RDF_TYPE), URIRef(f"{OWL}ObjectProperty")) in g
    ]
    assert len(edges) == 1
    _, edge, _ = edges[0]
    assert (edge, URIRef(f"{RDFS}range"), URIRef(CUSTOMER)) in g


def test_a_scope_the_role_reads_nothing_of_is_refused():
    model = Model()
    with pytest.raises(ScopeRefused):
        plan_export(model.vocab, model.domains(), "class", f"{BASE}/vocab/shop/nothing")
    with pytest.raises(ScopeRefused):
        plan_export(model.vocab, model.domains(), "domain", "hr")


def test_a_domain_scope_holds_every_class_of_the_domain_and_the_ontology_queries():
    model = Model()
    plan = plan_export(model.vocab, model.domains(), "domain", "shop")
    owl_classes = {s for s, p, o in plan.declarations if o == f"{OWL}Class"}
    assert owl_classes == {CUSTOMER, ORDER}
    assert any(f"{RDFS}subClassOf" in q for q in plan.queries)


def test_ntriples_batches_concatenate_to_one_document():
    model = Model()
    plan = _instance_plan(model, "class", CUSTOMER)

    async def collect() -> bytes:
        return b"".join([ntriples(b) async for b in batches(plan, model.run, BASE)])

    whole = Graph().parse(data=asyncio.run(collect()), format="nt")
    assert set(whole) == set(_customer_export(model))


def test_the_graph_store_load_posts_each_batch_and_reports_loaded_and_refused():
    model = Model()
    plan = _instance_plan(model, "class", CUSTOMER)
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if len(seen) == 2:
            return httpx.Response(400, text="Parse error")
        return httpx.Response(201)

    async def go():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await load(
                plan, model.run, BASE, client, "http://store/ds/data", "urn:g", {}, 5.0
            )

    report = asyncio.run(go())
    sizes = [len(Graph().parse(data=r.content, format="nt")) for r in seen]
    assert all(r.headers["content-type"] == "application/n-triples" for r in seen)
    assert all(r.url.params["graph"] == "urn:g" for r in seen)
    assert report.loaded == sum(sizes) - sizes[1]
    assert report.errors == [f"{sizes[1]} triples refused: HTTP 400: Parse error"]


def test_the_default_graph_is_loaded_when_no_graph_is_named():
    from provisa.sparql_query.rdf_export import graph_store_url

    assert graph_store_url("http://s/ds/data", None) == ("http://s/ds/data", {"default": ""})


# --- the route: every query through answer_rows (the one pipeline), at the request's time travel


def _request(body: dict) -> Request:
    raw = json.dumps(body).encode()

    async def receive():
        return {"type": "http.request", "body": raw, "more_body": False}

    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/data/sparql/export",
            "query_string": b"",
            "headers": [(b"content-type", b"application/json")],
        },
        receive,
    )


@pytest.fixture
def routed(monkeypatch):
    """The route with its role and pipeline answered by the model; ``calls`` is the as_of each
    query was run at, and ``versions`` the rows the store holds as of each recorded time."""
    from provisa.api import role_surface as role_surface_mod
    from provisa.api.rest import sparql_router

    model = Model()
    calls: list[str | None] = []
    surface = SimpleNamespace(context=model.ctx)

    async def vocabulary_for(state, role_id, ctx, label_map):
        return model.vocab

    def semantic_statement(compiled, graph, surface, role_id, state):
        assert compiled.ast is not None
        sql, _, graph_vars = cypher_to_sql(compiled.ast, graph, {})
        sql = apply_graph_rewrites(sql, graph_vars, graph)
        return make_semantic_sql(sql.sql(dialect="postgres"), model.ctx), graph_vars

    async def answer_rows(query, lowered, role_id, state, *, cache_hint, serve_cached, as_of):
        calls.append(as_of)
        return model.rows_of(lowered), None, None

    monkeypatch.setattr(sparql_router, "_resolve_role_id", lambda request, state: "analyst")
    monkeypatch.setattr(role_surface_mod, "role_surface", lambda state, role_id: surface)
    monkeypatch.setattr(sparql_router, "_build_label_map", lambda ctx, role_id, state: model.graph)
    monkeypatch.setattr(sparql_router, "vocabulary_for", vocabulary_for)
    monkeypatch.setattr(sparql_router, "semantic_statement", semantic_statement)
    monkeypatch.setattr(sparql_router, "answer_rows", answer_rows)
    # The ontology queries read ontology._axioms, a control-plane table this DuckDB model does
    # not hold: the route exports the instance queries here; the integration run reads the axioms.
    from provisa.sparql_query import rdf_export

    monkeypatch.setattr(
        rdf_export,
        "plan_export",
        lambda vocab, domains, scope, value=None: _instance_plan(model, scope, value),
    )
    return SimpleNamespace(model=model, calls=calls, router=sparql_router)


def _export(routed, body: dict):
    from provisa.api.rest.sparql_router import RdfFileRequest

    return asyncio.run(routed.router.sparql_export(_request(body), RdfFileRequest(**body)))


def test_every_query_runs_through_the_pipeline_at_the_requests_as_recorded(routed):
    body = {"scope": "class", "value": CUSTOMER, "as_recorded": "2026-11-01T00:00:00"}
    response = _export(routed, body)
    assert response.status_code == 200, bytes(response.body)
    assert response.media_type == "text/turtle"
    key = request_as_of(None, "2026-11-01T00:00:00")
    assert routed.calls and set(routed.calls) == {key}


def test_an_export_at_the_same_as_recorded_yields_the_same_triples(routed):
    body = {"scope": "class", "value": CUSTOMER, "as_recorded": "2026-11-01T00:00:00"}
    first = Graph().parse(data=bytes(_export(routed, body).body), format="turtle")
    second = Graph().parse(data=bytes(_export(routed, body).body), format="turtle")
    assert len(first) > 0 and set(first) == set(second)


def test_a_malformed_as_recorded_is_refused(routed):
    response = _export(routed, {"scope": "graph", "as_recorded": "yesterday"})
    assert response.status_code == 400
    assert json.loads(bytes(response.body))["code"] == "sparql.bad_time_travel"


def test_an_unknown_class_scope_is_refused_by_name(routed):
    response = _export(routed, {"scope": "class", "value": f"{BASE}/vocab/shop/nothing"})
    assert response.status_code == 404
    assert json.loads(bytes(response.body))["code"] == "sparql.export_scope_refused"
