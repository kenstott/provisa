// Copyright (c) 2026 Kenneth Stott
// Canary: 8f3a6d10-2c7e-4f41-b6d5-91e0c7a4b2f3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135, THE SPARQL EXPLORER AS AN RDF PERSON EXPECTS: IRIs and literals as an RDF client shows
// them, a data answer and an answer about the ontology drawn as RDF, a SELECT whose rows hold
// triples, what a merge adds to a frame, and a graph written as Turtle and N-Triples.

import { describe, expect, it } from "vitest";
import type { FrameData } from "../graph-model";
import { loadGraphState, saveGraphState } from "../graph-persistence";
import {
  compactIri,
  describeQuery,
  distinctTriples,
  effectivePrefixes,
  frameTriples,
  parseNTriples,
  queryPrefixes,
  refusalText,
  selectPairing,
  shownIri,
  shownLiteral,
  sparqlDrawn,
  sparqlStatement,
  triplesGraph,
  type RdfCell,
} from "../sparql";
import { exportFormats, writeNTriples, writeTurtle } from "../sparqlExport";

const BASE = "https://provisa.example";
const RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const RDFS = "http://www.w3.org/2000/01/rdf-schema#";
const OWL = "http://www.w3.org/2002/07/owl#";
const XSD = "http://www.w3.org/2001/XMLSchema#";
const P = {
  rdf: RDF,
  rdfs: RDFS,
  owl: OWL,
  xsd: XSD,
  sales: `${BASE}/vocab/sales/`,
  rel: `${BASE}/vocab/rel/`,
  id: `${BASE}/id/`,
};
const ORDER = `${BASE}/id/sales/orders/1`;
const CUSTOMER = `${BASE}/id/sales/customers/7`;
const ORDERS = `${BASE}/vocab/sales/orders`;
const CUSTOMERS = `${BASE}/vocab/sales/customers`;

const graph = (lines: string[]) => triplesGraph(parseNTriples(lines.join("\n"), P));
const nodeNamed = (g: ReturnType<typeof graph>, name: string) =>
  [...g.nodes.values()].find((n) => n.properties.name === name);

describe("a term shown", () => {
  it("is compacted in a cell even where a query could not write it so", () => {
    expect(shownIri(`${ORDERS}#total`, P)).toBe("sales:orders#total");
    expect(shownIri(ORDER, P)).toBe("id:sales/orders/1");
    expect(shownIri(`${RDF}type`, P)).toBe("rdf:type");
    // What a query is written with stays valid SPARQL.
    expect(compactIri(`${ORDERS}#total`, P)).toBe(`<${ORDERS}#total>`);
  });

  it("is the whole IRI where no prefix starts it", () => {
    expect(shownIri("https://elsewhere.example/x", P)).toBe("<https://elsewhere.example/x>");
    expect(shownIri(P.sales, P)).toBe(`<${P.sales}>`);
  });

  it("is a plain string or an integer plainly, a tag or a datatype compactly", () => {
    expect(shownLiteral("Ann", undefined, undefined, P)).toBe("Ann");
    expect(shownLiteral("Ann", `${XSD}string`, undefined, P)).toBe("Ann");
    expect(shownLiteral("41", `${XSD}integer`, undefined, P)).toBe("41");
    expect(shownLiteral("bonjour", undefined, "fr", P)).toBe('"bonjour"@fr');
    expect(shownLiteral("12.50", `${XSD}decimal`, undefined, P)).toBe('"12.50"^^xsd:decimal');
    expect(shownLiteral("x", "http://other/dt", undefined, P)).toBe('"x"^^<http://other/dt>');
  });

  it("shows by the prefixes the query declares, over the vocabulary's", async () => {
    expect(queryPrefixes("PREFIX ex: <http://x/>\nprefix : <http://y/>\nSELECT * {}")).toEqual({
      ex: "http://x/",
      "": "http://y/",
    });
    const st = await sparqlStatement(
      "PREFIX ex: <http://x/>\nSELECT ?a WHERE { ?a ?p ?o }",
      new Response(
        JSON.stringify({
          head: { vars: ["a"] },
          results: { bindings: [{ a: { type: "uri", value: "http://x/ann" } }] },
        }),
        { headers: { "content-type": "application/sparql-results+json" } },
      ),
      P,
    );
    expect((st.rows[0].a as RdfCell).shown).toBe("ex:ann");
  });
});

describe("the prefixes in use", () => {
  const vocabulary = { base: BASE, prefixes: { sales: P.sales, rel: P.rel } };

  it("are the vocabulary's and the id namespace, with the viewer's edits", () => {
    expect(effectivePrefixes(vocabulary, {})).toEqual({
      sales: P.sales,
      rel: P.rel,
      id: `${BASE}/id/`,
    });
    expect(
      effectivePrefixes(vocabulary, { rel: null, sales: "http://mine/", ex: "http://x/" }),
    ).toEqual({ sales: "http://mine/", id: `${BASE}/id/`, ex: "http://x/" });
  });
});

describe("a data answer drawn", () => {
  const g = graph([
    `<${ORDER}> <${RDF}type> <${ORDERS}> .`,
    `<${ORDER}> <${P.rel}PLACED_BY> <${CUSTOMER}> .`,
    `<${ORDER}> <${ORDERS}#total> "12.50"^^<${XSD}decimal> .`,
    `<${ORDER}> <${ORDERS}#note> "rush"@en .`,
    `<${CUSTOMER}> <${RDF}type> <${CUSTOMERS}> .`,
    `<${CUSTOMER}> <${CUSTOMERS}#name> "Ann" .`,
  ]);
  const order = [...g.nodes.values()].find((n) => n.properties.iri === ORDER)!;

  it("labels a resource by its rdf:type class", () => {
    expect(order.label).toBe("sales:orders");
    expect(nodeNamed(g, "Ann")?.label).toBe("sales:customers");
  });

  it("makes a resource-valued property an edge, named compactly", () => {
    const edge = [...g.edges.values()].find((e) => e.type === "rel:PLACED_BY")!;
    expect(edge.startNode.properties.iri).toBe(ORDER);
    expect(edge.endNode.properties.iri).toBe(CUSTOMER);
    expect(edge.properties.iri).toBe(`${P.rel}PLACED_BY`);
  });

  it("makes a literal a property of its node, compacted, never a node", () => {
    expect([...g.nodes.values()].every((n) => n.label !== "Literal")).toBe(true);
    expect(order.properties["sales:orders#total"]).toBe('"12.50"^^xsd:decimal');
    expect(order.properties["sales:orders#note"]).toBe('"rush"@en');
    // The property's full IRI stands behind its compacted name, for the hover.
    expect(order.propertyIris?.["sales:orders#total"]).toBe(`${ORDERS}#total`);
  });

  it("captions a resource by its value, without a tag's quotes", () => {
    const tagged = graph([`<${ORDER}> <${RDFS}label> "Order one"@en .`]);
    expect([...tagged.nodes.values()][0].properties.name).toBe("Order one");
  });

  it("gives a resource the same id in every drawing, so a merge finds it again", () => {
    const again = graph([`<${CUSTOMER}> <${RDF}type> <${CUSTOMERS}> .`]);
    const before = [...g.nodes.values()].find((n) => n.properties.iri === CUSTOMER)!;
    const after = [...again.nodes.values()].find((n) => n.properties.iri === CUSTOMER)!;
    expect(after.id).toBe(before.id);
  });
});

describe("an answer about the ontology drawn", () => {
  const STAFF = `${BASE}/vocab/term/Staff`;
  const PERSON = `${BASE}/vocab/term/Person`;
  const g = graph([
    `<${STAFF}> <${RDF}type> <${OWL}Class> .`,
    `<${PERSON}> <${RDF}type> <${OWL}Class> .`,
    `<${STAFF}> <${RDFS}subClassOf> <${PERSON}> .`,
    `<${P.rel}PLACED_BY> <${RDF}type> <${OWL}ObjectProperty> .`,
    `<${P.rel}PLACED_BY> <${RDFS}domain> <${ORDERS}> .`,
    `<${P.rel}PLACED_BY> <${RDFS}range> <${CUSTOMERS}> .`,
    `<${ORDERS}#total> <${RDFS}domain> <${ORDERS}> .`,
    `<${ORDERS}#total> <${RDFS}range> <${XSD}decimal> .`,
    `<${ORDERS}#note> <${RDFS}domain> <${ORDERS}> .`,
  ]);

  it("makes each class a node, not owl:Class a hub joined to them all", () => {
    expect(new Set([...g.nodes.values()].map((n) => n.properties.iri))).toEqual(
      new Set([STAFF, PERSON, ORDERS, CUSTOMERS, `${ORDERS}#note`]),
    );
    expect(nodeNamed(g, "Staff")?.label).toBe("owl:Class");
    expect([...g.edges.values()].some((e) => e.type === "rdf:type")).toBe(false);
  });

  it("makes rdfs:subClassOf an edge from the class to its superclass", () => {
    const sub = [...g.edges.values()].find((e) => e.type === "rdfs:subClassOf")!;
    expect(sub.startNode.properties.iri).toBe(STAFF);
    expect(sub.endNode.properties.iri).toBe(PERSON);
  });

  it("makes a property with a domain and a range an edge between them", () => {
    const edge = [...g.edges.values()].find((e) => e.type === "rel:PLACED_BY")!;
    expect(edge.startNode.properties.iri).toBe(ORDERS);
    expect(edge.endNode.properties.iri).toBe(CUSTOMERS);
  });

  it("makes a property whose range is a datatype a property of its class", () => {
    const orders = [...g.nodes.values()].find((n) => n.properties.iri === ORDERS)!;
    expect(orders.properties["sales:orders#total"]).toBe("xsd:decimal");
  });

  it("keeps a property with only a domain as a node joined to it", () => {
    const edge = [...g.edges.values()].find((e) => e.type === "rdfs:domain")!;
    expect(edge.startNode.properties.iri).toBe(`${ORDERS}#note`);
    expect(edge.endNode.properties.iri).toBe(ORDERS);
  });
});

const cell = (v: string, value = `http://x/${v}`): RdfCell => ({
  rdfTerm: "iri",
  value,
  shown: `ex:${v}`,
});
const lit = (value: string): RdfCell => ({ rdfTerm: "literal", value, shown: value });
const EX = { ex: "http://x/" };

describe("a SELECT's rows that hold triples", () => {
  it("pair ?s ?p ?o with a literal object, the default query's shape", () => {
    expect(
      selectPairing(
        "SELECT * WHERE {\n  ?s ?p ?o\n}\nLIMIT 25",
        ["s", "p", "o"],
        [{ s: cell("a"), p: cell("name"), o: lit("Ann") }],
      ),
    ).toEqual({ subject: "s", predicate: "p", object: "o" });
  });

  it("read the predicate the query writes between two variables", () => {
    const rows = [{ c: cell("a"), e: lit("a@x") }];
    for (const q of [
      "SELECT ?c ?e WHERE { ?c <http://x/email> ?e }",
      "PREFIX ex: <http://x/> SELECT ?c ?e WHERE { ?c a ex:C ; ex:email ?e }",
      "SELECT ?c ?e WHERE { ?c a ex:C . ?c ex:email ?e . }",
    ])
      expect(selectPairing(q, ["c", "e"], rows, EX)).toEqual({
        subject: "c",
        predicate: null,
        object: "e",
        iri: "http://x/email",
      });
  });

  it("read a, and a list of objects", () => {
    expect(
      selectPairing("SELECT ?s ?t WHERE { ?s a ?t }", ["s", "t"], [{ s: cell("a"), t: cell("T") }])
        ?.iri,
    ).toBe(`${RDF}type`);
    expect(
      selectPairing(
        "SELECT ?a ?b WHERE { ?a ex:k ?c , ?b }",
        ["a", "b"],
        [{ a: cell("a"), b: cell("b") }],
        EX,
      )?.iri,
    ).toBe("http://x/k");
  });

  it("stay a table where the literal's predicate is unknown", () => {
    expect(
      selectPairing(
        "SELECT ?a ?n WHERE { ?a nope:n ?n }",
        ["a", "n"],
        [{ a: cell("a"), n: lit("3") }],
        EX,
      ),
    ).toBeNull();
  });

  it("are drawn as the triples they hold, and written as RDF", () => {
    const query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }";
    const rows = [{ s: cell("a"), p: cell("name"), o: lit("Ann") }];
    const frame = {
      id: "f",
      query,
      status: "done",
      nodes: new Map(),
      edges: new Map(),
      rows,
      columns: ["s", "p", "o"],
      sparqlForm: "rows",
      sparqlPairing: selectPairing(query, ["s", "p", "o"], rows)!,
    } as FrameData;
    const drawn = sparqlDrawn(frame, false, true, EX);
    expect([...drawn.nodes.values()][0].properties["ex:name"]).toBe("Ann");
    expect(writeNTriples(frameTriples(frame))).toBe('<http://x/a> <http://x/name> "Ann" .\n');
  });

  it("hold no triples where no predicate names the edge", () => {
    const rows = [{ a: cell("a"), b: cell("b") }];
    const frame = {
      rows,
      sparqlPairing: selectPairing("SELECT ?a ?b WHERE { ?a ?x ?b }", ["a", "b"], rows)!,
    } as unknown as FrameData;
    expect(frame.sparqlPairing).toEqual({ subject: "a", predicate: null, object: "b" });
    expect(frameTriples(frame)).toEqual([]);
  });
});

describe("what a merge adds to a frame", () => {
  const answered = parseNTriples(`<${ORDER}> <${P.rel}PLACED_BY> <${CUSTOMER}> .`, P);
  const merged = parseNTriples(
    [
      `<${CUSTOMER}> <${RDF}type> <${CUSTOMERS}> .`,
      `<${CUSTOMER}> <${CUSTOMERS}#name> "Ann" .`,
      `<${ORDER}> <${P.rel}PLACED_BY> <${CUSTOMER}> .`,
    ].join("\n"),
    P,
  );
  const frame = {
    id: "f",
    query: "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
    status: "done",
    nodes: new Map(),
    edges: new Map(),
    rows: [],
    columns: ["subject", "predicate", "object"],
    sparqlForm: "graph",
    sparqlTriples: answered,
    sparqlExpansions: { [CUSTOMER]: merged },
  } as FrameData;

  it("joins the drawing and the table, each triple once", () => {
    const drawn = sparqlDrawn(frame, false, false, P);
    expect(drawn.rows).toHaveLength(3);
    expect(drawn.edges.size).toBe(2);
    const customer = [...drawn.nodes.values()].find((n) => n.properties.iri === CUSTOMER)!;
    expect(customer.label).toBe("sales:customers");
    expect(customer.properties["sales:customers#name"]).toBe("Ann");
  });

  it("is taken back with its expansion", () => {
    const back = sparqlDrawn({ ...frame, sparqlExpansions: {} }, false, false, P);
    expect(back.rows).toHaveLength(1);
  });

  it("draws in a frame that drew nothing yet", () => {
    const rows = sparqlDrawn(
      { ...frame, sparqlTriples: undefined, sparqlForm: "rows", rows: [{ s: lit("x") }] },
      false,
      false,
      P,
    );
    expect(rows.nodes.size).toBeGreaterThan(0);
    expect(rows.rows).toEqual([{ s: lit("x") }]);
  });

  it("is exported with the answer, each triple once", () => {
    expect(frameTriples(frame)).toHaveLength(3);
    expect(distinctTriples([...answered, ...answered])).toHaveLength(1);
  });
});

describe("prefixes changed after an answer", () => {
  it("show every term of the frame by the new ones", () => {
    const rows = [{ s: { rdfTerm: "iri", value: "http://x/a", shown: "<http://x/a>" } }];
    const frame = {
      id: "f",
      query: "SELECT ?s WHERE { ?s ?p ?o }",
      status: "done",
      nodes: new Map(),
      edges: new Map(),
      rows,
      columns: ["s"],
    } as unknown as FrameData;
    expect((sparqlDrawn(frame, false, false, EX).rows[0].s as RdfCell).shown).toBe("ex:a");
  });
});

describe("a DESCRIBE", () => {
  it("is asked of the resource, and answered as a graph", async () => {
    expect(describeQuery(ORDER)).toBe(`DESCRIBE <${ORDER}>`);
    const st = await sparqlStatement(
      describeQuery(ORDER),
      new Response(`<${ORDER}> <${ORDERS}#total> "1" .`, {
        headers: { "content-type": "application/n-triples" },
      }),
      P,
    );
    expect(st.sparqlForm).toBe("graph");
    expect(st.nodes.size).toBe(1);
  });
});

describe("a refusal", () => {
  it("is the reason the app's error body gives", async () => {
    const res = new Response(JSON.stringify({ detail: "No SELECT here", code: "sparql.refused" }), {
      status: 400,
    });
    expect(await refusalText(res)).toBe("No SELECT here");
  });
});

describe("a graph written out", () => {
  const triples = parseNTriples(
    [
      `<${ORDER}> <${RDF}type> <${ORDERS}> .`,
      `<${ORDER}> <${ORDERS}#total> "12.50"^^<${XSD}decimal> .`,
      `<${ORDER}> <${P.rel}PLACED_BY> <${CUSTOMER}> .`,
      `<${ORDER}> <${P.rel}PLACED_BY> <${ORDER}> .`,
      `<${CUSTOMER}> <${RDFS}label> "said \\"hi\\"\\n"@en .`,
    ].join("\n"),
    P,
  );

  it("as N-Triples reads back the same", () => {
    const text = writeNTriples(triples);
    expect(parseNTriples(text, P)).toEqual(triples);
    expect(text).toContain('"said \\"hi\\"\\n"@en');
  });

  it("as Turtle declares what it uses, and says each subject once", () => {
    expect(writeTurtle(triples, P)).toBe(
      [
        `@prefix rdfs: <${RDFS}> .`,
        `@prefix rel: <${P.rel}> .`,
        `@prefix sales: <${P.sales}> .`,
        `@prefix xsd: <${XSD}> .`,
        "",
        `<${ORDER}>`,
        "  a sales:orders ;",
        `  <${ORDERS}#total> "12.50"^^xsd:decimal ;`,
        `  rel:PLACED_BY <${CUSTOMER}> , <${ORDER}> .`,
        "",
        `<${CUSTOMER}>`,
        `  rdfs:label "said \\"hi\\"\\n"@en .`,
        "",
      ].join("\n"),
    );
  });

  it("is offered as Turtle and N-Triples where it holds triples", () => {
    const own = { id: "a", query: "q" };
    const keys = (f: Parameters<typeof exportFormats>[0]) => exportFormats(f).map((x) => x.key);
    expect(keys({ ...own, form: "graph" })).toEqual(["turtle", "ntriples", "jsonld"]);
    expect(keys({ ...own, form: "graph", triples })).toEqual(["turtle", "ntriples"]);
    expect(keys({ ...own, form: "rows", triples })).toEqual([
      "json",
      "csv",
      "tsv",
      "turtle",
      "ntriples",
    ]);
  });
});

describe("a SPARQL frame reloaded", () => {
  it("keeps its form, triples, pairing and expansions", () => {
    const triples = parseNTriples(`<${ORDER}> <${ORDERS}#total> "1" .`, P);
    const pairing = { subject: "s", predicate: "p", object: "o" };
    saveGraphState({
      language: "sparql",
      history: [],
      currentQuery: "",
      frames: [
        {
          id: "f",
          query: "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
          status: "done",
          nodes: new Map(),
          edges: new Map(),
          rows: [],
          columns: [],
          sparqlForm: "graph",
          sparqlTriples: triples,
          sparqlPairing: pairing,
          sparqlExpansions: { [ORDER]: triples },
        },
      ],
    });
    const [loaded] = loadGraphState("sparql").frames;
    expect(loaded.sparqlForm).toBe("graph");
    expect(loaded.sparqlTriples).toEqual(triples);
    expect(loaded.sparqlPairing).toEqual(pairing);
    expect(loaded.sparqlExpansions).toEqual({ [ORDER]: triples });
  });
});
