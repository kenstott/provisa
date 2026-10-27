// Copyright (c) 2026 Kenneth Stott
// Canary: 3b9f6d28-4e1a-4c57-8d03-a2f5e7b1c964
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135 phase 5: SPARQL in the graph visualizer -- IRIs compacted by the vocabulary's prefixes, a
// pill's pattern added to the query in the bar, and an answer made a frame.

import { describe, it, expect } from "vitest";
import {
  addPattern,
  compactIri,
  freshVariable,
  parseNTriples,
  sparqlStatement,
  triplesGraph,
  withPrefixes,
  LITERAL_LABEL,
} from "../sparql";

const BASE = "https://provisa.example";
const P = {
  rdf: "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
  xsd: "http://www.w3.org/2001/XMLSchema#",
  term: `${BASE}/vocab/term/`,
  sales: `${BASE}/vocab/sales/`,
  "sales-id": `${BASE}/id/sales/`,
};

describe("an IRI", () => {
  it("is compacted by the longest prefix whose namespace starts it", () => {
    expect(compactIri(`${BASE}/vocab/sales/orders`, P)).toBe("sales:orders");
    expect(compactIri(`${P.rdf}type`, P)).toBe("rdf:type");
  });

  it("stays whole, in angle brackets, where no prefix makes a plain local name", () => {
    expect(compactIri("https://elsewhere.example/x", P)).toBe("<https://elsewhere.example/x>");
    // A local name a prefixed name cannot hold.
    expect(compactIri(`${BASE}/vocab/sales/orders#amount`, P)).toBe(
      `<${BASE}/vocab/sales/orders#amount>`,
    );
  });
});

describe("a pill's pattern", () => {
  it("is added to the query's last group, with the PREFIX lines its terms need", () => {
    const q = "SELECT * WHERE {\n  ?s ?p ?o\n}\nLIMIT 25";
    expect(addPattern(q, "?s a sales:orders .", ["sales:orders"], P)).toBe(
      `PREFIX sales: <${P.sales}>\nSELECT * WHERE {\n  ?s ?p ?o .\n  ?s a sales:orders .\n}\nLIMIT 25`,
    );
  });

  it("declares a prefix once", () => {
    const q = `PREFIX sales: <${P.sales}>\nSELECT * WHERE { ?s a sales:orders . }`;
    expect(addPattern(q, "?s sales:amount ?o .", ["sales:amount"], P)).toBe(
      `PREFIX sales: <${P.sales}>\nSELECT * WHERE { ?s a sales:orders .\n  ?s sales:amount ?o .\n}`,
    );
  });

  it("ends the group's last triple first, where it is left open", () => {
    expect(addPattern("SELECT * WHERE { ?s ?p ?o }", "?s a sales:orders .", ["sales:orders"], P)).toBe(
      `PREFIX sales: <${P.sales}>\nSELECT * WHERE { ?s ?p ?o .\n  ?s a sales:orders .\n}`,
    );
  });

  it("starts a SELECT where the bar holds no group", () => {
    expect(addPattern("", "?s a sales:orders .", ["sales:orders"], P)).toBe(
      `PREFIX sales: <${P.sales}>\nSELECT * WHERE {\n  ?s a sales:orders .\n}\nLIMIT 25`,
    );
  });

  it("takes a variable the query does not name yet", () => {
    expect(freshVariable("SELECT ?o WHERE { ?s ?p ?o }", "o")).toBe("?o2");
    expect(freshVariable("SELECT ?o ?o2 WHERE { ?s ?p ?o . ?s ?q ?o2 }", "o")).toBe("?o3");
    expect(freshVariable("SELECT * WHERE { ?s ?p ?x }", "o")).toBe("?o");
  });

  it("is a declaration alone for a prefix", () => {
    expect(withPrefixes("ASK { ?s ?p ?o }", ["term"], P)).toBe(
      `PREFIX term: <${P.term}>\nASK { ?s ?p ?o }`,
    );
  });
});

const NT = [
  `<${BASE}/id/sales/orders/1> <${P.rdf}type> <${BASE}/vocab/sales/orders> .`,
  `<${BASE}/id/sales/orders/1> <${BASE}/vocab/sales/amount> "12.50"^^<${P.xsd}decimal> .`,
  `<${BASE}/id/sales/orders/2> <${BASE}/vocab/sales/amount> "12.50"^^<${P.xsd}decimal> .`,
  `<${BASE}/id/sales/orders/1> <${BASE}/vocab/sales/note> "said \\"hi\\""@en .`,
].join("\n");

describe("a CONSTRUCT's triples", () => {
  it("are read from N-Triples, each term as the answer gave it", () => {
    const triples = parseNTriples(NT, P);
    expect(triples).toHaveLength(4);
    expect(triples[1].o).toMatchObject({
      rdfTerm: "literal",
      value: "12.50",
      datatype: `${P.xsd}decimal`,
    });
    expect(triples[3].o).toMatchObject({ rdfTerm: "literal", value: 'said "hi"', lang: "en" });
    expect(triples[0].p.shown).toBe("rdf:type");
  });

  it("are drawn: resources by their class, each literal a node of its own", () => {
    const { nodes, edges } = triplesGraph(parseNTriples(NT, P));
    const resources = [...nodes.values()].filter((n) => n.label !== LITERAL_LABEL);
    const literals = [...nodes.values()].filter((n) => n.label === LITERAL_LABEL);
    // Order 1 is typed; order 2 has no type triple, so it is drawn by its namespace's prefix.
    expect(resources.map((n) => [n.properties.name, n.label]).sort()).toEqual([
      ["<https://provisa.example/id/sales/orders/1>", "sales:orders"],
      ["<https://provisa.example/id/sales/orders/2>", "Resource"],
      ["sales:orders", "sales"],
    ]);
    // The same literal under two subjects is two nodes.
    expect(literals.map((n) => n.properties.name).sort()).toEqual(["12.50", "12.50", 'said "hi"']);
    expect([...edges.values()].map((e) => e.type).sort()).toEqual([
      "rdf:type",
      "sales:amount",
      "sales:amount",
      "sales:note",
    ]);
  });

  it("are keyed as the canvas keys a node, so each edge is drawn between its ends", () => {
    // GraphCanvas draws an edge only when both its ends are in the frame's nodes under
    // `${label}:${id}`, as a Cypher frame keys them.
    const { nodes, edges } = triplesGraph(parseNTriples(NT, P));
    for (const [key, n] of nodes) expect(key).toBe(`${n.label}:${n.id}`);
    for (const e of edges.values()) {
      expect(nodes.has(`${e.startNode.label}:${e.startNode.id}`)).toBe(true);
      expect(nodes.has(`${e.endNode.label}:${e.endNode.id}`)).toBe(true);
    }
  });
});

const response = (body: string, type: string, status = 200) =>
  new Response(body, { status, headers: { "Content-Type": type } });

describe("an answer of /data/sparql", () => {
  it("is a SELECT's rows, each term compacted for showing and whole for its title", async () => {
    const st = await sparqlStatement(
      "SELECT ?s ?amount WHERE { ?s sales:amount ?amount }",
      response(
        JSON.stringify({
          head: { vars: ["s", "amount"] },
          results: {
            bindings: [
              {
                s: { type: "uri", value: `${BASE}/vocab/sales/x` },
                amount: { type: "literal", value: "3", datatype: `${P.xsd}integer` },
              },
              { s: { type: "uri", value: `${BASE}/vocab/sales/y` } },
            ],
          },
        }),
        "application/sparql-results+json",
      ),
      P,
    );
    expect(st.status).toBe("done");
    expect(st.columns).toEqual(["s", "amount"]);
    expect(st.rows[0].s).toMatchObject({ rdfTerm: "iri", shown: "sales:x" });
    expect(st.rows[0].amount).toMatchObject({ rdfTerm: "literal", shown: "3" });
    // An unbound variable is empty.
    expect(st.rows[1].amount).toBeNull();
    expect(st.nodes.size).toBe(0);
    // What its export is written as: SPARQL results.
    expect(st.sparqlForm).toBe("rows");
  });

  it("is an ASK's answer", async () => {
    const st = await sparqlStatement(
      "ASK { ?s ?p ?o }",
      response(JSON.stringify({ head: {}, boolean: true }), "application/sparql-results+json"),
      P,
    );
    expect(st.rows).toEqual([{ ask: true }]);
    expect(st.sparqlForm).toBe("boolean");
  });

  it("is a CONSTRUCT's graph, its triples the table", async () => {
    const st = await sparqlStatement(
      "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
      response(NT, "application/n-triples"),
      P,
    );
    expect(st.nodes.size).toBe(6);
    expect(st.edges.size).toBe(4);
    expect(st.columns).toEqual(["subject", "predicate", "object"]);
    // A graph: its export is RDF.
    expect(st.sparqlForm).toBe("graph");
  });

  it("is a refusal's message", async () => {
    const st = await sparqlStatement(
      "SELECT",
      response(JSON.stringify({ error: "Unexpected end of query" }), "application/json", 400),
      P,
    );
    expect(st).toMatchObject({ status: "error", error: "Unexpected end of query" });
  });
});
