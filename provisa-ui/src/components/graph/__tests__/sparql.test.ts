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

import type { FrameData } from "../graph-model";
import { describe, it, expect } from "vitest";
import {
  addPattern,
  compactIri,
  freshVariable,
  parseNTriples,
  sparqlStatement,
  triplesGraph,
  pickedQuery,
  relationshipPattern,
  type SparqlVocabulary,
  sparqlDrawn,
  withPrefixes,
  LITERAL_LABEL,
} from "../sparql";

const BASE = "https://provisa.example";
const P = {
  rdf: "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
  xsd: "http://www.w3.org/2001/XMLSchema#",
  term: `${BASE}/vocab/term/`,
  rel: `${BASE}/vocab/rel/`,
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
    expect(
      addPattern("SELECT * WHERE { ?s ?p ?o }", "?s a sales:orders .", ["sales:orders"], P),
    ).toBe(`PREFIX sales: <${P.sales}>\nSELECT * WHERE { ?s ?p ?o .\n  ?s a sales:orders .\n}`);
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

  it("are drawn with each literal object a property of its subject", () => {
    const { nodes, edges } = triplesGraph(parseNTriples(NT, P));
    expect([...nodes.values()].some((n) => n.label === LITERAL_LABEL)).toBe(false);
    // Only the rdf:type triple joins two resources.
    expect([...edges.values()].map((e) => e.type)).toEqual(["rdf:type"]);
    const byIri = (iri: string) =>
      [...nodes.values()].find((n) => n.properties.iri === `${BASE}/id/sales/orders/${iri}`);
    expect(byIri("1")?.properties).toMatchObject({
      "sales:amount": "12.50",
      "sales:note": 'said "hi"',
    });
    expect(byIri("2")?.properties).toMatchObject({ "sales:amount": "12.50" });
  });

  it("hold a predicate's several literals as a list", () => {
    const nt = [
      `<${BASE}/id/a> <${BASE}/vocab/sales/tag> "x" .`,
      `<${BASE}/id/a> <${BASE}/vocab/sales/tag> "y" .`,
    ].join("\n");
    const { nodes } = triplesGraph(parseNTriples(nt, P));
    expect([...nodes.values()][0].properties["sales:tag"]).toEqual(["x", "y"]);
  });

  it("are drawn, on the toggle: resources by their class, each literal a node of its own", () => {
    const { nodes, edges } = triplesGraph(parseNTriples(NT, P), true);
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
    const { nodes, edges } = triplesGraph(parseNTriples(NT, P), true);
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
    expect(st.nodes.size).toBe(3);
    expect(st.edges.size).toBe(1);
    expect(st.sparqlTriples).toHaveLength(4);
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

const PETS: SparqlVocabulary = {
  base: BASE,
  prefixes: { "pet-store": `${BASE}/vocab/pet-store/`, rel: `${BASE}/vocab/rel/` },
  classes: [
    {
      iri: `${BASE}/vocab/pet-store/owners`,
      kind: "table",
      labels: ["pet-store:owners"],
      superclasses: [],
    },
    {
      iri: `${BASE}/vocab/pet-store/pets`,
      kind: "table",
      labels: ["pet-store:pets"],
      superclasses: [],
    },
  ],
  predicates: [],
  // HAS_PETS runs owner -> pets.
  edges: [
    {
      iri: `${BASE}/vocab/rel/HAS_PETS`,
      type: "HAS_PETS",
      source: "pet-store:owners",
      target: "pet-store:pets",
    },
  ],
  concepts: [],
};
const HAS_PETS = `${BASE}/vocab/rel/HAS_PETS`;

describe("a relationship pill", () => {
  it("makes the variable that holds the source class the subject, and types the object", () => {
    const q = "SELECT * WHERE {\n  ?s a pet-store:owners .\n}";
    expect(relationshipPattern(q, HAS_PETS, PETS)).toEqual({
      pattern: "?s rel:HAS_PETS ?o . ?o a pet-store:pets .",
      terms: ["rel:HAS_PETS", "pet-store:pets"],
    });
  });

  it("makes the variable that holds the target class the object, under a fresh subject", () => {
    const q = "SELECT * WHERE {\n  ?s a pet-store:pets .\n}";
    expect(relationshipPattern(q, HAS_PETS, PETS)).toEqual({
      pattern: "?x rel:HAS_PETS ?s . ?x a pet-store:owners .",
      terms: ["rel:HAS_PETS", "pet-store:owners"],
    });
  });

  it("adds a fresh typed pair where no variable holds either class", () => {
    expect(relationshipPattern("SELECT * WHERE {\n}", HAS_PETS, PETS)).toEqual({
      pattern: "?s a pet-store:owners . ?s rel:HAS_PETS ?o . ?o a pet-store:pets .",
      terms: ["pet-store:owners", "rel:HAS_PETS", "pet-store:pets"],
    });
    // Names taken by the query are not reused.
    const q = "SELECT * WHERE {\n  ?s a pet-store:vets . ?o a pet-store:vets .\n}";
    expect(relationshipPattern(q, HAS_PETS, PETS).pattern).toBe(
      "?s2 a pet-store:owners . ?s2 rel:HAS_PETS ?o2 . ?o2 a pet-store:pets .",
    );
  });

  it("reads a class written in full as the class it is", () => {
    const q = `SELECT * WHERE {\n  ?s a <${BASE}/vocab/pet-store/owners> .\n}`;
    expect(relationshipPattern(q, HAS_PETS, PETS).pattern).toBe(
      "?s rel:HAS_PETS ?o . ?o a pet-store:pets .",
    );
  });

  it("after the pets class pill, asks for the owners that have them, ready to draw", () => {
    const first = pickedQuery("", { kind: "class", iri: `${BASE}/vocab/pet-store/pets` }, PETS);
    expect(first).toContain("?s a pet-store:pets .");
    const next = pickedQuery(first, { kind: "edge", iri: HAS_PETS }, PETS);
    expect(next).toContain("?x rel:HAS_PETS ?s . ?x a pet-store:owners .");
    expect(next).toContain("?s a pet-store:pets .");
    expect(next).not.toContain("?s rel:HAS_PETS ?o2");
    expect(next).toContain("PREFIX rel:");
  });

  it("is refused where the vocabulary has no class for an end", () => {
    const bare = { ...PETS, classes: PETS.classes.slice(0, 1) };
    expect(() => relationshipPattern("", HAS_PETS, bare)).toThrow(/no class for the label/);
    expect(() => relationshipPattern("", `${BASE}/vocab/rel/NOPE`, PETS)).toThrow(
      /no relationship/,
    );
  });
});

const iri = (path: string) => ({ type: "uri", value: `${BASE}/id/${path}` });
const answer = (vars: string[], bindings: Record<string, unknown>[]) =>
  response(
    JSON.stringify({ head: { vars }, results: { bindings } }),
    "application/sparql-results+json",
  );

describe("a SELECT of resources", () => {
  it("pairs two variables into subject and object, offered as a graph", async () => {
    const st = await sparqlStatement(
      "SELECT ?a ?b WHERE { ?a <http://x/knows> ?b }",
      answer(["a", "b"], [{ a: iri("ann"), b: iri("bob") }]),
      P,
    );
    expect(st.sparqlPairing).toEqual({ subject: "a", predicate: null, object: "b" });
    expect(st.nodes.size).toBe(0);
    const drawn = sparqlDrawn({ id: "f", ...st } as FrameData, false, true);
    expect(drawn.nodes.size).toBe(2);
    expect([...drawn.edges.values()].map((e) => e.type)).toEqual([""]);
    // Not asked: still a table.
    expect(sparqlDrawn({ id: "f", ...st } as FrameData, false, false).nodes.size).toBe(0);
  });

  it("names each edge by a bound predicate", async () => {
    const q = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }";
    const st = await sparqlStatement(
      q,
      answer(
        ["s", "p", "o"],
        [
          { s: iri("ann"), p: { type: "uri", value: `${P.rel}knows` }, o: iri("bob") },
          { s: iri("bob"), p: { type: "uri", value: `${P.rel}likes` }, o: iri("ann") },
        ],
      ),
      P,
    );
    expect(st.sparqlPairing).toEqual({ subject: "s", predicate: "p", object: "o" });
    const drawn = sparqlDrawn({ id: "f", ...st } as FrameData, false, true);
    expect(drawn.nodes.size).toBe(2);
    expect([...drawn.edges.values()].map((e) => e.type)).toEqual(["rel:knows", "rel:likes"]);
  });

  it("stays a table where a variable binds a literal, is unbound, or is not a pair", async () => {
    const lit = { type: "literal", value: "3" };
    const cases: [string, string[], Record<string, unknown>[]][] = [
      ["SELECT ?a ?n WHERE { ?a <http://x/n> ?n }", ["a", "n"], [{ a: iri("ann"), n: lit }]],
      ["SELECT ?a ?b WHERE { ?a <http://x/k> ?b }", ["a", "b"], [{ a: iri("ann") }]],
      [
        "SELECT ?a ?b ?c WHERE { ?a <http://x/k> ?b . ?b <http://x/k> ?c }",
        ["a", "b", "c"],
        [{ a: iri("a"), b: iri("b"), c: iri("c") }],
      ],
      ["SELECT ?a WHERE { ?a a <http://x/T> }", ["a"], [{ a: iri("ann") }]],
      ["SELECT ?a ?b WHERE { ?a <http://x/k> ?b }", ["a", "b"], []],
    ];
    for (const [q, vars, rows] of cases) {
      const st = await sparqlStatement(q, answer(vars, rows), P);
      expect(st.sparqlPairing).toBeUndefined();
    }
  });

  it("is never offered for an ASK", async () => {
    const st = await sparqlStatement(
      "ASK { ?s ?p ?o }",
      response(JSON.stringify({ head: {}, boolean: true }), "application/sparql-results+json"),
      P,
    );
    expect(st.sparqlPairing).toBeUndefined();
  });
});

describe("a CONSTRUCT frame's literal toggle", () => {
  it("draws the literals as nodes on request, and as properties otherwise", async () => {
    const st = await sparqlStatement(
      "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
      response(NT, "application/n-triples"),
      P,
    );
    const frame = { id: "f", ...st } as FrameData;
    expect(sparqlDrawn(frame, false, false).nodes.size).toBe(3);
    expect(sparqlDrawn(frame, true, false).nodes.size).toBe(6);
  });
});
