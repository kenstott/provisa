// Copyright (c) 2026 Kenneth Stott
// Canary: 4b7e2c91-6d3a-4f08-9c15-e2a8f0d7b364
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// What a SPARQL class's menu changes is read by the node's own label and the predicate as the node
// holds it, and what it reads is the literal's value, not the way Turtle writes it.

import { describe, expect, it } from "vitest";
import { computeLabelSizeRanges } from "../canvas/canvas-helpers";
import { nodeCaption } from "../graph-stylesheet";
import type { CyInstance } from "../cytoscape-types";
import type { FrameData, GNode } from "../graph-model";
import {
  allPrefixes,
  classLabel,
  classMenuProperties,
  classPrefixes,
  compactIri,
  edgeName,
  parseNTriples,
  pickedQuery,
  pillQuery,
  shownIri,
  shownLiteral,
  sparqlDrawn,
  triplesGraph,
  withUsedPrefixes,
  type SparqlVocabulary,
} from "../sparql";

const B = "https://x.example";
const RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const XSD = "http://www.w3.org/2001/XMLSchema#";
const PREFIXES = {
  "pet-store": `${B}/vocab/pet-store/`,
  term: `${B}/vocab/term/`,
  xsd: XSD,
};
const PETS = `${B}/vocab/pet-store/pets`;
const pet = (i: number) => `<${B}/id/pet-store/pets/${i}>`;
const typed = (i: number) => `${pet(i)} <${RDF_TYPE}> <${PETS}> .`;
const nodeOf = (nt: string): GNode =>
  [...triplesGraph(parseNTriples(nt, PREFIXES)).nodes.values()].find(
    (n) => n.label === "pet-store:pets",
  ) as GNode;

const VOCABULARY: SparqlVocabulary = {
  base: B,
  prefixes: PREFIXES,
  classes: [{ iri: PETS, kind: "table", labels: ["pet-store__pets"], parameterized: false, parameterized_labels: [], superclasses: [] }],
  predicates: [
    {
      iri: `${PETS}#breedName`,
      kind: "column",
      label: "pet-store__pets",
      property: "breedName",
      datatype: `${XSD}string`,
    },
    {
      iri: `${PETS}#price`,
      kind: "column",
      label: "pet-store__pets",
      property: "price",
      datatype: `${XSD}decimal`,
    },
    {
      iri: `${PETS}#age`,
      kind: "column",
      label: "pet-store__pets",
      property: "age",
      datatype: `${XSD}integer`,
    },
    {
      iri: `${B}/vocab/pet-store/owners#email`,
      kind: "column",
      label: "pet-store__owners",
      property: "email",
      datatype: `${XSD}string`,
    },
    {
      iri: `${B}/vocab/term/weight`,
      kind: "term",
      classes: [
        { class: PETS, label: "pet-store__pets", property: "kg", datatype: `${XSD}double` },
      ],
    },
  ],
  edges: [],
  concepts: [],
};

describe("a class's menu", () => {
  const menu = classMenuProperties(VOCABULARY.classes[0], VOCABULARY, PREFIXES);

  it("lists the class's predicates as a node holds them, by name", () => {
    expect(menu.properties).toEqual([
      "pets:age",
      "pets:breedName",
      "pets:price",
      "term:weight",
    ]);
    expect(menu.propertyNames["pets:breedName"]).toBe("breedName");
    expect(menu.propertyNames["term:weight"]).toBe("weight");
  });

  it("offers the numeric ones to size by", () => {
    expect(menu.numericProperties).toEqual(["pets:age", "pets:price", "term:weight"]);
  });

  it("names each predicate by the key the drawn node holds it under", () => {
    const nt = `${typed(1)}\n${pet(1)} <${PETS}#breedName> "Husky" .`;
    const [node] = triplesGraph(parseNTriples(nt, allPrefixes(VOCABULARY))).nodes.values();
    expect(menu.properties).toContain(Object.keys(node.propertyIris ?? {})[0]);
  });
});

describe("a class's label", () => {
  it("is the label a drawn node of the class has", () => {
    expect(classLabel(PETS, PREFIXES)).toBe(nodeOf(typed(1)).label);
  });

  it("is the drawn label for a class whose local name a query could not write", () => {
    const iri = `${B}/vocab/term/Hire%20Date`;
    const nt = `<${B}/id/a/1> <${RDF_TYPE}> <${iri}> .`;
    const [node] = triplesGraph(parseNTriples(nt, PREFIXES)).nodes.values();
    expect(classLabel(iri, PREFIXES)).toBe(node.label);
  });

  it("is the same for a SELECT that types its nodes by another name of the namespace", () => {
    const row = (i: number) => ({
      s: { rdfTerm: "iri" as const, value: `${B}/id/pet-store/pets/${i}`, shown: "" },
      o: { rdfTerm: "iri" as const, value: `${B}/id/pet-store/owners/${i}`, shown: "" },
    });
    const frame = {
      id: "f",
      query: `PREFIX ps: <${B}/vocab/pet-store/>\nSELECT ?s ?o WHERE { ?s a ps:pets . ?s ps:owner ?o }`,
      status: "done",
      nodes: new Map(),
      edges: new Map(),
      rows: [row(1)],
      columns: ["s", "o"],
      sparqlForm: "rows",
      sparqlPairing: { subject: "s", predicate: null, object: "o" },
    } as FrameData;
    const drawn = sparqlDrawn(frame, false, true, PREFIXES);
    expect([...drawn.nodes.values()].map((n) => n.label)).toContain("pet-store:pets");
  });
});

describe("a literal the menu reads", () => {
  const price = (i: number, v: string) => `${pet(i)} <${PETS}#price> "${v}"^^<${XSD}decimal> .`;

  it("keeps its Turtle form for the panel and its value for the drawing", () => {
    const n = nodeOf(`${typed(1)}\n${pet(1)} <${PETS}#nick> "Rex"@en .`);
    expect(n.properties["pet-store:pets#nick"]).toBe(`"Rex"@en`);
    expect(n.propertyValues?.["pet-store:pets#nick"]).toBe("Rex");
  });

  it("captions a node with the value", () => {
    const n = nodeOf(`${typed(1)}\n${price(1, "3.25")}`);
    const by = { "pet-store:pets": "pet-store:pets#price" };
    expect(nodeCaption(n, n.label, by)).toBe("3.25");
  });

  it("sizes nodes by a decimal", () => {
    const ns = [1, 2].map((i) => nodeOf(`${typed(i)}\n${price(i, String(i * 10.5))}`));
    const cy = {
      nodes: () => ({
        forEach: (f: (n: unknown) => void) =>
          ns.forEach((gn) =>
            f({
              data: (k: string) => (k === "label" ? gn.label : k === "_node" ? gn : undefined),
            }),
          ),
      }),
    } as unknown as CyInstance;
    const ranges = computeLabelSizeRanges(cy, { "pet-store:pets": "pet-store:pets#price" });
    expect(ranges.get("pet-store:pets#price")).toEqual({ min: 10.5, max: 21 });
  });

  it("leaves a node that has no value under its own name, not an internal number", () => {
    const n = nodeOf(`${typed(1)}\n${pet(1)} <${PETS}#name> "Rex" .`);
    const by = { "pet-store:pets": "pet-store:pets#price" };
    expect(nodeCaption(n, n.label, by)).toBe("Rex");
  });

  it("keeps a Cypher node's caption by its id where the property is absent", () => {
    const n: GNode = { id: 7, label: "Pets", tableLabel: "Pets", properties: {} };
    expect(nodeCaption(n, n.label, { Pets: "breed" })).toBe("7");
  });
});

describe("what a DESCRIBE said of a drawn node", () => {
  const nt = `${typed(1)}\n${pet(1)} <${B}/rel/owner> <${B}/id/pet-store/owners/1> .`;
  const literal = parseNTriples(`${pet(1)} <${PETS}#breedName> "Husky" .`, PREFIXES);
  const frame = {
    id: "f",
    query: "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
    status: "done",
    nodes: new Map(),
    edges: new Map(),
    rows: [],
    columns: [],
    sparqlForm: "graph",
    sparqlTriples: parseNTriples(nt, PREFIXES),
    sparqlDescribed: { [`${B}/id/pet-store/pets/1`]: { literals: literal } },
  } as FrameData;

  it("is on the node, so a chosen caption can read it", () => {
    const drawn = sparqlDrawn(frame, false, false, PREFIXES);
    const n = [...drawn.nodes.values()].find((x) => x.label === "pet-store:pets") as GNode;
    expect(n.properties["pet-store:pets#breedName"]).toBe("Husky");
    expect(n.propertyValues?.["pet-store:pets#breedName"]).toBe("Husky");
    expect(n.propertyIris?.["pet-store:pets#breedName"]).toBe(`${PETS}#breedName`);
  });

  it("shows by the prefixes in use, not those it was asked under", () => {
    const edited = { ...PREFIXES, shop: PREFIXES["pet-store"] };
    delete (edited as Record<string, string>)["pet-store"];
    const drawn = sparqlDrawn(frame, false, false, edited);
    const n = [...drawn.nodes.values()].find((x) => x.label === "shop:pets") as GNode;
    expect(n.properties["shop:pets#breedName"]).toBe("Husky");
  });
});

describe("a class pill's query", () => {
  const twoColumns: SparqlVocabulary = {
    ...VOCABULARY,
    predicates: VOCABULARY.predicates.filter((x) => x.property === "breedName" || x.property === "age"),
  };
  const wide = pillQuery({ kind: "class", iri: PETS }, twoColumns);

  it("asks one row per subject, with one OPTIONAL per property of the class", () => {
    expect(wide).toBe(
      [
        `PREFIX pet-store: <${B}/vocab/pet-store/>`,
        `PREFIX pets: <${PETS}#>`,
        "SELECT ?s ?age ?breedName WHERE {",
        "  ?s a pet-store:pets .",
        "  OPTIONAL { ?s pets:age ?age }",
        "  OPTIONAL { ?s pets:breedName ?breedName }",
        "}",
        "LIMIT 25",
      ].join("\n"),
    );
  });

  it("writes no # outside a PREFIX line, where SPARQL would read a comment", () => {
    const body = wide.split("\n").filter((l) => !l.startsWith("PREFIX"));
    expect(body.join("\n")).not.toContain("#");
  });

  it("writes a term by its prefix, percent escapes and all, and keeps each variable safe and its own", () => {
    const term = `${B}/vocab/term/hire%20date`;
    const vocabulary: SparqlVocabulary = {
      ...VOCABULARY,
      predicates: [
        {
          iri: term,
          kind: "term",
          classes: [{ class: PETS, label: "pet-store__pets", property: "hired", datatype: `${XSD}date` }],
        },
        { iri: `${PETS}#s`, kind: "column", label: "pet-store__pets", property: "s", datatype: `${XSD}string` },
      ],
    };
    const q = pillQuery({ kind: "class", iri: PETS }, vocabulary);
    expect(q).toContain("OPTIONAL { ?s term:hire%20date ?hire_date }");
    expect(q).toContain(`PREFIX term: <${B}/vocab/term/>`);
    expect(q).toContain("OPTIONAL { ?s pets:s ?s2 }");
    expect(q).toContain("SELECT ?s ?hire_date ?s2 WHERE {");
  });

  it("stays a plain pattern for a class with no property the role can read", () => {
    const vocabulary = { ...VOCABULARY, predicates: [] };
    expect(pillQuery({ kind: "class", iri: PETS }, vocabulary)).toBe(
      `PREFIX pet-store: <${B}/vocab/pet-store/>\nSELECT * WHERE {\n  ?s a pet-store:pets .\n}\nLIMIT 25`,
    );
  });

  it("keeps a frame's keys by the vocabulary's names, whatever prefix the query declares", () => {
    const nt = `${typed(1)}\n${pet(1)} <${PETS}#age> "4"^^<${XSD}integer> .`;
    const frame = {
      id: "f",
      query: wide.replace("PREFIX pets:", "PREFIX px:").replaceAll("pets:age", "px:age"),
      status: "done",
      nodes: new Map(),
      edges: new Map(),
      rows: [],
      columns: [],
      sparqlForm: "graph",
      sparqlTriples: parseNTriples(nt, PREFIXES),
    } as FrameData;
    const drawn = sparqlDrawn(frame, false, false, PREFIXES);
    const n = [...drawn.nodes.values()].find((x) => x.label === "pet-store:pets") as GNode;
    expect(Object.keys(n.propertyIris ?? {})).toEqual(["pet-store:pets#age"]);
    const named = allPrefixes(VOCABULARY);
    const shown = sparqlDrawn(frame, false, false, named);
    const m = [...shown.nodes.values()].find((x) => x.label === "pet-store:pets") as GNode;
    expect(Object.keys(m.propertyIris ?? {})).toEqual(["pets:age"]);
  });
});

describe("a literal as it is shown", () => {
  it.each([
    ["800.0", "double"],
    ["1.5", "float"],
    ["12.50", "decimal"],
    ["9", "long"],
    ["9", "nonNegativeInteger"],
    ["true", "boolean"],
    ["2026-10-31", "date"],
    ["2026-10-31T08:00:00Z", "dateTime"],
    ["08:00:00", "time"],
    ["2026", "gYear"],
    ["P1D", "duration"],
  ])("shows %s as xsd:%s plainly", (value, datatype) => {
    expect(shownLiteral(value, `${XSD}${datatype}`, undefined, PREFIXES)).toBe(value);
  });

  it("keeps a language tag, and a datatype that has no plain reading", () => {
    expect(shownLiteral("Rex", undefined, "en", PREFIXES)).toBe('"Rex"@en');
    expect(shownLiteral("AB", `${XSD}hexBinary`, undefined, PREFIXES)).toBe('"AB"^^xsd:hexBinary');
  });

  it("keeps the datatype for the node's hover, and for a DESCRIBE's literals", () => {
    const n = nodeOf(`${typed(1)}\n${pet(1)} <${PETS}#price> "3.25"^^<${XSD}decimal> .`);
    expect(n.properties["pet-store:pets#price"]).toBe("3.25");
    expect(n.propertyDatatypes?.["pet-store:pets#price"]).toBe(`${XSD}decimal`);
  });
});

describe("a class's own prefix", () => {
  const classOf = (domain: string, table: string) => ({
    iri: `${B}/vocab/${domain}/${table}`,
    kind: "table" as const,
    labels: [`${domain}__${table}`],
    parameterized: false,
    parameterized_labels: [],
    superclasses: [],
  });
  const v = (classes: SparqlVocabulary["classes"], prefixes = PREFIXES): SparqlVocabulary => ({
    ...VOCABULARY,
    prefixes,
    classes,
  });

  it("is the class's name, ending in its #", () => {
    expect(classPrefixes(VOCABULARY)).toEqual({ pets: `${PETS}#` });
  });

  it("is qualified by its domain where two domains share the class's name", () => {
    const prefixes = { ...PREFIXES, meta: `${B}/vocab/meta/` };
    const classes = [classOf("meta", "products"), classOf("pet-store", "products")];
    expect(classPrefixes(v(classes, prefixes))).toEqual({
      meta_products: `${B}/vocab/meta/products#`,
      "pet-store_products": `${B}/vocab/pet-store/products#`,
    });
  });

  it("is qualified where a panel prefix has the name", () => {
    expect(classPrefixes(v([classOf("pet-store", "term")]))).toEqual({
      "pet-store_term": `${B}/vocab/pet-store/term#`,
    });
  });

  it("names a predicate in a result, where a # could not", () => {
    expect(shownIri(`${PETS}#breedName`, allPrefixes(VOCABULARY))).toBe("pets:breedName");
    expect(compactIri(`${PETS}#breedName`, allPrefixes(VOCABULARY))).toBe("pets:breedName");
  });

  it("is what a property's click writes, declared", () => {
    expect(pickedQuery("", { kind: "predicate", iri: `${PETS}#breedName` }, VOCABULARY)).toBe(
      `PREFIX pets: <${PETS}#>\nSELECT * WHERE {\n  ?s pets:breedName ?o .\n}\nLIMIT 25`,
    );
  });
});

describe("a term's click", () => {
  it("uses the prefix, percent escapes and all", () => {
    const iri = `${B}/vocab/term/shift%20coverage`;
    expect(compactIri(iri, PREFIXES)).toBe("term:shift%20coverage");
    expect(pickedQuery("", { kind: "predicate", iri }, VOCABULARY)).toBe(
      `PREFIX term: <${PREFIXES.term}>\nSELECT * WHERE {\n  ?s term:shift%20coverage ?o .\n}\nLIMIT 25`,
    );
  });

  it("stays whole where the rest is no prefixed name", () => {
    expect(compactIri(`${B}/vocab/term/a/b`, PREFIXES)).toBe(`<${B}/vocab/term/a/b>`);
    expect(compactIri(`${B}/vocab/term/a%2`, PREFIXES)).toBe(`<${B}/vocab/term/a%2>`);
    expect(compactIri(`${B}/vocab/term/a.`, PREFIXES)).toBe(`<${B}/vocab/term/a.>`);
  });
});

describe("the prefixes a run adds", () => {
  const named = { "pet-store": `${B}/vocab/pet-store/`, xsd: XSD, rel: `${B}/vocab/rel/` };

  it("are those the query uses and does not declare", () => {
    expect(withUsedPrefixes("SELECT * WHERE { ?s a pet-store:pets }", named)).toBe(
      `PREFIX pet-store: <${named["pet-store"]}>\nSELECT * WHERE { ?s a pet-store:pets }`,
    );
  });

  it("leave a query's own PREFIX, wherever it stands", () => {
    const q =
      "PREFIX a: <http://a/> PREFIX pet-store: <http://mine/>\nSELECT * WHERE { ?s a pet-store:pets }";
    expect(withUsedPrefixes(q, named)).toBe(q);
  });

  it("do not read a string, an IRI or a comment", () => {
    const q = [
      "# rel:IN_A_COMMENT",
      'SELECT * WHERE { ?s <http://x/xsd:y> ?o FILTER(?o = "pet-store:in-a-string") }',
    ].join("\n");
    expect(withUsedPrefixes(q, named)).toBe(q);
    const long = `SELECT * WHERE { ?s ?p ${"'".repeat(3)}rel:x${"'".repeat(3)} }`;
    expect(withUsedPrefixes(long, named)).toBe(long);
  });

  it("read a datatype and a path", () => {
    expect(withUsedPrefixes('SELECT * WHERE { ?s ?p "1"^^xsd:integer }', named)).toContain(
      `PREFIX xsd: <${XSD}>`,
    );
    const path = withUsedPrefixes("SELECT * WHERE { ?s rel:A/rel:B ?o }", named);
    expect(path.match(/PREFIX rel:/g)).toHaveLength(1);
  });

  it("leave a prefix the panel does not have, for the server to refuse by name", () => {
    const q = "SELECT * WHERE { ?s a nope:x }";
    expect(withUsedPrefixes(q, named)).toBe(q);
  });
});

describe("a relationship's name", () => {
  it("drops rel:, every relationship being under it", () => {
    expect(edgeName("rel:BREED_INFO")).toBe("BREED_INFO");
  });

  it("keeps any other prefix", () => {
    expect(edgeName("sales:owner")).toBe("sales:owner");
    expect(edgeName("<http://x/y>")).toBe("<http://x/y>");
  });

  it("is what the graph draws, while its triple keeps the predicate", () => {
    const nt = `${pet(1)} <${B}/vocab/rel/BREED_INFO> ${pet(2)} .`;
    const prefixes = { ...PREFIXES, rel: `${B}/vocab/rel/` };
    const triples = parseNTriples(nt, prefixes);
    const [edge] = triplesGraph(triples).edges.values();
    expect(edge.type).toBe("BREED_INFO");
    expect(edge.properties.iri).toBe(`${B}/vocab/rel/BREED_INFO`);
    expect(triples[0].p.shown).toBe("rel:BREED_INFO");
  });
});
