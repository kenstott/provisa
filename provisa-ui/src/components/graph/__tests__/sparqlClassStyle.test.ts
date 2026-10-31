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
  classLabel,
  classMenuProperties,
  parseNTriples,
  sparqlDrawn,
  triplesGraph,
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
  classes: [{ iri: PETS, kind: "table", labels: ["pet-store__pets"], superclasses: [] }],
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
      "pet-store:pets#age",
      "pet-store:pets#breedName",
      "pet-store:pets#price",
      "term:weight",
    ]);
    expect(menu.propertyNames["pet-store:pets#breedName"]).toBe("breedName");
    expect(menu.propertyNames["term:weight"]).toBe("weight");
  });

  it("offers the numeric ones to size by", () => {
    expect(menu.numericProperties).toEqual([
      "pet-store:pets#age",
      "pet-store:pets#price",
      "term:weight",
    ]);
  });

  it("names each predicate by the key the drawn node holds it under", () => {
    const node = nodeOf(`${typed(1)}\n${pet(1)} <${PETS}#breedName> "Husky" .`);
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
    const n = nodeOf(`${typed(1)}\n${price(1, "3.25")}`);
    expect(n.properties["pet-store:pets#price"]).toBe(`"3.25"^^xsd:decimal`);
    expect(n.propertyValues?.["pet-store:pets#price"]).toBe("3.25");
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
