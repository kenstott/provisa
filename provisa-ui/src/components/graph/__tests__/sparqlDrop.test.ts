// Copyright (c) 2026 Kenneth Stott
// Canary: 19c4ae60-3fb7-4c12-96d8-e0a7f5d2b143
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135: what a pill dropped on a SPARQL frame does to its query.

import { describe, expect, it } from "vitest";
import { droppedQuery, type SparqlDrop, type SparqlDropped } from "../sparqlDrop";
import type { SparqlVocabulary } from "../sparql";
import type { CyElement, CyInstance } from "../cytoscape-types";
import { nodeAtPoint, nodeUnder } from "../canvas/canvas-helpers";

const B = "https://x.example/vocab";
const cls = (domain: string, name: string): SparqlVocabulary["classes"][number] => ({
  iri: `${B}/${domain}/${name}`,
  kind: "table",
  labels: [name],
  parameterized: false,
  parameterized_labels: [],
  superclasses: [],
});
const edge = (type: string, source: string, target: string) => ({
  iri: `${B}/rel/${type}`,
  type,
  source,
  target,
  join: `${type} ${source} ${target}`,
});
const vocabulary: SparqlVocabulary = {
  base: "https://x.example",
  prefixes: {
    rdf: "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    rel: `${B}/rel/`,
    ps: `${B}/ps/`,
    hr: `${B}/hr/`,
  },
  classes: [
    cls("ps", "pets"),
    cls("ps", "inquiries"),
    cls("ps", "users"),
    cls("ps", "vets"),
    cls("ps", "lonely"),
    cls("hr", "staff"),
    cls("hr", "far1"),
    cls("hr", "far2"),
    cls("hr", "far3"),
    cls("hr", "far4"),
    cls("hr", "far5"),
  ],
  predicates: [
    { iri: `${B}/ps/users#email`, kind: "column", label: "users", property: "email" },
    { iri: `${B}/ps/pets#name`, kind: "column", label: "pets", property: "name" },
  ],
  edges: [
    edge("HAS_INQUIRY", "pets", "inquiries"),
    edge("SUBMITTED", "users", "inquiries"),
    edge("SUBMITTED_BY", "inquiries", "users"),
    edge("TREATED_BY", "pets", "vets"),
    edge("EMPLOYS", "vets", "staff"),
    edge("F1", "staff", "far1"),
    edge("F2", "far1", "far2"),
    edge("F3", "far2", "far3"),
    edge("F4", "far3", "far4"),
    edge("F5", "far4", "far5"),
  ],
  concepts: [],
};

const FRAME = `PREFIX ps: <${B}/ps/>
PREFIX rel: <${B}/rel/>
SELECT * WHERE {
  ?s a ps:pets .
  ?s rel:HAS_INQUIRY ?o .
  ?o a ps:inquiries .
}
LIMIT 25`;

const iri = (name: string) => `${B}/ps/${name}`;
const drop = (query: string, what: SparqlDrop, on: { iri: string; label: string } | null = null) =>
  droppedQuery(query, what, vocabulary, on, []);
const queryOf = (d: SparqlDropped): string => {
  if ("refused" in d) throw new Error(d.refused.key);
  return d.query;
};
const refusal = (d: SparqlDropped) => ("refused" in d ? d.refused : null);

describe("a class dropped on a frame", () => {
  it("joins the class by a relationship, not by its type alone", () => {
    const q = queryOf(drop(FRAME, { kind: "class", iri: iri("users") }));
    expect(q).toContain("OPTIONAL { ?users rel:SUBMITTED ?o . ?users a ps:users . }");
    expect(q).toContain("OPTIONAL { ?o rel:SUBMITTED_BY ?users2 . ?users2 a ps:users . }");
    expect(q).not.toMatch(/^\s*\?users a ps:users \.\s*$/m);
  });

  it("adds a pattern per step, its intermediate classes typed, all in one OPTIONAL", () => {
    const q = queryOf(drop(FRAME, { kind: "class", iri: `${B}/hr/staff` }));
    expect(q).toContain(
      "OPTIONAL { ?s rel:TREATED_BY ?vets . ?vets a ps:vets . ?vets rel:EMPLOYS ?staff . ?staff a hr:staff . }",
    );
    expect(q).toContain(`PREFIX hr: <${B}/hr/>`);
  });

  it("says by name that a class joins nothing, and adds nothing", () => {
    const d = drop(FRAME, { kind: "class", iri: iri("lonely") });
    expect(refusal(d)).toEqual({
      key: "graphFrame.sparqlDropNoPath",
      values: { name: "ps:lonely", classes: "ps:pets, ps:inquiries" },
    });
  });

  it("says when the path is past the cap", () => {
    const d = drop(FRAME, { kind: "class", iri: `${B}/hr/far5` });
    expect(refusal(d)?.key).toBe("graphFrame.sparqlDropTooFar");
    expect(refusal(d)?.values).toMatchObject({ hops: 7, most: 4 });
  });

  it("adds at most five tied paths and says what it left out", () => {
    const v = {
      ...vocabulary,
      edges: [1, 2, 3, 4, 5, 6, 7].map((n) => edge(`R${n}`, "pets", "users")),
    };
    const q = `SELECT * WHERE { ?s a <${iri("pets")}> }`;
    const d = droppedQuery(q, { kind: "class", iri: iri("users") }, v, null, []);
    if ("refused" in d) throw new Error(d.refused.key);
    expect(d.query.match(/OPTIONAL/g)).toHaveLength(5);
    expect(d.notices).toEqual([
      { key: "graphFrame.sparqlDropLeftOut", values: { shown: 5, total: 7 } },
    ]);
  });

  it("keeps every variable unique over drops", () => {
    const once = queryOf(drop(FRAME, { kind: "class", iri: iri("users") }));
    const twice = queryOf(drop(once, { kind: "class", iri: iri("users") }));
    const introduced = [...twice.matchAll(/(\?\w+) a ps:users/g)].map((m) => m[1]);
    expect(new Set(introduced).size).toBe(introduced.length);
    expect(introduced.length).toBeGreaterThan(2);
  });

  it("names the new variables in a named SELECT", () => {
    const q = queryOf(
      drop(FRAME.replace("SELECT *", "SELECT DISTINCT ?s ?o"), {
        kind: "class",
        iri: `${B}/ps/vets`,
      }),
    );
    expect(q).toContain("SELECT DISTINCT ?s ?o ?vets WHERE {");
  });

  it("adds the path to a CONSTRUCT's template and its WHERE", () => {
    const q = queryOf(
      drop(`CONSTRUCT { ?s ?p ?x } WHERE { ?s a <${iri("pets")}> ; ?p ?x } LIMIT 25`, {
        kind: "class",
        iri: iri("vets"),
      }),
    );
    expect(q).toMatch(
      /CONSTRUCT \{ \?s \?p \?x \.\n {2}\?s rel:TREATED_BY \?vets \. \?vets a ps:vets \.\n\}/,
    );
    expect(q).toMatch(
      /OPTIONAL \{ \?s rel:TREATED_BY \?vets \. \?vets a ps:vets \. \}\n\} LIMIT 250/,
    );
  });

  it("writes out the short CONSTRUCT, whose pattern can hold no OPTIONAL", () => {
    const q = queryOf(
      drop(`CONSTRUCT WHERE { ?s a <${iri("pets")}> }`, { kind: "class", iri: iri("vets") }),
    );
    expect(q).toMatch(/CONSTRUCT \{\n {2}\?s a <[^>]+> \.\n {2}\?s rel:TREATED_BY/);
    expect(q).toMatch(/\} WHERE \{\n {2}\?s a <[^>]+>[\s\S]*OPTIONAL/);
  });

  it("reads classes from the frame's other triples too", () => {
    const q = queryOf(
      drop(`SELECT * WHERE { ?a <${B}/rel/TREATED_BY> ?b }`, {
        kind: "class",
        iri: `${B}/hr/staff`,
      }),
    );
    expect(q).toContain("OPTIONAL { ?b rel:EMPLOYS ?staff");
  });

  it("raises a LIMIT so rows are not cut, and keeps a query without one", () => {
    const d = drop(FRAME, { kind: "class", iri: iri("vets") });
    if ("refused" in d) throw new Error(d.refused.key);
    expect(d.query).toMatch(/LIMIT 250$/);
    expect(d.limit).toBe(250);
    const open = drop(FRAME.replace("LIMIT 25", ""), { kind: "class", iri: iri("vets") });
    if ("refused" in open) throw new Error(open.refused.key);
    expect(open.query).not.toContain("LIMIT");
    expect(open.limit).toBeUndefined();
  });

  it("starts from the node it was dropped on, and only there", () => {
    const node = { iri: "https://x.example/id/ps/pets/3", label: "ps:pets" };
    const q = queryOf(drop(FRAME, { kind: "class", iri: iri("vets") }, node));
    expect(q).toContain(
      `OPTIONAL { ?s rel:TREATED_BY ?vets . ?vets a ps:vets . FILTER(?s = <${node.iri}>) }`,
    );
  });

  it("refuses a node no variable holds", () => {
    const node = { iri: "https://x.example/id/ps/users/3", label: "ps:users" };
    expect(refusal(drop(FRAME, { kind: "class", iri: iri("vets") }, node))?.key).toBe(
      "graphFrame.sparqlDropNode",
    );
  });

  it("says when the result names no class to join to", () => {
    expect(
      refusal(
        drop("SELECT * WHERE { <urn:x> ?p ?o } LIMIT 25", { kind: "class", iri: iri("vets") }),
      )?.key,
    ).toBe("graphFrame.sparqlDropNoClass");
  });
});

describe("the shapes a drop refuses", () => {
  it.each([
    ["ASK { ?s ?p ?o }", "graphFrame.sparqlDropForm"],
    ["DESCRIBE <https://x.example/id/1>", "graphFrame.sparqlDropForm"],
    ["SELECT * WHERE { { ?s a <a> } UNION { ?s a <b> } }", "graphFrame.sparqlDropUnion"],
    ["SELECT ?s WHERE { ?s a <a> } GROUP BY ?s", "graphFrame.sparqlDropGroupBy"],
    ["SELECT (COUNT(?s) AS ?n) WHERE { ?s a <a> }", "graphFrame.sparqlDropAggregate"],
    ["SELECT * WHERE { { SELECT ?s WHERE { ?s a <a> } } }", "graphFrame.sparqlDropSubquery"],
  ])("%s", (query, key) => {
    expect(refusal(drop(query, { kind: "class", iri: iri("vets") }))?.key).toBe(key);
  });

  it("does not read a word in a string for a clause", () => {
    expect(
      refusal(
        drop(`SELECT * WHERE { ?s a ps:pets FILTER(?s != "GROUP BY UNION") }`, {
          kind: "class",
          iri: iri("vets"),
        }),
      ),
    ).toBeNull();
  });
});

describe("a relationship dropped on a frame", () => {
  it("attaches to the variable holding its source", () => {
    const q = queryOf(drop(FRAME, { kind: "edge", iri: `${B}/rel/TREATED_BY` }));
    expect(q).toContain("OPTIONAL { ?s rel:TREATED_BY ?vets . ?vets a ps:vets . }");
  });

  it("attaches to the variable holding its target, against the edge", () => {
    const q = queryOf(drop(FRAME, { kind: "edge", iri: `${B}/rel/SUBMITTED` }));
    expect(q).toContain("OPTIONAL { ?users rel:SUBMITTED ?o . ?users a ps:users . }");
  });

  it("says by name when neither end is in the result", () => {
    expect(refusal(drop(FRAME, { kind: "edge", iri: `${B}/rel/F3` }))).toEqual({
      key: "graphFrame.sparqlDropNoEnd",
      values: { name: "rel:F3" },
    });
  });
});

describe("a property dropped on a frame", () => {
  it("binds a fresh variable on the class that has it", () => {
    const q = queryOf(drop(FRAME, { kind: "predicate", iri: `${B}/ps/pets#name` }));
    expect(q).toContain("OPTIONAL { ?s pets:name ?name . }");
    expect(q).toContain(`PREFIX pets: <${B}/ps/pets#>`);
  });

  it("says by name when no class of the result has it", () => {
    expect(refusal(drop(FRAME, { kind: "predicate", iri: `${B}/ps/users#email` }))?.key).toBe(
      "graphFrame.sparqlDropNoProperty",
    );
  });
});

describe("a domain dropped on a frame", () => {
  it("reaches the nearest of its classes the result does not hold", () => {
    const q = queryOf(drop(FRAME, { kind: "domain", prefix: "hr" }));
    expect(q).toContain("?staff a hr:staff");
    expect(q).not.toContain("far1");
  });

  it("names the domain when nothing joins it", () => {
    const v = { ...vocabulary, edges: [] };
    const d = droppedQuery(FRAME, { kind: "domain", prefix: "hr" }, v, null, []);
    expect(refusal(d)).toMatchObject({
      key: "graphFrame.sparqlDropNoPath",
      values: { name: "hr" },
    });
  });
});

describe("the node at a point of a drawing", () => {
  const node = (id: string, x1: number, y1: number): CyElement =>
    ({
      id: () => id,
      data: () => ({ id: 1, label: id, tableLabel: id, properties: {} }),
      renderedPosition: () => ({ x: x1 + 10, y: y1 + 10 }),
      renderedBoundingBox: () => ({ x1, y1, x2: x1 + 20, y2: y1 + 20, w: 20, h: 20 }),
    }) as unknown as CyElement;
  const a = node("a", 0, 0);
  const b = node("b", 5, 5);
  const far = node("far", 100, 100);
  const cy = {
    nodes: () => [a, b, far],
    container: () => ({ getBoundingClientRect: () => ({ left: 200, top: 100 }) }),
  } as unknown as CyInstance;

  it("reads a page's point against the canvas", () => {
    expect(nodeAtPoint(cy, 310, 210)?.label).toBe("far");
    expect(nodeAtPoint(cy, 260, 160)).toBeNull();
  });

  it("finds what a dragged node was let go on, never itself", () => {
    expect(nodeUnder(cy, a)?.label).toBe("b");
    expect(nodeUnder(cy, far)).toBeNull();
  });
});

describe("REQ-2135 a parameterized table on a SPARQL frame", () => {
  const taken = (name: string) => ({
    ...cls("ps", name),
    parameterized: true,
    parameterized_labels: [name],
  });
  const withTaken: SparqlVocabulary = {
    ...vocabulary,
    classes: [...vocabulary.classes, taken("by_status")],
    edges: [
      edge("VIA", "pets", "by_status"),
      edge("BACK", "by_status", "staff"),
      ...vocabulary.edges,
    ],
  };
  const on = (what: SparqlDrop) => droppedQuery(FRAME, what, withTaken, null, []);

  it("is no way through: the path goes round it", () => {
    const q = queryOf(on({ kind: "class", iri: `${B}/hr/staff` }));
    expect(q).not.toContain("by_status");
    expect(q).toContain("rel:TREATED_BY");
    expect(q).toContain("rel:EMPLOYS");
  });

  it("is refused by name when dropped", () => {
    expect(refusal(on({ kind: "class", iri: iri("by_status") }))).toEqual({
      key: "graphFrame.sparqlDropParameterized",
      values: { name: "ps:by_status" },
    });
  });

  it("is refused by name as a domain of nothing else", () => {
    const v: SparqlVocabulary = {
      ...withTaken,
      prefixes: { ...withTaken.prefixes, only: `${B}/only/` },
      classes: [...withTaken.classes, { ...taken("x"), iri: `${B}/only/x` }],
    };
    const d = droppedQuery(FRAME, { kind: "domain", prefix: "only" }, v, null, []);
    expect(refusal(d)).toMatchObject({ key: "graphFrame.sparqlDropParameterized" });
  });

  it("is left out of a domain's classes", () => {
    const q = queryOf(on({ kind: "domain", prefix: "ps" }));
    expect(q).not.toContain("by_status");
  });

  it("is never what a relationship pill adds", () => {
    expect(refusal(on({ kind: "edge", iri: `${B}/rel/VIA` }))).toEqual({
      key: "graphFrame.sparqlDropEdgeParameterized",
      values: { name: "rel:VIA" },
    });
  });
});

describe("REQ-2135 a join declared from both ends on a SPARQL frame", () => {
  it("adds one path, with no left-out notice", () => {
    const v: SparqlVocabulary = {
      ...vocabulary,
      edges: vocabulary.edges.map((e) =>
        e.type === "SUBMITTED" || e.type === "SUBMITTED_BY" ? { ...e, join: "one join" } : e,
      ),
    };
    const d = droppedQuery(FRAME, { kind: "class", iri: iri("users") }, v, null, []);
    const q = queryOf(d);
    expect(q.match(/OPTIONAL/g)).toHaveLength(1);
    expect("notices" in d && d.notices).toEqual([]);
  });
});

describe("REQ-2135 a pill dropped on ?s ?p ?o", () => {
  const OPEN = `PREFIX ps: <${B}/ps/>
PREFIX rel: <${B}/rel/>
SELECT * WHERE {
  ?s ?p ?o
}
LIMIT 25`;

  it("fans a class out over the classes that join it, each in its own OPTIONAL", () => {
    const q = queryOf(drop(OPEN, { kind: "class", iri: iri("users") }));
    expect(q).toContain("OPTIONAL { ?users rel:SUBMITTED ?s . ?users a ps:users . }");
    expect(q.match(/OPTIONAL/g)).toHaveLength(2);
    expect(q).toContain("LIMIT 250");
  });

  it("starts a relationship from ?s, from either end", () => {
    const q = queryOf(drop(OPEN, { kind: "edge", iri: `${B}/rel/TREATED_BY` }));
    expect(q.match(/OPTIONAL/g)).toHaveLength(2);
    expect(q).toContain("?s rel:TREATED_BY ?vets");
    expect(q).toContain("rel:TREATED_BY ?s");
  });

  it("reads a property from ?s", () => {
    const q = queryOf(drop(OPEN, { kind: "predicate", iri: `${B}/ps/users#email` }));
    expect(q).toContain("OPTIONAL { ?s users:email ?email . }");
  });

  it("names the variable when nothing joins the class", () => {
    const d = drop(OPEN, { kind: "class", iri: iri("lonely") });
    expect(refusal(d)).toEqual({
      key: "graphFrame.sparqlDropNoPath",
      values: { name: "ps:lonely", classes: "?s" },
    });
  });

  it("starts at the node dropped on, of that node's class", () => {
    const rows = [{ s: { value: `https://x.example/id/ps/pets/1` } }];
    const node = { iri: `https://x.example/id/ps/pets/1`, label: "ps:pets" };
    const q = queryOf(
      droppedQuery(OPEN, { kind: "edge", iri: `${B}/rel/TREATED_BY` }, vocabulary, node, rows),
    );
    expect(q.match(/OPTIONAL/g)).toHaveLength(1);
    expect(q).toContain("FILTER(?s = <https://x.example/id/ps/pets/1>)");
  });

  it("stays open after a drop: what an OPTIONAL says of ?s types it only there", () => {
    const once = queryOf(drop(OPEN, { kind: "edge", iri: `${B}/rel/HAS_INQUIRY` }));
    const twice = queryOf(drop(once, { kind: "edge", iri: `${B}/rel/EMPLOYS` }));
    expect(twice).toContain("rel:EMPLOYS ?staff");
  });

  it("stays open in a CONSTRUCT, whose template a drop has added to", () => {
    const construct = `PREFIX rel: <${B}/rel/>
CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }`;
    const once = queryOf(drop(construct, { kind: "edge", iri: `${B}/rel/HAS_INQUIRY` }));
    const twice = queryOf(drop(once, { kind: "edge", iri: `${B}/rel/EMPLOYS` }));
    expect(twice).toContain("rel:EMPLOYS ?staff");
  });

  it("leaves a typed subject typed", () => {
    const typed = OPEN.replace("?s ?p ?o", "?s ?p ?o . ?s a ps:pets");
    const q = queryOf(drop(typed, { kind: "class", iri: iri("inquiries") }));
    expect(q.match(/OPTIONAL/g)).toHaveLength(1);
    expect(q).toContain("rel:HAS_INQUIRY");
  });
});
