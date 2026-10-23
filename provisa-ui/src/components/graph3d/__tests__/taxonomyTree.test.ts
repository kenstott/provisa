// Copyright (c) 2026 Kenneth Stott
// Canary: 57cf7d02-8ae6-4f2d-a5af-8514ada9d243
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Ontology View's Taxonomy: a top-down tree whose levels only KIND_OF and PART_OF set -- the
// general above the specific, the whole above the part -- with each column a term IS hung a level
// below the term, under it, and each term in no such edge a root beside the others.

import { describe, expect, it } from "vitest";
import { GLOSSARY_SCOPE, NO_FILTERS, layoutGraph, visibleGraph, type LayerId } from "../graphModel";
import { glossaryGraph, type TermEdgeRow, type TermRow } from "../glossaryGraphData";

const ref = (table: number, column: string) => ({
  column_key: `${table}:${column}`,
  column_name: column,
  table_id: table,
  table_name: `t${table}`,
  domain_id: "pets",
  table: { id: table, product: null },
});

const term = (id: number, name: string, refs: ReturnType<typeof ref>[] = []): TermRow => ({
  id,
  name,
  definition: `${name}.`,
  ref_count: 0,
  deprecated: false,
  retired: false,
  live: true,
  domains: [{ domain_id: "pets" }],
  refs,
});

// animal <- mammal <- dog (KIND_OF), tail PART_OF dog; referral in no hierarchy edge, only
// RELATED_TO dog.
const TERMS = [
  term(1, "animal"),
  term(2, "mammal"),
  term(3, "dog", [ref(10, "id"), ref(10, "breed")]),
  term(4, "tail"),
  term(5, "referral", [ref(20, "id")]),
];
const EDGES: TermEdgeRow[] = [
  { id: 1, from_term_id: 2, to_term_id: 1, rel_type: "KIND_OF" },
  { id: 2, from_term_id: 3, to_term_id: 2, rel_type: "KIND_OF" },
  { id: 3, from_term_id: 4, to_term_id: 3, rel_type: "PART_OF" },
  { id: 4, from_term_id: 5, to_term_id: 3, rel_type: "RELATED_TO" },
];
const G = glossaryGraph(TERMS, EDGES);
const tax = GLOSSARY_SCOPE.presets.taxonomy as NonNullable<typeof GLOSSARY_SCOPE.presets.taxonomy>;

function tree(layers: LayerId[], edgeTypes: string[] = tax.edgeTypes ?? [], from = G) {
  const g = visibleGraph(
    from,
    new Set(layers),
    { ...NO_FILTERS, edgeTypes },
    new Set(),
    GLOSSARY_SCOPE,
  );
  const at = layoutGraph(g, tax.layout, []);
  const x = (id: string) => (at.get(id) as number[])[0];
  const y = (id: string) => (at.get(id) as number[])[1];
  return { g, x, y };
}

describe("the Taxonomy tree", () => {
  it("sets its levels by KIND_OF and PART_OF alone, the general and the whole above", () => {
    // RELATED_TO drawn too: it sets no level.
    const { y } = tree(["ontology"], []);
    expect(y("term:1")).toBeGreaterThan(y("term:2"));
    expect(y("term:2")).toBeGreaterThan(y("term:3"));
    expect(y("term:3")).toBeGreaterThan(y("term:4"));
    // A term in no hierarchy edge is a root, level with the others.
    expect(y("term:5")).toBe(y("term:1"));
  });

  it("hangs each column a term IS a level below the term, under it", () => {
    const { x, y } = tree(["ontology", "columns"]);
    for (const c of ["c:10:id", "c:10:breed"]) {
      expect(y(c)).toBe(y("term:4"));
      expect(y(c)).toBeLessThan(y("term:3"));
    }
    // dog is centred over its part and its columns; referral's one column straight below it.
    const under = ["term:4", "c:10:id", "c:10:breed"].map(x);
    expect(x("term:3")).toBe((Math.min(...under) + Math.max(...under)) / 2);
    expect(x("c:20:id")).toBe(x("term:5"));
    expect(y("c:20:id")).toBeLessThan(y("term:5"));
    // The chain stands one above another; the roots side by side.
    expect(x("term:1")).toBe(x("term:2"));
    expect(x("term:5")).not.toBe(x("term:1"));
  });

  it("draws terms with no hierarchy among them as roots in a row, their columns below", () => {
    // As a search for "pet" found them: terms no KIND_OF or PART_OF joins, with their columns.
    const { g, y } = tree(["ontology", "columns"], tax.edgeTypes ?? [], glossaryGraph(TERMS, []));
    const terms = g.nodes.filter((n) => n.type === "term").map((n) => n.id);
    const columns = g.nodes.filter((n) => n.type === "column").map((n) => n.id);
    expect(columns.length).toBeGreaterThan(0);
    const top = Math.max(...terms.map(y));
    for (const c of columns) expect(y(c)).toBeLessThan(top);
  });
});
