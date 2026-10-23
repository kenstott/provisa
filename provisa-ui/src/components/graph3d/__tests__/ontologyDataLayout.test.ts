// Copyright (c) 2026 Kenneth Stott
// Canary: 62b084dc-6e04-4f93-b022-51ffa4ed62a0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Ontology View's force layout with Data on. A data product whose datasets' columns no term is
// drawn with nothing joining it to the terms but its domain; the layout fits the graph's large parts
// and draws a small part it does not reach in to their rim, so turning Data on leaves the term and
// column clusters as spread out as with Data off.

import { describe, expect, it } from "vitest";
import { forceLayout } from "../../../pages/tables/profile3d/forceLayout";
import { GLOSSARY_SCOPE, layoutGraph, visibleGraph, NO_FILTERS, type LayerId } from "../graphModel";
import { glossaryGraph, type ProductRow, type TermRow } from "../glossaryGraphData";

type At = Map<string, [number, number, number]>;

/** Two domains of eight terms, each term the two columns of a table of its own. */
const TERMS: TermRow[] = ["pet-store", "shelter"].flatMap((domain, d) =>
  Array.from({ length: 8 }, (_, i) => {
    const table = 100 * (d + 1) + i;
    return {
      id: 10 * (d + 1) + i,
      name: `${domain} term ${i}`,
      definition: null,
      ref_count: 0,
      deprecated: false,
      retired: false,
      live: true,
      domains: [{ domain_id: domain }],
      refs: ["id", "name"].map((column) => ({
        column_key: `${table}:${column}`,
        column_name: column,
        table_id: table,
        table_name: `t${table}`,
        domain_id: domain,
        table: { id: table, product: null, source_relationships: [] },
      })),
    };
  }),
);

/** A product with one dataset of eight columns no term is. */
const product = (id: string, domain: string, table: number): ProductRow => ({
  id,
  name: id,
  domain_id: domain,
  member_tables: [
    {
      id: table,
      domain_id: domain,
      table_name: `t${table}`,
      table_columns: Array.from({ length: 8 }, (_, i) => ({
        column_name: `c${i}`,
        column_key: `${table}:c${i}`,
        glossary_ref: null,
      })),
      source_relationships: [],
    },
  ],
});

// Workforce Ops is in a domain a term is in; Payroll in one no term is, so no domain is drawn to
// hold it.
const PRODUCTS = [product("workforce_ops", "shelter", 900), product("payroll", "hr", 901)];

const G = glossaryGraph(TERMS, [], PRODUCTS);
const view = (layers: LayerId[]) =>
  visibleGraph(G, new Set(layers), NO_FILTERS, new Set(), GLOSSARY_SCOPE);

/** The median distance of ``ids`` from their centre. */
function spread(at: At, ids: string[]): number {
  const ps = ids.map((id) => at.get(id) as [number, number, number]);
  const mid = [0, 1, 2].map((ax) => ps.reduce((s, p) => s + p[ax], 0) / ps.length);
  const ds = ps.map((p) => Math.hypot(p[0] - mid[0], p[1] - mid[1], p[2] - mid[2]));
  return ds.sort((a, b) => a - b)[Math.floor(ds.length / 2)];
}

const core = (ids: string[]) =>
  ids.filter((id) => id.startsWith("term:") || id.startsWith("c:1") || id.startsWith("c:2"));

describe("the Ontology View's layout with Data on", () => {
  it("joins a data product to its domain, as the data product graph does", () => {
    const g = view(["ontology", "domains", "columns", "data"]);
    expect(
      g.edges.some(
        (e) => e.kind === "in_domain" && e.a === "p:workforce_ops" && e.b === "d:shelter",
      ),
    ).toBe(true);
    expect(g.nodes.map((n) => n.id)).not.toContain("d:hr");
  });

  for (const attraction of [0.5, 1, 2])
    it(`keeps the terms and columns as spread out as with Data off (attraction ${attraction})`, () => {
      const off = view(["ontology", "domains", "columns"]);
      const on = view(["ontology", "domains", "columns", "data"]);
      expect(on.nodes.map((n) => n.id)).toEqual(
        expect.arrayContaining(["p:workforce_ops", "p:payroll", "c:901:c7"]),
      );
      const ids = core(off.nodes.map((n) => n.id));
      const before = spread(layoutGraph(off, "force", [], attraction), ids);
      const after = spread(layoutGraph(on, "force", [], attraction), ids);
      // Collapsed, it was two thirds as spread; it may widen a little, the products drawn among it.
      expect(after / before).toBeGreaterThan(0.9);
      expect(after / before).toBeLessThan(1.25);
    });
});

describe("a force layout with a part its springs do not reach", () => {
  // A ring of forty, and a pair joined to nothing else.
  const RING = Array.from({ length: 40 }, (_, i) => `n${i}`);
  const SPRINGS = RING.map((id, i) => ({ a: id, b: RING[(i + 1) % 40], strength: 0.5 }));
  const PAIR = [{ a: "x", b: "y", strength: 0.8 }];

  it("fits the large part as it would without the small one, and draws the small one in", () => {
    for (const attraction of [0.5, 1]) {
      const alone = forceLayout(RING, SPRINGS, 6, attraction);
      const both = forceLayout([...RING, "x", "y"], [...SPRINGS, ...PAIR], 6, attraction);
      expect(spread(both, RING) / spread(alone, RING)).toBeGreaterThan(0.9);
      expect(spread(both, RING) / spread(alone, RING)).toBeLessThan(1.1);
      // Just past the fitted part's rim, not wherever the charge pushed it.
      for (const id of ["x", "y"])
        expect(Math.hypot(...(both.get(id) as number[]))).toBeLessThan((6 / attraction) * 1.5);
    }
  });
});
