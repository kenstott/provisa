// Copyright (c) 2026 Kenneth Stott
// Canary: 0f47fee7-4b12-4b99-8600-b370a977631f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-1586: dropping a table pill onto a graph frame expands it into its instances. One junction
// table backs several relationship types between the very same two tables, so an expansion that
// stopped at the first type found would draw one kind of edge and silently hide the others.

import { describe, it, expect } from "vitest";
import { buildDropExpansion } from "../pages/graph-drop";
import type { Relationship } from "../types/admin";

function rel(over: Partial<Relationship>): Relationship {
  return {
    id: 1,
    sourceTableId: 1,
    targetTableId: 2,
    sourceTableName: "glossary_term",
    sourceDomainId: "meta",
    targetTableName: "glossary_term",
    sourceColumn: "id",
    targetColumn: "id",
    cardinality: "many-to-many",
    materialize: false,
    refreshInterval: 0,
    targetFunctionName: null,
    functionArg: null,
    alias: null,
    graphqlAlias: null,
    physicalName: null,
    computedCypherAlias: null,
    autoSuggested: false,
    disableCypher: false,
    viaTableId: null,
    viaTableName: null,
    viaSourceColumn: null,
    viaTargetColumn: null,
    viaTypeColumn: null,
    viaTypeValue: null,
    viaLabelSource: null,
    parameters: [],
    ...over,
  };
}

function junction(id: number, typeValue: string): Relationship {
  return rel({
    id,
    viaTableId: 99,
    viaTableName: "glossary_term_edges",
    viaSourceColumn: "from_term_id",
    viaTargetColumn: "to_term_id",
    viaTypeColumn: "rel_type",
    viaTypeValue: typeValue,
    viaLabelSource: "column",
  });
}

const L2T = { GlossaryTerm: "GlossaryTerm" };

describe("REQ-1586 buildDropExpansion", () => {
  it("expands one hop per relationship type the junction backs", () => {
    const out = buildDropExpansion(
      "MATCH (t:GlossaryTerm) RETURN t",
      "GlossaryTerm",
      [junction(1, "kind_of"), junction(2, "related_to"), junction(3, "part_of")],
      L2T,
    );
    expect(out.query).toContain("OPTIONAL MATCH (t)-[rGlossaryTerm:KIND_OF]-");
    expect(out.query).toContain(":RELATED_TO]-");
    expect(out.query).toContain(":PART_OF]-");
    expect(out.targetVars).toHaveLength(3);
    // Every hop binds its own pair, and every pair is returned.
    for (const v of out.targetVars) expect(out.query).toContain(`, ${v}`);
  });

  it("never writes type alternation, which a single hop cannot resolve", () => {
    const out = buildDropExpansion(
      "MATCH (t:GlossaryTerm) RETURN t",
      "GlossaryTerm",
      [junction(1, "kind_of"), junction(2, "related_to")],
      L2T,
    );
    expect(out.query).not.toContain("|");
  });

  it("skips a relationship the steward hid from Cypher", () => {
    const out = buildDropExpansion(
      "MATCH (t:GlossaryTerm) RETURN t",
      "GlossaryTerm",
      [junction(1, "kind_of"), { ...junction(2, "related_to"), disableCypher: true }],
      L2T,
    );
    expect(out.query).toContain(":KIND_OF]-");
    expect(out.query).not.toContain(":RELATED_TO]-");
    expect(out.targetVars).toHaveLength(1);
  });

  it("falls back to a bare node pattern when nothing on the surface reaches it", () => {
    const out = buildDropExpansion("MATCH (x:Other) RETURN x", "GlossaryTerm", [], L2T);
    expect(out.query).toContain("OPTIONAL MATCH (mGlossaryTerm:GlossaryTerm)");
    expect(out.query).not.toContain("]-");
  });

  it("keeps an FK/PK edge on its ordinary alias", () => {
    const out = buildDropExpansion(
      "MATCH (t:GlossaryTerm) RETURN t",
      "GlossaryTerm",
      [rel({ alias: "sees_also" })],
      L2T,
    );
    expect(out.query).toContain(":SEES_ALSO]-");
  });
});

describe("a drop keeps the result's own paging", () => {
  const drop = (query: string) =>
    buildDropExpansion(query, "GlossaryTerm", [junction(1, "kind_of")], L2T).query;

  it("keeps a LIMIT after the widened RETURN", () => {
    expect(drop("MATCH (t:GlossaryTerm) RETURN t LIMIT 25")).toBe(
      "MATCH (t:GlossaryTerm)\n" +
        "OPTIONAL MATCH (t)-[rGlossaryTerm:KIND_OF]-(mGlossaryTerm:GlossaryTerm)\n" +
        "RETURN t, rGlossaryTerm, mGlossaryTerm\n" +
        "LIMIT 25",
    );
  });

  it("keeps ORDER BY and LIMIT, in that order", () => {
    expect(drop("MATCH (t:GlossaryTerm) RETURN t ORDER BY t.name DESC LIMIT 10")).toBe(
      "MATCH (t:GlossaryTerm)\n" +
        "OPTIONAL MATCH (t)-[rGlossaryTerm:KIND_OF]-(mGlossaryTerm:GlossaryTerm)\n" +
        "RETURN t, rGlossaryTerm, mGlossaryTerm\n" +
        "ORDER BY t.name DESC LIMIT 10",
    );
  });

  it("keeps a SKIP", () => {
    expect(drop("MATCH (t:GlossaryTerm)\nRETURN t\nSKIP 50 LIMIT 25")).toBe(
      "MATCH (t:GlossaryTerm)\n" +
        "OPTIONAL MATCH (t)-[rGlossaryTerm:KIND_OF]-(mGlossaryTerm:GlossaryTerm)\n" +
        "RETURN t, rGlossaryTerm, mGlossaryTerm\n" +
        "SKIP 50 LIMIT 25",
    );
  });

  it("adds no LIMIT to a query that had none", () => {
    const out = drop("MATCH (t:GlossaryTerm) RETURN t");
    expect(out).toBe(
      "MATCH (t:GlossaryTerm)\n" +
        "OPTIONAL MATCH (t)-[rGlossaryTerm:KIND_OF]-(mGlossaryTerm:GlossaryTerm)\n" +
        "RETURN t, rGlossaryTerm, mGlossaryTerm",
    );
    expect(out).not.toMatch(/LIMIT|SKIP|ORDER BY/);
  });

  it("does not take a property spelled limit for the clause", () => {
    expect(drop("MATCH (t:GlossaryTerm) RETURN t, t.limit LIMIT 5")).toContain(
      "RETURN t, t.limit, rGlossaryTerm, mGlossaryTerm\nLIMIT 5",
    );
  });
});
