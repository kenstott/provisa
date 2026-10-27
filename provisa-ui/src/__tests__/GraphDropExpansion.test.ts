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
import type { SchemaRel } from "../components/graph/graph-schema-types";

/** A relationship type as /data/graph-schema answers it for the acting role (its label map). */
function rt(type: string, source = "GlossaryTerm", target = "GlossaryTerm"): SchemaRel {
  return { type, source, target, properties: [] };
}

describe("REQ-1586 buildDropExpansion", () => {
  it("expands one hop per relationship type the junction backs", () => {
    const out = buildDropExpansion("MATCH (t:GlossaryTerm) RETURN t", "GlossaryTerm", [
      rt("KIND_OF"),
      rt("RELATED_TO"),
      rt("PART_OF"),
    ]);
    expect(out.query).toContain("OPTIONAL MATCH (t)-[rGlossaryTerm:KIND_OF]-");
    expect(out.query).toContain(":RELATED_TO]-");
    expect(out.query).toContain(":PART_OF]-");
    expect(out.targetVars).toHaveLength(3);
    // Every hop binds its own pair, and every pair is returned.
    for (const v of out.targetVars) expect(out.query).toContain(`, ${v}`);
  });

  it("never writes type alternation, which a single hop cannot resolve", () => {
    const out = buildDropExpansion("MATCH (t:GlossaryTerm) RETURN t", "GlossaryTerm", [
      rt("KIND_OF"),
      rt("RELATED_TO"),
    ]);
    expect(out.query).not.toContain("|");
  });

  it("writes only the types the role's label map holds (one hidden from Cypher is not there)", () => {
    const out = buildDropExpansion("MATCH (t:GlossaryTerm) RETURN t", "GlossaryTerm", [
      rt("KIND_OF"),
    ]);
    expect(out.query).toContain(":KIND_OF]-");
    expect(out.query).not.toContain(":RELATED_TO]-");
    expect(out.targetVars).toHaveLength(1);
  });

  it("falls back to a bare node pattern when nothing on the surface reaches it", () => {
    const out = buildDropExpansion("MATCH (x:Other) RETURN x", "GlossaryTerm", []);
    expect(out.query).toContain("OPTIONAL MATCH (mGlossaryTerm:GlossaryTerm)");
    expect(out.query).not.toContain("]-");
  });

  it("keeps an FK/PK edge under the type the server names it", () => {
    const out = buildDropExpansion("MATCH (t:GlossaryTerm) RETURN t", "GlossaryTerm", [
      rt("SEES_ALSO"),
    ]);
    expect(out.query).toContain(":SEES_ALSO]-");
  });

  it("names only the types the server registers between the two labels, either way round", () => {
    // The maintainer's case: Shelter:Employees drawn, Shelter:Assignments dropped. The role's label
    // map holds IS_EMPLOYEE from assignments to employees; a type between other labels is no hop.
    const out = buildDropExpansion("MATCH (n:Shelter:Employees) RETURN n", "Shelter:Assignments", [
      rt("IS_EMPLOYEE", "Shelter:Assignments", "Shelter:Employees"),
      rt("PETS_OF_BREED", "Shelter:AnimalBreeds", "PetStore:Pets"),
    ]);
    expect(out.query).toContain(
      "OPTIONAL MATCH (n)-[rAssignments:IS_EMPLOYEE]-(mAssignments:Shelter:Assignments)",
    );
    expect([...out.query.matchAll(/:([A-Z_]+)\]-/g)].map((m) => m[1])).toEqual(["IS_EMPLOYEE"]);
    expect(out.targetVars).toEqual(["mAssignments"]);
  });
});

describe("a drop keeps the result's own paging", () => {
  const drop = (query: string) => buildDropExpansion(query, "GlossaryTerm", [rt("KIND_OF")]).query;

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
