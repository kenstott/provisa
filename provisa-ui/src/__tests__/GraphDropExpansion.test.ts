// Copyright (c) 2026 Kenneth Stott
// Canary: 0f47fee7-4b12-4b99-8600-b370a977631f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-1586: dropping a table pill onto a graph frame expands it into its instances. One junction
// table backs several relationship types between the very same two tables, so an expansion that
// stopped at the first type found would draw one kind of edge and silently hide the others.

import { describe, it, expect } from "vitest";
import {
  buildDropExpansion,
  nodePathQuery,
  type DropExpansion,
  type PathScope,
} from "../pages/graph-drop";
import type { GNode } from "../components/graph/graph-model";
import type { SchemaRel } from "../components/graph/graph-schema-types";

/** A relationship type as /data/graph-schema answers it for the acting role (its label map). */
function rt(type: string, source = "GlossaryTerm", target = "GlossaryTerm"): SchemaRel {
  return { type, source, target, properties: [], join: `${type} ${source} ${target}` };
}

/** Every label the relationships join, each of a data domain, none selected: no way is out of scope
 * here (the selected domains' own cases are GraphDropDomains.test.ts). */
function everyLabel(rels: SchemaRel[]): PathScope {
  const labels = [...new Set(rels.flatMap((r) => [r.source, r.target]))];
  return {
    nodeLabels: labels.map((label) => {
      const [domainLabel, tableLabel] = label.includes(":")
        ? label.split(":")
        : [null, label];
      return { domainLabel, domainId: domainLabel, tableLabel, systemDomain: false };
    }),
    checkedDomains: new Set(),
  };
}

/** The expansion of a drop that joins the frame. */
function expand(
  query: string,
  label: string,
  rels: SchemaRel[],
  parameterized: string[] = [],
): DropExpansion {
  const done = buildDropExpansion(query, label, rels, new Set(parameterized), everyLabel(rels));
  if ("refused" in done) throw new Error(done.refused.key);
  return done;
}

/** Why a drop changed nothing. */
function refusal(query: string, label: string, rels: SchemaRel[], parameterized: string[] = []) {
  const done = buildDropExpansion(query, label, rels, new Set(parameterized), everyLabel(rels));
  if ("refused" in done) return done.refused;
  throw new Error(`added: ${done.query}`);
}

describe("REQ-1586 buildDropExpansion", () => {
  it("expands one hop per relationship type the junction backs", () => {
    const out = expand("MATCH (t:GlossaryTerm) RETURN t", "GlossaryTerm", [
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
    const out = expand("MATCH (t:GlossaryTerm) RETURN t", "GlossaryTerm", [
      rt("KIND_OF"),
      rt("RELATED_TO"),
    ]);
    expect(out.query).not.toContain("|");
  });

  it("writes only the types the role's label map holds (one hidden from Cypher is not there)", () => {
    const out = expand("MATCH (t:GlossaryTerm) RETURN t", "GlossaryTerm", [rt("KIND_OF")]);
    expect(out.query).toContain(":KIND_OF]-");
    expect(out.query).not.toContain(":RELATED_TO]-");
    expect(out.targetVars).toHaveLength(1);
  });

  it("names the dropped label when nothing on the surface reaches it, and adds nothing", () => {
    expect(refusal("MATCH (x:Other) RETURN x", "GlossaryTerm", [])).toEqual({
      key: "graphFrame.sparqlDropNoPath",
      values: { name: "GlossaryTerm", classes: "Other" },
    });
  });

  it("keeps an FK/PK edge under the type the server names it", () => {
    const out = expand("MATCH (t:GlossaryTerm) RETURN t", "GlossaryTerm", [rt("SEES_ALSO")]);
    expect(out.query).toContain(":SEES_ALSO]-");
  });

  it("names only the types the server registers between the two labels, either way round", () => {
    // The maintainer's case: Shelter:Employees drawn, Shelter:Assignments dropped. The role's label
    // map holds IS_EMPLOYEE from assignments to employees; a type between other labels is no hop.
    const out = expand("MATCH (n:Shelter:Employees) RETURN n", "Shelter:Assignments", [
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
  const drop = (query: string) => expand(query, "GlossaryTerm", [rt("KIND_OF")]).query;

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

describe("buildDropExpansion anchors on every variable the query binds", () => {
  // The maintainer's case: a domain n with two data-product hops; RegisteredTables dropped on it.
  const DROPPED = "Meta:RegisteredTables";
  const RELS = [
    rt("IS_REGISTERED_TABLE", "Meta:Domains", DROPPED),
    rt("MEMBER_TABLES", "Meta:DataProducts", DROPPED),
    rt("PRODUCT", DROPPED, "Meta:DataProducts"),
  ];

  it("expands from the domain as well as from each data product", () => {
    const out = expand(
      "MATCH (n:Meta:Domains)-[r1:HAS_PRODUCT]-(p1:Meta:DataProducts)-[r2:FEEDS]-(p2:Meta:DataProducts) RETURN n, r1, p1, r2, p2",
      DROPPED,
      RELS,
    );
    const hops = [...out.query.matchAll(/OPTIONAL MATCH \((\w+)\)-\[\w+:(\w+)\]-\((\w+):/g)].map(
      ([, anchor, type]) => `${anchor}:${type}`,
    );
    expect(hops).toEqual([
      "n:IS_REGISTERED_TABLE",
      "p1:MEMBER_TABLES",
      "p2:MEMBER_TABLES",
      "p1:PRODUCT",
      "p2:PRODUCT",
    ]);
    // Each hop has its own target: none is shared across anchors (that would intersect them).
    expect(new Set(out.targetVars).size).toBe(hops.length);
    for (const v of out.targetVars) expect(out.query).toContain(`, ${v}`);
  });

  it("anchors on both variables a label is bound twice to", () => {
    const out = expand("MATCH (a:Meta:Domains) MATCH (b:Meta:Domains) RETURN a, b", DROPPED, RELS);
    expect(out.query).toContain("OPTIONAL MATCH (a)-[");
    expect(out.query).toContain("OPTIONAL MATCH (b)-[");
    expect(out.targetVars).toHaveLength(2);
  });

  it("names every class the query holds when no relationship reaches the dropped label", () => {
    expect(
      refusal("MATCH (a:Meta:Domains) MATCH (b:Meta:Tables) RETURN a, b", DROPPED, []),
    ).toEqual({
      key: "graphFrame.sparqlDropNoPath",
      values: { name: DROPPED, classes: "Meta:Domains, Meta:Tables" },
    });
  });
});

describe("REQ-2135 a dropped label a relationship does not touch", () => {
  const rels = [rt("A_B", "A", "B"), rt("B_C", "B", "C")];

  it("is reached by its schema path, each hop bound and returned", () => {
    const out = expand("MATCH (a:A) RETURN a", "C", rels);
    expect(out.query).toContain("OPTIONAL MATCH (a)-[rB:A_B]-(mB:B)-[rC:B_C]-(mC:C)");
    expect(out.query).toContain("RETURN a, rB, mB, rC, mC");
    expect(out.targetVars).toEqual(["mC"]);
  });

  it("adds at most five tied paths and says what it left out", () => {
    const ties = [1, 2, 3, 4, 5, 6].flatMap((n) => [rt(`X${n}`, "A", "B")]);
    const out = expand("MATCH (a:A) RETURN a", "C", [...ties, rt("B_C", "B", "C")]);
    expect(out.query.match(/OPTIONAL MATCH/g)).toHaveLength(5);
    expect(out.notices).toEqual([
      { key: "graphFrame.sparqlDropLeftOut", values: { shown: 5, total: 6 } },
    ]);
  });

  it("keeps a hop's ties whole: a junction's types are not cut", () => {
    const ties = [1, 2, 3, 4, 5, 6, 7].map((n) => rt(`X${n}`, "A", "C"));
    const out = expand("MATCH (a:A) RETURN a", "C", ties);
    expect(out.query.match(/OPTIONAL MATCH/g)).toHaveLength(7);
    expect(out.notices).toEqual([]);
  });

  it("says when the path is past the cap, and adds nothing", () => {
    const labels = ["A", "B", "C", "D", "E", "F"];
    const chain = labels.slice(1).map((x, i) => rt(`R${i}`, labels[i], x));
    expect(refusal("MATCH (a:A) RETURN a", "F", chain)).toEqual({
      key: "graphFrame.sparqlDropTooFar",
      values: { name: "F", classes: "A", hops: 5, most: 4 },
    });
  });

  it("names the class of a node written with properties", () => {
    const out = expand("MATCH (a:A {id: 1}) RETURN a", "C", rels);
    expect(out.query).toContain("OPTIONAL MATCH (a)-[rB:A_B]-(mB:B)-[rC:B_C]-(mC:C)");
  });

  it("says a query of no class has nothing to join to", () => {
    expect(refusal("MATCH (n) RETURN n", "C", rels)).toEqual({
      key: "graphFrame.sparqlDropNoClass",
    });
  });
});

describe("REQ-2135 a parameterized table", () => {
  // The maintainer's repro: Employees drawn, and a way to Pets through assignments_by_breed.
  const EMPLOYEES = "Shelter:Employees";
  const BY_BREED = "Shelter:AssignmentsByBreed";
  const held = "MATCH (n:Shelter:Employees) RETURN n";

  it("is no way through: the path goes round it", () => {
    const rels = [
      rt("ASSIGNMENT_BREED", EMPLOYEES, BY_BREED),
      rt("BREED_PETS", BY_BREED, "PetStore:Pets"),
      rt("IS_EMPLOYEE", "Shelter:Assignments", EMPLOYEES),
      rt("FOR_BREED", "Shelter:Assignments", "Shelter:Breeds"),
      rt("OF_BREED", "Shelter:Breeds", "PetStore:Pets"),
    ];
    const out = expand(held, "PetStore:Pets", rels, [BY_BREED]);
    expect(out.query).not.toContain("ASSIGNMENT_BREED");
    expect(out.query).not.toContain(BY_BREED);
    expect(out.query).toContain(":IS_EMPLOYEE]-");
  });

  it("is refused by name when dropped, whatever joins it", () => {
    const rels = [rt("ASSIGNMENT_BREED", EMPLOYEES, BY_BREED)];
    expect(refusal(held, BY_BREED, rels, [BY_BREED])).toEqual({
      key: "graphFrame.sparqlDropParameterized",
      values: { name: BY_BREED },
    });
  });

  it("leaves no path where it was the only way, and says so", () => {
    const rels = [rt("A", EMPLOYEES, BY_BREED), rt("B", BY_BREED, "PetStore:Pets")];
    expect(refusal(held, "PetStore:Pets", rels, [BY_BREED])).toEqual({
      key: "graphFrame.sparqlDropNoPath",
      values: { name: "PetStore:Pets", classes: EMPLOYEES },
    });
  });

  it("can be the start of a path when the query holds it", () => {
    const out = expand(
      `MATCH (b:${BY_BREED}) RETURN b`,
      EMPLOYEES,
      [rt("ASSIGNMENT_BREED", EMPLOYEES, BY_BREED)],
      [BY_BREED],
    );
    expect(out.query).toContain(":ASSIGNMENT_BREED]-");
  });
});

describe("REQ-2135 a join declared from both ends", () => {
  it("adds one hop, not two tied paths", () => {
    const rels: SchemaRel[] = [
      { type: "SUBMITTED", source: "U", target: "I", properties: [], join: "one" },
      { type: "SUBMITTED_BY", source: "I", target: "U", properties: [], join: "one" },
    ];
    const out = expand("MATCH (u:U) RETURN u", "I", rels);
    expect(out.query.match(/OPTIONAL MATCH/g)).toHaveLength(1);
    expect(out.query).toContain("[rI:SUBMITTED]");
  });
});

describe("REQ-2135 a node dropped on a node", () => {
  const node = (label: string, properties: Record<string, unknown>): GNode => ({
    id: 1,
    label,
    tableLabel: label,
    properties,
  });
  const key = (label: string) => ({ A: "id", B: "code" })[label];

  const rels = [rt("R", "A", "B")];
  const paths = (from: GNode, to: GNode) =>
    nodePathQuery(from, to, key, rels, new Set(), everyLabel(rels));

  it("asks every shortest path between the two, with a hop limit and a row cap", () => {
    expect(paths(node("A", { id: 1 }), node("B", { code: "x'1" }))).toEqual({
      query: [
        "MATCH p = allShortestPaths((a:A)-[:R*..4]-(b:B))",
        "WHERE a.id = 1 AND b.code = 'x\\'1'",
        "RETURN p",
        "LIMIT 25",
      ].join("\n"),
    });
  });

  it("says by name a node whose key is not known, and asks nothing", () => {
    const noKey = (from: string, to: string) => ({
      refused: { key: "graphFrame.cypherPathNoKey", values: { from, to } },
    });
    expect(paths(node("Z", {}), node("B", { code: "x" }))).toEqual(noKey("Z", "B"));
    expect(paths(node("A", {}), node("B", { code: "x" }))).toEqual(noKey("A", "B"));
  });
});
