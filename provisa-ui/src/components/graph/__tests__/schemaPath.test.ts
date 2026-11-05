// Copyright (c) 2026 Kenneth Stott
// Canary: 6d2f0b93-81ce-47a5-a3d4-c7e5192b0f38
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135: the walk a Cypher and a SPARQL drop share.

import { describe, expect, it } from "vitest";
import { MAX_SCHEMA_HOPS, shortestSchemaPaths, type SchemaEdge } from "../schemaPath";

const e = (source: string, target: string, type = "T", join = `${type} ${source} ${target}`) => ({
  source,
  target,
  type,
  join,
});
const paths = <E extends SchemaEdge>(
  edges: readonly E[],
  from: readonly string[],
  to: readonly string[],
  parameterized: readonly string[] = [],
) => shortestSchemaPaths(edges, from, to, new Set(parameterized));
const walked = (steps: { from: string; to: string; edge: { type: string } }[]) =>
  steps.map((s) => `${s.from}-${s.edge.type}-${s.to}`).join(" ");

describe("shortestSchemaPaths", () => {
  it("walks a relationship either way", () => {
    const found = paths([e("users", "inquiries")], ["inquiries"], ["users"]);
    expect(found.hops).toBe(1);
    expect(found.paths.map(walked)).toEqual(["inquiries-T-users"]);
  });

  it("goes through the classes between", () => {
    const found = paths([e("a", "b"), e("b", "c")], ["a"], ["c"]);
    expect(found.paths.map(walked)).toEqual(["a-T-b b-T-c"]);
  });

  it("starts from the nearest class of several", () => {
    const edges = [e("pets", "inquiries"), e("users", "inquiries")];
    const found = paths(edges, ["pets", "inquiries"], ["users"]);
    expect(found.hops).toBe(1);
    expect(found.paths.map(walked)).toEqual(["inquiries-T-users"]);
  });

  it("holds every tie, by parallel relationship and by a class between", () => {
    const edges = [e("a", "b", "X"), e("a", "b", "Y"), e("a", "c"), e("c", "d"), e("b", "d")];
    const found = paths(edges, ["a"], ["d"]);
    expect(found.hops).toBe(2);
    expect(found.paths.map(walked).sort()).toEqual(["a-T-c c-T-d", "a-X-b b-T-d", "a-Y-b b-T-d"]);
  });

  it("gives no paths and no hops where nothing joins them", () => {
    expect(paths([e("a", "b")], ["a"], ["z"])).toEqual({ paths: [], hops: null });
  });

  it("gives the distance and no path past the cap", () => {
    const chain = ["a", "b", "c", "d", "e", "f", "g"];
    const edges = chain.slice(1).map((x, i) => e(chain[i], x));
    const found = paths(edges, ["a"], ["g"]);
    expect(found).toEqual({ paths: [], hops: 6 });
    expect(MAX_SCHEMA_HOPS).toBeLessThan(6);
    expect(paths(edges, ["a"], ["e"]).paths).toHaveLength(1);
  });

  it("reaches a class the result holds through a relationship", () => {
    const found = paths([e("t", "t", "KIND_OF")], ["t"], ["t"]);
    expect(found.paths.map(walked)).toEqual(["t-KIND_OF-t"]);
  });

  it("goes round a parameterized table, however short the way through it", () => {
    const edges = [e("a", "p"), e("p", "c"), e("a", "b"), e("b", "x"), e("x", "c")];
    const found = paths(edges, ["a"], ["c"], ["p"]);
    expect(found.hops).toBe(3);
    expect(found.paths.map(walked)).toEqual(["a-T-b b-T-x x-T-c"]);
  });

  it("has no path where a parameterized table is the only way", () => {
    expect(paths([e("a", "p"), e("p", "c")], ["a"], ["c"], ["p"])).toEqual({
      paths: [],
      hops: null,
    });
  });

  it("never ends on a parameterized table", () => {
    expect(paths([e("a", "p")], ["a"], ["p"], ["p"])).toEqual({ paths: [], hops: null });
    const found = paths([e("a", "p"), e("a", "b")], ["a"], ["p", "b"], ["p"]);
    expect(found.paths.map(walked)).toEqual(["a-T-b"]);
  });

  it("starts from a parameterized table the query holds, its arguments given", () => {
    const found = paths([e("p", "b"), e("b", "c")], ["p"], ["c"], ["p"]);
    expect(found.paths.map(walked)).toEqual(["p-T-b b-T-c"]);
  });

  it("takes a join declared from both ends once", () => {
    const edges = [
      e("users", "inquiries", "SUBMITTED", "one join"),
      e("inquiries", "users", "SUBMITTED_BY", "one join"),
    ];
    const found = paths(edges, ["users"], ["inquiries"]);
    expect(found.paths.map(walked)).toEqual(["users-SUBMITTED-inquiries"]);
  });

  it("takes each hop of a path once where every hop is declared twice", () => {
    const edges = [
      e("a", "b", "AB", "ab"),
      e("b", "a", "BA", "ab"),
      e("b", "c", "BC", "bc"),
      e("c", "b", "CB", "bc"),
    ];
    expect(paths(edges, ["a"], ["c"]).paths).toHaveLength(1);
  });

  it("keeps two joins between the same labels as ties", () => {
    const edges = [e("a", "b", "ONE"), e("a", "b", "TWO")];
    expect(paths(edges, ["a"], ["b"]).paths).toHaveLength(2);
  });
});
