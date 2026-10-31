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
import { MAX_SCHEMA_HOPS, shortestSchemaPaths } from "../schemaPath";

const e = (source: string, target: string, type = "T") => ({ source, target, type });
const walked = (steps: { from: string; to: string; edge: { type: string } }[]) =>
  steps.map((s) => `${s.from}-${s.edge.type}-${s.to}`).join(" ");

describe("shortestSchemaPaths", () => {
  it("walks a relationship either way", () => {
    const found = shortestSchemaPaths([e("users", "inquiries")], ["inquiries"], ["users"]);
    expect(found.hops).toBe(1);
    expect(found.paths.map(walked)).toEqual(["inquiries-T-users"]);
  });

  it("goes through the classes between", () => {
    const found = shortestSchemaPaths([e("a", "b"), e("b", "c")], ["a"], ["c"]);
    expect(found.paths.map(walked)).toEqual(["a-T-b b-T-c"]);
  });

  it("starts from the nearest class of several", () => {
    const edges = [e("pets", "inquiries"), e("users", "inquiries")];
    const found = shortestSchemaPaths(edges, ["pets", "inquiries"], ["users"]);
    expect(found.hops).toBe(1);
    expect(found.paths.map(walked)).toEqual(["inquiries-T-users"]);
  });

  it("holds every tie, by parallel relationship and by a class between", () => {
    const edges = [e("a", "b", "X"), e("a", "b", "Y"), e("a", "c"), e("c", "d"), e("b", "d")];
    const found = shortestSchemaPaths(edges, ["a"], ["d"]);
    expect(found.hops).toBe(2);
    expect(found.paths.map(walked).sort()).toEqual([
      "a-T-c c-T-d",
      "a-X-b b-T-d",
      "a-Y-b b-T-d",
    ]);
  });

  it("gives no paths and no hops where nothing joins them", () => {
    expect(shortestSchemaPaths([e("a", "b")], ["a"], ["z"])).toEqual({ paths: [], hops: null });
  });

  it("gives the distance and no path past the cap", () => {
    const chain = ["a", "b", "c", "d", "e", "f", "g"];
    const edges = chain.slice(1).map((x, i) => e(chain[i], x));
    const found = shortestSchemaPaths(edges, ["a"], ["g"]);
    expect(found).toEqual({ paths: [], hops: 6 });
    expect(MAX_SCHEMA_HOPS).toBeLessThan(6);
    expect(shortestSchemaPaths(edges, ["a"], ["e"]).paths).toHaveLength(1);
  });

  it("reaches a class the result holds through a relationship", () => {
    const found = shortestSchemaPaths([e("t", "t", "KIND_OF")], ["t"], ["t"]);
    expect(found.paths.map(walked)).toEqual(["t-KIND_OF-t"]);
  });
});
