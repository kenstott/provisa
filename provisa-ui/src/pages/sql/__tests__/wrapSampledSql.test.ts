// Copyright (c) 2026 Kenneth Stott
// Canary: 8e2b6f14-9a3d-4c07-b5e1-2f6a8d1c4b90
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1730: "first" must never trap the user's own ORDER BY inside an unordered derived table —
// standard SQL gives that no ordering guarantee once wrapped by an outer query with none of its
// own, and Trino's redis connector was observed to not preserve it in practice.

import { describe, it, expect } from "vitest";
import { wrapSampledSql } from "../sqlHelpers";

describe("wrapSampledSql", () => {
  it("first: appends LIMIT directly, preserving the query's own ORDER BY", () => {
    const inner = "SELECT agent_id, name FROM pet_store.support_agent ORDER BY agent_id";
    expect(wrapSampledSql(inner, "first", 100)).toBe(`${inner}\nLIMIT 100`);
  });

  it("first: leaves a query that already has its own LIMIT untouched", () => {
    const inner = "SELECT id FROM t ORDER BY id LIMIT 10";
    expect(wrapSampledSql(inner, "first", 100)).toBe(inner);
  });

  it("first: an unordered query still just gets LIMIT appended", () => {
    const inner = "SELECT id FROM t";
    expect(wrapSampledSql(inner, "first", 50)).toBe(`${inner}\nLIMIT 50`);
  });

  it("last: wraps and reverses by the first column", () => {
    const inner = "SELECT id FROM t";
    expect(wrapSampledSql(inner, "last", 100)).toBe(
      `SELECT * FROM (\n${inner}\n) _sample ORDER BY 1 DESC LIMIT 100`,
    );
  });

  it("random: wraps and orders randomly", () => {
    const inner = "SELECT id FROM t";
    expect(wrapSampledSql(inner, "random", 100)).toBe(
      `SELECT * FROM (\n${inner}\n) _sample ORDER BY random() LIMIT 100`,
    );
  });
});
