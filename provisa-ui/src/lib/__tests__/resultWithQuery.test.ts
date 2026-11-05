// Copyright (c) 2026 Kenneth Stott
// Canary: ed463fa8-bfc8-4375-b8b7-9d426330bc74
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// An explorer tab keeps its last result with the exact query that produced it, and restores the
// result only beside that query: a query edited since shows no old result, never another query's.

import { describe, expect, it } from "vitest";
import { kept, restored } from "../resultWithQuery";

describe("a kept result", () => {
  it("is restored beside the query that produced it", () => {
    const k = kept("{ glossary_terms { name } }", { data: 1 });
    expect(restored(k, "{ glossary_terms { name } }")).toEqual({ data: 1 });
  });

  it("is not restored beside another query (the editor was edited after the run)", () => {
    // The maintainer's case: glossary_terms' answer kept, ps__petCompanions in the editor.
    const k = kept("{ glossary_terms { name } }", { data: 1 });
    expect(restored(k, "{ ps__petCompanions }")).toBeNull();
  });

  it("ignores only the whitespace around the query", () => {
    const k = kept("  SELECT 1  ", "rows");
    expect(restored(k, "SELECT 1")).toBe("rows");
    expect(restored(k, "SELECT  1")).toBeNull();
  });

  it("restores nothing when nothing was kept, or when it was kept with no query", () => {
    expect(restored(null, "SELECT 1")).toBeNull();
    expect(restored(undefined, "SELECT 1")).toBeNull();
    // A result stored before results were kept with their query has no query: not shown.
    expect(restored({ result: "rows" } as never, "SELECT 1")).toBeNull();
  });
});
