// Copyright (c) 2026 Kenneth Stott
// Canary: 1b9e4f60-7c25-4d83-a0f6-e3d7c8125a94
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1937: "When the grid holds only part of the result -- cut at the row limit or with more
// pages to fetch -- it says the filter applies to the rows loaded." Whether it holds part is
// decided from what the server said of the answer and from the statement as it was sent, not
// from the text in the editor.

import { describe, it, expect } from "vitest";
import { resultIsPartial, wrapSampledSql } from "../sqlHelpers";

const CUT = { code: "statement.rows_cut", params: { limit: 500, kind: "role" }, message: "…" };
const UNCHECKED = { ...CUT, code: "statement.rows_cut_unchecked" };
const OTHER = { code: "statement.served_from_cache", params: {}, message: "…" };

describe("resultIsPartial", () => {
  it("is partial when the server says its row limit cut the answer", () => {
    // The statement spells no LIMIT: only the server knows.
    expect(resultIsPartial("SELECT * FROM t", 500, [CUT])).toBe(true);
    expect(resultIsPartial("SELECT * FROM t", 500, [OTHER, UNCHECKED])).toBe(true);
  });

  it("is partial when the statement as sent carried a LIMIT and the answer filled it", () => {
    // The explorer's own sample size is in the statement it sends, not in the editor's text.
    const sent = wrapSampledSql("SELECT * FROM t", "first", 100);
    expect(resultIsPartial(sent, 100, [])).toBe(true);
    expect(resultIsPartial("SELECT * FROM t LIMIT 20 OFFSET 40", 20, undefined)).toBe(true);
  });

  it("is whole when nothing cut it", () => {
    expect(resultIsPartial("SELECT * FROM t", 37, [])).toBe(false);
    expect(resultIsPartial("SELECT * FROM t", 37, [OTHER])).toBe(false);
    expect(resultIsPartial(wrapSampledSql("SELECT * FROM t", "first", 100), 37, [])).toBe(false);
    expect(resultIsPartial("SELECT * FROM t LIMIT 0", 0, [])).toBe(false);
  });
});
