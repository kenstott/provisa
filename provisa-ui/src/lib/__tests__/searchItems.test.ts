// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The glossary search's items, the same as the server's (test_glossary_repository.py).

import { describe, expect, it } from "vitest";
import { matchesSearch, searchItems } from "../searchItems";

describe("a search's items", () => {
  it("are its quoted phrases and its words, lower-cased", () => {
    expect(searchItems('"shift coverage" clinic')).toEqual(["shift coverage", "clinic"]);
    expect(searchItems("  Shift   CLINIC ")).toEqual(["shift", "clinic"]);
    expect(searchItems('shift" "" clinic')).toEqual(["shift", "clinic"]);
    expect(searchItems("")).toEqual([]);
  });

  it("match a text holding any one of them", () => {
    const items = searchItems('"shift coverage" clinic');
    expect(matchesSearch(["Shift coverage"], items)).toBe(true);
    expect(matchesSearch(["referral clinic"], items)).toBe(true);
    expect(matchesSearch(["shift start"], items)).toBe(false);
    expect(matchesSearch([undefined, "a clinic visit"], items)).toBe(true);
  });
});
