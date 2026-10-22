// Copyright (c) 2026 Kenneth Stott
// Canary: 682a22b0-313d-405e-ade2-54aa80a98fed
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The glossary search's items, the same as the server's (test_glossary_repository.py).

import { describe, expect, it } from "vitest";
import { matchesQuery, matchesSearch, searchItems } from "../searchItems";

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

describe("a page's search", () => {
  it("keeps every row when empty or only spaces and stray quotes", () => {
    for (const q of ["", "   ", '""', '" "']) expect(matchesQuery(["anything"], q)).toBe(true);
  });

  it("keeps a row when any word is in any of its texts, case aside", () => {
    expect(matchesQuery(["orders", "sales"], "ORDERS invoices")).toBe(true);
    expect(matchesQuery(["orders", "sales"], "invoices SALES")).toBe(true);
    expect(matchesQuery(["orders", "sales"], "invoices refunds")).toBe(false);
  });

  it("reads a quoted phrase as one item, spaces and all", () => {
    expect(matchesQuery(["shift coverage"], '"shift coverage"')).toBe(true);
    expect(matchesQuery(["shift start", "coverage map"], '"shift coverage"')).toBe(false);
    expect(matchesQuery(["clinic"], '"shift coverage" clinic')).toBe(true);
  });

  it("matches inside a word, and skips a text that is absent", () => {
    expect(matchesQuery(["pet_store.pets"], "store")).toBe(true);
    expect(matchesQuery([null, undefined, "x"], "x")).toBe(true);
    expect(matchesQuery([null, undefined], "x")).toBe(false);
  });
});
