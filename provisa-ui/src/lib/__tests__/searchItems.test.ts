// Copyright (c) 2026 Kenneth Stott
// Canary: 682a22b0-313d-405e-ade2-54aa80a98fed
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2143 (PAGE SEARCH USES THE WEB-SEARCH SYNTAX): the page search, answering the cases the
// server answers too (tests/fixtures/page_search_cases.json, test_glossary_repository.py).

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { matchesQuery, matchesSearch, searchItems } from "../searchItems";

interface Case {
  query: string;
  texts: (string | null)[];
  kept: boolean;
  was_kept?: boolean;
}

const CASES = (
  JSON.parse(
    readFileSync(resolve(__dirname, "../../../../tests/fixtures/page_search_cases.json"), "utf8"),
  ) as { cases: Case[] }
).cases;

describe("a page's search, the cases the server shares", () => {
  it.each(CASES.map((c) => [c.query, JSON.stringify(c.texts), c.kept, c] as const))(
    "%s over %s keeps it: %s",
    (_q, _t, _k, c) => {
      expect(matchesQuery(c.texts, c.query)).toBe(c.kept);
    },
  );

  it("changes what several searches keep from the any-of rule", () => {
    expect(CASES.filter((c) => c.was_kept !== undefined).length).toBeGreaterThan(0);
  });
});

describe("a search's items", () => {
  const item = (text: string, excluded = false) => ({ text, excluded });

  it("are its quoted phrases and its words, lower-cased, each required", () => {
    expect(searchItems('"shift coverage" clinic')).toEqual([
      [item("shift coverage")],
      [item("clinic")],
    ]);
    expect(searchItems("  Shift   CLINIC ")).toEqual([[item("shift")], [item("clinic")]]);
    expect(searchItems('shift" "" clinic')).toEqual([[item("shift")], [item("clinic")]]);
    expect(searchItems("")).toEqual([]);
  });

  it("joins the items either side of or, which binds tighter than a space", () => {
    expect(searchItems("blue red OR green")).toEqual([
      [item("blue")],
      [item("red"), item("green")],
    ]);
    expect(searchItems("or a or or b or")).toEqual([[item("a"), item("b")]]);
  });

  it("excludes an item with a leading -, a word or a phrase, and drops a lone -", () => {
    expect(searchItems('red -blue -"pet store" - pet-store')).toEqual([
      [item("red")],
      [item("blue", true)],
      [item("pet store", true)],
      [item("pet-store")],
    ]);
  });

  it("reads a quoted or and a quoted -x as text, and + as text", () => {
    expect(searchItems('"or" "-x" +red')).toEqual([[item("or")], [item("-x")], [item("+red")]]);
  });

  it("matches a text that is absent as holding nothing", () => {
    expect(matchesSearch([undefined, null, "a clinic visit"], searchItems("clinic"))).toBe(true);
    expect(matchesSearch([null, undefined], searchItems("x"))).toBe(false);
    expect(matchesSearch([null, undefined], searchItems("-x"))).toBe(true);
  });
});
