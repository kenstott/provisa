// Copyright (c) 2026 Kenneth Stott
// Canary: 2a7c9e14-58d3-4b0f-9e61-c3f8a7d05b92
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A list page's filter finds a row by every text the row shows, partially and in any case. Every
// file that draws a standard list under a FilterInput is held to it by a test that renders the
// list and searches for each shown text (expectFilterFindsEveryCell, test-utils/listFilter.ts). A
// list page with no such test fails here, and a filter that misses a shown text fails that test.

import { readFileSync, readdirSync, statSync } from "node:fs";
import { basename, join, relative } from "node:path";
import { describe, expect, it } from "vitest";

const SRC = join(__dirname, "..", "..", "..");

function files(dir: string, keep: (name: string) => boolean): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) return name === "node_modules" ? [] : files(path, keep);
    return keep(name) ? [path] : [];
  });
}

const isTest = (name: string) => /\.test\.tsx?$/.test(name);
const sources = files(SRC, (n) => /\.tsx$/.test(n) && !isTest(n));
const tests = files(SRC, isTest);

/** A list page: a filter box and a standard list. */
const LIST_PAGE = {
  test: (text: string) =>
    /<FilterInput\b/.test(text) && /<(ListTable|SortGroupTable|ListItems)\b/.test(text),
};

/** The list pages a test holds to every shown text: the modules it imports, by file name. */
function covered(): Set<string> {
  const names = new Set<string>();
  for (const path of tests) {
    const text = readFileSync(path, "utf8");
    if (!text.includes("expectFilterFindsEveryCell(")) continue;
    for (const m of text.matchAll(/from\s+["']([^"']+)["']/g)) names.add(basename(m[1]));
  }
  return names;
}

describe("every list page's filter finds every text its rows show", () => {
  it("knows a list page", () => {
    expect(LIST_PAGE.test("<FilterInput value={q} />\n<ListTable testId='x'>")).toBe(true);
    expect(LIST_PAGE.test("<ListTable testId='x'>")).toBe(false);
  });

  it("has a test of every shown text for each list page", () => {
    const held = covered();
    const missing = sources
      .filter((path) => LIST_PAGE.test(readFileSync(path, "utf8")))
      .filter((path) => !held.has(basename(path, ".tsx")))
      .map((path) => relative(SRC, path));
    expect(missing).toEqual([]);
  });
});
