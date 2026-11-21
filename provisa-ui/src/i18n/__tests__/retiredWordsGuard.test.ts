// Copyright (c) 2026 Kenneth Stott
// Canary: 32ed5d8e-6555-4d9f-99fc-aecc13c1a5c2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The UI says Show/Hide, Expand/Collapse and Enlarge/Full screen, each pair meaning one thing; fold
// and unfold are not used. This guard reads every English catalog (en.json and en/*.json) and
// fails on any value that holds fold or unfold as a word, so the retired words cannot come back.
// "folder" and other words that only begin with fold are not caught.

import { describe, it, expect } from "vitest";
import { readFileSync, readdirSync } from "node:fs";
import { join, resolve } from "node:path";

const LOCALES = resolve(__dirname, "../locales");
const RETIRED = /\b(un)?fold(s|ed|ing)?\b/i;

function values(node: unknown, path: string, out: [string, string][]): [string, string][] {
  if (typeof node === "string") out.push([path, node]);
  else if (node && typeof node === "object")
    for (const [k, v] of Object.entries(node)) values(v, path ? `${path}.${k}` : k, out);
  return out;
}

const catalogs = [
  "en.json",
  ...readdirSync(join(LOCALES, "en"))
    .filter((f) => f.endsWith(".json"))
    .map((f) => `en/${f}`),
];

describe("retired words in the English catalogs", () => {
  it("reads every English catalog", () => {
    expect(catalogs.length).toBeGreaterThan(100);
  });

  it("finds the retired words when they are there", () => {
    expect(RETIRED.test("Fold columns")).toBe(true);
    expect(RETIRED.test("stay folded into it")).toBe(true);
    expect(RETIRED.test("Unfold")).toBe(true);
    expect(RETIRED.test("Choose folder")).toBe(false);
  });

  it("no value says fold or unfold", () => {
    const found = catalogs.flatMap((file) =>
      values(JSON.parse(readFileSync(join(LOCALES, file), "utf8")), "", [])
        .filter(([, v]) => RETIRED.test(v))
        .map(([k, v]) => `${file}::${k}: ${v}`),
    );
    expect(found).toEqual([]);
  });
});
