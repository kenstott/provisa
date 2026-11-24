// Copyright (c) 2026 Kenneth Stott
// Canary: 036a2a51-4484-4807-803b-0036871bca13
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Each locale's .tm.json records, per "<lng>/<ns>.json::<key>" (or "<lng>.json::<key>"), the English
// string its translation was made from. A key whose English has changed since, or whose entry is
// missing or keyed without the locale prefix, carries a translation of an older string. This guard
// fails on any such key and on any TM entry that names no current English key.

import { describe, it, expect } from "vitest";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { join, resolve } from "node:path";

const LOCALES = resolve(__dirname, "../locales");

function flatten(node: unknown, path: string, out: Record<string, string>): Record<string, string> {
  if (node && typeof node === "object")
    for (const [k, v] of Object.entries(node)) flatten(v, path ? `${path}.${k}` : k, out);
  else out[path] = String(node);
  return out;
}

const read = (p: string): unknown => JSON.parse(readFileSync(p, "utf8"));

const namespaces = readdirSync(join(LOCALES, "en")).filter((f) => f.endsWith(".json") && !f.startsWith("."));

const locales = readdirSync(LOCALES, { withFileTypes: true })
  .filter((d) => d.isDirectory() && d.name !== "en" && existsSync(join(LOCALES, d.name, ".tm.json")))
  .map((d) => d.name);

describe("translation memory is current for every locale", () => {
  it("finds locales to check", () => {
    expect(locales.length).toBeGreaterThan(0);
  });

  for (const lng of locales) {
    it(`${lng}: every key was translated from the current English`, () => {
      const tm = read(join(LOCALES, lng, ".tm.json")) as Record<string, string>;
      const expected: Record<string, string> = {};
      const pairs: [string, string][] = [
        ["en.json", `${lng}.json`],
        ...namespaces.map((ns): [string, string] => [`en/${ns}`, `${lng}/${ns}`]),
      ];
      for (const [en, target] of pairs)
        for (const [k, v] of Object.entries(flatten(read(join(LOCALES, en)), "", {})))
          expected[`${target}::${k}`] = v;
      const stale = Object.entries(expected)
        .filter(([key, en]) => tm[key] !== en)
        .map(([key]) => key);
      const orphan = Object.keys(tm).filter((key) => !(key in expected));
      expect({ stale, orphan }).toEqual({ stale: [], orphan: [] });
    });
  }
});
