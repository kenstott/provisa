// Copyright (c) 2026 Kenneth Stott
// Canary: 3c9e7a52-6b1d-4f08-8d4e-1a2b5c7d9e30
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// One Save (maintainer, 2026-11-12: a panel head shows the Save a form's foot shows).
// components/form/SaveButton.tsx draws it: filled primary, the check, one size. No other file
// labels a button with the shared Save word, so a head, a foot and a dialog cannot drift apart.

import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative, resolve } from "node:path";
import { describe, expect, it } from "vitest";

const SRC = resolve(__dirname, "../../..");
const THE_SAVE = "components/form/SaveButton.tsx";

function componentFiles(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const p = join(dir, name);
    if (statSync(p).isDirectory()) return name === "__tests__" ? [] : componentFiles(p);
    return name.endsWith(".tsx") && !name.includes(".test.") ? [relative(SRC, p)] : [];
  });
}

describe("no file but the Save labels a button with the shared Save word", () => {
  it("the Save itself is filled, full size and labelled with the shared word", () => {
    const text = readFileSync(join(SRC, THE_SAVE), "utf8");
    expect(text).toContain('t("common.save")');
    expect(text).toContain('variant="filled"');
    expect(text).toContain('size="sm"');
  });

  for (const file of componentFiles(SRC).filter((f) => f !== THE_SAVE)) {
    it(file, () => {
      expect(
        readFileSync(join(SRC, file), "utf8"),
        `${file}: a Save of its own; use SaveButton (${THE_SAVE})`,
      ).not.toMatch(/\bt\(\s*["']common\.save["']/);
    });
  }
});
