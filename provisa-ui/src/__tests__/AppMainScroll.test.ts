// Copyright (c) 2026 Kenneth Stott
// Canary: 9e4d1b73-6a02-4c58-b7f1-2d8c0a5e3b19
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const appCss = readFileSync(resolve(__dirname, "../App.css"), "utf8");

// REQ-1836: `main` clips and scrolls its own content, so a tall admin form scrolls inside `main`
// instead of growing #root past 100vh and dragging the docked chat panel with the document.

function rule(selector: string): string {
  const m = new RegExp(`(?:^|\\n)${selector}\\s*\\{([^}]*)\\}`).exec(appCss);
  if (!m) throw new Error(`no top-level ${selector} rule in App.css`);
  return m[1].replace(/\/\*[\s\S]*?\*\//g, "");
}

describe("App.css main scroll containment", () => {
  it("gives main overflow-y: auto so it clips to its flex height", () => {
    expect(rule("main")).toMatch(/overflow-y:\s*auto/);
  });

  it("keeps main shrinkable inside the flex column", () => {
    expect(rule("main")).toMatch(/min-height:\s*0/);
  });
});
