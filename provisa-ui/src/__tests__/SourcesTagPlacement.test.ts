// Copyright (c) 2026 Kenneth Stott
// Canary: c0b44d25-ab81-41bc-89b9-643c177bf496
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1940: a source's tag chips and its edit-tags control sit where a table's do — after the
// identifier in its own cell — not among the row's actions.

import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const page = readFileSync(resolve(__dirname, "../pages/SourcesPage.tsx"), "utf8");

describe("Sources tag placement", () => {
  it("puts the tags in the identifier cell, after the source id", () => {
    const idCell = page.indexOf("{s.id}\n");
    const tags = page.indexOf('<TagControl objectType="source"');
    const typeCell = page.indexOf("{sourceTypeLabel(s.type", idCell);
    expect(idCell).toBeGreaterThan(-1);
    expect(tags).toBeGreaterThan(idCell);
    expect(tags).toBeLessThan(typeCell);
  });

  it("keeps the tags out of the actions cell", () => {
    expect(page.match(/<TagControl objectType="source"/g)).toHaveLength(1);
  });
});
