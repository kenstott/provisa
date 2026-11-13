// Copyright (c) 2026 Kenneth Stott
// Canary: a4d75bee-e67d-4878-8476-9df28a86e4fa
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: every query plan diagram is the one canvas (MermaidDiagram, with the shared controls):
// the SQL stats view, the plan panes (inline and in the Query plan dialog) and the GraphQL table
// view's stats. Nothing else draws Mermaid.
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { describe, expect, it } from "vitest";

const SRC = join(__dirname, "..", "..");
const read = (p: string) => readFileSync(join(SRC, p), "utf8");

function sources(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const full = join(dir, name);
    if (statSync(full).isDirectory()) return name === "__tests__" ? [] : sources(full);
    return /\.tsx?$/.test(name) && !/\.test\.tsx?$/.test(name) ? [full] : [];
  });
}

describe("the plan diagram's uses", () => {
  it("the three plan diagrams mount the one component", () => {
    const uses: [string, RegExp][] = [
      ["pages/sql/ResultsPanel.tsx", /<MermaidDiagram chart=\{stats\.mermaid\}/],
      ["pages/sql/AnalyzePanes.tsx", /<MermaidDiagram\s[^>]*chart=\{plan\.mermaid\}/],
      ["plugins/table-view.tsx", /<MermaidDiagram chart=\{queryStats\.mermaid\}/],
    ];
    for (const [file, use] of uses) expect(read(file), file).toMatch(use);
  });

  it("only the one component draws Mermaid", () => {
    const drawers = sources(SRC)
      .filter((f) =>
        /import\(\s*["']mermaid["']\s*\)|from\s+["']mermaid["']/.test(readFileSync(f, "utf8")),
      )
      .map((f) => relative(SRC, f));
    expect(drawers).toEqual(["components/MermaidDiagram.tsx"]);
  });
});
