// Copyright (c) 2026 Kenneth Stott
// Canary: 8a1d4e7f-3b29-4c56-9f0e-6d2b8a5c1e47
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277 guard: every Explore surface stands in the one Explore shell, and every one that shows
// results shows them in the one result panel. A surface keeps no frame of its own: no `.page`
// gutter of its own, no height computed from the window. A new Explore surface is added here.

import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const SRC = resolve(__dirname, "../../..");
const read = (file: string) => readFileSync(resolve(SRC, file), "utf8");

interface Surface {
  /** The component that draws the surface's frame. */
  page: string;
  /** Where its results are drawn, when it shows results of its own. */
  results?: string;
  /** Why it has no result panel: what draws its results instead. */
  noResultPanel?: string;
  /** Its own stylesheet. */
  css?: string;
}

const SURFACES: Readonly<Record<string, Surface>> = {
  GraphQL: {
    page: "pages/QueryPage.tsx",
    css: "pages/QueryPage.css",
    noResultPanel: "GraphiQL draws its own response pane (kept as is, REQ-2277)",
  },
  Cypher: { page: "pages/GraphPage.tsx", results: "pages/GraphPage.tsx", css: "pages/GraphPage.css" },
  SPARQL: { page: "pages/GraphPage.tsx", results: "pages/GraphPage.tsx", css: "pages/GraphPage.css" },
  SQL: { page: "pages/SqlPage.tsx", results: "pages/sql/ResultsPanel.tsx" },
  gRPC: { page: "pages/GrpcPage.tsx", results: "pages/GrpcPage.tsx", css: "pages/GrpcPage.css" },
  "JSON:API": {
    page: "pages/JsonApiPage.tsx",
    results: "pages/JsonApiPage.tsx",
    css: "pages/JsonApiPage.css",
  },
  OData: {
    page: "pages/ODataPage.tsx",
    results: "pages/odata/ODataResults.tsx",
    css: "pages/odata/ODataPage.css",
  },
  OpenAPI: {
    page: "pages/OpenApiPage.tsx",
    css: "pages/OpenApiPage.css",
    noResultPanel: "the embedded API reader answers each call in place",
  },
  MCP: {
    page: "pages/McpExplorePage.tsx",
    noResultPanel: "a conversation: each answer is a message of the chat",
  },
  Python: {
    page: "pages/DataSciencePage.tsx",
    results: "pages/DataSciencePage.tsx",
    css: "pages/DataSciencePage.css",
  },
  Notebook: {
    page: "pages/DataSciencePage.tsx",
    noResultPanel: "the embedded notebook draws each cell's output under the cell",
  },
  NL: { page: "pages/NlPage.tsx", results: "pages/NlPage.tsx", css: "pages/NlPage.css" },
};

describe("every Explore surface uses the shared shell and result panel", () => {
  it.each(Object.entries(SURFACES))("%s stands in the Explore shell", (_name, surface) => {
    expect(read(surface.page)).toMatch(/<ExploreShell\b/);
  });

  it.each(Object.entries(SURFACES))("%s shows its results in the result panel", (_name, s) => {
    if (s.results === undefined) {
      expect(s.noResultPanel, "a surface without results says what draws them").toBeTruthy();
      return;
    }
    expect(read(s.results)).toMatch(/<ResultPanel\b/);
  });

  it.each(Object.entries(SURFACES))("%s keeps no frame of its own", (_name, surface) => {
    // The shell's gutter is the only one: no `.page` padding on the surface's root.
    expect(read(surface.page)).not.toMatch(/className="[^"]*\bpage\b[^"]*"\s*(?:style|>|data-)/);
    if (surface.css) expect(read(surface.css)).not.toMatch(/height:\s*calc\(100vh/);
  });

  it("covers every Explore surface the maintainer named", () => {
    expect(Object.keys(SURFACES).sort()).toEqual(
      [
        "GraphQL",
        "Cypher",
        "SPARQL",
        "SQL",
        "gRPC",
        "JSON:API",
        "OData",
        "OpenAPI",
        "MCP",
        "Python",
        "Notebook",
        "NL",
      ].sort(),
    );
  });
});
