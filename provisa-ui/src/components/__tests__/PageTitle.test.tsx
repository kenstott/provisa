// Copyright (c) 2026 Kenneth Stott
// Canary: 288c515e-91d6-4a72-9782-a48782db318e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { Database } from "lucide-react";
import { describe, expect, it } from "vitest";
import { fireEvent, render, screen } from "../../test-utils/render";
import { PageTitle } from "../PageTitle";

describe("PageTitle", () => {
  it("renders an aria-hidden icon before the title text", () => {
    render(<PageTitle icon={Database}>Sources</PageTitle>, { initialEntries: ["/sources"] });
    const root = screen.getByTestId("page-title");
    const svg = root.querySelector("svg");
    expect(svg).not.toBeNull();
    expect(svg?.getAttribute("aria-hidden")).toBe("true");
    expect(screen.getByRole("heading", { name: "Sources" })).toBeTruthy();
    expect(root.firstElementChild).toBe(svg);
  });

  // REQ-2277 rule 8: the page's (?) help is part of the title, read from the registry by route.
  it("carries the route's page help after the title, with its purpose and docs link", async () => {
    render(<PageTitle icon={Database}>Sources</PageTitle>, { initialEntries: ["/sources"] });
    const help = screen.getByRole("button", { name: "About this page" });
    expect(screen.getByTestId("page-title").lastElementChild).toBe(help);
    fireEvent.click(help);
    const card = await screen.findByTestId("page-help-card");
    expect(card).toHaveTextContent(
      "Register the databases, files and services Provisa reads from, and set how each one is reached.",
    );
    expect(screen.getByTestId("page-help-docs")).toHaveAttribute("href", "/docs?page=sources/");
  });

  it("draws no help on a route whose help another header carries", () => {
    render(<PageTitle icon={Database}>SQL</PageTitle>, { initialEntries: ["/sql"] });
    expect(screen.queryByTestId("page-help")).toBeNull();
  });
});

// Each top-level page (and the Glossary admin tab) heads itself through PageTitle with an icon.
const PAGES: [string, string][] = [
  ["../../pages/SourcesPage.tsx", "Database"],
  ["../../pages/TablesPage.tsx", "Table2"],
  ["../../pages/RelationshipsPage.tsx", "Link2"],
  ["../../pages/MetricsPage.tsx", "Gauge"],
  ["../../pages/CommandsPage.tsx", "Terminal"],
  ["../../pages/DataProductsPage.tsx", "Package"],
  ["../../pages/SecurityPage.tsx", "Shield"],
  ["../../pages/AdminPage.tsx", "Settings"],
  ["../../pages/McpExplorePage.tsx", "Compass"],
  ["../../pages/LineagePage.tsx", "GitBranch"],
  ["../../pages/RequestsPage.tsx", "ListOrdered"],
  ["../../pages/TeamPage.tsx", "Users"],
  ["../admin/GlossaryTab.tsx", "BookOpen"],
];

describe("page headings use PageTitle", () => {
  it.each(PAGES)("%s", (path, icon) => {
    const src = readFileSync(resolve(__dirname, path), "utf8");
    expect(src).toMatch(new RegExp(`<PageTitle[^>]*icon=\\{[^}]*${icon}`));
  });
});
