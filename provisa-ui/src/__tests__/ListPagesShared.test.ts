// Copyright (c) 2026 Kenneth Stott
// Canary: 7a2f9d36-1c4e-4b85-a0d7-3e6b8c1f5a92
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1940: every admin list page renders its list through the shared list component; a page
// differs only in columns and actions, never in its own table styling.

import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const LIST_PAGES = [
  "pages/TablesPage.tsx",
  "pages/SourcesPage.tsx",
  "pages/SecurityPage.tsx",
  "pages/RelationshipsPage.tsx",
  "components/relationships/RelationshipRow.tsx",
  "pages/DataProductsPage.tsx",
  "pages/MetricsPage.tsx",
  "pages/CommandsPage.tsx",
  "pages/RequestsPage.tsx",
  "pages/TeamPage.tsx",
  "pages/AdminPage.tsx",
  "components/admin/RolesTab.tsx",
  "components/admin/LocalUsersTab.tsx",
  "components/admin/OrgsTab.tsx",
  "components/admin/TagsTab.tsx",
  "components/admin/ScheduledTasks.tsx",
  "components/admin/SystemHealth.tsx",
  "components/admin/McpServerTab.tsx",
  "components/admin/GlossaryTab.tsx",
  "components/admin/MergeRequestsPanel.tsx",
  "components/admin/EnvironmentsTab.tsx",
  "components/admin/SecretsTab.tsx",
  "components/admin/CacheManager.tsx",
  "components/admin/SyntheticDatasetsPanel.tsx",
  "components/admin/AiModelsTab.tsx",
];

const src = (rel: string) => readFileSync(resolve(process.cwd(), "src", rel), "utf8");

describe("REQ-1940 list pages use the shared list component", () => {
  it.each(LIST_PAGES.map((p) => [p]))("%s", (page) => {
    const tsx = src(page);
    expect(tsx).toMatch(/from "(\.\.\/)+(components\/)?list\/ListTable"/);
    // No page-local list styling: no striped/bordered Mantine tables or scroll containers.
    // Item rows are ListRow, so every list gets the stripe and hover.
    // (RelationshipsPage delegates its item rows to RelationshipRow, which is listed itself.)
    if (page !== "pages/RelationshipsPage.tsx") expect(tsx).toContain("<ListRow");
    expect(tsx).not.toContain("<Table.ScrollContainer");
    expect(tsx).not.toMatch(/<Table\s+striped/);
  });
});

// REQ-1430 / REQ-1940: a list page's loading state is the PageLoading spinner, never a bare text
// line or an in-table row.
describe("REQ-1940 list pages load with PageLoading", () => {
  it.each(
    [
      "pages/TablesPage.tsx",
      "pages/MetricsPage.tsx",
      "pages/DataProductsPage.tsx",
      "pages/RelationshipsPage.tsx",
      "pages/CommandsPage.tsx",
    ].map((p) => [p]),
  )("%s", (page) => {
    const tsx = src(page);
    expect(tsx).toContain("<PageLoading");
    expect(tsx).not.toContain("ListLoading");
  });

  it("the shared list parts define no loading variant of their own", () => {
    expect(src("components/list/ListTable.tsx")).not.toContain("Loading");
  });
});

// REQ-1940: sort and group are standard on every list. Exempt: RelationshipRow (row markup only —
// its page wires the sort), SystemHealth (a fixed component-status board, not an item list) and
// AiModelsTab (inline-editable config grids addressed by array index, which a reorder would break).
const SORT_EXEMPT = new Set([
  "components/relationships/RelationshipRow.tsx",
  "components/admin/SystemHealth.tsx",
  "components/admin/AiModelsTab.tsx",
]);

// Lists with no categorical column (nothing to group by).
const NO_GROUP = new Set([
  "components/admin/McpServerTab.tsx",
  "components/admin/SecretsTab.tsx",
  "components/admin/ScheduledTasks.tsx",
]);

describe("REQ-1940 list pages wire sort and group", () => {
  it.each(LIST_PAGES.filter((p) => !SORT_EXEMPT.has(p)).map((p) => [p]))("%s", (page) => {
    const tsx = src(page);
    expect(tsx).toMatch(/useListSortGroup\(|<SortGroupTable/);
    expect(tsx).toContain("sortValue:");
    if (!NO_GROUP.has(page)) expect(tsx).toContain("groupValue:");
    // No page keeps its own sort/group state or header controls.
    expect(tsx).not.toMatch(/useState<[^>]*"asc" \| "desc"/);
    expect(tsx).not.toContain("ArrowUpDown");
    expect(tsx).not.toContain("setCollapsedGroups");
  });

  it("every { col } header cell has the sortGroup that draws it", () => {
    for (const page of LIST_PAGES) {
      const tsx = src(page);
      if (/\{ col: "/.test(tsx) && !tsx.includes("<SortGroupTable")) {
        expect(tsx, page).toContain("sortGroup={");
      }
    }
  });

  it("TablesPage keeps its sort/group test ids through the shared prefix", () => {
    expect(src("pages/TablesPage.tsx")).toContain(
      // REQ-1922: the region selection narrows the filtered list before it is sorted and grouped.
      'useListSortGroup(regionTables, listColumns, "tables")',
    );
  });
});
