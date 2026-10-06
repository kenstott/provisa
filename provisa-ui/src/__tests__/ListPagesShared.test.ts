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
    expect(tsx).toContain("<ListRow");
    expect(tsx).not.toContain("<Table.ScrollContainer");
    expect(tsx).not.toMatch(/<Table\s+striped/);
  });
});
