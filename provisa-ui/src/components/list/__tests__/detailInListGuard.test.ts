// Copyright (c) 2026 Kenneth Stott
// Canary: 206d32c8-7043-463e-b3f5-c93f9df05bfd
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277 rule 9: on every object management page and the ontology, a detail panel shows an item
// only while it is a row of the list the page shows. One mechanism decides it: useRememberedDetail,
// given the rows the list shows (DetailList), gates the open item and names the one the address
// names that the list leaves out (`outOfList`), which the page says once above its list -- through
// SplitPane's `outOfList`, or DetailOutOfList where the page lays its list out itself. A page with
// a list and a detail panel that does not, fails here.

import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { describe, expect, it } from "vitest";

const SRC = join(__dirname, "..", "..", "..");
const HOOK = join("components", "list", "useDetailParam.ts");
const PANE = join("components", "list", "SplitPane.tsx");

function sources(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) return name === "__tests__" ? [] : sources(path);
    return /\.tsx?$/.test(name) && !/\.test\.tsx?$/.test(name) ? [path] : [];
  });
}

/** The text of a file with its comments taken out: a comment may name what it does not do. */
function code(path: string): string {
  return readFileSync(path, "utf8")
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .replace(/(^|[^:])\/\/.*$/gm, "$1");
}

// A hook that opens the page's detail for another file, which renders the notice.
const NOTICE_IN: Record<string, string> = {
  [join("components", "admin", "useGlossaryPanel.ts")]: join(
    "components",
    "admin",
    "GlossaryTab.tsx",
  ),
};

// A list beside a panel whose selection is the view's own state: no address names it, nothing
// remembers it, and the list has no search or filter that could leave the selected row out.
const LOCAL_SELECTION: Record<string, string> = {
  [join("components", "admin", "GrantEditor.tsx")]: "a role's grants, picked in the editor",
  [join("components", "admin", "ontologyImport", "OntologyImportsView.tsx")]:
    "the ontology imports, picked in place of the term list",
  [join("pages", "ODataPage.tsx")]: "the OData model's entity sets, picked in the page",
};

// The pages the requirement names: each must be found as a list beside a detail.
const COVERED = [
  "pages/SourcesPage.tsx",
  "pages/TablesPage.tsx",
  "pages/RelationshipsPage.tsx",
  "pages/CommandsPage.tsx",
  "pages/MetricsPage.tsx",
  "components/admin/useGlossaryPanel.ts",
  "components/admin/TagsTab.tsx",
  "components/admin/ClassificationKindsTab.tsx",
  "pages/library/LibraryPage.tsx",
  "pages/DataProductsPage.tsx",
  "pages/SecurityPage.tsx",
  "pages/security/SecurityRlsPage.tsx",
  "pages/RequestsPage.tsx",
  "pages/TeamPage.tsx",
  "components/admin/DomainsTab.tsx",
  "components/admin/SecretsTab.tsx",
  "components/admin/PackageRegistriesTab.tsx",
  "components/admin/ContainerRegistriesTab.tsx",
  "components/admin/ReportsTab.tsx",
  "components/admin/OrgsTab.tsx",
  "components/admin/ScheduledTasks.tsx",
  "components/admin/PlatformRegionsTab.tsx",
  "components/admin/EnvironmentsTab.tsx",
  "components/admin/syncing/SyncingTab.tsx",
].map((p) => join(...p.split("/")));

const files = sources(SRC).filter((path) => {
  const rel = relative(SRC, path);
  return rel !== HOOK && rel !== PANE;
});
const text = new Map(files.map((path) => [relative(SRC, path), code(path)]));
const withDetail = [...text]
  .filter(([, body]) => body.includes("<SplitPane") || body.includes("useRememberedDetail("))
  .map(([rel]) => rel)
  .sort();

describe("every list beside a detail shows only an item the list shows (REQ-2277 rule 9)", () => {
  it("finds every page the requirement names", () => {
    for (const page of COVERED) expect(withDetail).toContain(page);
  });

  it.each(withDetail)("%s", (rel) => {
    const body = text.get(rel)!;
    if (rel in LOCAL_SELECTION) {
      // Its selection is its own; a search, a filter or an address deciding it would make it a
      // detail like any other page's.
      expect(body).not.toContain("useRememberedDetail(");
      expect(body).not.toMatch(/useDetailParam\(|useUrlFilter\(|matchesQuery\(|<FilterInput/);
      return;
    }
    // Its detail is gated by the list shown.
    expect(body).toContain("useRememberedDetail(");
    // And the item a link names that the list leaves out is said above the list.
    const notice = NOTICE_IN[rel] === undefined ? body : text.get(NOTICE_IN[rel])!;
    expect(notice).toMatch(/outOfList=\{/);
    if (NOTICE_IN[rel] !== undefined) {
      expect(body).toContain("outOfList");
      expect(notice).toContain("<DetailOutOfList");
    }
  });

  it("gives every SplitPane beside a remembered detail the notice", () => {
    const missing = withDetail.filter((rel) => {
      const body = text.get(rel)!;
      return (
        body.includes("<SplitPane") &&
        body.includes("useRememberedDetail(") &&
        (body.match(/<SplitPane\b/g) ?? []).length !== (body.match(/\boutOfList=\{/g) ?? []).length
      );
    });
    expect(missing).toEqual([]);
  });
});
