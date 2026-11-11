// Copyright (c) 2026 Kenneth Stott
// Canary: daab5454-f251-4dfc-bc3b-dd4660248d29
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2241: each list page's open panel is read and written through the one shared hook,
// useDetailParam, under the parameter DETAIL_VIEWS names for it. A page that reads or writes that
// parameter itself would be a second place deciding what is open, so it fails here.

import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { DETAIL_VIEWS, detailHref, type DetailPage } from "../useDetailParam";

const SRC = join(__dirname, "..", "..", "..");

// Where each page's panel is wired, and how it names itself to the hook.
const PAGES: Record<DetailPage, { file: string; call: string }> = {
  sources: { file: "pages/SourcesPage.tsx", call: 'useDetailParam("sources")' },
  tables: { file: "pages/TablesPage.tsx", call: 'useDetailParam(viewsOnly ? "views" : "tables")' },
  views: { file: "pages/TablesPage.tsx", call: 'useDetailParam(viewsOnly ? "views" : "tables")' },
  relationships: { file: "pages/RelationshipsPage.tsx", call: 'useDetailParam("relationships")' },
  ontology: { file: "components/admin/useGlossaryPanel.ts", call: 'useDetailParam("ontology")' },
  library: { file: "pages/library/LibraryPage.tsx", call: 'useDetailParam("library")' },
  metrics: { file: "pages/MetricsPage.tsx", call: 'useDetailParam("metrics")' },
  products: { file: "pages/DataProductsPage.tsx", call: 'useDetailParam("products")' },
  roles: { file: "pages/SecurityPage.tsx", call: 'useDetailParam("roles")' },
  rls: { file: "pages/security/SecurityRlsPage.tsx", call: 'useDetailParam("rls")' },
  commands: { file: "pages/CommandsPage.tsx", call: 'useDetailParam("commands")' },
};

describe("every list page's detail panel is in the URL through useDetailParam (REQ-2241)", () => {
  it.each(Object.keys(DETAIL_VIEWS) as DetailPage[])("%s", (page) => {
    const { file, call } = PAGES[page];
    const text = readFileSync(join(SRC, file), "utf8");
    expect(text).toContain(call);
    const { param } = DETAIL_VIEWS[page];
    const own = new RegExp(`\\.(get|has|set|delete)\\("${param}"`, "g");
    expect(text.match(own) ?? []).toEqual([]);
  });

  it("builds the same link the page writes", () => {
    expect(detailHref("tables", 7, "columns")).toBe("/tables?table=7&section=columns");
    expect(detailHref("tables", 7, "identity")).toBe("/tables?table=7");
    expect(detailHref("sources", "pets")).toBe("/sources?expanded=pets");
    expect(detailHref("ontology", 3)).toBe("/admin/ontology?term=3");
    expect(() => detailHref("metrics", "m", "x")).toThrow();
  });
});
