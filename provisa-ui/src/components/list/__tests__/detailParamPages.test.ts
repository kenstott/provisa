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
// REQ-2260: and a page whose panel opens an item's detail does so through useRememberedDetail, so
// it reopens what was left open; a page added to DETAIL_VIEWS without it fails here. A page that
// only marks a linked row (no detail opens or closes there) names itself to useDetailParam.

import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  DETAIL_VIEWS,
  commandId,
  derivedId,
  detailHref,
  readCommandId,
  readDerivedId,
  type DetailPage,
} from "../useDetailParam";

const SRC = join(__dirname, "..", "..", "..");

// Where each page's panel is wired, and how it names itself to the hook.
// `marks`: the page only marks a linked row.
const PAGES: Record<DetailPage, { file: string; call: string; marks?: true }> = {
  sources: { file: "pages/SourcesPage.tsx", call: 'useRememberedDetail(\n    "sources",' },
  tables: {
    file: "pages/TablesPage.tsx",
    call: 'useRememberedDetail(\n    viewsOnly ? "views" : "tables",',
  },
  views: {
    file: "pages/TablesPage.tsx",
    call: 'useRememberedDetail(\n    viewsOnly ? "views" : "tables",',
  },
  relationships: {
    file: "pages/RelationshipsPage.tsx",
    call: 'useRememberedDetail(\n    "relationships",',
  },
  ontology: {
    file: "components/admin/useGlossaryPanel.ts",
    call: 'useRememberedDetail("ontology",',
  },
  library: { file: "pages/library/LibraryPage.tsx", call: 'useRememberedDetail(\n    "library",' },
  metrics: { file: "pages/MetricsPage.tsx", call: 'useRememberedDetail("metrics",' },
  products: {
    file: "pages/DataProductsPage.tsx",
    call: 'useRememberedDetail(\n    "products",',
  },
  roles: { file: "pages/SecurityPage.tsx", call: 'useRememberedDetail("roles",' },
  derived: { file: "pages/SecurityPage.tsx", call: 'useRememberedDetail(\n    "derived",' },
  rls: { file: "pages/security/SecurityRlsPage.tsx", call: 'useRememberedDetail("rls",' },
  commands: { file: "pages/CommandsPage.tsx", call: 'useRememberedDetail(\n    "commands",' },
  domains: { file: "pages/AdminPage.tsx", call: 'useDetailParam("domains").id', marks: true },
  tags: { file: "components/admin/TagsTab.tsx", call: 'useDetailParam("tags").id', marks: true },
};

describe("every list page's detail panel is in the URL through useDetailParam (REQ-2241)", () => {
  it.each(Object.keys(DETAIL_VIEWS) as DetailPage[])("%s", (page) => {
    const { file, call, marks } = PAGES[page];
    const text = readFileSync(join(SRC, file), "utf8");
    expect(text).toContain(call);
    // REQ-2260: a page with a detail remembers it; it never takes the bare hook.
    if (!marks) expect(text).not.toContain("useDetailParam(");
    const { param } = DETAIL_VIEWS[page];
    const own = new RegExp(`\\.(get|has|set|delete)\\("${param}"`, "g");
    expect(text.match(own) ?? []).toEqual([]);
  });

  it("builds the same link the page writes", () => {
    expect(detailHref("tables", 7, "columns")).toBe("/tables?table=7&section=columns");
    expect(detailHref("tables", 7, "identity")).toBe("/tables?table=7");
    expect(detailHref("sources", "pets")).toBe("/sources?expanded=pets");
    expect(detailHref("ontology", 3)).toBe("/context/ontology?term=3");
    expect(() => detailHref("metrics", "m", "x")).toThrow();
    // REQ-2240: a mark the panel keeps with its object (a column row), and only such a one.
    expect(detailHref("tables", 7, "columns", { column: "amount" })).toBe(
      "/tables?table=7&section=columns&column=amount",
    );
    expect(() => detailHref("metrics", "m", undefined, { column: "x" })).toThrow();
    expect(detailHref("domains", "sales")).toBe("/admin/domains?domain=sales");
    expect(detailHref("tags", "pii")).toBe("/admin/tags?tag=pii");
  });

  it("names a webhook by its kind and a function by its bare name", () => {
    expect(commandId("function", "notify_ops")).toBe("notify_ops");
    expect(commandId("webhook", "notify_ops")).toBe("webhook:notify_ops");
    expect(readCommandId("notify_ops")).toEqual({ kind: "function", name: "notify_ops" });
    expect(readCommandId("webhook:notify_ops")).toEqual({ kind: "webhook", name: "notify_ops" });
    expect(detailHref("commands", commandId("webhook", "n"))).toBe("/commands?command=webhook%3An");
  });

  it("names a derived role by its attribute and the value picked under it", () => {
    expect(derivedId("region", null)).toBe("region");
    expect(derivedId("region", "eu")).toBe("region:eu");
    expect(readDerivedId("region")).toEqual({ attribute: "region", value: null });
    expect(readDerivedId("region:eu:west")).toEqual({ attribute: "region", value: "eu:west" });
    expect(detailHref("derived", derivedId("region", "eu"))).toBe(
      "/security/roles?derived=region%3Aeu",
    );
  });
});
