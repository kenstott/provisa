// Copyright (c) 2026 Kenneth Stott
// Canary: 3baa959b-3382-4941-9434-d06b4c9bfba0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Every collapsible panel of the application is the one collapsible (components/collapsible): no
// component draws Mantine's Accordion or Collapse itself, or a toggle of its own that opens and
// closes a panel, so all panels look the same open and closed and each opens as it was left. The
// toggles below are not panels, each for the reason it gives.

import { describe, it, expect } from "vitest";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative, resolve } from "node:path";

const SRC = resolve(__dirname, "../../..");
const SHARED = "components/collapsible/";

/** Files that open and close something that is not a panel, by what it is. */
const NOT_PANELS: Readonly<Record<string, string>> = {
  "components/NavBar.tsx": "a menu of the navigation bar",
  "components/graph/NodeContextMenu.tsx": "a context menu's submenu",
  "components/graph/GraphSidebar.tsx": "an explorer's tree of labels and keys",
  "components/sql-modeling/SchemaSidebar.tsx": "an explorer's schema tree",
  "pages/sql/SchemaBrowser.tsx": "an explorer's schema tree",
  "components/list/ListTable.tsx": "a list's group headings",
  "components/relationships/RelationshipRow.tsx": "a list row's selection",
  "pages/DataProductsPage.tsx": "a list row's expansion",
  "components/admin/LocalUsersTab.tsx": "a list row's detail",
  "components/admin/OrgsTab.tsx": "a list row's members",
  "pages/tables/ColumnsTable.tsx": "a column row's mask list, opened beneath the row",
  "pages/tables/TableColumnsSection.tsx": "a column row's mask list, opened beneath the row",
  "pages/JsonApiPage.tsx": "a request builder's field and include pickers",
  "pages/LoginPage.tsx": "the sign-in page's other sign-in method",
  "tour/tourSteps.ts": "a tour step names a control's state",
};

function sources(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) return name === "__tests__" ? [] : sources(path);
    return /\.(ts|tsx)$/.test(name) && !/\.test\.tsx?$/.test(name) ? [path] : [];
  });
}

const FILES = sources(SRC).map((path) => ({
  file: relative(SRC, path).split("\\").join("/"),
  text: readFileSync(path, "utf8"),
}));

const MANTINE_COLLAPSIBLE =
  /import\s*\{[^}]*\b(Accordion|Collapse)\b[^}]*\}\s*from\s*["']@mantine\/core["']/s;

describe("every collapsible panel is the one collapsible", () => {
  it("no component draws Mantine's Accordion or Collapse but the shared one", () => {
    const drawing = FILES.filter(
      ({ file, text }) =>
        !file.startsWith(SHARED) && !(file in NOT_PANELS) && MANTINE_COLLAPSIBLE.test(text),
    ).map(({ file }) => file);
    expect(drawing).toEqual([]);
  });

  it("no component opens and closes a panel with a toggle of its own", () => {
    const toggling = FILES.filter(
      ({ file, text }) =>
        !file.startsWith(SHARED) && !(file in NOT_PANELS) && /aria-expanded/.test(text),
    ).map(({ file }) => file);
    expect(toggling).toEqual([]);
  });

  it("an exemption names a file that still has a toggle", () => {
    const stale = Object.keys(NOT_PANELS).filter((file) => {
      const found = FILES.find((f) => f.file === file);
      return !found || !(/aria-expanded/.test(found.text) || MANTINE_COLLAPSIBLE.test(found.text));
    });
    expect(stale).toEqual([]);
  });
});
