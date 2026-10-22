// Copyright (c) 2026 Kenneth Stott
// Canary: a7d7e39d-4dec-43c0-9079-c10baf60191f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// One style of tooltip (maintainer, 2026-10-11: "there are 3 styles of tooltips. Standardize them
// all using the style that the tables page erd icon button uses"). components/Tip.tsx draws every
// hint -- the one-line Tip and the titled HelpBubble card -- on one surface. This guard keeps the
// other two styles from coming back: the library's own Tooltip/HoverCard, and the browser's
// native `title` on a control.
//
// Not tooltips, and not read: a `title` PROP that is a component's heading (Modal, Alert,
// HelpBubble, ConfirmDialog, SplitPane, CollapsibleSection, Drawer) and the `title` of an
// <iframe> (its accessible name: OrgBrandingSettings, DocsPage, OpenApiPage, SchemaExplorer).

import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative, resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { checkTipRules, tipsInsideTriggers, type TipViolation } from "./tipRules";

const SRC = resolve(__dirname, "../..");

/** The file that IS the tooltip: the only one that may import the library's pieces. */
const THE_TIP = "components/Tip.tsx";

/**
 * Shrinking: files that still import the library's Tooltip or HoverCard. A file leaves this list
 * in the commit that moves it to Tip; a listed file that no longer imports one fails the guard.
 * Nothing is added here.
 */
const STILL_LIBRARY_TOOLTIP: readonly string[] = [];

/**
 * Shrinking: files that still put a native `title` on a control. Same rule: a file leaves in the
 * commit that converts it, and nothing is added.
 */
const STILL_TITLE_ON_CONTROL: readonly string[] = [];

function componentFiles(dir: string): string[] {
  const out: string[] = [];
  for (const name of readdirSync(dir)) {
    const p = join(dir, name);
    if (statSync(p).isDirectory()) {
      if (name !== "__tests__" && name !== "node_modules") out.push(...componentFiles(p));
    } else if (name.endsWith(".tsx") && !name.includes(".test.")) {
      out.push(relative(SRC, p));
    }
  }
  return out;
}

const violationsOf = (file: string, rule: TipViolation["rule"]) =>
  checkTipRules(file, readFileSync(join(SRC, file), "utf8")).filter((v) => v.rule === rule);

const describeAll = (file: string, vs: TipViolation[]) =>
  `${file}: ${vs.map((v) => `line ${v.line} ${v.rule} (${v.detail})`).join("; ")} -- use Tip or HelpBubble from components/Tip.tsx`;

describe("the tooltip rules fail what they are meant to fail", () => {
  const rules = (source: string) => checkTipRules("Sample.tsx", source).map((v) => v.rule);

  it("the library's Tooltip or HoverCard imported", () => {
    expect(rules(`import { Button, Tooltip } from "@mantine/core";`)).toEqual(["library-tooltip"]);
    expect(rules(`import { HoverCard } from "@mantine/core";`)).toEqual(["library-tooltip"]);
    expect(rules(`import { Button } from "@mantine/core";`)).toEqual([]);
  });

  it("a native title on a control", () => {
    expect(rules(`const x = <ActionIcon title="Copy" />;`)).toEqual(["title-on-control"]);
    expect(rules(`const x = <button title="Copy">c</button>;`)).toEqual(["title-on-control"]);
  });

  it("but not a heading prop, nor a frame's name", () => {
    expect(rules(`const x = <Modal title="Rename" opened onClose={f} />;`)).toEqual([]);
    expect(rules(`const x = <Alert title="Refused">no</Alert>;`)).toEqual([]);
    expect(rules(`const x = <iframe title="Documentation" src="/docs" />;`)).toEqual([]);
  });
});

/**
 * Every control that has a hint AND is the trigger of a menu or popover, by file. A new one is
 * added here on purpose, after its menu has been opened by a click with the hint in place (the
 * first four were, in the render harness: tip-menus.cjs; the grant picker's in grants.cjs). A Tip
 * passes the trigger through; this list is what makes someone look.
 */
const TIPS_INSIDE_TRIGGERS: Readonly<Record<string, number>> = {
  "components/GrantPicker.tsx": 1, // a column grant's "combine roles" popover
  "components/TagControl.tsx": 1, // the tag picker's toggle
  "components/admin/GlossaryTab.tsx": 1, // the Ontology page's AI menu (ontology-page.cjs)
  "components/graph/GraphFrame.tsx": 1, // the frame's download menu
  "components/graph/Inspector.tsx": 1, // the inspector's colour chip
  "pages/sql/ColumnFilterMenu.tsx": 1, // a result column's filter
};

describe("a hint on a menu's or popover's trigger", () => {
  const sites = componentFiles(SRC)
    .map((file) => ({
      file,
      found: tipsInsideTriggers(file, readFileSync(join(SRC, file), "utf8")),
    }))
    .filter((s) => s.found.length > 0);

  it("is one of the listed sites, each opened by a click before it was listed", () => {
    expect(Object.fromEntries(sites.map((s) => [s.file, s.found.length]))).toEqual(
      TIPS_INSIDE_TRIGGERS,
    );
  });

  it("is a Tip, which passes the trigger through, never a HelpBubble, which does not", () => {
    for (const s of sites)
      for (const f of s.found)
        expect(f.tag, `${s.file}:${f.line} puts a ${f.tag} directly inside a trigger`).not.toBe(
          "HelpBubble",
        );
  });

  it("is found by the rule", () => {
    const found = tipsInsideTriggers(
      "Sample.tsx",
      `const x = <Menu><Menu.Target><Tip text="t"><ActionIcon /></Tip></Menu.Target></Menu>;`,
    );
    expect(found.map((f) => f.tag)).toEqual(["Tip"]);
    expect(
      tipsInsideTriggers(
        "Sample.tsx",
        `const x = <Menu><Menu.Target><ActionIcon /></Menu.Target></Menu>;`,
      ),
    ).toEqual([]);
  });
});

describe("every component uses the one tooltip", () => {
  const files = componentFiles(SRC);
  const stillLibrary = new Set(STILL_LIBRARY_TOOLTIP);
  const stillTitle = new Set(STILL_TITLE_ON_CONTROL);

  it("finds the components and the tooltip itself", () => {
    expect(files.length).toBeGreaterThan(200);
    expect(files).toContain(THE_TIP);
    expect(stillLibrary.size).toBe(STILL_LIBRARY_TOOLTIP.length);
    expect(stillTitle.size).toBe(STILL_TITLE_ON_CONTROL.length);
    for (const f of [...STILL_LIBRARY_TOOLTIP, ...STILL_TITLE_ON_CONTROL])
      expect(files, `${f} is listed but does not exist`).toContain(f);
  });

  it.each(files.filter((f) => f !== THE_TIP && !stillLibrary.has(f)))(
    "%s imports no library tooltip",
    (file) => {
      const vs = violationsOf(file, "library-tooltip");
      expect(vs, describeAll(file, vs)).toEqual([]);
    },
  );

  it.each(files.filter((f) => !stillTitle.has(f)))("%s has no title on a control", (file) => {
    const vs = violationsOf(file, "title-on-control");
    expect(vs, describeAll(file, vs)).toEqual([]);
  });

  it.each([...STILL_LIBRARY_TOOLTIP])(
    "%s still imports one: once it does not, it leaves the list",
    (file) => {
      expect(violationsOf(file, "library-tooltip").length).toBeGreaterThan(0);
    },
  );

  it.each([...STILL_TITLE_ON_CONTROL])(
    "%s still has one: once it does not, it leaves the list",
    (file) => {
      expect(violationsOf(file, "title-on-control").length).toBeGreaterThan(0);
    },
  );
});
