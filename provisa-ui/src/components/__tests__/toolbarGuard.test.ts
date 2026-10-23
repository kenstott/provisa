// Copyright (c) 2026 Kenneth Stott
// Canary: cb98298e-5b4d-4167-9103-a62161cf5722
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A toolbar reads the same way everywhere: its named controls first, then its icon-only buttons,
// then its (?) help last; and an icon-only button is borderless (subtle, transparent, or light for a
// toggle while it is on). This guard reads every component's TSX (toolbarRules.ts). The files below
// still hold bordered icon buttons from before the rule; the list only shrinks, so a new one fails.

import { describe, it, expect } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative, resolve } from "node:path";
import { checkToolbarRules, type ToolbarViolation } from "./toolbarRules";

const SRC = resolve(__dirname, "../..");

/** The icon-only buttons drawn filled or bordered on purpose, and why: each file's count. */
const DELIBERATE: Readonly<Record<string, [number, string]>> = {
  "components/ChatPanel.tsx": [
    2,
    "Polly's launcher floats over every page and must stand out from it; the mic fills red " +
      "only while it is listening, so a reader sees it is recording",
  ],
  "components/graph/QueryBar.tsx": [
    1,
    "Run is the query bar's send: the one filled button, as a chat's send is",
  ],
  "components/sql-modeling/SchemaSidebar.tsx": [
    1,
    "the sidebar's collapse handle sits on the panel's edge, and its border is what shows it there",
  ],
  "pages/sql/SchemaBrowser.tsx": [
    1,
    "the browser's collapse handle sits on the panel's edge, and its border is what shows it there",
  ],
};

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

const found = componentFiles(SRC).map((file) => ({
  file,
  violations: checkToolbarRules(file, readFileSync(join(SRC, file), "utf8")),
}));
const of = (rule: ToolbarViolation["rule"]) =>
  found
    .map(({ file, violations }) => ({ file, hits: violations.filter((v) => v.rule === rule) }))
    .filter(({ hits }) => hits.length > 0);

describe("the toolbar rules fail what they are meant to fail", () => {
  const rules = (source: string) => checkToolbarRules("Sample.tsx", source).map((v) => v.rule);

  it("a bordered or filled icon-only button, but not a borderless one", () => {
    expect(rules(`const x = <ActionIcon variant="default" aria-label="a" />;`)).toEqual([
      "bordered-icon-button",
    ]);
    expect(rules(`const x = <ActionIcon aria-label="a" />;`)).toEqual(["bordered-icon-button"]);
    expect(rules(`const x = <ActionIcon variant={on ? "filled" : "subtle"} />;`)).toEqual([
      "bordered-icon-button",
    ]);
    expect(
      rules(`const x = <ActionIcon variant={mode === "tree" ? "light" : "subtle"} />;`),
    ).toEqual([]);
    expect(rules(`const x = <ActionIcon variant="transparent" />;`)).toEqual([]);
  });

  it("leaves a panel head's actions to the head's own style", () => {
    expect(
      rules(`const x = <PanelActions><ActionIcon aria-label="edit" /></PanelActions>;`),
    ).toEqual([]);
    expect(
      rules(`const x = <PanelHeadStyle><ActionIcon aria-label="close" /></PanelHeadStyle>;`),
    ).toEqual([]);
  });

  it("a named control after an icon one, or anything after the help", () => {
    const bar = (inner: string) => `const x = <Group className="p3d-toolbar">${inner}</Group>;`;
    expect(rules(bar(`<ActionIcon variant="subtle" /><Button>Go</Button>`))).toEqual([
      "toolbar-order",
    ]);
    expect(rules(bar(`<HelpBubble title="t" /><ExpandButton onClick={f} testId="e" />`))).toEqual([
      "toolbar-order",
    ]);
    expect(
      rules(
        bar(
          `<SegmentedControl data={[]} /><Tip text="t"><Button>Reset</Button></Tip>` +
            `{q && <OpenAsQuery query={q} />}<ExpandButton onClick={f} testId="e" />` +
            `<HelpBubble title="t" target={<ActionIcon variant="subtle" />} /><HelpBubble title="?" />`,
        ),
      ),
    ).toEqual([]);
  });

  it("reads a group marked data-toolbar as a toolbar", () => {
    expect(
      rules(`const x = <Group data-toolbar><OntologyToggle open /><Button>Term</Button></Group>;`),
    ).toEqual(["toolbar-order"]);
  });

  it("reads a page's head actions as a toolbar too", () => {
    expect(
      rules(
        `const x = <div className="page-actions"><HelpBubble title="t" /><Button>Add</Button></div>;`,
      ),
    ).toEqual(["toolbar-order"]);
  });
});

describe("every toolbar", () => {
  it("puts its named controls first, its icon buttons next and its help last", () => {
    const wrong = of("toolbar-order").map(
      ({ file, hits }) => `${file}: ${hits.map((v) => `line ${v.line} ${v.detail}`).join("; ")}`,
    );
    expect(wrong).toEqual([]);
  });
});

describe("every icon-only button", () => {
  it("is borderless, but for the few drawn filled or bordered on purpose", () => {
    expect(
      Object.fromEntries(of("bordered-icon-button").map((f) => [f.file, f.hits.length])),
    ).toEqual(Object.fromEntries(Object.entries(DELIBERATE).map(([f, [count]]) => [f, count])));
  });
});
