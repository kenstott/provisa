// Copyright (c) 2026 Kenneth Stott
// Canary: 384a8722-c892-45b4-80da-61eee3aaf920
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

// REQ-1587: a pinned header is a property of which element owns the scroll, and jsdom computes no
// layout, so the browser spec (provisa-ui/e2e/relationships-header.spec.ts) measures the pinning
// itself. What is checkable here is the arrangement the pinning depends on — the rules a later edit
// would quietly undo: the page owning the overflow, the rows' container being the only scrollport,
// and that scrollport carrying no padding. Each of those, alone, moves the scrollport and lets the
// header ride away with the rows.
const src = (rel: string) => readFileSync(resolve(process.cwd(), "src", rel), "utf8");

const css = src("App.css");

// Return the declaration block of the first rule whose selector list matches exactly.
function ruleBody(selector: string): string {
  const idx = css.indexOf(`\n${selector} {`);
  expect(idx, `no rule for "${selector}" in App.css`).toBeGreaterThan(-1);
  const start = css.indexOf("{", idx);
  // Comments are stripped: the rules here are commented between declarations, and the prose names
  // the same properties it explains.
  return css.slice(start + 1, css.indexOf("}", start)).replace(/\/\*[\s\S]*?\*\//g, "");
}

// A declaration may be followed by a value or by a comment, so match the property, not the line.
const decl = (body: string, prop: string): string | null => {
  const m = new RegExp(`(?:^|;)\\s*${prop}\\s*:\\s*([^;}]+)`).exec(body);
  return m ? m[1].trim() : null;
};

describe("REQ-1587 pinned list headers", () => {
  it("the page is the flex column that hides its own overflow", () => {
    const body = ruleBody(".page-sticky-head");
    expect(decl(body, "display")).toBe("flex");
    expect(decl(body, "flex-direction")).toBe("column");
    expect(decl(body, "overflow")).toBe("hidden");
    // Without this a flex item refuses to shrink below its content, so the page grows instead of
    // handing the leftover height to the scrollport.
    expect(decl(body, "min-height")).toBe("0");
  });

  it("everything above the table holds its place", () => {
    expect(decl(ruleBody(".page-sticky-head > *"), "flex")).toBe("0 0 auto");
  });

  it("the rows' container is the scrollport and carries no padding", () => {
    const body = ruleBody(".page-sticky-head > .table-scroll");
    expect(decl(body, "overflow")).toBe("auto");
    expect(decl(body, "min-height")).toBe("0");
    // A sticky offset is measured from the padding edge: padding here parks the header below the
    // visible top and lets a row show above it.
    expect(decl(body, "padding")).toBeNull();
    expect(decl(body, "padding-top")).toBeNull();
  });

  it("the table does not take the scrollport back", () => {
    // .data-table sets overflow: hidden for its border radius; a scrolling ancestor nearer than the
    // page would be the one the header sticks to.
    expect(decl(ruleBody(".page-sticky-head .data-table"), "overflow")).toBe("visible");
  });

  it("the header sticks to the top and paints its own background", () => {
    const body = ruleBody(".page-sticky-head .data-table th");
    expect(decl(body, "position")).toBe("sticky");
    expect(decl(body, "top")).toBe("0");
    // border-collapse drops a sticky cell's border, so the divider is an inset shadow; without an
    // opaque background the rows show through as they pass underneath.
    expect(decl(body, "background")).toBeTruthy();
    expect(decl(body, "box-shadow")).toContain("inset");
  });

  // A reader meets the same header on every list page built this way.
  it.each([["pages/RelationshipsPage.tsx"], ["pages/TablesPage.tsx"], ["pages/SourcesPage.tsx"]])(
    "%s opts its page root in and wraps its rows in the scrollport",
    (page) => {
      const tsx = src(page);
      expect(tsx).toContain('className="page page-sticky-head"');
      expect(tsx).toContain('className="table-scroll"');
    },
  );
});
