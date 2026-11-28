// Copyright (c) 2026 Kenneth Stott
// Canary: 435c2928-14cd-47ee-af43-3f2a7a71960e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Maintainer 2026-11-28: every (?) help and every page command links its documentation. Each docs
// target is written as a literal (toolbarGuard fails one that is not); this reads every one in the
// UI's source -- `docs="..."` on a PageCommand or a HelpBubble, `docs: "..."` in the page-help
// registry and elsewhere -- and fails one with no page under docs/, a page mkdocs.yml's nav does
// not publish, or a section the page does not have.

import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative, resolve } from "node:path";
import { docsTargetProblem, NAV_PAGES } from "./docsSite";

const SRC = resolve(__dirname, "../..");

function sourceFiles(dir: string): string[] {
  const out: string[] = [];
  for (const name of readdirSync(dir)) {
    const p = join(dir, name);
    if (statSync(p).isDirectory()) {
      if (name !== "__tests__" && name !== "node_modules") out.push(...sourceFiles(p));
    } else if (/\.tsx?$/.test(name) && !name.includes(".test.")) out.push(p);
  }
  return out;
}

/** Every literal docs target in the source, with the file that names it. */
const TARGETS: [string, string][] = sourceFiles(SRC).flatMap((file) =>
  [...readFileSync(file, "utf8").matchAll(/\bdocs(?:=|:\s*)"([^"]+)"/g)].map(
    (m): [string, string] => [m[1], relative(SRC, file)],
  ),
);

describe("docs targets", () => {
  it("reads the docs site and the targets", () => {
    expect(NAV_PAGES.has("configuration.md")).toBe(true);
    const files = new Set(TARGETS.map(([, file]) => file));
    for (const f of ["components/pageHelpRegistry.ts", "pages/TablesPage.tsx"])
      expect(files.has(f), f).toBe(true);
    expect(TARGETS.length).toBeGreaterThan(100);
  });

  it("fails a target that is not a published page and section", () => {
    expect(docsTargetProblem("configuration/#tables")).toBeNull();
    expect(docsTargetProblem("configuration")).toMatch(/ends in \//);
    expect(docsTargetProblem("no-such-page/")).toMatch(/no page under docs/);
    expect(docsTargetProblem("testing-plan/")).toMatch(/not in mkdocs.yml's nav/);
    expect(docsTargetProblem("configuration/#no-such-section")).toMatch(/no section/);
  });

  it.each(TARGETS)("%s (%s) is a published page and section", (target) => {
    expect(docsTargetProblem(target)).toBeNull();
  });
});
