// Copyright (c) 2026 Kenneth Stott
// Canary: 161bff7c-5f96-41f3-bca3-3a76f08d1741
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The published docs site as the UI's docs targets (components/DocsLink.tsx) read it: the pages
// under docs/, the ones mkdocs.yml publishes (its nav), and each page's section ids.

import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";

export const DOCS = resolve(__dirname, "../../../../docs");
const MKDOCS = readFileSync(resolve(DOCS, "../mkdocs.yml"), "utf8");

/** The section ids of a docs page as mkdocs' toc writes them: explicit `{: #id }`, else the
 *  heading's text slugified (python-markdown's default slugify). */
export function sectionIds(md: string): Set<string> {
  const ids = new Set<string>();
  for (const m of md.matchAll(/^#{1,6}\s+(.+?)\s*$/gm)) {
    const explicit = /\{:\s*#([\w-]+)\s*\}$/.exec(m[1]);
    if (explicit !== null) {
      ids.add(explicit[1]);
      continue;
    }
    const text = m[1]
      .replace(/`/g, "")
      .normalize("NFKD")
      .replace(/[^ -~]/g, "");
    ids.add(
      text
        .replace(/[^\w\s-]/g, "")
        .trim()
        .toLowerCase()
        .replace(/[-\s]+/g, "-"),
    );
  }
  return ids;
}

/** The markdown files mkdocs.yml's nav lists, relative to docs/. */
export const NAV_PAGES: ReadonlySet<string> = (() => {
  const nav = /^nav:\n([\s\S]*?)^\S/m.exec(MKDOCS);
  if (nav === null) throw new Error("mkdocs.yml has no nav");
  return new Set([...nav[1].matchAll(/:\s*"?([\w./-]+\.md)"?\s*$/gm)].map((m) => m[1]));
})();

/** The docs/ file a page path (ending in /) is built from: page.md or page/index.md. */
export function pageFile(page: string): string | undefined {
  const stem = page.slice(0, -1);
  return [`${stem}.md`, `${stem}/index.md`].find((f) => existsSync(resolve(DOCS, f)));
}

/** What is wrong with a docs target, or null when it is a published page and section. */
export function docsTargetProblem(target: string): string | null {
  const [page, section] = target.split("#");
  if (!page.endsWith("/")) return `${target}: a page path ends in /`;
  const file = pageFile(page);
  if (file === undefined) return `${target}: no page under docs/`;
  if (!NAV_PAGES.has(file)) return `${target}: ${file} is not in mkdocs.yml's nav`;
  if (section !== undefined && !sectionIds(readFileSync(resolve(DOCS, file), "utf8")).has(section))
    return `${target}: ${file} has no section #${section}`;
  return null;
}
