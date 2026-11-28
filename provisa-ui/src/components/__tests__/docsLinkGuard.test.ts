// Copyright (c) 2026 Kenneth Stott
// Canary: 46480144-2098-47e0-9b80-f69db278fb70
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Maintainer 2026-11-28: every link to the documentation opens in a new browser tab. DocsLink
// (components/DocsLink.tsx) draws one; this guard reads every component and fails a JSX element
// whose href or to names the docs (/docs, /docs-site/, the hosted site, a docsHref call) without
// target="_blank" and rel="noopener noreferrer". The docs page's own navigation between docs pages
// (pages/DocsPage.tsx) runs inside its frame's site, which no JSX here draws, and stays in its tab.

import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { describe, expect, it } from "vitest";

const SRC = join(__dirname, "..", "..");

/** A brace expression, nested up to three deep (an arrow function's body, an object literal). */
const BRACES = String.raw`\{(?:[^{}]|\{(?:[^{}]|\{[^{}]*\})*\})*\}`;
const ATTR = String.raw`(?:[^\s=<>{}/]+(?:=(?:"[^"]*"|'[^']*'|${BRACES}))?|\{\.\.\.[^{}]*\})`;
/** A JSX opening (or self-closing) tag: its name and its attributes. */
const TAG = new RegExp(String.raw`<([A-Za-z][\w.]*)((?:\s+${ATTR})*)\s*\/?>`, "g");
const LINK_ATTR = new RegExp(String.raw`\s(?:href|to)=("[^"]*"|'[^']*'|${BRACES})`);

/** What names the documentation in an href or to value. */
const DOCS_VALUE = /\/docs\b|docsHref\(/;

/** DocsLink sets target and rel itself. */
const DRAWS_ITS_OWN = new Set(["DocsLink"]);

/** Each docs link in a source text drawn without a new tab, as "line: tag". */
function docsLinksInPlace(text: string): string[] {
  const out: string[] = [];
  for (const m of text.matchAll(TAG)) {
    const [whole, tag, attrs] = m;
    if (DRAWS_ITS_OWN.has(tag)) continue;
    const link = LINK_ATTR.exec(attrs);
    if (link === null || !DOCS_VALUE.test(link[1])) continue;
    if (/\starget="_blank"/.test(attrs) && /\srel="noopener noreferrer"/.test(attrs)) continue;
    const line = text.slice(0, m.index).split("\n").length;
    out.push(`${line}: ${whole.replace(/\s+/g, " ").slice(0, 120)}`);
  }
  return out;
}

function components(dir: string): string[] {
  const out: string[] = [];
  for (const name of readdirSync(dir)) {
    const p = join(dir, name);
    if (statSync(p).isDirectory()) {
      if (name !== "__tests__" && name !== "node_modules") out.push(...components(p));
    } else if (name.endsWith(".tsx") && !name.includes(".test.")) out.push(p);
  }
  return out;
}

/** Every docs link in the UI's components, with whether it opens a new tab. */
const FILES = components(SRC).map((file) => ({
  file: relative(SRC, file),
  text: readFileSync(file, "utf8"),
}));

describe("docs links open in a new tab", () => {
  it("fails a docs link drawn in place", () => {
    expect(
      docsLinksInPlace(`<Anchor component={Link} to={docsHref(target)} size="xs">`),
    ).toHaveLength(1);
    expect(docsLinksInPlace(`<a href="/docs?page=sources/">Docs</a>`)).toHaveLength(1);
    expect(
      docsLinksInPlace(`<Menu.Item component={NavLink} to="/docs" data-testid="x">`),
    ).toHaveLength(1);
    expect(docsLinksInPlace(`<a href="/docs-site/ontology/" target="_blank">`)).toHaveLength(1);
    expect(
      docsLinksInPlace(`<a href="https://provisa.dev/docs/" rel="noopener noreferrer">`),
    ).toHaveLength(1);
    expect(
      docsLinksInPlace(
        `const x = (\n  <Anchor\n    href={docsHref(t)}\n    onClick={() => { go({ a: 1 }); }}\n  >`,
      ),
    ).toEqual([expect.stringMatching(/^2: <Anchor/)]);
  });

  it("passes a docs link in a new tab, a DocsLink, and a link elsewhere", () => {
    expect(
      docsLinksInPlace(`<a href="/docs" target="_blank" rel="noopener noreferrer">Docs</a>`),
    ).toEqual([]);
    expect(docsLinksInPlace(`<DocsLink target="sources/" testId="t" />`)).toEqual([]);
    expect(docsLinksInPlace(`<DocsLink href="/docs-site/x/" testId="t">x</DocsLink>`)).toEqual([]);
    expect(docsLinksInPlace(`<NavLink to="/tables">Tables</NavLink>`)).toEqual([]);
  });

  it("reads the components that draw a docs link", () => {
    const reads = FILES.filter(({ text }) =>
      [...text.matchAll(TAG)].some(([, tag, attrs]) => {
        const link = LINK_ATTR.exec(attrs);
        return tag !== "DocsLink" && link !== null && DOCS_VALUE.test(link[1]);
      }),
    ).map(({ file }) => file);
    expect(reads).toContain("components/NavBar.tsx");
  });

  it.each(FILES.map(({ file }) => [file]))("%s draws every docs link in a new tab", (file) => {
    const { text } = FILES.find((f) => f.file === file)!;
    expect(docsLinksInPlace(text)).toEqual([]);
  });
});
