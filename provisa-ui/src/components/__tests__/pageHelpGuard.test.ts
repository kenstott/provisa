// Copyright (c) 2026 Kenneth Stott
// Canary: 2fa828e3-fec0-425b-8bb7-1c1590d4f469
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277 rule 8: every page has the page help (?) in its header, its content in one registry keyed
// by route. This guard reads the app's route table (App.tsx) and fails:
// - a page route with no registry entry, and an entry for no route;
// - an entry whose purpose or paragraph has no English string;
// - a docs link to no page of docs/ that mkdocs.yml publishes, or to no section of that page;
// - an entry whose placement no header of the page draws: "title" needs a PageTitle in the page,
//   "subnav" a page of a navigation group with a subnav and no PageTitle, "own" a RoutePageHelp in
//   the page's own header.

import { describe, it, expect } from "vitest";
import { existsSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import i18n from "../../i18n";
import { PAGE_HELP } from "../pageHelpRegistry";
import { activeGroupId } from "../navGroups";
import { docsTargetProblem } from "./docsSite";

const SRC = resolve(__dirname, "../..");
const APP = readFileSync(resolve(SRC, "App.tsx"), "utf8");

/** Elements a route's element wraps its page in, never the page itself. */
const WRAPPERS = /Gate$|^Suspense$|^NotAuthorized$|^PageLoading$/;
/** A route whose element only sends the reader on: it has no page of its own. */
const REDIRECTS = new Set(["Navigate", "HomeEntry", "AdminEntry"]);
/** Routes drawn outside the shell's pages: the demo opener, and the catch-all for signed-out use. */
const OUTSIDE_SHELL = new Set(['"*"', "{DEMO_ROUTE}"]);
/** Routes whose element is empty because their page is mounted once beside the routes, so it
 *  keeps its state (the Python worker and the notebook) while the reader is elsewhere. */
const MOUNTED_BESIDE: Record<string, string> = {
  "/python": "DataScienceHost",
  "/notebook": "DataScienceHost",
};

interface PageRoute {
  path: string;
  component: string;
}

/** The first page component drawn in `seg`, past the gates and wrappers. */
function firstPage(seg: string): string {
  return [...seg.matchAll(/<([A-Z][A-Za-z]*)/g)]
    .map((m) => m[1])
    .filter((t) => t !== "Route" && !WRAPPERS.test(t))[0];
}

/** Every page route of App.tsx with the component that draws it. */
function pageRoutes(): PageRoute[] {
  const out: PageRoute[] = [];
  // Routes written one by one.
  const starts = [...APP.matchAll(/path=/g)].map((m) => m.index);
  starts.forEach((start, i) => {
    const seg = APP.slice(start, starts[i + 1] ?? start + 3000);
    const raw = /^path=(\{[^}]*\}|"[^"]*")/.exec(seg)![1];
    if (OUTSIDE_SHELL.has(raw) || raw === "{path}") return;
    const page = firstPage(seg);
    if (!REDIRECTS.has(page)) out.push({ path: raw.slice(1, -1), component: page });
  });
  // The admin rights table: [path, right] pairs mapped onto one element.
  const table = /\{\(\s*\[([\s\S]*?)\]\s*as const\s*\)\.map\(\(\[path, capability\]\)/.exec(APP)!;
  const tablePage = firstPage(APP.slice(table.index + table[0].length));
  for (const m of table[1].matchAll(/\[\s*"(\/[^"]+)"\s*,/g)) {
    out.push({ path: m[1], component: tablePage });
  }
  // Paths mapped onto an empty element, their page mounted beside the routes.
  const list = /\{\[((?:\s*"\/[^"]+",?)+)\s*\]\.map\(\(path\)/.exec(APP)!;
  for (const m of list[1].matchAll(/"(\/[^"]+)"/g)) {
    out.push({ path: m[1], component: MOUNTED_BESIDE[m[1]] });
  }
  return out;
}

/** The source file a component of App.tsx is lazily imported from. */
function appComponentFile(component: string): string {
  const m = new RegExp(`const ${component} = lazyPage\\(\\(\\) =>\\s*import\\("([^"]+)"\\)`).exec(
    APP,
  );
  if (m === null) throw new Error(`App.tsx does not lazy-load ${component}`);
  return resolve(SRC, `${m[1]}.tsx`);
}

/** A file's source, and the files of the components it draws that it imports locally. */
function drawnFiles(file: string): string[] {
  const src = readFileSync(file, "utf8");
  const children: string[] = [];
  for (const m of src.matchAll(/import \{([^}]+)\} from "(\.[^"]+)"/g)) {
    const names = m[1].split(",").map((n) => n.trim().replace(/^type /, ""));
    if (!names.some((n) => new RegExp(`<${n}[\\s/>]`).test(src))) continue;
    const child = resolve(dirname(file), `${m[2]}.tsx`);
    if (existsSync(child)) children.push(child);
  }
  return [file, ...children];
}

const ROUTES = pageRoutes();

describe("page help registry (REQ-2277 rule 8)", () => {
  it("reads the route table", () => {
    expect(ROUTES.length).toBeGreaterThan(60);
    expect(ROUTES.map((r) => r.path)).toEqual(
      expect.arrayContaining(["/sources", "/admin/tags", "/admin/overview", "/python"]),
    );
  });

  it.each(ROUTES.map((r) => [r.path]))("%s has help content", (path) => {
    expect(PAGE_HELP[path], `${path} has no entry in pageHelpRegistry.ts`).toBeDefined();
  });

  it("holds no entry for a route the app does not have", () => {
    const paths = new Set(ROUTES.map((r) => r.path));
    expect(Object.keys(PAGE_HELP).filter((p) => !paths.has(p))).toEqual([]);
  });

  it.each(Object.entries(PAGE_HELP))("%s has its strings in English", (_path, entry) => {
    for (const key of [entry.purpose, ...(entry.more ?? [])]) {
      expect(i18n.exists(key, { lng: "en" }), key).toBe(true);
    }
  });

  it.each(Object.entries(PAGE_HELP))("%s links a docs page and section", (_path, entry) => {
    expect(docsTargetProblem(entry.docs)).toBeNull();
  });

  it.each(ROUTES.map((r) => [r.path, r.component]))(
    "%s draws its help where the registry places it",
    (path, component) => {
      const entry = PAGE_HELP[path];
      expect(entry).toBeDefined();
      const files = drawnFiles(appComponentFile(component));
      const has = (re: RegExp) => files.some((f) => re.test(readFileSync(f, "utf8")));
      const titled = has(/<PageTitle[\s>]/);
      if (entry.placement === "title") expect(titled, `${path}: no PageTitle`).toBe(true);
      if (entry.placement === "subnav") {
        expect(titled, `${path} has a PageTitle: place its help there`).toBe(false);
        const group = activeGroupId(path);
        expect(group, `${path} is in no navigation group with a subnav`).not.toBeNull();
        expect(group).not.toBe("admin");
      }
      if (entry.placement === "own") {
        expect(titled, `${path} has a PageTitle: place its help there`).toBe(false);
        expect(has(/<RoutePageHelp placement="own"/), `${path}: no RoutePageHelp`).toBe(true);
      }
    },
  );
});
