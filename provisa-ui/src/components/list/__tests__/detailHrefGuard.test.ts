// Copyright (c) 2026 Kenneth Stott
// Canary: f0f5b144-c4b6-48e5-930b-fcfa61912f05
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2240, REQ-2241: one place builds a link into a detail panel -- detailHref, from DETAIL_VIEWS.
// A source file that writes a detail page's route followed by its own query string is a second
// place deciding a panel's URL, so it fails here.

import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { describe, expect, it } from "vitest";
import { DETAIL_VIEWS } from "../useDetailParam";

const SRC = join(__dirname, "..", "..", "..");
const BUILDER = join("components", "list", "useDetailParam.ts");

function sources(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) return name === "__tests__" ? [] : sources(path);
    return /\.tsx?$/.test(name) && !/\.test\.tsx?$/.test(name) ? [path] : [];
  });
}

/** The text of a file with its comments taken out: a comment may show a link's shape. */
function code(path: string): string {
  return readFileSync(path, "utf8")
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .replace(/(^|[^:])\/\/.*$/gm, "$1");
}

const escape = (s: string) => s.replace(/[.*+?^${}()|[\]\\/]/g, "\\$&");
// A detail page's route opening a string, with one of that page's panel parameters written in the
// query string after it. A page's other parameters (a list's filter or search) are not a panel's.
const PANEL_PARAMS = new Map<string, Set<string>>();
for (const view of Object.values(DETAIL_VIEWS) as {
  route: string;
  param: string;
  tab?: { param: string };
  alsoClears?: readonly string[];
}[]) {
  const params = PANEL_PARAMS.get(view.route) ?? new Set<string>();
  params.add(view.param);
  if (view.tab) params.add(view.tab.param);
  for (const p of view.alsoClears ?? []) params.add(p);
  PANEL_PARAMS.set(view.route, params);
}
const HAND_BUILT = new RegExp(
  [...PANEL_PARAMS]
    .map(
      ([route, params]) =>
        `["'\`]${escape(route)}\\?([^"'\`\\n]*?[&?])?(${[...params].join("|")})=`,
    )
    .join("|"),
  "g",
);

describe("no file outside useDetailParam.ts builds a detail URL by hand", () => {
  it("finds a hand-built one", () => {
    expect("navigate(`/tables?table=${id}`)".match(HAND_BUILT)).toEqual(["`/tables?table="]);
    expect('to="/commands?command=x"'.match(HAND_BUILT)).toEqual(['"/commands?command=']);
    expect("`/tables?view=erd&section=quality`".match(HAND_BUILT)).toEqual([
      "`/tables?view=erd&section=",
    ]);
    // A list's own filter is not a panel's link.
    expect("navigate(`/tables?source=${id}`)".match(HAND_BUILT)).toBeNull();
  });

  it("has none", () => {
    const found = sources(SRC)
      .filter((path) => relative(SRC, path) !== BUILDER)
      .flatMap((path) =>
        (code(path).match(HAND_BUILT) ?? []).map((m) => `${relative(SRC, path)}: ${m}`),
      );
    expect(found).toEqual([]);
  });
});
