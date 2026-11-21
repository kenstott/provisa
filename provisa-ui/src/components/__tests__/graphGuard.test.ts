// Copyright (c) 2026 Kenneth Stott
// Canary: 0e9a6c3f-7b18-4d52-a4e1-c8f2b5d9a736
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: every 2D graph view is drawn by the one graph component (components/provisa-graph/),
// built on cytoscape, and by nothing else. This guard reads every source file outside the
// component and fails on one that imports cytoscape or a cytoscape extension (cytoscape-elk,
// cytoscape-fcose, cytoscape-svg, ...): a view hands the component its nodes, edges, groups and a
// layout preset, and never reaches the library itself. Tests are not read: a layout's test may
// lay a graph out headless.

import { describe, expect, it } from "vitest";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative, resolve } from "node:path";

const SRC = resolve(__dirname, "../..");

/** The component: the only code that may import the library. */
const THE_COMPONENT = "components/provisa-graph/";

/**
 * Shrinking: 2D graph views not yet drawn by the component. A file leaves this list in the commit
 * that moves it onto the component, and the guard fails a listed file that no longer imports the
 * library. Nothing is added here.
 */
const NOT_YET_CONVERTED: readonly string[] = [
  "components/graph/GraphCanvas.tsx",
  "components/graph/canvas/cytoscape-extensions.ts",
  "components/lineage/LineageDag.tsx",
];

/** An import of the library or one of its extensions: static, type-only, dynamic or required. */
const LIBRARY_IMPORT =
  /(?:\bfrom\s*|\bimport\s*\(\s*|\bimport\s+|\brequire\s*\(\s*)["'](cytoscape(?:-[\w-]+)?)["']/g;

/** The library modules ``text`` imports. */
export function libraryImports(text: string): string[] {
  return [...text.matchAll(LIBRARY_IMPORT)].map((m) => m[1]);
}

function sourceFiles(dir: string, found: string[] = []): string[] {
  for (const name of readdirSync(dir)) {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) {
      if (name !== "__tests__" && name !== "__mocks__" && name !== "test-utils") {
        sourceFiles(path, found);
      }
    } else if (/\.(ts|tsx)$/.test(name) && !name.includes(".test.")) {
      found.push(relative(SRC, path));
    }
  }
  return found;
}

const importsOf = (file: string) => libraryImports(readFileSync(join(SRC, file), "utf8"));

describe("the graph library is imported only by the one graph component", () => {
  const files = sourceFiles(SRC);
  const pending = new Set(NOT_YET_CONVERTED);

  it("finds the sources and the component", () => {
    expect(files.length).toBeGreaterThan(200);
    expect(files.filter((f) => f.startsWith(THE_COMPONENT) && importsOf(f).length > 0)).not.toEqual(
      [],
    );
    for (const f of NOT_YET_CONVERTED) expect(files, `${f} is listed but does not exist`).toContain(f);
  });

  it.each(files.filter((f) => !f.startsWith(THE_COMPONENT) && !pending.has(f)))(
    "%s imports no cytoscape",
    (file) => {
      expect(importsOf(file), `${file} imports the graph library outside ProvisaGraph`).toEqual([]);
    },
  );

  it.each([...NOT_YET_CONVERTED])("%s still imports it: once it does not, it leaves the list", (file) => {
    expect(importsOf(file).length).toBeGreaterThan(0);
  });

  it("is found by the rule", () => {
    expect(libraryImports(`import cytoscape from "cytoscape";`)).toEqual(["cytoscape"]);
    expect(libraryImports(`import elk from 'cytoscape-elk';`)).toEqual(["cytoscape-elk"]);
    expect(libraryImports(`import type { Core } from "cytoscape";`)).toEqual(["cytoscape"]);
    expect(libraryImports(`const m = await import("cytoscape-fcose");`)).toEqual([
      "cytoscape-fcose",
    ]);
    expect(libraryImports(`import "cytoscape-svg";`)).toEqual(["cytoscape-svg"]);
    expect(libraryImports(`const c = require("cytoscape");`)).toEqual(["cytoscape"]);
    expect(libraryImports(`import { cytoscape } from "../provisa-graph/core";`)).toEqual([]);
    expect(libraryImports(`// the cytoscape canvas`)).toEqual([]);
  });
});
