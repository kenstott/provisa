// Copyright (c) 2026 Kenneth Stott
// Canary: 0e9a6c3f-7b18-4d52-a4e1-c8f2b5d9a736
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: every 2D model diagram (the ERD, the ontology graph in 2D, product lineage and the
// Lineage page) is drawn by the one graph component (components/provisa-graph/), built on
// cytoscape, and by nothing else. This guard reads every source file outside the component and
// fails on one that imports cytoscape or a cytoscape extension (cytoscape-elk, cytoscape-fcose,
// cytoscape-svg, ...): a view hands the component its nodes, edges, groups and a layout preset,
// and never reaches the library itself. Tests are not read: a layout's test may lay a graph out
// headless.
//
// (Amended 2026-11-27.) Two kinds of graph keep their own look and are exempt by rule, not listed:
// the network graph (the Cypher and SPARQL explorers, components/graph/, which mimic Neo4j
// Browser) and the 3D view (a scene drawn with three.js).

import { describe, expect, it } from "vitest";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative, resolve } from "node:path";

const SRC = resolve(__dirname, "../..");

/** The component: the only code that may import the library. */
const THE_COMPONENT = "components/provisa-graph/";

/** The network graph: the Cypher and SPARQL explorers, drawn after Neo4j Browser. */
const NETWORK_GRAPH = "components/graph/";

/** A 3D view draws its scene with three.js. */
const THREE_IMPORT = /\bfrom\s*["']three(?:\/[\w./-]+)?["']/;

/** Whether ``file`` (with source ``text``) is a graph of a kind that keeps its own look. */
export function ownLook(file: string, text: string): boolean {
  return file.startsWith(NETWORK_GRAPH) || THREE_IMPORT.test(text);
}

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

const textOf = (file: string) => readFileSync(join(SRC, file), "utf8");
const importsOf = (file: string) => libraryImports(textOf(file));

describe("the graph library is imported only by the one graph component", () => {
  const files = sourceFiles(SRC);
  const exempt = new Set(files.filter((f) => ownLook(f, textOf(f))));

  it("finds the sources and the component", () => {
    expect(files.length).toBeGreaterThan(200);
    expect(files.filter((f) => f.startsWith(THE_COMPONENT) && importsOf(f).length > 0)).not.toEqual(
      [],
    );
    // The rule finds both kinds: the network graph's canvas and the 3D ontology view's scene.
    expect(exempt).toContain("components/graph/GraphCanvas.tsx");
    expect(exempt).toContain("components/graph3d/graphScene.ts");
  });

  it.each(files.filter((f) => !f.startsWith(THE_COMPONENT) && !exempt.has(f)))(
    "%s imports no cytoscape",
    (file) => {
      expect(importsOf(file), `${file} imports the graph library outside ProvisaGraph`).toEqual([]);
    },
  );

  it("draws lineage with the component", () => {
    expect(exempt.has("components/lineage/LineageDag.tsx")).toBe(false);
    expect(importsOf("components/lineage/LineageDag.tsx")).toEqual([]);
  });

  it("exempts by kind only", () => {
    expect(ownLook("components/graph/GraphCanvas.tsx", "")).toBe(true);
    expect(ownLook("components/graph3d/graphScene.ts", `import * as THREE from "three";`)).toBe(true);
    expect(
      ownLook("pages/x/scene.ts", `import { OrbitControls } from "three/examples/jsm/controls/OrbitControls.js";`),
    ).toBe(true);
    expect(ownLook("components/lineage/LineageDag.tsx", `import cytoscape from "cytoscape";`)).toBe(false);
    expect(ownLook("components/graph3d/LaneStage.tsx", `// a three-lane diagram`)).toBe(false);
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
