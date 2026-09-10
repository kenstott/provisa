// Copyright (c) 2026 Kenneth Stott
// Canary: 9c4b7e21-5a3d-4f80-b6e9-2d1c8a7f3e54
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A DAG mounted inside a closed section (the product page's lineage Collapse keeps it mounted at
// zero height) laid out against a canvas of no size. The first time the container has an area the
// graph is fitted to it; later resizes only resize, so a chosen zoom survives a window drag.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render } from "../../../test-utils/render";

// Enough of cytoscape's surface for the DAG to mount: the box layout is a no-op here.
const { cy, cytoscape } = vi.hoisted(() => {
  // An empty collection: nothing is connected, so the DAG takes the no-layout path.
  const collection: Record<string, unknown> = {};
  Object.assign(collection, {
    on: vi.fn(),
    union: () => collection,
    filter: () => collection,
    descendants: () => collection,
    forEach: vi.fn(),
    nonempty: () => false,
    reduce: () => 0,
    boundingBox: () => ({ x1: 0, y1: 0, x2: 0, y2: 0, w: 0, h: 0 }),
    shift: vi.fn(),
  });
  const cy = {
    on: vi.fn(),
    resize: vi.fn(),
    fit: vi.fn(),
    destroy: vi.fn(),
    png: vi.fn(),
    batch: (fn: () => void) => fn(),
    nodes: () => collection,
    edges: () => collection,
    getElementById: () => ({ position: vi.fn() }),
    layout: () => ({ run: vi.fn(), on: vi.fn(), one: vi.fn(), stop: vi.fn() }),
  };
  return { cy, cytoscape: Object.assign(() => cy, { use: vi.fn() }) };
});
vi.mock("cytoscape", () => ({ default: cytoscape }));
vi.mock("cytoscape-elk", () => ({ default: vi.fn() }));

import { LineageDag } from "../LineageDag";

let observe: (() => void) | null = null;
let size = { w: 0, h: 0 };

beforeEach(() => {
  vi.clearAllMocks();
  observe = null;
  size = { w: 0, h: 0 };
  // The container's box is whatever the test says it is; jsdom lays nothing out.
  Object.defineProperty(HTMLElement.prototype, "clientWidth", {
    configurable: true,
    get: () => size.w,
  });
  Object.defineProperty(HTMLElement.prototype, "clientHeight", {
    configurable: true,
    get: () => size.h,
  });
  vi.stubGlobal(
    "ResizeObserver",
    class {
      constructor(cb: () => void) {
        observe = cb;
      }
      observe() {}
      disconnect() {}
    },
  );
});
afterEach(() => vi.unstubAllGlobals());

const GRAPH = {
  nodes: [{ id: "t.a", column: "a", relation: "t", kind: "source", materialized: false }],
  edges: [],
  outputs: [],
  cycles: [],
} as never;

describe("LineageDag fit on first visibility", () => {
  it("fits once the hidden container gains an area, and only resizes afterwards", () => {
    render(<LineageDag graph={GRAPH} />);
    expect(observe).not.toBeNull();

    // The layout fits once on its own when it lands; what is under test is the visibility fit.
    const afterLayout = cy.fit.mock.calls.length;
    observe!(); // observed while still collapsed: no area, nothing to fit
    expect(cy.fit).toHaveBeenCalledTimes(afterLayout);

    size = { w: 800, h: 320 };
    observe!(); // the section opens
    expect(cy.fit).toHaveBeenCalledTimes(afterLayout + 1);

    size = { w: 1200, h: 320 };
    observe!(); // a window drag: resize only
    expect(cy.resize).toHaveBeenCalledTimes(3);
    expect(cy.fit).toHaveBeenCalledTimes(afterLayout + 1);
  });
});
