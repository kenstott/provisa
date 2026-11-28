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
// zero height) laid out against a canvas of no size. Each time the container has an area again the
// graph is fitted to it; a container with none fits nothing. REQ-2239: the DAG is drawn by the one
// graph component, which follows its canvas's size (REQ-2277): the opening view is opened again.
//
// The real cytoscape, headless (jsdom has no 2D context), with elkjs's answer held until the test
// gives it, and the component's fit watched.

import { act } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render } from "../../../test-utils/render";

const elk = vi.hoisted(() => ({
  /** The graphs ELK was asked to lay out, each with its answer. */
  asked: [] as {
    graph: unknown;
    resolve: (g: unknown) => void;
    reject: (e: unknown) => void;
  }[],
  /** The cores made, in order. */
  cores: [] as unknown[],
}));

vi.mock("elkjs/lib/elk.bundled.js", () => ({
  default: class {
    layout(graph: unknown) {
      return new Promise((resolve, reject) => elk.asked.push({ graph, resolve, reject }));
    }
  },
}));

vi.mock("cytoscape", async (importOriginal) => {
  type Real = ((...a: unknown[]) => unknown) & { use: (e: unknown) => void };
  const real = (await importOriginal<{ default: Real }>()).default;
  const make = (...args: unknown[]) => {
    // cytoscape("layout", name): an extension's registration.
    if (typeof args[0] === "string") return real(...args);
    const o = args[0] as Record<string, unknown>;
    const cy = real({ ...o, container: undefined, headless: true, styleEnabled: true });
    elk.cores.push(cy);
    return cy;
  };
  return { default: Object.assign(make, { use: real.use.bind(real) }) };
});

// A resize the graph follows fits it again unless the reader holds the view (followResize: the
// whole diagram is fitted whole again, the opening view opened again); one it does not follow (a
// canvas of no area) fits nothing.
const fits = vi.hoisted(() => ({ count: 0 }));
vi.mock("../../provisa-graph/viewport", async (importOriginal) => {
  const real = await importOriginal<typeof import("../../provisa-graph/viewport")>();
  return {
    ...real,
    followResize: (...a: Parameters<typeof real.followResize>) => {
      const mode = real.followResize(...a);
      if (mode !== "kept") fits.count += 1;
      return mode;
    },
  };
});

import { LineageDag } from "../LineageDag";

interface ElkGraph {
  id: string;
  x?: number;
  y?: number;
  children?: ElkGraph[];
}

/** ELK has been asked: the layout has started. */
const started = () =>
  act(async () => {
    await vi.waitFor(() => expect(elk.asked).toHaveLength(1));
  });

/** ELK's answer: every node at a place of its own, 200 apart. */
const placed = (graph: ElkGraph): ElkGraph => ({
  ...graph,
  x: graph.x ?? 0,
  y: graph.y ?? 0,
  children: graph.children?.map((c, i) => ({ ...placed(c), x: i * 200, y: i * 200 })),
});
const answer = () => act(async () => elk.asked[0].resolve(placed(elk.asked[0].graph as ElkGraph)));

let observe: (() => void) | null = null;
let size = { w: 0, h: 0 };

beforeEach(() => {
  elk.asked = [];
  elk.cores = [];
  fits.count = 0;
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
afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

const GRAPH = {
  nodes: [
    { id: "raw.a.x", column: "x", relation: "raw.a", kind: "source", materialized: false },
    { id: "t.b.x", column: "x", relation: "t.b", kind: "source", materialized: false },
  ],
  edges: [
    { source: "raw.a.x", target: "t.b.x", transform: "x", ops: [{ name: "x", kind: "identity" }] },
  ],
  outputs: [],
  cycles: [],
} as never;

describe("LineageDag fit on visibility", () => {
  it("fits once the hidden container gains an area", async () => {
    render(<LineageDag graph={GRAPH} />);
    await started();
    await answer();
    expect(observe).not.toBeNull();
    // The layout fits on its own when it lands; what is under test is the visibility fit.
    const afterLayout = fits.count;
    act(() => observe!()); // observed while still collapsed: no area, nothing to fit
    expect(fits.count).toBe(afterLayout);
    size = { w: 800, h: 320 };
    act(() => observe!()); // the section opens
    expect(fits.count).toBe(afterLayout + 1);
  });

  it("fits again each time a hidden container comes back, as a tab does", async () => {
    render(<LineageDag graph={GRAPH} />);
    await started();
    await answer();
    size = { w: 800, h: 320 };
    act(() => observe!());
    const opened = fits.count;
    size = { w: 0, h: 0 };
    act(() => observe!()); // the tab is left: no area, nothing to fit
    expect(fits.count).toBe(opened);
    size = { w: 800, h: 320 };
    act(() => observe!()); // and opened again
    expect(fits.count).toBe(opened + 1);
  });
});
