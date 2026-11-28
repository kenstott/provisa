// Copyright (c) 2026 Kenneth Stott
// Canary: a56a4baa-3e99-49a4-a894-7e896fecba67
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A DAG torn down while ELK still lays it out (the product page redrew it as the Lineage panel
// opened) ends without an error: ELK answers later and places the nodes of the core it was asked
// for, and a core destroyed by then threw there -- "Cannot read properties of null (reading
// 'notify') at Core.endBatch". REQ-2239: the DAG is drawn by the one graph component, whose core
// draws into a canvas of its own that outlives the DAG while a layout is pending (cyCanvas).
//
// The real cytoscape, headless (jsdom has no 2D context), with elkjs's answer held until the test
// gives it.

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

beforeEach(() => {
  elk.asked = [];
  elk.cores = [];
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe() {}
      disconnect() {}
    },
  );
});
afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("LineageDag torn down mid-layout", () => {
  it("ends without ELK's answer throwing", async () => {
    const errors = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const { unmount } = render(<LineageDag graph={GRAPH} />);
    await started();
    unmount();
    await expect(answer()).resolves.toBeUndefined();
    expect(errors).not.toHaveBeenCalled();
  });
});
