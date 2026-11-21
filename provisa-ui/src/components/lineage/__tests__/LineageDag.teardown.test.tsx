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
// for, and a core destroyed by then throws there -- "Cannot read properties of null (reading
// 'notify') at Core.endBatch". The core below keeps that rule.

import { describe, expect, it, vi } from "vitest";
import { render } from "../../../test-utils/render";

const { cytoscape, pending } = vi.hoisted(() => {
  const pending: (() => void)[] = [];
  // Every box connected: with lanes each one goes through ELK.
  const collection: Record<string, unknown> = {};
  Object.assign(collection, {
    union: () => collection,
    filter: () => collection,
    descendants: () => collection,
    forEach: vi.fn(),
    nonempty: () => true,
  });
  const cytoscape = (options: unknown) => {
    if (typeof options === "string") return true;
    let destroyed = false;
    return {
      on: vi.fn(),
      resize: vi.fn(),
      fit: vi.fn(),
      batch: (fn: () => void) => fn(),
      nodes: () => collection,
      edges: () => collection,
      collection: () => collection,
      layout: () => {
        let stop: (() => void) | null = null;
        return {
          one: (_e: string, fn: () => void) => {
            stop = fn;
          },
          run: () =>
            pending.push(() => {
              if (destroyed)
                throw new TypeError("Cannot read properties of null (reading 'notify')");
              stop?.();
            }),
        };
      },
      unmount: vi.fn(),
      destroy: () => {
        destroyed = true;
      },
    };
  };
  return { cytoscape: Object.assign(cytoscape, { use: vi.fn() }), pending };
});
vi.mock("cytoscape", () => ({ default: cytoscape }));

import { LineageDag } from "../LineageDag";

vi.stubGlobal(
  "ResizeObserver",
  class {
    observe() {}
    disconnect() {}
  },
);

const GRAPH = {
  nodes: [
    { id: "raw.a.x", column: "x", relation: "raw.a", kind: "source", materialized: false },
    { id: "t.b.x", column: "x", relation: "t.b", kind: "source", materialized: false },
  ],
  edges: [{ source: "raw.a.x", target: "t.b.x", kind: "copy" }],
  outputs: [],
  cycles: [],
} as never;

describe("LineageDag torn down mid-layout", () => {
  it("ends without ELK's answer throwing", () => {
    const { unmount } = render(
      <LineageDag
        graph={GRAPH}
        lanes={{ members: new Set(["t.b"]), inputs: new Set(["raw.a"]), title: (l) => String(l) }}
      />,
    );
    expect(pending).toHaveLength(1);
    unmount();
    expect(() => pending.splice(0).forEach((answer) => answer())).not.toThrow();
  });
});
