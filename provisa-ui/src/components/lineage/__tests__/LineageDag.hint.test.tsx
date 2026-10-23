// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Pointing at a dataset box shows its hint, held open at the box, without throwing: the hint's
// Tooltip once mounted before its target existed, and Mantine's refusal blanked the whole app.

import { describe, expect, it, vi } from "vitest";
import { act } from "react";
import { render, screen } from "../../../test-utils/render";

const { cy, cytoscape } = vi.hoisted(() => {
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
vi.stubGlobal(
  "ResizeObserver",
  class {
    observe() {}
    disconnect() {}
  },
);

import { LineageDag } from "../LineageDag";

const GRAPH = {
  nodes: [{ id: "t.a", column: "a", relation: "t", kind: "source", materialized: false }],
  edges: [],
  outputs: [],
  cycles: [],
} as never;

describe("a dataset box's hint", () => {
  it("shows at the box pointed at, without throwing", async () => {
    render(<LineageDag graph={GRAPH} onToggleRelation={vi.fn()} />);
    const over = cy.on.mock.calls.find((c) => c[0] === "mouseover" && c[1] === 'node[box = "yes"]');
    if (!over) throw new Error("no box hint handler");
    const box = { data: (k: string) => ({ relation: "t", collapsed: "yes" })[k] };
    act(() => over[2]({ target: box, renderedPosition: { x: 40, y: 60 } }));
    expect(await screen.findByTestId("lineage-box-hint")).toHaveTextContent("Click to expand");
  });
});
