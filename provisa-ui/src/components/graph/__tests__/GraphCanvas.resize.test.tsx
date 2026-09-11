// Copyright (c) 2026 Kenneth Stott
// Canary: 22770263-59f9-4f9c-9279-91860de6dd0e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Graph Explorer's .gf-canvas is width/height:100% (responsive CSS), but cytoscape reads its
// canvas pixel size once at creation and never again on its own. Without a resize observer calling
// cy.resize(), the rendered graph keeps whatever size it was born with as the browser window (or
// the panel around it) resizes — the bug reported against the Cypher/Graph explorer page.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render } from "../../../test-utils/render";

const { cy, cytoscape } = vi.hoisted(() => {
  const collection: Record<string, unknown> = {};
  Object.assign(collection, {
    on: vi.fn(),
    forEach: vi.fn(),
    length: 0,
  });
  const cy = {
    on: vi.fn(),
    resize: vi.fn(),
    fit: vi.fn(),
    destroy: vi.fn(),
    batch: (fn: () => void) => fn(),
    nodes: () => collection,
    edges: () => collection,
    elements: () => collection,
    style: vi.fn(),
    container: () => document.createElement("div"),
    getElementById: () => ({ position: vi.fn() }),
    layout: () => ({ run: vi.fn(), on: vi.fn(), one: vi.fn(), stop: vi.fn() }),
    userZoomingEnabled: vi.fn(),
  };
  return { cy, cytoscape: Object.assign(() => cy, { use: vi.fn() }) };
});
vi.mock("cytoscape", () => ({ default: cytoscape }));

import { GraphCanvas } from "../GraphCanvas";
import type { CanvasProps } from "../canvas/canvas-types";

let observe: (() => void) | null = null;

beforeEach(() => {
  vi.clearAllMocks();
  observe = null;
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

const BASE_PROPS: CanvasProps = {
  nodes: new Map(),
  edges: new Map(),
  overlayNodes: new Map(),
  overlayEdges: new Map(),
  onSelect: vi.fn(),
  colorOverrides: {},
  sizeOverrides: {},
  labelProperty: {},
  sizeByProperty: {},
  sizeMultiplier: {},
  relLineOverrides: {},
  onExcludeNode: vi.fn(),
  pkMap: {},
  labelToTableLabel: {},
  relationships: [],
  showingChildrenNatural: new Set(),
  onToggleChildren: vi.fn(),
  onToggleChildrenBatch: vi.fn(),
  showingChildrenCircular: new Set(),
  onToggleChildrenCircular: vi.fn(),
  showingParents: new Set(),
  onToggleParents: vi.fn(),
  onToggleParentsBatch: vi.fn(),
  showingParentsCircular: new Set(),
  onToggleParentsCircular: vi.fn(),
  clusterLevel: "none",
};

describe("GraphCanvas tracks its container's size", () => {
  it("observes the canvas container and calls cy.resize() on every change", () => {
    render(<GraphCanvas {...BASE_PROPS} />);
    expect(observe).not.toBeNull();

    observe!();
    observe!();
    expect(cy.resize).toHaveBeenCalledTimes(2);
  });

  it("stops observing on unmount", () => {
    const { unmount } = render(<GraphCanvas {...BASE_PROPS} />);
    unmount();
    observe!();
    // The container (and its cytoscape instance) is gone; a stale callback must not throw.
    expect(cy.resize).not.toHaveBeenCalled();
  });
});
