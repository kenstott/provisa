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
    // An Overview pill's highlight is applied on mount (canvas/highlight.ts).
    removeClass: () => collection,
    // A caption-field or size change restyles every node (graph-stylesheet restyleCaptions).
    updateStyle: () => collection,
    addClass: () => collection,
    filter: () => collection,
    map: () => [],
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
  return {
    cy,
    cytoscape: Object.assign(
      vi.fn((_options: unknown) => cy),
      { use: vi.fn() },
    ),
  };
});
vi.mock("cytoscape", () => ({ default: cytoscape }));

const { followResize, keepView } = vi.hoisted(() => ({
  followResize: vi.fn(),
  keepView: vi.fn(),
}));
vi.mock("../../provisa-graph/viewport", async (actual) => ({
  ...(await actual<typeof import("../../provisa-graph/viewport")>()),
  followResize,
  keepView,
}));

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
  exportName: "cypher-graph",
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

describe("the mouse wheel over a graph zooms it, never scrolls", () => {
  it("lets the user zoom, whatever the frame around it", () => {
    render(<GraphCanvas {...BASE_PROPS} />);
    expect(cytoscape.mock.calls.at(-1)?.[0]).toMatchObject({ userZoomingEnabled: true });
    expect(cy.userZoomingEnabled).not.toHaveBeenCalledWith(false);
  });

  it("keeps the wheel from scrolling the page or the results under the canvas", () => {
    const { container } = render(<GraphCanvas {...BASE_PROPS} />);
    const canvas = container.querySelector(".gf-canvas") as HTMLElement;
    const wheel = new WheelEvent("wheel", { deltaY: 120, bubbles: true, cancelable: true });
    canvas.dispatchEvent(wheel);
    expect(wheel.defaultPrevented).toBe(true);
  });
  // REQ-2239 (amended 2026-11-27): every graph shows the pan and zoom hint; here the wheel zooms.
  it("says so in the pan and zoom hint, in the shared graph's corner", () => {
    const { getByTestId } = render(<GraphCanvas {...BASE_PROPS} />);
    const hint = getByTestId("graph-canvas-hint");
    expect(hint).toHaveTextContent("Drag to pan · scroll to zoom");
    expect(hint).toHaveClass("pg-hint");
    expect(hint.parentElement).toHaveClass("pg-corner");
  });
});

// REQ-2277, on every graph surface (the lead's decision): the Cypher result graph keeps its view's
// mode across a resize as the ERD does. It was re-sized only: a graph fitted whole was cut off by a
// narrower canvas, and widened it floated in the corner.
describe("a resize keeps the result graph's view mode (REQ-2277)", () => {
  let width = 1200;
  const sizes: { mockRestore: () => void }[] = [];
  beforeEach(() => {
    width = 1200;
    sizes.push(
      vi.spyOn(HTMLElement.prototype, "clientWidth", "get").mockImplementation(() => width),
      vi.spyOn(HTMLElement.prototype, "clientHeight", "get").mockImplementation(() => 600),
    );
  });
  afterEach(() => sizes.splice(0).forEach((s) => s.mockRestore()));

  /** The handler ``cy.on`` was given for ``events``. */
  const handler = (events: string) =>
    cy.on.mock.calls.find((c: unknown[]) => c[0] === events)?.at(-1) as () => void;

  it("follows a resize from the size the canvas was, once the graph is laid out", () => {
    render(<GraphCanvas {...BASE_PROPS} />);
    // A layout running fits the graph when it ends: a resize before then only re-sizes.
    width = 900;
    observe!();
    expect(cy.resize).toHaveBeenCalledTimes(1);
    expect(followResize).not.toHaveBeenCalled();
    handler("layoutstop")();
    width = 700;
    observe!();
    expect(followResize).toHaveBeenCalledWith(cy, { width: 900, height: 600 });
  });

  it("a hidden canvas is re-sized, not followed, and the next size follows the last shown", () => {
    render(<GraphCanvas {...BASE_PROPS} />);
    handler("layoutstop")();
    width = 0;
    observe!();
    expect(followResize).not.toHaveBeenCalled();
    width = 800;
    observe!();
    expect(followResize).toHaveBeenCalledWith(cy, { width: 1200, height: 600 });
  });

  it("a wheel zoom, a pinch or a drag of the canvas makes the view the reader's own", () => {
    render(<GraphCanvas {...BASE_PROPS} />);
    handler("scrollzoom pinchzoom dragpan")();
    expect(keepView).toHaveBeenCalledWith(cy);
  });
});
