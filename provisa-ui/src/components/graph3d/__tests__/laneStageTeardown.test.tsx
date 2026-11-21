// Copyright (c) 2026 Kenneth Stott
// Canary: 62e3352a-b67d-4656-a6a0-db4080678fec
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174: a lane diagram redrawn while ELK still lays out the one before it stays drawn. ELK
// answers asynchronously; the core it answers for, torn down by then, is destroyed when it does
// -- and a destroyed core empties its container (cytoscape's destroyRenderer). On the product page
// the Input Ports link opening the Lineage panel redrew the diagram mid-layout, and the old core's
// end emptied the stage the new one was drawing in: the graph went blank. jsdom draws no canvas:
// cytoscape is replaced by a core that keeps those two rules -- its renderer puts a canvas in its
// container and its destroy empties it; a layout answering a destroyed core throws, as
// Core.endBatch does on a null renderer.

import { act } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
import { laneDiagram } from "../laneDiagram";
import { DATA } from "./fixture";

const cys = vi.hoisted(() => ({
  pending: [] as (() => void)[],
}));
vi.mock("cytoscape", () => {
  const cytoscape = (options: unknown) => {
    // cytoscape("layout", name): an extension is registered.
    if (typeof options === "string") return true;
    const container = (options as { container: HTMLElement }).container;
    container.appendChild(document.createElement("canvas"));
    let destroyed = false;
    const empty = () => {
      while (container.firstChild) container.removeChild(container.firstChild);
    };
    const none = {
      empty: () => true,
      nonempty: () => false,
      forEach: () => undefined,
      filter: () => none,
    };
    return {
      // The element it draws in, as cytoscape's own container() gives it (a fit reads it).
      container: () => container,
      on: vi.fn(),
      layout: () => {
        let stop: (() => void) | null = null;
        return {
          one: (_e: string, fn: () => void) => {
            stop = fn;
          },
          // ELK answers later: the test lets it.
          run: () =>
            cys.pending.push(() => {
              if (destroyed)
                throw new TypeError("Cannot read properties of null (reading 'notify')");
              stop?.();
            }),
        };
      },
      nodes: () => none,
      // The lanes preset reads the lines to place the term lane: none drawn here.
      edges: () => none,
      elements: () => none,
      getElementById: () => ({ ...none, position: () => 0 }),
      add: vi.fn(),
      fit: vi.fn(),
      resize: vi.fn(),
      zoom: () => 1,
      center: vi.fn(),
      batch: (fn: () => void) => fn(),
      // A renderer drawing nothing takes the canvas renderer's place.
      unmount: empty,
      destroy: () => {
        destroyed = true;
        empty();
      },
    };
  };
  return { default: Object.assign(cytoscape, { use: vi.fn() }) };
});

import { LaneStage } from "../LaneStage";

/** ELK answers every layout asked so far, and the ones those answers ask. */
const answerAll = () => {
  while (cys.pending.length > 0) (cys.pending.shift() as () => void)();
};

const stage = (diagram: ReturnType<typeof laneDiagram>) => (
  <LaneStage
    diagram={diagram}
    selected={null}
    setSelected={vi.fn()}
    hover={null}
    setHover={vi.fn()}
    onMore={vi.fn()}
    command={null}
    testId="lanes"
  />
);

beforeEach(() => {
  cys.pending = [];
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe() {}
      disconnect() {}
    },
  );
});

describe("LaneStage redrawn mid-layout", () => {
  it("keeps the new diagram's canvas when the old layout ends", () => {
    const { rerender } = render(stage(laneDiagram(DATA, DATA, new Set())));
    // A new diagram while the first is still being laid out.
    rerender(stage(laneDiagram(DATA, DATA, new Set())));
    act(() => answerAll());
    expect(screen.getByTestId("lanes").querySelectorAll("canvas")).toHaveLength(1);
  });

  it("is laid out when drawn once", () => {
    render(stage(laneDiagram(DATA, DATA, new Set())));
    act(() => answerAll());
    expect(screen.getByTestId("lanes").querySelectorAll("canvas")).toHaveLength(1);
  });

  it("ends a core whose layout is pending without its layout throwing", () => {
    const { unmount } = render(stage(laneDiagram(DATA, DATA, new Set())));
    unmount();
    expect(() => answerAll()).not.toThrow();
  });
});
