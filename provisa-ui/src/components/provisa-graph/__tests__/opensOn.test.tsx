// Copyright (c) 2026 Kenneth Stott
// Canary: b83e1d07-5c2a-4f96-a1d8-2e7f04c9b513
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: a graph with a focus (``opensOn``) opens with it in view, and Reset view comes back to
// it: ProvisaGraph brings it in (showFocus) after the opening fit and after Reset view's fit. The
// pan itself is opensOnFocus.test.ts's.
//
// The real cytoscape, headless (jsdom has no 2D context).

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
import type { GraphSpec } from "../model";

vi.mock("cytoscape", async (importOriginal) => {
  type Real = ((...a: unknown[]) => unknown) & { use: (e: unknown) => void };
  const real = (await importOriginal<{ default: Real }>()).default;
  const make = (...args: unknown[]) => {
    if (typeof args[0] === "string") return real(...args);
    const o = args[0] as Record<string, unknown>;
    return real({ ...o, container: undefined, headless: true, styleEnabled: true });
  };
  return { default: Object.assign(make, { use: real.use.bind(real) }) };
});

const calls = vi.hoisted(() => ({ fit: [] as string[] }));
vi.mock("../viewport", async (importOriginal) => {
  const real = await importOriginal<typeof import("../viewport")>();
  return {
    ...real,
    fitClear: vi.fn((...a: Parameters<typeof real.fitClear>) => {
      calls.fit.push("fitClear");
      real.fitClear(...a);
    }),
    showFocus: vi.fn((...a: Parameters<typeof real.showFocus>) => {
      calls.fit.push(`showFocus:${a[1] ?? "none"}:${a[2]}`);
      real.showFocus(...a);
    }),
  };
});

import { ProvisaGraph, type GraphCommand } from "../ProvisaGraph";

const GRAPH: GraphSpec = {
  nodes: [
    { id: "a", type: "term", name: "Shelter", lane: "terms" },
    { id: "c", type: "table", name: "beds", lane: "tables" },
    { id: "d", type: "column", name: "bed_count", lane: "columns" },
  ],
  edges: [
    { id: "ac", source: "a", target: "c" },
    { id: "cd", source: "c", target: "d" },
  ],
};
const LANES = {
  lanes: [
    { id: "columns", title: "Columns" },
    { id: "tables", title: "Tables" },
    { id: "terms", title: "Terms" },
  ],
};

const graphOf = (opensOn: string | null, command: GraphCommand | null = null) => (
  <ProvisaGraph
    graph={GRAPH}
    preset="lanes"
    lanes={LANES}
    testId="g"
    exportName="g"
    opensOn={opensOn}
    command={command}
  />
);

/** The layout has run and the drawing is shown. */
const drawn = () =>
  vi.waitFor(() => expect(screen.queryByTestId("g-laying-out")).not.toBeInTheDocument());

beforeEach(() => {
  calls.fit = [];
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe() {}
      disconnect() {}
    },
  );
});
afterEach(() => vi.unstubAllGlobals());

describe("a graph's focus on open and on Reset view", () => {
  it("brings the focus into view after the opening fit", async () => {
    render(graphOf("a"));
    await drawn();
    expect(calls.fit).toEqual(["fitClear", "showFocus:a:24"]);
  });

  it("brings it into view again after Reset view's fit", async () => {
    const { rerender } = render(graphOf("a"));
    await drawn();
    calls.fit = [];
    rerender(graphOf("a", { kind: "reset" }));
    expect(calls.fit).toEqual(["fitClear", "showFocus:a:24"]);
  });

  it("opens a graph with no focus as fitted", async () => {
    render(graphOf(null));
    await drawn();
    expect(calls.fit).toEqual(["fitClear", "showFocus:none:24"]);
  });
});
