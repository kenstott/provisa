// Copyright (c) 2026 Kenneth Stott
// Canary: 033498fe-9041-4a4f-a648-62bff373ebc2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (amended 2026-11-27): every graph's hover command bar is the shared one. The lineage
// graph's Enlarge (into a dialog) is its own command after the shared Full screen, and the two show
// different icons: Enlarge takes the result graph's enlarge icon, Full screen keeps Expand.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, within } from "../../../test-utils/render";

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

const GRAPH = {
  nodes: [{ id: "t.a", column: "a", relation: "t", kind: "source", materialized: false }],
  edges: [],
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
});

describe("the lineage graph's command bar", () => {
  it("draws Enlarge with an icon of its own, not Full screen's", () => {
    render(<LineageDag graph={GRAPH} onOpenModal={vi.fn()} />);
    const bar = screen.getByTestId("lineage-dag-canvas-bar");
    const fullscreen = within(bar).getByTestId("lineage-dag-fullscreen");
    const enlarge = within(bar).getByTestId("lineage-open-modal");
    expect(fullscreen.querySelector(".lucide-expand")).not.toBeNull();
    expect(enlarge.querySelector(".lucide-expand")).toBeNull();
    expect(enlarge.querySelector("svg")).not.toBeNull();
  });
});
