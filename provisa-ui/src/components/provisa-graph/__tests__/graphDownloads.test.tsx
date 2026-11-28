// Copyright (c) 2026 Kenneth Stott
// Canary: 6c2e8b40-7f19-4d3a-a5c6-1e9b0d4f8a27
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (amended 2026-11-27): every graph ProvisaGraph draws (the ERD, lineage, the 2D ontology
// graph) downloads its image through the one image export (exportImage.ts), from its hover command
// bar: on the canvas colour, named after the view, with no hulls (these views draw none).
//
// The real cytoscape, headless (jsdom has no 2D context).

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
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

const { exportSvg, exportPng, exportJpeg } = vi.hoisted(() => ({
  exportSvg: vi.fn(),
  exportPng: vi.fn(),
  exportJpeg: vi.fn(),
}));
vi.mock("../exportImage", async (actual) => ({
  ...(await actual<typeof import("../exportImage")>()),
  exportSvg,
  exportPng,
  exportJpeg,
}));

import { ProvisaGraph } from "../ProvisaGraph";

const GRAPH: GraphSpec = {
  nodes: [
    { id: "a", type: "table", name: "orders" },
    { id: "b", type: "table", name: "customers" },
  ],
  edges: [{ id: "ab", source: "a", target: "b" }],
};

beforeEach(() => {
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
  vi.clearAllMocks();
});

describe("a ProvisaGraph view's image downloads go through the one image export", () => {
  it.each(["erd", "lineage", "ontology-lanes"])("%s", async (name) => {
    render(<ProvisaGraph graph={GRAPH} preset="layered" testId="g" exportName={name} />);
    await vi.waitFor(() => expect(screen.queryByTestId("g-laying-out")).not.toBeInTheDocument());
    const canvas = (document.querySelector(".pg-stage") as HTMLElement).style.getPropertyValue(
      "--pg-canvas",
    );
    expect(canvas).not.toBe("");
    const image = { cy: expect.anything(), bg: canvas, name, hulls: null };
    for (const [key, fn] of [
      ["svg", exportSvg],
      ["png", exportPng],
      ["jpeg", exportJpeg],
    ] as const) {
      fireEvent.click(screen.getByTestId("g-download"));
      fireEvent.click(await screen.findByTestId(`g-export-${key}`));
      expect(fn).toHaveBeenLastCalledWith(image);
    }
  });
});
