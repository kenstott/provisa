// Copyright (c) 2026 Kenneth Stott
// Canary: 9a3f6d18-2b74-4c5e-8e01-7d4c2b9f6a53
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (amended 2026-11-27): image downloads live only on the graph's hover command bar; a frame
// header keeps only the data downloads (JSON, CSV). The network graph's image is named after its view.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen, within } from "../../../test-utils/render";
import { GraphFrame } from "../GraphFrame";
import type { FrameData, GNode } from "../graph-model";
import type { GraphLanguage } from "../graph-persistence";

// The canvas draws into a 2D context jsdom does not have; what is asked of here is the header and
// what the frame hands the canvas.
const canvas = vi.hoisted(() => ({ props: {} as Record<string, unknown> }));
vi.mock("../GraphCanvas", () => ({
  GraphCanvas: (props: Record<string, unknown>) => {
    canvas.props = props;
    return null;
  },
  GraphCutNote: () => null,
}));

const saved = vi.hoisted(() => ({ files: [] as string[] }));
vi.mock("../graph-export", async (actual) => ({
  ...(await actual<typeof import("../graph-export")>()),
  downloadBlob: (_blob: Blob, name: string) => saved.files.push(name),
}));

const ORDER: GNode = { id: 1, label: "Order", tableLabel: "orders", properties: {} };

function frame(rows: Record<string, unknown>[]): FrameData {
  return {
    id: "f1",
    query: "MATCH (n) RETURN n",
    status: "done",
    nodes: new Map([["1", ORDER]]),
    edges: new Map(),
    rows,
    columns: rows.length > 0 ? ["n"] : [],
  };
}

function show(f: FrameData, language: GraphLanguage = "cypher") {
  return render(
    <GraphFrame
      frame={f}
      language={language}
      onClose={() => {}}
      onRerun={() => {}}
      schemaRels={[]}
      parameterized={new Set()}
      pathScope={{ nodeLabels: [], checkedDomains: new Set() }}
      colorOverrides={{}}
      sizeOverrides={{}}
      labelProperty={{}}
      sizeByProperty={{}}
      sizeMultiplier={{}}
      relLineOverrides={{}}
      onColorChange={() => {}}
      pkMap={{}}
      labelToTableLabel={{}}
      labelToTableId={{}}
    />,
  );
}

describe("a frame header's downloads", () => {
  it("are the data only: JSON and CSV, no image", async () => {
    saved.files = [];
    show(frame([{ n: 1 }]));
    fireEvent.click(screen.getByTestId("graph-frame-download-btn"));
    const menu = await screen.findByTestId("graph-frame-download-menu");
    expect(within(menu).getAllByRole("menuitem").map((i) => i.textContent)).toEqual([
      "JSON",
      "CSV",
    ]);
    fireEvent.click(within(menu).getByTestId("graph-frame-download-json"));
    fireEvent.click(screen.getByTestId("graph-frame-download-btn"));
    fireEvent.click(await screen.findByTestId("graph-frame-download-csv"));
    expect(saved.files).toEqual(["result.json", "result.csv"]);
  });

  it("a graph with no rows has no header download: its image is on the command bar", () => {
    show(frame([]));
    expect(screen.queryByTestId("graph-frame-download-btn")).toBeNull();
  });
});

describe("the network graph's image is named after its view", () => {
  it.each(["cypher", "sparql"] as const)("%s", (language) => {
    show(frame([]), language);
    expect(canvas.props.exportName).toBe(`${language}-graph`);
  });
});
