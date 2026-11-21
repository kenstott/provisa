// Copyright (c) 2026 Kenneth Stott
// Canary: a38f7606-5b9a-4f43-be61-869299e6435d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1519, REQ-2239: the plan diagram and the operator list beside it are two readings of one
// tree. Picking an operator in the diagram marks its row and scrolls the list to it; picking a row
// brings its node into view in the diagram.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";
import { AnalyzePanes } from "../AnalyzePanes";
import type { ExplainResponse } from "../../../api/admin";
import { mockSvg, sizeStages, viewOf } from "../../../components/__tests__/mermaidMock";

vi.mock("mermaid", () => ({
  default: {
    initialize: vi.fn(),
    parse: async () => ({ diagramType: "flowchart-v2" }),
    render: async (id: string, chart: string) => ({ svg: mockSvg(id, chart) }),
  },
}));

const op = (name: string, children: ExplainResponse["plan"] = []) => ({
  op: name,
  detail: {},
  rows: 10,
  cost: 1,
  actual_ms: null,
  children,
});

// The server draws operator n of the list (depth-first, top-down) as Mermaid node p{n}.
const PLAN: ExplainResponse = {
  route: "ENGINE",
  route_reason: "",
  dialect: "trino",
  analyzed: false,
  sources: ["pg"],
  optimizations: [],
  sql: "SELECT 1",
  semantic_sql: null,
  plan: [op("Output", [op("HashJoin", [op("Scan orders")]), op("Scan users")])],
  mermaid: [
    "flowchart TD",
    "%% size 4000x3000",
    "%% node route 2000 30",
    "%% node p1 2000 120",
    "%% node p2 1000 220",
    "%% node p3 3800 2900",
    "%% node p4 3000 220",
  ].join("\n"),
};

let restore: () => void;
beforeEach(() => {
  restore = sizeStages(800, 500);
});
afterEach(() => restore());

describe("plan diagram and operator list", () => {
  it("picking an operator in the diagram marks its row and scrolls the list to it", async () => {
    const { container } = render(<AnalyzePanes plan={PLAN} />);
    await waitFor(() => expect(container.querySelector(".plan-stage svg")).not.toBeNull());
    const row = screen.getByTestId("analyze-row-2");
    const scrolled = vi.spyOn(row, "scrollIntoView");
    const node = container.querySelector('g.node[id*="-flowchart-p3-"] rect') as Element;
    fireEvent.click(node);
    await waitFor(() => expect(row.getAttribute("aria-selected")).toBe("true"));
    expect(scrolled).toHaveBeenCalled();
    expect(screen.getByTestId("analyze-row-0").getAttribute("aria-selected")).toBe("false");
    expect(
      container
        .querySelector('g.node[id*="-flowchart-p3-"]')
        ?.classList.contains("plan-node-selected"),
    ).toBe(true);
  });

  it("picking a row brings its node into view and marks it", async () => {
    const { container } = render(<AnalyzePanes plan={PLAN} />);
    await waitFor(() => expect(container.querySelector(".plan-stage svg")).not.toBeNull());
    fireEvent.click(screen.getByTestId("analyze-row-2"));
    await waitFor(() => {
      const v = viewOf(container);
      const x = v.x + 3800 * v.zoom;
      const y = v.y + 2900 * v.zoom;
      expect(x > 0 && x < 800 && y > 0 && y < 500).toBe(true);
    });
    expect(
      container
        .querySelector('g.node[id*="-flowchart-p3-"]')
        ?.classList.contains("plan-node-selected"),
    ).toBe(true);
    expect(screen.getByTestId("analyze-row-2").getAttribute("aria-selected")).toBe("true");
  });
});
