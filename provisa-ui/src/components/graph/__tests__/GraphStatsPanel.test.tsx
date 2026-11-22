// Copyright (c) 2026 Kenneth Stott
// Canary: 5d0c7e92-3b41-4f6a-9a18-c2e84b7d1f30
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-642: the Graph Explorer computes statistics for the canvas graph in the browser, with no
// request to the server: counts, density, degree, isolated nodes, components, diameter, average
// path length, top hubs, and counts by label and by edge type.

import { describe, it, expect, vi, afterEach } from "vitest";
import { screen, waitFor, within } from "@testing-library/react";
import { render } from "../../../test-utils/render";
import { GraphStatsPanel } from "../GraphStatsModal";
import type { GEdge, GNode } from "../graph-model";

function node(id: number, label: string, name: string): GNode {
  return { id, label, tableLabel: label, properties: { name } };
}

function edge(id: number, a: GNode, b: GNode, type: string): GEdge {
  return {
    identity: `e${id}`,
    start: a.id,
    end: b.id,
    type,
    properties: {},
    startNode: a,
    endNode: b,
  };
}

// Path 1-2-3, isolated 4, pair 5-6.
const n1 = node(1, "Person", "ann");
const n2 = node(2, "Person", "bob");
const n3 = node(3, "Person", "cat");
const n4 = node(4, "Org", "acme");
const n5 = node(5, "Order", "o5");
const n6 = node(6, "Order", "o6");
const NODES = new Map<string, GNode>(
  [n1, n2, n3, n4, n5, n6].map((n) => [`n${n.id}`, n]),
);
const EDGES = new Map<string, GEdge>([
  ["e1", edge(1, n1, n2, "KNOWS")],
  ["e2", edge(2, n2, n3, "KNOWS")],
  ["e3", edge(3, n5, n6, "PLACED")],
]);

function rowValue(label: string): string {
  const row = screen.getByText(label).parentElement as HTMLElement;
  return row.lastElementChild?.textContent ?? "";
}

afterEach(() => vi.restoreAllMocks());

describe("GraphStatsPanel (REQ-642)", () => {
  it("computes every statistic from the displayed graph with no server request", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    render(<GraphStatsPanel nodes={NODES} edges={EDGES} />);

    await screen.findByTestId("graph-stats-grid");
    // Async cards settle: components, diameter, average path length.
    await waitFor(() => expect(rowValue("Components")).toBe("3"));
    await waitFor(() => expect(rowValue("Diameter")).toBe("2"));

    expect(rowValue("Nodes")).toBe("6");
    expect(rowValue("Edges")).toBe("3");
    expect(rowValue("Density")).toBe("1.00e-1");
    expect(rowValue("Average")).toBe("1.00");
    expect(rowValue("Maximum")).toBe("2");
    expect(rowValue("Isolated nodes")).toBe("1 (17%)");
    expect(rowValue("Largest component")).toBe("3 (50%)");
    expect(rowValue("Avg path length")).toBe("1.25");

    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("lists the top hub by degree and the counts by label and by edge type", async () => {
    render(<GraphStatsPanel nodes={NODES} edges={EDGES} />);
    await screen.findByTestId("graph-stats-grid");

    const hubs = screen.getByText("Top hubs by degree").closest(".mantine-Card-root") as HTMLElement;
    expect(within(hubs).getByText("bob")).toBeTruthy();
    expect(within(hubs).getByText("2")).toBeTruthy();

    const labels = screen.getByText("Nodes by label").closest(".mantine-Card-root") as HTMLElement;
    expect(within(labels).getByText("Person").nextElementSibling?.textContent).toBe("3");
    expect(within(labels).getByText("Order").nextElementSibling?.textContent).toBe("2");
    expect(within(labels).getByText("Org").nextElementSibling?.textContent).toBe("1");

    const types = screen.getByText("Edges by type").closest(".mantine-Card-root") as HTMLElement;
    expect(within(types).getByText("KNOWS").nextElementSibling?.textContent).toBe("2");
    expect(within(types).getByText("PLACED").nextElementSibling?.textContent).toBe("1");
  });
});
