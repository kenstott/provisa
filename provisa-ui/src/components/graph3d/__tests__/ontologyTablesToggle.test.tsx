// Copyright (c) 2026 Kenneth Stott
// Canary: a80831c1-326c-4c55-8dd2-fd520ba5459b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Ontology View's Tables checkbox, beside Columns: off at first and offered only with Columns
// on; ticked, it draws the tables owning the columns drawn, and unticked takes them away. The
// relationships between those tables are in the edge-type filter by name.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";
import type { GraphData } from "../graphModel";

type Calls = Record<string, ReturnType<typeof vi.fn>>;
const made = vi.hoisted(() => ({ scenes: [] as Calls[] }));
vi.mock("../graphScene", () => ({
  GraphScene: class {
    constructor() {
      const calls: Calls = {};
      for (const m of [
        "setTheme",
        "setRotate",
        "setGrid",
        "dispose",
        "resetView",
        "setHover",
        "setData",
        "setSelected",
        "focus",
        "setFlat",
        "setArcs",
      ])
        calls[m] = vi.fn();
      Object.assign(this, calls);
      made.scenes.push(calls);
    }
  },
}));

import { GraphExplorer } from "../GraphExplorer";
import { GLOSSARY_SCOPE } from "../graphModel";
import { glossaryGraph, type TermRow } from "../glossaryGraphData";
import { setStored } from "../../../lib/browserStorage";

const ref = (table: number, column: string) => ({
  column_key: `${table}:${column}`,
  column_name: column,
  table_id: table,
  table_name: `t${table}`,
  domain_id: "sales",
  table: {
    id: table,
    product: null,
    source_relationships:
      table === 11
        ? [
            // One a steward named, one served under its derived type; neither shown by its id.
            { id: "orders_customer", cypher_type: "PLACED_BY", target_table_id: 10 },
            { id: "orders-to-customers", cypher_type: "BELONGS_TO_CUSTOMER", target_table_id: 10 },
          ]
        : [],
  },
});
const term = (id: number, name: string, refs: ReturnType<typeof ref>[]): TermRow => ({
  id,
  name,
  definition: null,
  ref_count: 0,
  deprecated: false,
  retired: false,
  live: true,
  domains: [{ domain_id: "sales" }],
  refs,
});
const data: GraphData = glossaryGraph(
  [term(1, "Customer", [ref(10, "email")]), term(2, "Order", [ref(11, "id")])],
  [{ id: 1, from_term_id: 2, to_term_id: 1, rel_type: "PART_OF" }],
);

const tablesDrawn = () =>
  (made.scenes[0].setData.mock.lastCall?.[0] as GraphData).nodes
    .filter((n) => n.type === "table")
    .map((n) => n.id)
    .sort();

beforeEach(() => {
  made.scenes.length = 0;
});

/** The Layers dropdown, opened, and the option asked for in it. */
async function layer(value: string): Promise<HTMLElement> {
  const id = `x-layers-menu-option-${value}`;
  if (!screen.queryByTestId(id)) fireEvent.click(screen.getByTestId("x-layers-menu"));
  return screen.findByTestId(id);
}
const ticked = async (value: string) => (await layer(value)).hasAttribute("data-combobox-active");
const enabled = async (value: string) =>
  !(await layer(value)).hasAttribute("data-combobox-disabled");

describe("the Ontology View's Tables layer", () => {
  it("goes with Columns, and draws the owning tables while it is ticked", async () => {
    render(
      <GraphExplorer
        data={data}
        scope={GLOSSARY_SCOPE}
        preset="ontology"
        settingsKey={{ kind: "ontology", subject: null }}
        exportName="ontology"
        testId="x"
      />,
    );
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    expect(await ticked("ownerTables")).toBe(false);
    expect(tablesDrawn()).toEqual([]);

    // Columns off: Tables cannot be ticked.
    fireEvent.click(await layer("columns"));
    await waitFor(async () => expect(await enabled("ownerTables")).toBe(false));
    fireEvent.click(await layer("columns"));
    await waitFor(async () => expect(await enabled("ownerTables")).toBe(true));

    fireEvent.click(await layer("ownerTables"));
    await waitFor(() => expect(tablesDrawn()).toEqual(["t:10", "t:11"]));
    fireEvent.click(await layer("ownerTables"));
    await waitFor(() => expect(tablesDrawn()).toEqual([]));
  });

  it("lists the relationships by their served Cypher types among the edge types", async () => {
    render(
      <GraphExplorer
        data={data}
        scope={GLOSSARY_SCOPE}
        preset="ontology"
        settingsKey={{ kind: "ontology", subject: null }}
        exportName="ontology"
        testId="x"
      />,
    );
    fireEvent.click(screen.getByTestId("x-filter-edge-type"));
    expect(await screen.findByTestId("x-filter-edge-type-option-PLACED_BY")).toBeInTheDocument();
    expect(screen.getByTestId("x-filter-edge-type-option-BELONGS_TO_CUSTOMER")).toBeInTheDocument();
    expect(screen.getByTestId("x-filter-edge-type-option-PART_OF")).toBeInTheDocument();
    // A relationship's id is configuration, never an option.
    expect(screen.queryByTestId("x-filter-edge-type-option-orders_customer")).toBeNull();
    expect(screen.queryByTestId("x-filter-edge-type-option-orders-to-customers")).toBeNull();
  });

  it("is remembered with the graph's other settings", async () => {
    const view = () => (
      <GraphExplorer
        data={data}
        scope={GLOSSARY_SCOPE}
        preset="ontology"
        settingsKey={{ kind: "ontology", subject: null }}
        exportName="ontology"
        testId="x"
      />
    );
    const first = render(view());
    fireEvent.click(await layer("ownerTables"));
    await waitFor(async () => expect(await ticked("ownerTables")).toBe(true));
    first.unmount();

    render(view());
    expect(await ticked("ownerTables")).toBe(true);
    expect(screen.getByTestId("x-layers-menu")).toHaveTextContent("Layers: all");
  });

  it("draws the terms and their edges whatever was remembered, an old Ontology off too", async () => {
    setStored("graph3d.ontology.org", JSON.stringify({ layers: ["domains"] }));
    render(
      <GraphExplorer
        data={data}
        scope={GLOSSARY_SCOPE}
        preset="ontology"
        settingsKey={{ kind: "ontology", subject: null }}
        exportName="ontology"
        testId="x"
      />,
    );
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    // No Ontology to choose: the terms are the graph.
    fireEvent.click(screen.getByTestId("x-layers-menu"));
    expect(await screen.findByTestId("x-layers-menu-option-domains")).toBeInTheDocument();
    expect(screen.queryByTestId("x-layers-menu-option-ontology")).toBeNull();
    const calls = made.scenes[0].setData.mock.calls;
    const graph = calls[calls.length - 1][0] as { nodes: { type: string }[] };
    expect(graph.nodes.some((n) => n.type === "term")).toBe(true);
    expect(screen.getByTestId("x-layers-menu")).toHaveTextContent("Layers: Domains");
  });
});
