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
        "setFrontal",
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
      table === 11 ? [{ id: "orders_customer", alias: "PLACED_BY", target_table_id: 10 }] : [],
  },
});
const term = (id: number, name: string, refs: ReturnType<typeof ref>[]): TermRow => ({
  id,
  name,
  definition: null,
  is_abstract: false,
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

describe("the Ontology View's Tables checkbox", () => {
  it("goes with Columns, and draws the owning tables while it is ticked", async () => {
    render(
      <GraphExplorer
        data={data}
        scope={GLOSSARY_SCOPE}
        preset="ontology"
        zero={[]}
        settingsKey={{ kind: "ontology", subject: null }}
        testId="x"
      />,
    );
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    const tables = screen.getByTestId("x-tables");
    expect(tables).not.toBeChecked();
    expect(tablesDrawn()).toEqual([]);

    // Columns off: Tables cannot be ticked.
    screen.getByTestId("x-columns").click();
    await waitFor(() => expect(screen.getByTestId("x-tables")).toBeDisabled());
    screen.getByTestId("x-columns").click();
    await waitFor(() => expect(screen.getByTestId("x-tables")).toBeEnabled());

    fireEvent.click(screen.getByTestId("x-tables"));
    await waitFor(() => expect(tablesDrawn()).toEqual(["t:10", "t:11"]));
    fireEvent.click(screen.getByTestId("x-tables"));
    await waitFor(() => expect(tablesDrawn()).toEqual([]));
  });

  it("lists the relationships by name among the edge types", async () => {
    render(
      <GraphExplorer
        data={data}
        scope={GLOSSARY_SCOPE}
        preset="ontology"
        zero={[]}
        settingsKey={{ kind: "ontology", subject: null }}
        testId="x"
      />,
    );
    fireEvent.click(screen.getByTestId("x-filter-edge-type"));
    expect(await screen.findByTestId("x-filter-edge-type-option-PLACED_BY")).toBeInTheDocument();
    expect(screen.getByTestId("x-filter-edge-type-option-PART_OF")).toBeInTheDocument();
  });

  it("is remembered with the graph's other settings", async () => {
    const view = () => (
      <GraphExplorer
        data={data}
        scope={GLOSSARY_SCOPE}
        preset="ontology"
        zero={[]}
        settingsKey={{ kind: "ontology", subject: null }}
        testId="x"
      />
    );
    const first = render(view());
    fireEvent.click(screen.getByTestId("x-tables"));
    await waitFor(() => expect(screen.getByTestId("x-tables")).toBeChecked());
    first.unmount();

    render(view());
    expect(screen.getByTestId("x-tables")).toBeChecked();
  });
});
