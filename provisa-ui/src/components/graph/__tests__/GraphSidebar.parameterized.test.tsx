// Copyright (c) 2026 Kenneth Stott
// Canary: 570afed6-b7cb-4d4a-b6fd-a1592f370703
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Database tab's node labels in two groups: "Nodes" and "Parameterized nodes", so a reader sees
// before clicking which labels need values; the parameterized chip's hover names its parameters.

import type { ComponentProps } from "react";
import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import { Sidebar as GraphSidebar } from "../GraphSidebar";
import type { SchemaNodeLabel } from "../graph-schema-types";

const label = (
  table: string,
  nativeFilterColumns: { name: string; type: string }[] = [],
): SchemaNodeLabel => ({
  domainLabel: null,
  domainId: null,
  tableLabel: table,
  tableId: 1,
  properties: [],
  pkColumns: [],
  idColumn: null,
  nativeFilterColumns,
  propertyTypes: {},
  scl1: null,
  scl2: null,
  scl3: null,
});

const BREED = [{ name: "breed_name", type: "text" }];
const TWO = [
  { name: "breed_name", type: "text" },
  { name: "region", type: "text" },
];

const props = (
  labels: SchemaNodeLabel[],
  onLabelClick = vi.fn(),
): ComponentProps<typeof GraphSidebar> => ({
  schemaNodeLabels: labels,
  schemaRels: [],
  schemaLoading: false,
  history: [],
  colorOverrides: {},
  sizeOverrides: {},
  labelProperty: {},
  sizeByProperty: {},
  sizeMultiplier: {},
  relLineOverrides: {},
  onHistorySelect: vi.fn(),
  onLabelClick,
  onDomainClick: vi.fn(),
  onRelClick: vi.fn(),
  onColorChange: vi.fn(),
  onSizeChange: vi.fn(),
  onLabelPropertyChange: vi.fn(),
  onSizeByPropertyChange: vi.fn(),
  onSizeMultiplierChange: vi.fn(),
  onRelLineChange: vi.fn(),
  numericPropsByLabel: {},
  width: 280,
  onWidthChange: vi.fn(),
  maxNodes: 100,
  onMaxNodesChange: vi.fn(),
});

const group = (testId: string) => screen.getByTestId(testId).closest(".graph-schema-section")!;

describe("the Database tab's node groups", () => {
  it("lists plain labels under Nodes and parameterized ones under Parameterized nodes", () => {
    render(
      <GraphSidebar
        {...props([label("OWNER"), label("BREED", BREED), label("PET"), label("SHOW", TWO)])}
      />,
    );
    const nodes = group("graph-sidebar-node-labels-toggle") as HTMLElement;
    const param = group("graph-sidebar-param-nodes-toggle") as HTMLElement;
    expect(nodes).toHaveTextContent("Nodes");
    expect(param).toHaveTextContent("Parameterized nodes");
    const names = (el: HTMLElement) =>
      [...el.querySelectorAll("[data-testid^='graph-label-']")].map((b) => b.textContent);
    expect(names(nodes)).toEqual(["OWNER", "PET"]);
    expect(names(param)).toEqual(["BREED", "SHOW"]);
  });

  it("shows only Nodes, and no second heading, when no label needs parameters", () => {
    render(<GraphSidebar {...props([label("OWNER"), label("PET")])} />);
    expect(screen.getByTestId("graph-sidebar-node-labels-toggle")).toHaveTextContent("Nodes");
    expect(screen.queryByText("Parameterized nodes")).toBeNull();
    expect(screen.queryByTestId("graph-sidebar-param-nodes-toggle")).toBeNull();
  });

  it("names a parameterized chip's parameters on hover", async () => {
    render(<GraphSidebar {...props([label("SHOW", TWO)])} />);
    fireEvent.mouseEnter(screen.getByTestId("graph-label-SHOW"));
    expect(await screen.findByText("Needs: breed_name, region")).toBeInTheDocument();
  });

  it("keeps the plain chip's hover as its query, and clicks either chip through onLabelClick", async () => {
    const onLabelClick = vi.fn();
    render(<GraphSidebar {...props([label("PET"), label("BREED", BREED)], onLabelClick)} />);
    fireEvent.mouseEnter(screen.getByTestId("graph-label-PET"));
    expect(await screen.findByText("MATCH (n:PET) RETURN n LIMIT 25")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("graph-label-PET"));
    fireEvent.click(screen.getByTestId("graph-label-BREED"));
    expect(onLabelClick.mock.calls).toEqual([["PET"], ["BREED"]]);
  });
});
