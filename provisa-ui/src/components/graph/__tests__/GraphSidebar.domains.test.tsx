// Copyright (c) 2026 Kenneth Stott
// Canary: 24c4be19-6618-4696-8872-0212d159a710
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Database tab's node labels under a domain selection: deselecting meta takes meta's labels
// away and keeps the others', whichever page of the list was open; and their counts, a count the
// engine could not make said as such.

import type { ComponentProps } from "react";
import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import { Sidebar as GraphSidebar } from "../GraphSidebar";
import type { SchemaNodeLabel } from "../graph-schema-types";

const label = (domain: string, table: string): SchemaNodeLabel => ({
  domainLabel: domain,
  domainId: domain.toLowerCase(),
  tableLabel: table,
  tableId: 1,
  properties: [],
  pkColumns: [],
  idColumn: null,
  nativeFilterColumns: [],
  propertyTypes: {},
  scl1: null,
  scl2: null,
  scl3: null,
});

// Meta's sixty labels fill more than one page; PetStore's two sort after them.
const META = Array.from({ length: 60 }, (_, i) => label("Meta", `M${String(i).padStart(2, "0")}`));
const PETSTORE = [label("PetStore", "PETINVENTORY"), label("PetStore", "PETINQUIRIES")];

const props = (labels: SchemaNodeLabel[]): ComponentProps<typeof GraphSidebar> => ({
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
  onLabelClick: vi.fn(),
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

describe("the Database tab's node labels", () => {
  it("keep the other domains' labels when meta is deselected on a later page", () => {
    const { rerender } = render(<GraphSidebar {...props([...META, ...PETSTORE])} />);
    fireEvent.click(screen.getAllByLabelText("Next page")[0]);
    expect(screen.getByRole("button", { name: "PETINVENTORY" })).toBeInTheDocument();

    rerender(<GraphSidebar {...props(PETSTORE)} />);
    expect(screen.getByRole("button", { name: "PETINVENTORY" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "PETINQUIRIES" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "M00" })).toBeNull();
  });

  it("show a count the engine could not make as ?, the * pill naming each one and why", async () => {
    render(
      <GraphSidebar
        {...props(PETSTORE)}
        counts={{
          node_count: null,
          rel_count: 0,
          label_counts: {
            "PetStore:PETINVENTORY": { count: 4 },
            "PetStore:PETINQUIRIES": { count: null, error: "Catalog petstore is not attached" },
          },
          rel_counts: {},
        }}
      />,
    );
    const all = screen.getByTestId("graph-all-nodes");
    expect(all).toHaveTextContent("*(?)");
    expect(screen.getByRole("button", { name: /^PETINVENTORY\s*\(4\)$/ })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /^PETINQUIRIES\s*\(\?\)$/ })).toBeInTheDocument();
    fireEvent.mouseEnter(all);
    expect(
      await screen.findByText(
        "Not counted: PetStore:PETINQUIRIES (Catalog petstore is not attached)",
      ),
    ).toBeInTheDocument();
  });
});
