// Copyright (c) 2026 Kenneth Stott
// Canary: 6e2b9d47-1c8a-4f35-b7e0-4a9c2d5f8b13
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A data product's read view is cards, each the application's collapsible: one look, open or closed
// as this viewer left it, its content kept inside it. The Graph spans the panel's width,
// below the cards.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, within } from "../../../test-utils/render";
import { DataProductDetailPanel } from "../DataProductDetailPanel";
import type { ProductLineage } from "../../../api/lineage";
import type { DataProduct } from "../../../types/admin";
import { lifecycleProps } from "./lifecycleProps";

vi.mock("../../../components/OwnerResolution", () => ({ OwnerResolutionInline: () => null }));
vi.mock("../../../components/graph3d/GraphPanel", () => ({
  GraphPanel: () => <div data-testid="graph-panel" />,
}));

const PRODUCT = {
  id: "pet_health",
  domainId: "pet-store",
  name: "Pet Health",
  customProperties: {},
} as unknown as DataProduct;

// REQ-2126: the product's nearest physical inputs, long names that cannot wrap.
const LINEAGE: ProductLineage = {
  nodes: [],
  edges: [],
  outputs: [],
  members: [],
  consumers: [],
  dq: { expectations: [], promises: [], related: [], unattached: [] },
  inputs: Array.from({ length: 12 }, (_, i) => ({
    relation: `pet_store.a_rather_long_table_name_${i}`,
    kind: "source_table" as const,
    table_id: 100 + i,
    domain_id: "pet-store",
    table_name: `a_rather_long_table_name_${i}`,
    alias: null,
    product_id: null,
    source_id: null,
    source_type: null,
    source_schema: null,
    source_table: null,
  })),
};

function panel() {
  return render(
    <DataProductDetailPanel
      p={PRODUCT}
      core={{ product: PRODUCT, queries: { graphql: "query X { x }", sql: "", jsonapi: "" } }}
      coreError={null}
      reader={null}
      canEdit={false}
      canDelete={false}
      owner
      lineageLoading={false}
      lineageError=""
      lineageGraph={LINEAGE}
      onEdit={vi.fn()}
      onDelete={vi.fn()}
      {...lifecycleProps()}
    />,
  );
}

const card = (section: string) => screen.getByTestId(`data-product-card-${section}-pet_health`);

describe("a data product's cards", () => {
  beforeEach(() => window.localStorage.clear());

  it("are each the application's collapsible, headed by its label", () => {
    panel();
    for (const [section, label] of [
      ["details", "Details"],
      ["customProperties", "Custom Properties"],
      ["inputPorts", "Input Ports"],
      ["graph", "Graph"],
    ]) {
      expect(card(section).querySelector(".provisa-collapsible")).not.toBeNull();
      expect(
        screen.getByTestId(`data-product-card-${section}-pet_health-toggle`),
      ).toHaveTextContent(label);
    }
  });

  it("open again as the viewer left them", () => {
    const first = panel();
    const toggle = () => screen.getByTestId("data-product-card-inputPorts-pet_health-toggle");
    expect(toggle()).toHaveAttribute("aria-expanded", "true");
    fireEvent.click(toggle());
    expect(toggle()).toHaveAttribute("aria-expanded", "false");
    first.unmount();
    panel();
    expect(toggle()).toHaveAttribute("aria-expanded", "false");
    expect(screen.getByTestId("data-product-card-graph-pet_health-toggle")).toHaveAttribute(
      "aria-expanded",
      "true",
    );
  });

  it("keep their content inside them: what cannot wrap scrolls within the card", () => {
    panel();
    const ports = within(card("inputPorts")).getByTestId("data-product-detail-inputs-pet_health");
    let body = ports.parentElement as HTMLElement;
    while (body && body.style.overflowX !== "auto") body = body.parentElement as HTMLElement;
    expect(body).not.toBeNull();
    expect(card("inputPorts").contains(body)).toBe(true);
    expect(body.style.overflowWrap).toBe("anywhere");
  });

  it("have no Queries card: the panel's own read is not shown", () => {
    panel();
    expect(screen.queryByTestId("data-product-card-queries-pet_health")).toBeNull();
    expect(screen.queryByText("query DataProductPanel", { exact: false })).toBeNull();
  });

  it("keep the Graph a card among them, and the lineage DAG below them, across the panel", () => {
    panel();
    const columns = card("details").parentElement as HTMLElement;
    expect(columns.style.columnWidth).toBe("320px");
    expect(columns.contains(card("graph"))).toBe(true);
    const lineage = screen.getByTestId("data-product-detail-lineage-toggle-pet_health");
    expect(columns.contains(lineage)).toBe(false);
  });
});
