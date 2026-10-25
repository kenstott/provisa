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
// as this viewer left it, its content kept inside it. The Knowledge graph spans the panel's width,
// below the cards.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, within } from "../../../test-utils/render";
import { DataProductDetailPanel } from "../DataProductDetailPanel";
import type { CoreEdge } from "../productCore";
import type { DataProduct } from "../../../types/admin";

vi.mock("../../../components/OwnerResolution", () => ({ OwnerResolutionInline: () => null }));
vi.mock("../../../components/graph3d/KnowledgeGraphPanel", () => ({
  KnowledgeGraphPanel: () => <div data-testid="kg-panel" />,
}));

const PRODUCT = {
  id: "pet_health",
  domainId: "pet-store",
  name: "Pet Health",
  customProperties: {},
} as unknown as DataProduct;

// REQ-2126: the product's nearest physical inputs, long names that cannot wrap.
const INPUTS: CoreEdge[] = Array.from({ length: 12 }, (_, i) => ({
  id: `pet_health:lineage:${i}`,
  kind: "lineage",
  direction: "upstream",
  target_product_id: null,
  target_table_id: null,
  target_relation: `pet_store.a_rather_long_table_name_${i}`,
  direct: true,
  hops: 1,
  path_count: 1,
  aliases: null,
  supporting_tables: null,
  vias: null,
  nearest_input: true,
}));

function panel() {
  return render(
    <DataProductDetailPanel
      p={PRODUCT}
      core={{
        product: PRODUCT,
        edges: INPUTS,
        queries: { graphql: "query X { x }", sql: "", jsonapi: "" },
      }}
      coreError={null}
      reader={null}
      canEdit={false}
      canSeeLineage
      lineageLoading={false}
      lineageError=""
      lineageGraph={null}
      onEdit={vi.fn()}
      onDelete={vi.fn()}
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
      ["knowledgeGraph", "Knowledge graph"],
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
    expect(
      screen.getByTestId("data-product-card-knowledgeGraph-pet_health-toggle"),
    ).toHaveAttribute("aria-expanded", "true");
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

  it("keep the Knowledge graph a card among them, and the lineage DAG below them, across the panel", () => {
    panel();
    const columns = card("details").parentElement as HTMLElement;
    expect(columns.style.columnWidth).toBe("320px");
    expect(columns.contains(card("knowledgeGraph"))).toBe(true);
    const lineage = screen.getByTestId("data-product-detail-lineage-toggle-pet_health");
    expect(columns.contains(lineage)).toBe(false);
  });
});
