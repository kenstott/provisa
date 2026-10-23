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

const INPUT_PORTS = Array.from({ length: 12 }, (_, i) => ({
  inputs: [`native:pet_store.pets.a_rather_long_column_name_${i}`],
  transform: "cast(varchar) / rename",
  output: `pet_store.pets.a_rather_long_column_name_${i}`,
}));

function panel() {
  return render(
    <DataProductDetailPanel
      p={PRODUCT}
      core={{ product: PRODUCT, queries: { graphql: "query X { x }", sql: "", jsonapi: "" } }}
      coreError={null}
      reader={null}
      sources={[]}
      internalRelationships={[]}
      canEdit={false}
      canSeeLineage
      inputPorts={INPUT_PORTS}
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
      ["queries", "Queries"],
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
    expect(screen.getByTestId("data-product-card-queries-pet_health-toggle")).toHaveAttribute(
      "aria-expanded",
      "true",
    );
  });

  it("keep their content inside them: what cannot wrap scrolls within the card", () => {
    panel();
    const table = within(card("inputPorts")).getByTestId(
      "data-product-detail-input-ports-pet_health",
    );
    let body = table.parentElement as HTMLElement;
    while (body && body.style.overflowX !== "auto") body = body.parentElement as HTMLElement;
    expect(body).not.toBeNull();
    expect(card("inputPorts").contains(body)).toBe(true);
    expect(body.style.overflowWrap).toBe("anywhere");
  });

  it("put the Knowledge graph below the columns, across the panel", () => {
    panel();
    const columns = card("details").parentElement as HTMLElement;
    expect(columns.style.columnWidth).toBe("320px");
    expect(columns.contains(card("knowledgeGraph"))).toBe(false);
    expect(columns.contains(card("queries"))).toBe(true);
  });

  it("name a source read in the lineage by the table it reads", () => {
    const edge = (over: Record<string, unknown>) => ({
      kind: "lineage",
      direction: "upstream",
      target_product_id: null,
      target_table_id: null,
      target_relation: null,
      direct: true,
      hops: 1,
      path_count: 1,
      aliases: null,
      vias: null,
      target_product: null,
      target_table: null,
      ...over,
    });
    render(
      <DataProductDetailPanel
        p={PRODUCT}
        core={
          {
            product: PRODUCT,
            queries: { graphql: "", sql: "", jsonapi: "" },
            edges: [
              edge({
                target_table_id: 1,
                target_table: { domain_id: "pet-store", table_name: "pets" },
              }),
              edge({
                target_table_id: 2,
                target_table: { domain_id: "pet-store", table_name: "vets" },
              }),
              edge({ direction: "downstream", target_relation: "pet_store.dim_pet" }),
            ],
          } as never
        }
        coreError={null}
        reader={null}
        sources={[]}
        internalRelationships={[]}
        canEdit={false}
        canSeeLineage
        inputPorts={[]}
        onEdit={vi.fn()}
        onDelete={vi.fn()}
      />,
    );
    const items = (dir: string) =>
      within(screen.getByTestId(`data-product-detail-lineage-${dir}-pet_health`))
        .getAllByRole("listitem")
        .map((li) => li.textContent);
    expect(items("upstream")).toEqual([
      expect.stringContaining("pet-store.pets"),
      expect.stringContaining("pet-store.vets"),
    ]);
    expect(items("downstream")).toEqual([expect.stringContaining("pet_store.dim_pet")]);
  });
});
