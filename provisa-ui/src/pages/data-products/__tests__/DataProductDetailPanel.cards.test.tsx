// Copyright (c) 2026 Kenneth Stott
// Canary: 6e2b9d47-1c8a-4f35-b7e0-4a9c2d5f8b13
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180 OFFERS (maintainer 2026-11-11, replacing the grid of detail cards): a data product's
// panel is a header -- owner, domain, freshness, a query to run now, and its values in the
// application's collapsible, open or closed as this viewer left it -- then the same tabs for the
// read view and the edit view: Offers (the default), Quality, Profile, Lineage, Related and, for
// its owner alone, Requests. Each tab keeps its content inside it.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent } from "../../../test-utils/render";
import { DataProductDetailPanel } from "../DataProductDetailPanel";
import type { ProductLineage } from "../../../api/lineage";
import type { DataProduct } from "../../../types/admin";
import { lifecycleProps } from "./lifecycleProps";
import { openTab } from "./panelTabs";

vi.mock("../../../components/OwnerResolution", () => ({ OwnerResolutionInline: () => null }));
vi.mock("../../../components/graph3d/OntologyGraph", () => ({
  OntologySnippet: () => <div data-testid="graph-panel" />,
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
    product_ids: [],
    source_id: null,
    source_type: null,
    source_schema: null,
    source_table: null,
  })),
};

function panel(owner = true) {
  return render(
    <DataProductDetailPanel
      p={PRODUCT}
      core={{ product: PRODUCT, queries: { graphql: "query X { x }", sql: "", jsonapi: "" } }}
      coreError={null}
      reader={null}
      canEdit={false}
      canDelete={false}
      owner={owner}
      seesConsumers={owner}
      lineageLoading={false}
      lineageError=""
      lineageGraph={LINEAGE}
      onEdit={vi.fn()}
      onDelete={vi.fn()}
      {...lifecycleProps()}
    />,
  );
}

const tab = (name: string) => screen.queryByTestId(`data-product-tab-${name}-pet_health`);

describe("a data product's panel", () => {
  beforeEach(() => window.localStorage.clear());

  it("is a header, then its tabs, Offers first and in view", () => {
    panel();
    const header = screen.getByTestId("data-product-header-pet_health");
    const tabs = ["offers", "quality", "profile", "lineage", "related", "requests"].map(
      (name) => tab(name) as HTMLElement,
    );
    expect(tabs.map((t) => t.textContent)).toEqual([
      "Offers",
      "Quality",
      "Profile",
      "Lineage",
      "Related",
      "Requests",
    ]);
    // The header comes before the tabs.
    expect(header.compareDocumentPosition(tabs[0]) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(tabs[0]).toHaveAttribute("aria-selected", "true");
    expect(screen.getByTestId("data-product-offers-pet_health")).toBeInTheDocument();
  });

  it("shows Requests to its owner alone", () => {
    panel(false);
    expect(tab("offers")).not.toBeNull();
    expect(tab("requests")).toBeNull();
  });

  it("mounts only the tab in view", () => {
    panel();
    expect(screen.queryByTestId("data-product-related-pet_health")).toBeNull();
    openTab("related", "pet_health");
    expect(screen.getByTestId("data-product-related-pet_health")).toBeInTheDocument();
    expect(screen.queryByTestId("data-product-offers-pet_health")).toBeNull();
  });

  it("keeps its values in the application's collapsible, open again as the viewer left it", () => {
    const first = panel();
    const details = screen.getByTestId("data-product-card-details-pet_health");
    expect(details.closest(".provisa-collapsible")).not.toBeNull();
    const toggle = () => screen.getByTestId("data-product-card-details-pet_health-toggle");
    expect(toggle()).toHaveTextContent("Details");
    // Open at first: the read view shows every value.
    expect(toggle()).toHaveAttribute("aria-expanded", "true");
    fireEvent.click(toggle());
    expect(toggle()).toHaveAttribute("aria-expanded", "false");
    first.unmount();
    panel();
    expect(toggle()).toHaveAttribute("aria-expanded", "false");
  });

  it("keeps a tab's content inside it: what cannot wrap scrolls within the tab", () => {
    panel();
    openTab("quality", "pet_health");
    const ports = screen.getByTestId("data-product-detail-inputs-pet_health");
    let body = ports.parentElement as HTMLElement;
    while (body && body.style.overflowX !== "auto") body = body.parentElement as HTMLElement;
    expect(body).not.toBeNull();
    expect(body.getAttribute("role")).toBe("tabpanel");
    expect(body.style.overflowWrap).toBe("anywhere");
  });

  it("has no Queries card: the panel's own read is not shown", () => {
    panel();
    expect(screen.queryByTestId("data-product-card-queries-pet_health")).toBeNull();
    expect(screen.queryByText("query DataProductPanel", { exact: false })).toBeNull();
  });

  it("draws the ontology graph in Related, and lineage in a tab of its own", () => {
    panel();
    expect(screen.queryByTestId("graph-panel")).toBeNull();
    openTab("related", "pet_health");
    expect(screen.getByTestId("graph-panel")).toBeInTheDocument();
    openTab("lineage", "pet_health");
    expect(screen.queryByTestId("graph-panel")).toBeNull();
  });
});
