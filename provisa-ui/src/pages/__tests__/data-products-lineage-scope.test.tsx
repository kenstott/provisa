// Copyright (c) 2026 Kenneth Stott
// Canary: 8b2d6f14-9c37-4a50-b1e8-3f6a9d2c7e05
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1666, REQ-2127: the product detail's lineage is the members' whole upstream ancestry and one
// hop downstream -- never the checker's results tables. The scoping is data_product_edges'
// (REQ-2126, checker members start no lineage); the panel draws its lineage rows, the farthest
// ancestor first, and draws no Lineage section for a role without view_governance.

import { describe, it, expect, vi } from "vitest";
import { render, screen, within } from "../../test-utils/render";
import { DataProductDetailPanel } from "../data-products/DataProductDetailPanel";
import type { CoreEdge, ProductCore } from "../data-products/productCore";
import type { DataProduct } from "../../types/admin";

vi.mock("../../components/graph3d/KnowledgeGraphPanel", () => ({
  KnowledgeGraphPanel: () => null,
}));

const P = {
  id: "c360",
  domainId: "sales",
  name: "Customer 360",
  purpose: "",
  limitations: "",
  usage: "",
  customProperties: {},
} as DataProduct;

const lineage = (
  relation: string,
  direction: "upstream" | "downstream",
  hops: number,
  vias: string | null,
): CoreEdge => ({
  kind: "lineage",
  direction,
  target_product_id: null,
  target_table_id: null,
  target_relation: relation,
  direct: hops === 1,
  hops,
  path_count: 1,
  aliases: null,
  supporting_tables: null,
  vias,
});

const CORE: ProductCore = {
  product: P,
  tables: [],
  edges: [
    lineage("raw.orders", "upstream", 1, "sales.v_orders"),
    lineage("landing.events", "upstream", 3, null),
    lineage("mart.churn", "downstream", 1, "mart.v_churn"),
  ],
  queries: { graphql: "", sql: "", jsonapi: "" },
};

function panel(canSeeLineage: boolean) {
  render(
    <DataProductDetailPanel
      p={P}
      core={CORE}
      coreError={null}
      reader={null}
      sources={[]}
      internalRelationships={[]}
      canEdit={false}
      canSeeLineage={canSeeLineage}
      onEdit={vi.fn()}
      onDelete={vi.fn()}
    />,
  );
}

describe("product detail lineage scope (REQ-1666, REQ-2127)", () => {
  it("lists the ancestry, the farthest first, and the one hop downstream", () => {
    panel(true);
    const up = screen.getByTestId("data-product-detail-lineage-upstream-c360");
    expect(
      within(up)
        .getAllByRole("listitem")
        .map((li) => li.textContent),
    ).toEqual(["landing.events 3 hops", "raw.orders 1 hop · via sales.v_orders"]);
    const down = screen.getByTestId("data-product-detail-lineage-downstream-c360");
    expect(
      within(down)
        .getAllByRole("listitem")
        .map((li) => li.textContent),
    ).toEqual(["mart.churn via mart.v_churn"]);
  });

  it("draws no Lineage section for a role without view_governance", () => {
    panel(false);
    expect(screen.queryByTestId("data-product-detail-lineage-c360")).toBeNull();
  });
});
