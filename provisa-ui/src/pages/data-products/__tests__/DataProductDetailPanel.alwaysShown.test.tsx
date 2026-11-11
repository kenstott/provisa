// Copyright (c) 2026 Kenneth Stott
// Canary: 3c7e1f58-92ab-4d06-8e41-b5a7d0c9e264
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2160: INPUT PORTS AND CONSUMERS ARE ALWAYS SHOWN. The owner or team sees the list, or None
// when it is empty; any other reader sees a muted line that the list is the owner's, never None.
// REQ-2174 (amended 2026-11-07), REQ-2219: the Consumers card is shown to those who own the product
// alone; no one else sees the card at all.

import { describe, it, expect, vi } from "vitest";
import { render, screen, within } from "../../../test-utils/render";
import { DataProductDetailPanel } from "../DataProductDetailPanel";
import type { ProductLineage } from "../../../api/lineage";
import type { DataProduct } from "../../../types/admin";
import { lifecycleProps } from "./lifecycleProps";

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

const EMPTY: ProductLineage = {
  nodes: [],
  edges: [],
  outputs: [],
  members: [],
  consumers: [],
  inputs: [],
  dq: { expectations: [], promises: [], related: [], unattached: [] },
};

const FULL: ProductLineage = {
  ...EMPTY,
  inputs: [
    {
      relation: "pet_store.visits",
      kind: "source_table",
      table_id: 7,
      domain_id: "pet-store",
      table_name: "visits",
      alias: null,
      product_id: null,
      source_id: null,
      source_type: null,
      source_schema: null,
      source_table: null,
    },
  ],
  consumers: [
    {
      relation: "pet_store.visit_report",
      kind: "view",
      table_id: 8,
      domain_id: "pet-store",
      table_name: "visit_report",
      alias: null,
      product_ids: [],
    },
  ],
} as unknown as ProductLineage;

function panel(over: {
  owner: boolean;
  lineageGraph: ProductLineage | null;
  loading?: boolean;
  seesConsumers?: boolean;
}) {
  return render(
    <DataProductDetailPanel
      p={PRODUCT}
      core={{ product: PRODUCT, queries: { graphql: "query X { x }", sql: "", jsonapi: "" } }}
      coreError={null}
      reader={null}
      canEdit={false}
      canDelete={false}
      owner={over.owner}
      seesConsumers={over.seesConsumers ?? over.owner}
      lineageLoading={over.loading ?? false}
      lineageError=""
      lineageGraph={over.lineageGraph}
      onEdit={vi.fn()}
      onDelete={vi.fn()}
      {...lifecycleProps()}
    />,
  );
}

const card = (s: string) => screen.getByTestId(`data-product-card-${s}-pet_health`);
const SECTIONS = [
  ["inputPorts", "data-product-detail-inputs-pet_health"],
  ["consumers", "data-product-detail-consumers-pet_health"],
] as const;

describe.each(SECTIONS)("the %s card", (section, listId) => {
  it("lists the entries for the owner", () => {
    panel({ owner: true, lineageGraph: FULL });
    expect(within(card(section)).getByTestId(listId)).toBeInTheDocument();
  });

  it("says None for the owner when the list is empty", () => {
    panel({ owner: true, lineageGraph: EMPTY });
    expect(within(card(section)).queryByTestId(listId)).toBeNull();
    expect(within(card(section)).getByText("None")).toBeInTheDocument();
  });

  it("shows the owner no false None while lineage loads or is null", () => {
    panel({ owner: true, lineageGraph: null, loading: true });
    expect(within(card(section)).queryByText("None")).toBeNull();
    expect(within(card(section)).getByText("Loading lineage…")).toBeInTheDocument();
  });

  it("shows the owner no false None when lineage is null and not loading", () => {
    panel({ owner: true, lineageGraph: null });
    expect(card(section)).toBeInTheDocument();
    expect(within(card(section)).queryByText("None")).toBeNull();
  });
});

describe("the Input Ports card for any other reader", () => {
  const listId = "data-product-detail-inputs-pet_health";

  it("tells them the list is the owner's, with no list and no None", () => {
    panel({ owner: false, lineageGraph: FULL });
    const c = card("inputPorts");
    expect(within(c).queryByTestId(listId)).toBeNull();
    expect(within(c).queryByText("None")).toBeNull();
    expect(within(c).getByText(/visible to the product's owner/i)).toBeInTheDocument();
  });

  it("tells them the same while lineage is still unread", () => {
    panel({ owner: false, lineageGraph: null, loading: true });
    expect(
      within(card("inputPorts")).getByText(/visible to the product's owner/i),
    ).toBeInTheDocument();
  });
});

describe("the Consumers card", () => {
  it("is not shown at all to a reader who does not own the product", () => {
    panel({ owner: false, lineageGraph: FULL, seesConsumers: false });
    expect(screen.queryByTestId("data-product-card-consumers-pet_health")).toBeNull();
    expect(screen.queryByTestId("data-product-detail-consumers-pet_health")).toBeNull();
  });

  it("is not shown to a holder of the owner's view who does not own the product", () => {
    // The owner's view of Input Ports (REQ-2188's data_product_rw elsewhere) is not ownership.
    panel({ owner: true, lineageGraph: FULL, seesConsumers: false });
    expect(screen.queryByTestId("data-product-card-consumers-pet_health")).toBeNull();
    expect(
      within(card("inputPorts")).getByTestId("data-product-detail-inputs-pet_health"),
    ).toBeInTheDocument();
  });

  it("is shown, with its list, to the product's owner", () => {
    panel({ owner: true, lineageGraph: FULL, seesConsumers: true });
    expect(
      within(card("consumers")).getByTestId("data-product-detail-consumers-pet_health"),
    ).toBeInTheDocument();
  });
});
