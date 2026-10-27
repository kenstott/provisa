// Copyright (c) 2026 Kenneth Stott
// Canary: 225457fc-d564-4c7d-942d-0d7ad3e83316
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Each Related Tables entry -- its table, its relationship types and paths, what it goes through,
// its description -- shows at most three lines, its ellipsis opening the entry in place and
// closing it again (ClampedDetail). jsdom lays nothing out, so an entry's height is set here.

import { afterEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import { DataProductDetailPanel } from "../DataProductDetailPanel";
import { lifecycleProps } from "./lifecycleProps";
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

const EDGE: CoreEdge = {
  kind: "relationship",
  direction: null,
  target_product_id: null,
  target_table_id: 12,
  target_relation: null,
  direct: false,
  hops: 3,
  path_count: 2,
  aliases: "HAS_OWNER, VISITED_BY",
  supporting_tables: "pet-store.visits, pet-store.appointments, pet-store.clinics",
  vias: null,
  target_table: {
    id: 12,
    domain_id: "pet-store",
    table_name: "owners",
    description: "Everyone who has adopted or fostered a pet, with how to reach them.",
  },
};

function laidOut(lines: number) {
  vi.spyOn(HTMLElement.prototype, "scrollHeight", "get").mockReturnValue(lines * 20);
  vi.spyOn(HTMLElement.prototype, "clientHeight", "get").mockReturnValue(Math.min(lines, 3) * 20);
}

afterEach(() => vi.restoreAllMocks());

function panel() {
  render(
    <DataProductDetailPanel
      p={PRODUCT}
      core={{
        product: PRODUCT,
        edges: [EDGE],
        queries: { graphql: "", sql: "", jsonapi: "" },
      }}
      coreError={null}
      reader={null}
      canEdit={false}
      canSeeLineage={false}
      lineageLoading={false}
      lineageError=""
      lineageGraph={null}
      inputPorts={[]}
      onEdit={vi.fn()}
      onDelete={vi.fn()}
      {...lifecycleProps()}
    />,
  );
}

describe("a Related Tables entry", () => {
  it("is held to three lines, its ellipsis opening the whole entry in place and closing it", () => {
    laidOut(6);
    panel();
    const toggle = screen.getByTestId("data-product-related-pet_health-12");
    const text = screen.getByTestId("data-product-related-pet_health-12-text");
    expect(text).toHaveTextContent("pet-store.owners");
    expect(text).toHaveTextContent("HAS_OWNER, VISITED_BY");
    expect(text).toHaveTextContent("Everyone who has adopted");
    expect(toggle).toHaveAttribute("aria-expanded", "false");
    expect(text).toHaveStyle({ overflow: "hidden" });
    fireEvent.click(toggle);
    expect(toggle).toHaveAttribute("aria-expanded", "true");
    expect(text).not.toHaveStyle({ overflow: "hidden" });
    fireEvent.click(toggle);
    expect(toggle).toHaveAttribute("aria-expanded", "false");
  });

  it("that fits in three lines shows no ellipsis", () => {
    laidOut(2);
    panel();
    expect(screen.queryByTestId("data-product-related-pet_health-12")).toBeNull();
    expect(screen.getByTestId("data-product-related-pet_health-12-text")).toHaveTextContent(
      "pet-store.owners",
    );
  });
});
