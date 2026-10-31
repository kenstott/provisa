// Copyright (c) 2026 Kenneth Stott
// Canary: 8c1f4a37-5d29-4e60-9b7a-2e3d6f08b415
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2158: a contract's cards flow in 320px columns -- Details, Versions & changes, SLAs & freshness,
// Support and team, Servers, Pricing, Custom properties and tags -- and the Columns then Promises
// cards run full width beneath them. Who consumes a port is the owner's, not the contract's
// (REQ-2160). jsdom lays nothing out, so this proves the structure and the column styles that make
// the layout.

import { describe, expect, it } from "vitest";
import { render, screen } from "../../../test-utils/render";
import { ContractView } from "../ContractView";
import type { OdcsContract } from "../../../api/dataProductContracts";
import type { ProductDqEntry } from "../../../api/lineage";

const CONTRACT = { id: "c1", version: "1.0.0", status: "active" } as unknown as OdcsContract;

const promise = (id: number, table: string, outcome: "pass" | "fail" | null): ProductDqEntry => ({
  checker_table_id: id,
  domain_id: "pet-store",
  table_name: `${table}_quality`,
  alias: null,
  description: `Quality scan of ${table}`,
  owner_product_id: "pet_health",
  checked_table_id: id + 100,
  checked_table: { id: id + 100, domain_id: "pet-store", table_name: table, alias: null },
  rules: [{ check_type: "expect_column_values_to_not_be_null", column_name: "id", args: {} }],
  latest_outcome: outcome,
  latest_run_at: null,
});
const PROMISES = [
  promise(3, "pets", "pass"),
  promise(4, "vets", "fail"),
  promise(5, "visits", null),
  promise(6, "owners", "pass"),
];

function renderView() {
  render(
    <ContractView
      contract={CONTRACT}
      versions={[]}
      tableId={7}
      promises={PROMISES}
    />,
  );
}

describe("a contract's card layout", () => {
  it("flows the top cards in 320px columns, with Columns then Promises full width below", () => {
    renderView();
    const root = screen.getByTestId("data-contract-7");
    const [flow, columns, promises] = Array.from(root.children) as HTMLElement[];
    expect(flow.style.columnWidth).toBe("320px");
    expect(Array.from(flow.children).map((c) => c.getAttribute("data-testid"))).toEqual(
      [
        "details",
        "versions",
        "sla",
        "support",
        "servers",
        "pricing",
        "custom",
      ].map((k) => `data-contract-7-${k}`),
    );
    expect(columns).toBe(screen.getByTestId("data-contract-7-fields"));
    expect(promises).toBe(screen.getByTestId("data-contract-7-quality"));
    for (const wide of [columns, promises]) {
      expect(wide.style.breakInside).toBe("");
      expect(flow.contains(wide)).toBe(false);
    }
  });

  it("has no Consumers card", () => {
    renderView();
    expect(screen.queryByTestId("data-contract-7-consumers")).toBeNull();
    expect(screen.queryByTestId("data-contract-consumers-7")).toBeNull();
    expect(screen.getByTestId("data-contract-7")).not.toHaveTextContent("Consumers");
  });

  it("heads each further section as a card of its own, none of them inside Details", () => {
    renderView();
    const details = screen.getByTestId("data-contract-7-details");
    const labels = {
      support: "Support and team",
      servers: "Servers",
      pricing: "Pricing",
      custom: "Custom properties and tags",
    };
    for (const [key, label] of Object.entries(labels)) {
      const card = screen.getByTestId(`data-contract-7-${key}`);
      expect(details.contains(card)).toBe(false);
      expect(screen.getByTestId(`data-contract-7-${key}-toggle`)).toHaveTextContent(label);
    }
  });
});
