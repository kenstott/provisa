// Copyright (c) 2026 Kenneth Stott
// Canary: 8c1f4a37-5d29-4e60-9b7a-2e3d6f08b415
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2158: a contract's cards flow in 320px columns -- Details, Versions & changes, Columns, SLAs &
// freshness, Consumers -- and the Promises card runs full width beneath them, each promise one
// unbroken item, the items flowing in columns of the same width. jsdom lays nothing out, so this
// proves the structure and the column styles that make the layout.

import { describe, expect, it } from "vitest";
import { render, screen, within } from "../../../test-utils/render";
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
      consumers={[]}
    />,
  );
}

describe("a contract's card layout", () => {
  it("flows the top cards in 320px columns, in order, with Promises full width below", () => {
    renderView();
    const root = screen.getByTestId("data-contract-7");
    const [flow, promises] = Array.from(root.children) as HTMLElement[];
    expect(flow.style.columnWidth).toBe("320px");
    expect(Array.from(flow.children).map((c) => c.getAttribute("data-testid"))).toEqual(
      ["details", "versions", "fields", "sla", "consumers"].map((k) => `data-contract-7-${k}`),
    );
    expect(promises).toBe(screen.getByTestId("data-contract-7-quality"));
    expect(promises.style.breakInside).toBe("");
    expect(flow.contains(promises)).toBe(false);
  });

  it("renders each promise as its own unbroken item, flowing in columns of the same width", () => {
    renderView();
    const list = screen.getByTestId("data-contract-promises-7");
    expect(list).toHaveTextContent("4 promises");
    const items = PROMISES.map((p) =>
      screen.getByTestId(`data-contract-promises-7-item-${p.checker_table_id}`),
    );
    expect(new Set(items).size).toBe(4);
    for (const item of items) {
      expect(item.style.breakInside).toBe("avoid");
      expect(item.parentElement!.style.columnWidth).toBe("320px");
    }
    expect(within(items[1]).getByText("Quality scan of vets")).toBeInTheDocument();
    expect(items[0]).not.toContainElement(items[1]);
  });
});
