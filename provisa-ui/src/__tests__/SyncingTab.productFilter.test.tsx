// Copyright (c) 2026 Kenneth Stott
// Canary: e5c07a92-3d1b-4f68-9a24-b81f6d0c3e57
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-2259: Context Sync can be narrowed to the entries that publish chosen data products, from
 * the record of every publish each entry carries. The Data Products list links here with
 * ?product=<id>, so the address opens the list narrowed; the filter is the standard MultiSelect.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, within } from "../test-utils/render";
import { SyncingTab } from "../components/admin/syncing/SyncingTab";
import type { ProductSyncStatus, SyncEntry, SyncingState } from "../api/syncing";

vi.mock("../api/syncing", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/syncing")>()),
  fetchSyncing: vi.fn(),
}));

vi.mock("../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminQueries")>()),
  useDomains: () => ({ domains: [{ id: "sales" }], loading: false, refetch: vi.fn() }),
}));

import { blankEntry, entryUpdate, fetchSyncing } from "../api/syncing";

const mockFetch = vi.mocked(fetchSyncing);
const AT = "2026-11-22T14:00:00+00:00";

const published = (entry: string, product: string): ProductSyncStatus => ({
  entry,
  product_id: product,
  outcome: "published",
  error: null,
  published_at: AT,
});

const catalog = (id: string, products: string[]): SyncEntry => ({
  ...blankEntry("data_catalog"),
  id,
  provider: "openlineage",
  endpoint: `http://${id}`,
  role: "steward",
  products: products.map((p) => published(id, p)),
});

const ENTRIES = [
  catalog("marquez", ["orders", "revenue"]),
  catalog("purview", ["revenue"]),
  catalog("lab", []),
];

function state(entries: SyncEntry[]): SyncingState {
  return {
    entitled: true,
    required_tier: "premium",
    providers: ["openlineage"],
    beta_providers: [],
    publishable_roles: ["steward"],
    reconcile_cron: "",
    entries,
  };
}

const rows = () =>
  within(screen.getByTestId("syncing-section-data_catalog"))
    .queryAllByTestId(/^syncing-row-[a-z]+$/)
    .map((r) => r.getAttribute("data-testid")?.replace("syncing-row-", ""));

const filter = () => screen.getByRole("textbox", { name: "Data products" });

describe("Context Sync's data product filter", () => {
  beforeEach(() => {
    mockFetch.mockReset();
    mockFetch.mockResolvedValue(state(ENTRIES));
  });

  it("lists every entry until a product is chosen", async () => {
    render(<SyncingTab />);
    await screen.findByTestId("syncing-row-marquez");
    expect(rows()).toEqual(["lab", "marquez", "purview"]);
  });

  it("offers every product a recorded publish carried", async () => {
    render(<SyncingTab />);
    await screen.findByTestId("syncing-row-marquez");
    await userEvent.click(filter());
    const options = await screen.findAllByRole("option", { hidden: true });
    expect(options.map((o) => o.textContent)).toEqual(["orders", "revenue"]);
  });

  it("narrows the list to the entries that publish a chosen product", async () => {
    render(<SyncingTab />);
    await screen.findByTestId("syncing-row-marquez");
    await userEvent.click(filter());
    await userEvent.click(await screen.findByRole("option", { name: "revenue", hidden: true }));

    expect(rows()).toEqual(["marquez", "purview"]);
  });

  it("opens narrowed to the product the address names, as the Data Products link does", async () => {
    render(<SyncingTab />, { initialEntries: ["/context/sync?product=orders"] });
    await screen.findByTestId("syncing-row-marquez");

    expect(rows()).toEqual(["marquez"]);
    // The other headings say no entry publishes it, not that none exist.
    expect(screen.getByTestId("syncing-none-ontology")).toHaveTextContent(
      "No entry publishes the chosen data products.",
    );
  });

  it("shows a product no publish carried yet as chosen, with no entry under it", async () => {
    render(<SyncingTab />, { initialEntries: ["/context/sync?product=stock"] });
    await screen.findByTestId("syncing-none-data_catalog");

    expect(rows()).toEqual([]);
    expect(screen.getByTestId("syncing-product-filter")).toHaveTextContent("stock");
  });

  it("never sends the products back when an entry is saved", () => {
    expect(entryUpdate(ENTRIES[0])).not.toHaveProperty("products");
  });
});
