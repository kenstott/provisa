// Copyright (c) 2026 Kenneth Stott
// Canary: 6e1f9a24-3b7c-4d08-8e5a-2c9f4b16d7a3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

/**
 * REQ-1659: `?product=<id>` is the deep link a catalog listing (Snowflake Horizon's documentation
 * box, an Analytics Hub listing) carries back to the Data Products page. It opens that product,
 * ahead of the last-expanded row remembered in localStorage, and expanding a row writes the
 * parameter so the address bar is itself a link to the open product.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "../../test-utils/render";

vi.mock("../../context/AuthContext", () => ({
  useAuth: () => ({ role: { id: "admin", capabilities: [] } }),
}));
vi.mock("../../hooks/useCapability", () => ({
  useCapability: () => false,
}));
vi.mock("../../api/actions", () => ({
  fetchActions: () => Promise.resolve({ functions: [] }),
  saveFunction: vi.fn(),
}));
vi.mock("../../api/lineage", () => ({
  fetchFederationGraph: () => Promise.resolve({ nodes: [], edges: [] }),
}));
vi.mock("../../api/glossary", () => ({
  fetchRelatedGlossaryTerms: () => Promise.resolve([]),
}));
vi.mock("../data-products/DataProductDetailPanel", () => ({
  DataProductDetailPanel: ({ p }: { p: { id: string } }) => (
    <div data-testid="detail-panel">{p.id}</div>
  ),
}));

const products = [
  {
    id: "pet_health",
    domainId: "care",
    name: "Pet Health",
    purpose: "Pet health records",
    limitations: "",
    usage: "",
    customProperties: {},
  },
  {
    id: "workforce_ops",
    domainId: "ops",
    name: "Workforce Ops",
    purpose: "Shelter staffing",
    limitations: "",
    usage: "",
    customProperties: {},
  },
];

// One stable empty list: the page's related-terms effect depends on `tables`, and a fresh array
// per render would re-fire it forever (the real hook hands back Apollo's cached reference).
const none: never[] = [];

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useDataProducts: () => ({ dataProducts: products, loading: false, error: null, refetch: vi.fn() }),
  useDomains: () => ({ domains: none, loading: false, refetch: vi.fn() }),
  useTables: () => ({ tables: none, loading: false, refetch: vi.fn() }),
  useSources: () => ({ sources: none, loading: false, refetch: vi.fn() }),
  useRelationships: () => ({ relationships: none, loading: false, refetch: vi.fn() }),
  useRoles: () => ({ roles: none, loading: false, refetch: vi.fn() }),
  useCreateDataProduct: () => ({ createDataProduct: vi.fn(), loading: false }),
  useDeleteDataProduct: () => ({ deleteDataProduct: vi.fn(), loading: false }),
  useUpdateTable: () => ({ updateTable: vi.fn(), loading: false }),
}));

import { DataProductsPage, EXPANDED_STORAGE_KEY } from "../DataProductsPage";

describe("Data Products deep link (REQ-1659)", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it("opens the product named by ?product= ahead of the remembered row", async () => {
    localStorage.setItem(EXPANDED_STORAGE_KEY, "pet_health");
    render(<DataProductsPage />, { initialEntries: ["/data-products?product=workforce_ops"] });
    await waitFor(() => expect(screen.getByTestId("data-product-detail")).toBeInTheDocument());
    expect(screen.getByTestId("detail-panel")).toHaveTextContent("workforce_ops");
    expect(screen.getByTestId("data-products-row-workforce_ops")).toHaveAttribute(
      "aria-expanded",
      "true",
    );
    expect(screen.getByTestId("data-products-row-pet_health")).toHaveAttribute(
      "aria-expanded",
      "false",
    );
  });

  it("expanding a row writes ?product= so the address is a link to it", async () => {
    render(<DataProductsPage />, { initialEntries: ["/data-products"] });
    await waitFor(() => expect(screen.getByTestId("data-products-row-pet_health")).toBeInTheDocument());
    fireEvent.click(screen.getByTestId("data-products-row-pet_health"));
    await waitFor(() => expect(screen.getByTestId("detail-panel")).toHaveTextContent("pet_health"));
    expect(localStorage.getItem(EXPANDED_STORAGE_KEY)).toBe("pet_health");
    fireEvent.click(screen.getByTestId("data-products-row-pet_health"));
    await waitFor(() => expect(screen.queryByTestId("detail-panel")).not.toBeInTheDocument());
    expect(localStorage.getItem(EXPANDED_STORAGE_KEY)).toBeNull();
  });
});
