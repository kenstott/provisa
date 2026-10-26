// Copyright (c) 2026 Kenneth Stott
// Canary: 3b9e7d15-2c48-4f06-a1d3-6e8c0f2b9a47
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

/**
 * REQ-1634: a new data product is created and an open one is updated. A create refuses an id that
 * exists, so an edit saved through it would be refused; the page saves an edit as an update, and
 * a refused save keeps the form open saying why.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "../../test-utils/render";

// REQ-2127: the list and the panel core are read from the meta domain; the reads are held by
// their own tests (productCore.test.ts).
vi.mock("../data-products/useProductCore", () => ({
  useMetaProducts: () => ({
    data: products,
    unreadable: false,
    error: null,
    loading: false,
    reload: vi.fn(),
  }),
  useProductCore: () => ({
    data: null,
    unreadable: false,
    error: null,
    loading: false,
    reload: vi.fn(),
  }),
  useMetaReader: () => null,
}));
vi.mock("../../components/graph3d/GraphPanel", () => ({
  ProductMapButton: () => null,
  GraphPanel: () => null,
}));
vi.mock("../../context/AuthContext", () => ({
  useAuth: () => ({
    role: { id: "admin", capabilities: [] },
    selectedRoles: [{ id: "admin" }],
    availableRoles: [],
  }),
}));
vi.mock("../../hooks/useCapability", () => ({
  useCapability: () => true,
}));
vi.mock("../../api/actions", () => ({
  fetchActions: () => Promise.resolve({ functions: [] }),
  saveFunction: vi.fn(),
}));
vi.mock("../../api/lineage", () => ({
  fetchProductLineage: () =>
    Promise.resolve({
      nodes: [],
      edges: [],
      outputs: [],
      members: [],
      inputs: [],
      dq: { expectations: [], promises: [], related: [], unattached: [] },
    }),
}));
vi.mock("../../api/glossary", () => ({
  fetchRelatedGlossaryTerms: () => Promise.resolve([]),
}));
vi.mock("../data-products/DataProductDetailPanel", () => ({
  DataProductDetailPanel: ({ p, onEdit }: { p: { id: string }; onEdit: () => void }) => (
    <button data-testid={`edit-${p.id}`} onClick={onEdit}>
      {p.id}
    </button>
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
const DOMAINS = [{ id: "care", description: "" }];
const saves = vi.hoisted(() => ({
  create: vi.fn<(input: Record<string, unknown>) => Promise<Record<string, unknown>>>(),
  update: vi.fn<(input: Record<string, unknown>) => Promise<Record<string, unknown>>>(),
}));

vi.mock("../../hooks/useDataProductMutations", () => ({
  useCreateDataProduct: () => ({ createDataProduct: saves.create, loading: false }),
  useUpdateDataProduct: () => ({ updateDataProduct: saves.update, loading: false }),
  useDeleteDataProduct: () => ({ deleteDataProduct: vi.fn(), loading: false }),
}));

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useDomains: () => ({ domains: DOMAINS, loading: false, refetch: vi.fn() }),
  useTables: () => ({ tables: none, loading: false, refetch: vi.fn() }),
  useSources: () => ({ sources: none, loading: false, refetch: vi.fn() }),
  useRelationships: () => ({ relationships: none, loading: false, refetch: vi.fn() }),
  useRoles: () => ({ roles: none, loading: false, refetch: vi.fn() }),
  useUpdateTable: () => ({ updateTable: vi.fn(), loading: false }),
}));

import { DataProductsPage } from "../DataProductsPage";

describe("Data Products: saving (REQ-1634)", () => {
  beforeEach(() => {
    localStorage.clear();
    saves.create.mockReset();
    saves.update.mockReset();
  });

  it("saves an open product as an update, never a create", async () => {
    saves.update.mockResolvedValueOnce({ success: true, message: "updated" });
    render(<DataProductsPage />, { initialEntries: ["/data-products?product=pet_health"] });
    fireEvent.click(await screen.findByTestId("edit-pet_health"));
    fireEvent.click(await screen.findByTestId("data-product-save-button"));
    await waitFor(() => expect(saves.update).toHaveBeenCalledTimes(1));
    expect(saves.update.mock.calls[0][0]).toMatchObject({ id: "pet_health", domainId: "care" });
    expect(saves.create).not.toHaveBeenCalled();
  });

  it("creates a new product, and keeps the form open when the create is refused", async () => {
    saves.create.mockResolvedValueOnce({
      success: false,
      message: "Data product 'pet_health' already exists",
      code: "schema.data_product_id_taken",
    });
    render(<DataProductsPage />, { initialEntries: ["/data-products"] });
    fireEvent.click(await screen.findByTestId("data-products-new-button"));
    fireEvent.change(screen.getByTestId("data-product-id-input"), {
      target: { value: "pet_health" },
    });
    fireEvent.change(screen.getByTestId("data-product-name-input"), {
      target: { value: "Pet Health" },
    });
    fireEvent.click(screen.getByTestId("data-product-domain-input"));
    fireEvent.click(await screen.findByRole("option", { name: "care" }));
    fireEvent.click(screen.getByTestId("data-product-save-button"));
    expect(await screen.findByTestId("data-product-form-error")).toHaveTextContent(
      "Data product 'pet_health' already exists",
    );
    expect(saves.update).not.toHaveBeenCalled();
    expect(screen.getByTestId("data-product-form")).toBeInTheDocument();
  });
});
