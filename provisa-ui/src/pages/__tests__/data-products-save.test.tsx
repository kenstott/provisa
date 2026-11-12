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
vi.mock("../../components/graph3d/OntologyGraph", () => ({
  ProductMapButton: () => null,
  OntologySnippet: () => null,
}));
vi.mock("../../context/AuthContext", () => ({
  useAuth: () => ({
    role: { id: "admin", capabilities: [] },
    selectedRoles: [{ id: "admin", effectiveCapabilities: [], domain_access: [] }],
    availableRoles: [{ id: "admin", capabilities: ["data_product_rw"] }],
  }),
}));
vi.mock("../../hooks/useCapability", () => ({
  useCapability: () => true,
}));
vi.mock("../../api/actions", () => ({
  fetchActions: () => Promise.resolve({ functions: [] }),
  saveFunction: vi.fn(),
}));
// REQ-2158: the open product's ODCS contracts; none here.
vi.mock("../../api/dataProductContracts", () => ({
  fetchProductContract: () => Promise.resolve({}),
  fetchContractVersions: () => Promise.resolve([]),
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
    supportContact: "vets@petstore.example",
    purpose: "Pet health records",
    limitations: "",
    usage: "",
    customProperties: {},
  },
  {
    id: "care_ops",
    domainId: "care",
    name: "Care Ops",
    purpose: "Care operations",
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
  members:
    vi.fn<
      (id: string, tableIds: number[], commands: string[]) => Promise<Record<string, unknown>>
    >(),
  // The tables the page lists, each with the products it belongs to (REQ-1634).
  tables: [] as unknown[],
}));

vi.mock("../../hooks/useDataProductMutations", () => ({
  useCreateDataProduct: () => ({ createDataProduct: saves.create, loading: false }),
  useUpdateDataProduct: () => ({ updateDataProduct: saves.update, loading: false }),
  useDeleteDataProduct: () => ({ deleteDataProduct: vi.fn(), loading: false }),
  useDeleteDataProducts: () => vi.fn(),
  useSetDataProductMembers: () => ({ setDataProductMembers: saves.members, loading: false }),
  useSaveDataContract: () => ({ saveDataContract: vi.fn(), loading: false }),
  useDataProductFindings: () => ({ findings: new Map(), refetch: vi.fn(), error: undefined }),
  useDataProductStatus: () => ({
    publish: vi.fn(),
    deprecate: vi.fn(),
    retire: vi.fn(),
    restore: vi.fn(),
  }),
}));

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useDomains: () => ({ domains: DOMAINS, loading: false, refetch: vi.fn() }),
  useTables: () => ({ tables: saves.tables, loading: false, refetch: vi.fn() }),
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
    saves.members.mockReset();
    saves.tables = [];
  });

  it("saves an open product as an update, never a create", async () => {
    saves.update.mockResolvedValueOnce({ success: true, message: "updated" });
    render(<DataProductsPage />, { initialEntries: ["/data-products?product=pet_health"] });
    fireEvent.click(await screen.findByTestId("edit-pet_health"));
    fireEvent.click(await screen.findByTestId("data-product-save-button"));
    await waitFor(() => expect(saves.update).toHaveBeenCalledTimes(1));
    // Every field the form holds is saved: the support contact stays as it was.
    expect(saves.update.mock.calls[0][0]).toMatchObject({
      id: "pet_health",
      domainId: "care",
      supportContact: "vets@petstore.example",
    });
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

  // REQ-1634: a table is in any number of products. Two tables in the "care" domain: "pets" is in
  // both Pet Health and Care Ops, "vets" in Care Ops alone.
  const table = (id: number, tableName: string, productIds: string[]) => ({
    id,
    domainId: "care",
    schemaName: "care",
    tableName,
    alias: null,
    dqContract: null,
    productIds,
    columns: [],
  });
  const openEditor = async (product: string) => {
    render(<DataProductsPage />, { initialEntries: [`/data-products?product=${product}`] });
    fireEvent.click(await screen.findByTestId(`edit-${product}`));
    return screen.findByTestId("data-product-tables-input");
  };

  it("shows a table that is in two products as a member of each", async () => {
    saves.tables = [table(1, "pets", ["pet_health", "care_ops"]), table(2, "vets", ["care_ops"])];
    const picker = await openEditor("pet_health");
    const field = picker.closest(".mantine-InputWrapper-root") as HTMLElement;
    expect(field).toHaveTextContent("pets");
    expect(field).not.toHaveTextContent("vets");
  });

  it("shows the other product's members too: the shared table is under it as well", async () => {
    saves.tables = [table(1, "pets", ["pet_health", "care_ops"]), table(2, "vets", ["care_ops"])];
    const picker = await openEditor("care_ops");
    const field = picker.closest(".mantine-InputWrapper-root") as HTMLElement;
    expect(field).toHaveTextContent("pets");
    expect(field).toHaveTextContent("vets");
  });

  it("saves no membership when the members are as they were", async () => {
    saves.update.mockResolvedValueOnce({ success: true, message: "updated" });
    saves.tables = [table(1, "pets", ["pet_health", "care_ops"])];
    await openEditor("pet_health");
    fireEvent.click(await screen.findByTestId("data-product-save-button"));
    await waitFor(() => expect(saves.update).toHaveBeenCalledTimes(1));
    expect(saves.members).not.toHaveBeenCalled();
  });

  it("sets the product's whole member list on the product, leaving other products' alone", async () => {
    saves.update.mockResolvedValueOnce({ success: true, message: "updated" });
    saves.members.mockResolvedValueOnce({ success: true, message: "set" });
    saves.tables = [table(1, "pets", ["pet_health", "care_ops"]), table(2, "vets", ["care_ops"])];
    const picker = await openEditor("pet_health");
    fireEvent.focus(picker);
    fireEvent.click(picker);
    fireEvent.click(await screen.findByRole("option", { name: /vets/, hidden: true }));
    fireEvent.click(screen.getByTestId("data-product-save-button"));
    await waitFor(() => expect(saves.members).toHaveBeenCalledTimes(1));
    // Both tables, as numbers; "pets" stays in Care Ops, which this save does not name.
    expect(saves.members).toHaveBeenCalledWith("pet_health", [1, 2], []);
  });

  it("keeps the form open, saying why, when the membership is refused", async () => {
    saves.update.mockResolvedValueOnce({ success: true, message: "updated" });
    saves.members.mockResolvedValueOnce({
      success: false,
      message: "table 2 is in another domain",
      code: "data_product.members.domain",
    });
    saves.tables = [table(1, "pets", ["pet_health"]), table(2, "vets", [])];
    const picker = await openEditor("pet_health");
    fireEvent.focus(picker);
    fireEvent.click(picker);
    fireEvent.click(await screen.findByRole("option", { name: /vets/, hidden: true }));
    fireEvent.click(screen.getByTestId("data-product-save-button"));
    expect(await screen.findByTestId("data-product-form-error")).toHaveTextContent(
      "table 2 is in another domain",
    );
  });
});
