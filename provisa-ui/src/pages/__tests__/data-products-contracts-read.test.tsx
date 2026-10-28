// Copyright (c) 2026 Kenneth Stott
// Canary: 14b2e2fe-23ec-4c80-a5a6-b3801ab1b591
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

/**
 * REQ-2158: the page reads a product's ODCS contracts (contracts.json) only for a product whose
 * panel shows a Contracts card. A product with no contract, such as a new one with no members, has
 * no document on the server (404), and reading it anyway would put that failure in the console on
 * every status change.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "../../test-utils/render";

const fetchProductContracts = vi.fn((_id: string) => Promise.resolve({}));
let coreData: unknown = null;

vi.mock("../data-products/useProductCore", () => ({
  useMetaProducts: () => ({
    data: products,
    unreadable: false,
    error: null,
    loading: false,
    reload: vi.fn(),
  }),
  useProductCore: () => ({
    data: coreData,
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
  useCapability: () => false,
}));
vi.mock("../../api/actions", () => ({
  fetchActions: () => Promise.resolve({ functions: [] }),
  saveFunction: vi.fn(),
}));
vi.mock("../../api/dataProductContracts", () => ({
  fetchProductContracts: (id: string) => fetchProductContracts(id),
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
  DataProductDetailPanel: ({ p }: { p: { id: string } }) => (
    <div data-testid="detail-panel">{p.id}</div>
  ),
}));

const products = [
  {
    id: "pet_health",
    domainId: "care",
    name: "Pet Health",
    status: "live",
    purpose: "Pet health records",
    limitations: "",
    usage: "",
    customProperties: {},
  },
];

const none: never[] = [];

vi.mock("../../hooks/useDataProductMutations", () => ({
  useCreateDataProduct: () => ({ createDataProduct: vi.fn(), loading: false }),
  useUpdateDataProduct: () => ({ updateDataProduct: vi.fn(), loading: false }),
  useDeleteDataProduct: () => ({ deleteDataProduct: vi.fn(), loading: false }),
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
  useDomains: () => ({ domains: none, loading: false, refetch: vi.fn() }),
  useTables: () => ({ tables: none, loading: false, refetch: vi.fn() }),
  useSources: () => ({ sources: none, loading: false, refetch: vi.fn() }),
  useRelationships: () => ({ relationships: none, loading: false, refetch: vi.fn() }),
  useRoles: () => ({ roles: none, loading: false, refetch: vi.fn() }),
  useUpdateTable: () => ({ updateTable: vi.fn(), loading: false }),
}));

import { DataProductsPage } from "../DataProductsPage";

const member = (contract: unknown) => ({
  id: 7,
  source_id: "pg",
  domain_id: "care",
  schema_name: "public",
  table_name: "pets",
  alias: null,
  description: null,
  dq_contract: null,
  contract,
});

const core = (tables: unknown[]) => ({
  product: products[0],
  tables,
  queries: { graphql: "", sql: "", jsonapi: "" },
});

describe("the open product's ODCS contracts (REQ-2158)", () => {
  beforeEach(() => {
    localStorage.clear();
    fetchProductContracts.mockClear();
  });

  it("are not read for a product with no member", async () => {
    coreData = core([]);
    render(<DataProductsPage />, { initialEntries: ["/data-products?product=pet_health"] });
    await screen.findByTestId("detail-panel");
    // The lineage read starts in the same commit as the contracts read would.
    await new Promise((r) => setTimeout(r, 0));
    expect(fetchProductContracts).not.toHaveBeenCalled();
  });

  it("are not read for a product whose members have no contract", async () => {
    coreData = core([member(null)]);
    render(<DataProductsPage />, { initialEntries: ["/data-products?product=pet_health"] });
    await screen.findByTestId("detail-panel");
    await new Promise((r) => setTimeout(r, 0));
    expect(fetchProductContracts).not.toHaveBeenCalled();
  });

  it("are read for a product with a member's contract", async () => {
    coreData = core([member({ version: "1.0.0", status: "active" })]);
    render(<DataProductsPage />, { initialEntries: ["/data-products?product=pet_health"] });
    await waitFor(() => expect(fetchProductContracts).toHaveBeenCalledWith("pet_health"));
  });
});
