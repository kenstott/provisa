// Copyright (c) 2026 Kenneth Stott
// Canary: 14b2e2fe-23ec-4c80-a5a6-b3801ab1b591
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

/**
 * REQ-2158 (amended 2026-10-29): a contract is read when its row in the product's contracts list
 * opens, not when the product does. Opening a product reads no contract, whatever its members --
 * a product with no contract has no document on the server (404), and one with several reads only
 * the rows its reader opens.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "../../test-utils/render";

const fetchProductContract = vi.fn((_id: string, _offer: string, _member: string) =>
  Promise.resolve({}),
);
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
vi.mock("../../components/graph3d/OntologyGraph", () => ({
  ProductMapButton: () => null,
  OntologySnippet: () => null,
}));
vi.mock("../../context/AuthContext", () => ({
  useAuth: () => ({
    role: { id: "admin", capabilities: [] },
    selectedRoles: [{ id: "admin", effectiveCapabilities: [], domain_access: [] }],
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
  fetchProductContract: (id: string, offer: string, member: string) =>
    fetchProductContract(id, offer, member),
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
  useDeleteDataProducts: () => vi.fn(),
  useSetDataProductMembers: () => ({ setDataProductMembers: vi.fn(), loading: false }),
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

// REQ-2180: a member's contracts are its offers' (read by the Offers tab), never a column of it.
const member = () => ({
  id: 7,
  sourceId: "pg",
  domainId: "care",
  schemaName: "public",
  tableName: "pets",
  alias: null,
  description: null,
  dqContract: null,
});

const core = (tables: unknown[]) => ({
  product: products[0],
  tables,
  queries: { graphql: "", sql: "", jsonapi: "" },
});

describe("the open product's ODCS contracts (REQ-2158)", () => {
  beforeEach(() => {
    localStorage.clear();
    fetchProductContract.mockClear();
  });

  it("are not read for a product with no member", async () => {
    coreData = core([]);
    render(<DataProductsPage />, { initialEntries: ["/data-products?product=pet_health"] });
    await screen.findByTestId("detail-panel");
    // The lineage read starts in the same commit as the contracts read would.
    await new Promise((r) => setTimeout(r, 0));
    expect(fetchProductContract).not.toHaveBeenCalled();
  });

  it("are not read for a product with a member, until a contract is opened", async () => {
    coreData = core([member()]);
    render(<DataProductsPage />, { initialEntries: ["/data-products?product=pet_health"] });
    await screen.findByTestId("detail-panel");
    await new Promise((r) => setTimeout(r, 0));
    expect(fetchProductContract).not.toHaveBeenCalled();
  });

  it("are not read when a product opens: an offer's contract rows read them", async () => {
    coreData = core([member()]);
    render(<DataProductsPage />, { initialEntries: ["/data-products?product=pet_health"] });
    await screen.findByTestId("detail-panel");
    await waitFor(() => expect(screen.getByTestId("detail-panel")).toHaveTextContent("pet_health"));
    await new Promise((r) => setTimeout(r, 0));
    expect(fetchProductContract).not.toHaveBeenCalled();
  });
});
