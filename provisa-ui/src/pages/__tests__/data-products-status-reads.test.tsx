// Copyright (c) 2026 Kenneth Stott
// Canary: 5d0b7c1e-9a24-4f3b-8e61-2c7a4f90d8b3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

/**
 * REQ-1634 STATUS: what a status change reads again. Publishing, deprecating and restoring change
 * a product's status and nothing it is drawn from, so the page reads the products, their findings
 * and the panel's core again, and nothing else: the tables (every registered table), the commands,
 * the lineage and the contracts are what a retirement changes, by moving the members to the
 * successor, so only a retirement reads them again.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../test-utils/render";

const fetchProductContracts = vi.fn((_id: string) => Promise.resolve({}));
const fetchProductLineage = vi.fn(() =>
  Promise.resolve({
    nodes: [],
    edges: [],
    outputs: [],
    members: [],
    inputs: [],
    dq: { expectations: [], promises: [], related: [], unattached: [] },
  }),
);
const fetchActions = vi.fn(() => Promise.resolve({ functions: [] }));
const refetchTables = vi.fn(() => Promise.resolve());
const reloadProducts = vi.fn();
const refetchFindings = vi.fn(() => Promise.resolve());
let coreData: unknown = null;

vi.mock("../data-products/useProductCore", () => ({
  useMetaProducts: () => ({
    data: products,
    unreadable: false,
    error: null,
    loading: false,
    reload: reloadProducts,
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
  fetchActions: () => fetchActions(),
  saveFunction: vi.fn(),
}));
vi.mock("../../api/dataProductContracts", () => ({
  fetchProductContracts: (id: string) => fetchProductContracts(id),
}));
vi.mock("../../api/lineage", () => ({
  fetchProductLineage: () => fetchProductLineage(),
}));
vi.mock("../../api/glossary", () => ({
  fetchRelatedGlossaryTerms: () => Promise.resolve([]),
}));
vi.mock("../data-products/DataProductDetailPanel", () => ({
  DataProductDetailPanel: ({
    p,
    onStatusChanged,
  }: {
    p: { id: string };
    onStatusChanged: (message: string, membersMoved: boolean) => void;
  }) => (
    <div data-testid="detail-panel">
      {p.id}
      <button data-testid="published" onClick={() => onStatusChanged("published", false)} />
      <button data-testid="retired" onClick={() => onStatusChanged("retired", true)} />
    </div>
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
  useDataProductFindings: () => ({ findings: new Map(), refetch: refetchFindings, error: undefined }),
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
  useTables: () => ({ tables: none, loading: false, refetch: refetchTables }),
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

const reads = [
  fetchProductContracts,
  fetchProductLineage,
  fetchActions,
  refetchTables,
  reloadProducts,
  refetchFindings,
];

async function opened() {
  coreData = core([member({ version: "1.0.0", status: "active" })]);
  render(<DataProductsPage />, { initialEntries: ["/data-products?product=pet_health"] });
  await screen.findByTestId("detail-panel");
  await waitFor(() => expect(fetchProductContracts).toHaveBeenCalledTimes(1));
  await waitFor(() => expect(fetchProductLineage).toHaveBeenCalledTimes(1));
  await waitFor(() => expect(fetchActions).toHaveBeenCalledTimes(1));
  for (const read of reads) read.mockClear();
}

describe("a status change's reads (REQ-1634 STATUS)", () => {
  beforeEach(() => {
    localStorage.clear();
    for (const read of reads) read.mockClear();
  });

  it("reads the products and their findings, not the tables, commands, lineage or contracts", async () => {
    await opened();
    fireEvent.click(screen.getByTestId("published"));
    await waitFor(() => expect(reloadProducts).toHaveBeenCalledTimes(1));
    expect(refetchFindings).toHaveBeenCalledTimes(1);
    await new Promise((r) => setTimeout(r, 0));
    expect(refetchTables).not.toHaveBeenCalled();
    expect(fetchActions).not.toHaveBeenCalled();
    expect(fetchProductLineage).not.toHaveBeenCalled();
    expect(fetchProductContracts).not.toHaveBeenCalled();
  });

  it("after a retirement, which moves the members, reads them all again", async () => {
    await opened();
    fireEvent.click(screen.getByTestId("retired"));
    await waitFor(() => expect(fetchProductLineage).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(fetchProductContracts).toHaveBeenCalledTimes(1));
    expect(reloadProducts).toHaveBeenCalledTimes(1);
    expect(refetchFindings).toHaveBeenCalledTimes(1);
    expect(refetchTables).toHaveBeenCalledTimes(1);
    expect(fetchActions).toHaveBeenCalledTimes(1);
  });
});
