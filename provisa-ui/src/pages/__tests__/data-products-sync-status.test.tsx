// Copyright (c) 2026 Kenneth Stott
// Canary: 4b8e2d17-9f63-4a0c-b5d1-6e3a7c0f92d8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-2259: the Data Products list shows each product's sync status from the record of every
 * publish -- synced, failed on some entry, or not synced -- linking to Context Sync filtered to the
 * product, drawn as a link only for a viewer holding that page's right (org_settings).
 */

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "../../test-utils/render";

// REQ-2127: the list and the panel core are read from the meta domain; the reads are held by
// their own tests (productCore.test.ts).
vi.mock("../data-products/useProductCore", () => ({
  useMetaProducts: () => ({
    data: metaRead.unreadable ? null : products,
    unreadable: metaRead.unreadable,
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
// The viewer's rights: Context Sync's link is drawn only for a holder of org_settings.
const auth = vi.hoisted(() => ({ capabilities: ["data_product_read"] as string[] }));
vi.mock("../../context/AuthContext", () => ({
  useAuth: () => ({
    role: { id: "admin", capabilities: [] },
    selectedRoles: [{ id: "admin", effectiveCapabilities: [], domain_access: [] }],
    availableRoles: [],
    capabilities: auth.capabilities,
  }),
}));
// REQ-2259: each product's latest outcome per entry, as the status endpoint returns it.
const syncRead = vi.hoisted(() => ({
  fetchProductSyncStatus: vi.fn<() => Promise<Record<string, unknown[]>>>(),
}));
vi.mock("../../api/syncing", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/syncing")>()),
  fetchProductSyncStatus: syncRead.fetchProductSyncStatus,
}));
vi.mock("../../hooks/useCapability", () => ({
  useCapability: () => false,
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
  DataProductDetailPanel: ({ p }: { p: { id: string } }) => (
    <div data-testid="detail-panel">{p.id}</div>
  ),
}));

// Whether the acting role reads the meta domain's product list.
const metaRead = vi.hoisted(() => ({ unreadable: false }));

// REQ-2146: Pet Health has a finding; Workforce Ops has none.
const FINDINGS = new Map([
  [
    "pet_health",
    [{ kind: "no_owner", code: "data_product.finding.no_owner", message: "", params: {} }],
  ],
]);

const products = [
  {
    id: "eu_customers",
    domainId: "sales",
    name: "EU Customers",
    status: "live",
    owners: [],
    purpose: "Customers resident in the EU",
    limitations: "",
    usage: "",
    customProperties: {},
  },
  {
    id: "pet_health",
    domainId: "care",
    name: "Pet Health",
    status: "live",
    owners: [],
    purpose: "Pet health records",
    limitations: "",
    usage: "",
    customProperties: {},
  },
  {
    id: "workforce_ops",
    domainId: "ops",
    name: "Workforce Ops",
    status: "draft",
    owners: [],
    purpose: "Shelter staffing",
    limitations: "",
    usage: "",
    customProperties: {},
  },
];

// One stable empty list: the page's related-terms effect depends on `tables`, and a fresh array
// per render would re-fire it forever (the real hook hands back Apollo's cached reference).
const none: never[] = [];

vi.mock("../../hooks/useDataProductMutations", () => ({
  useCreateDataProduct: () => ({ createDataProduct: vi.fn(), loading: false }),
  useUpdateDataProduct: () => ({ updateDataProduct: vi.fn(), loading: false }),
  useDeleteDataProduct: () => ({ deleteDataProduct: vi.fn(), loading: false }),
  useDeleteDataProducts: () => vi.fn(),
  useSetDataProductMembers: () => ({ setDataProductMembers: vi.fn(), loading: false }),
  useDataProductFindings: () => ({ findings: FINDINGS, refetch: vi.fn(), error: undefined }),
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

// Each product's tags: EU Customers residency:eu and risk:high; Pet Health risk:high; Workforce
// Ops none. A table's tag is no product's.
vi.mock("../../hooks/useTagQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useTagQueries")>()),
  useTagAssignments: () => ({
    tagAssignments: ASSIGNMENTS,
    loading: false,
    error: undefined,
    refetch: vi.fn(),
  }),
}));
const ASSIGNMENTS = [
  { tagId: "residency:eu", objectType: "product", productId: "eu_customers" },
  { tagId: "risk:high", objectType: "product", productId: "eu_customers" },
  { tagId: "risk:high", objectType: "product", productId: "pet_health" },
  { tagId: "pii", objectType: "table", tableId: 4 },
];

import { DataProductsPage } from "../DataProductsPage";

const AT = "2026-11-22T14:00:00+00:00";
const STATUS = {
  eu_customers: [
    {
      entry: "marquez",
      product_id: "eu_customers",
      outcome: "published",
      error: null,
      published_at: AT,
    },
    {
      entry: "purview",
      product_id: "eu_customers",
      outcome: "published",
      error: null,
      published_at: AT,
    },
  ],
  pet_health: [
    {
      entry: "marquez",
      product_id: "pet_health",
      outcome: "published",
      error: null,
      published_at: AT,
    },
    {
      entry: "purview",
      product_id: "pet_health",
      outcome: "failed",
      error: "422 owner unknown",
      published_at: AT,
    },
  ],
};

beforeEach(() => {
  auth.capabilities = ["data_product_read"];
  syncRead.fetchProductSyncStatus.mockReset();
  syncRead.fetchProductSyncStatus.mockResolvedValue(STATUS);
});

describe("the Data Products list's sync status", () => {
  it("shows each product synced, failed on an entry, or not synced", async () => {
    render(<DataProductsPage />);

    await waitFor(() =>
      expect(screen.getByTestId("data-products-sync-eu_customers")).toHaveTextContent(
        "Synced to 2",
      ),
    );
    expect(screen.getByTestId("data-products-sync-pet_health")).toHaveTextContent("1 failed");
    expect(screen.getByTestId("data-products-sync-workforce_ops")).toHaveTextContent("Not synced");
    expect(screen.getByRole("columnheader", { name: /Sync/ })).toBeInTheDocument();
  });

  it("links to Context Sync filtered to the product for a holder of org_settings", async () => {
    auth.capabilities = ["data_product_read", "org_settings"];
    render(<DataProductsPage />);

    await waitFor(() =>
      expect(screen.getByTestId("data-products-sync-pet_health").tagName).toBe("A"),
    );
    const link = screen.getByTestId("data-products-sync-pet_health");
    expect(link).toHaveAttribute("href", "/context/sync?product=pet_health");
  });

  it("names the status in plain text for a viewer without Context Sync's right", async () => {
    render(<DataProductsPage />);

    await waitFor(() =>
      expect(screen.getByTestId("data-products-sync-pet_health").tagName).toBe("SPAN"),
    );
    const cell = screen.getByTestId("data-products-sync-pet_health");
    expect(cell.closest("a")).toBeNull();
  });

  it("says when the status could not be read rather than showing every product not synced", async () => {
    syncRead.fetchProductSyncStatus.mockRejectedValue(new Error("Sync status fetch failed (500)"));
    render(<DataProductsPage />);

    expect(await screen.findByTestId("data-products-sync-error")).toHaveTextContent(
      "Sync status fetch failed (500)",
    );
  });

  it("reads the status once with the list", async () => {
    render(<DataProductsPage />);
    await waitFor(() => expect(syncRead.fetchProductSyncStatus).toHaveBeenCalledTimes(1));
  });
});
