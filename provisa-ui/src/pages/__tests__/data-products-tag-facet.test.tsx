// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-2180 (THE PRODUCT PAGE IS SUMMARY ROWS): the products list is faceted by the products' tags,
 * parameterized ones included (residency:eu, risk:high), with the list's standard filter controls:
 * a multi-select of every tag a listed product carries. Each tag chosen narrows the list to the
 * products carrying it.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor } from "../../test-utils/render";

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

const listed = () =>
  ["eu_customers", "pet_health", "workforce_ops"].filter(
    (id) => screen.queryByTestId(`data-products-row-${id}`) !== null,
  );

/** The facet's input, named as the list's tag filter. */
const facet = () => screen.getByRole("textbox", { name: "Tags" });

/** Picks ``tag`` in the list's tag facet. */
async function pick(tag: string) {
  await userEvent.click(facet());
  await userEvent.click(await screen.findByRole("option", { name: tag, hidden: true }));
}

describe("the Data Products list's tag facet", () => {
  beforeEach(() => localStorage.clear());

  it("offers every tag a product carries, parameterized ones included, and no other", async () => {
    render(<DataProductsPage />, { initialEntries: ["/data-products"] });
    await waitFor(() => expect(listed()).toHaveLength(3));
    await userEvent.click(facet());
    await screen.findByRole("option", { name: "risk:high", hidden: true });
    expect(screen.getAllByRole("option", { hidden: true }).map((o) => o.textContent)).toEqual([
      "residency:eu",
      "risk:high",
    ]);
  });

  it("narrows the list to the products carrying each tag chosen", async () => {
    render(<DataProductsPage />, { initialEntries: ["/data-products"] });
    await waitFor(() => expect(listed()).toHaveLength(3));
    await pick("risk:high");
    expect(listed()).toEqual(["eu_customers", "pet_health"]);
    await pick("residency:eu");
    expect(listed()).toEqual(["eu_customers"]);
  });
});
