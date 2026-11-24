// Copyright (c) 2026 Kenneth Stott
// Canary: 79b60b1e-6b8c-4966-9851-e12fc75e49a7
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
import { render, screen, waitFor, within } from "../../test-utils/render";

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
    namedOwners: [],
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
    namedOwners: [],
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
    namedOwners: [],
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

/** The options of the dropdown ``input`` controls; each facet keeps its own list. */
const optionsOf = (input: HTMLElement) => {
  const list = document.getElementById(input.getAttribute("aria-controls") ?? "");
  expect(list).not.toBeNull();
  return within(list as HTMLElement)
    .getAllByRole("option", { hidden: true })
    .map((o) => o.textContent);
};

/** Picks ``tag`` in the list's tag facet. */
async function pick(tag: string) {
  await userEvent.click(facet());
  await userEvent.click(await screen.findByRole("option", { name: tag, hidden: true }));
}

describe("the Data Products list's tag facet", () => {
  beforeEach(() => {
    localStorage.clear();
    metaRead.unreadable = false;
  });

  it("offers every tag a product carries, parameterized ones included, and no other", async () => {
    render(<DataProductsPage />, { initialEntries: ["/data-products"] });
    await waitFor(() => expect(listed()).toHaveLength(3));
    await userEvent.click(facet());
    await screen.findByRole("option", { name: "risk:high", hidden: true });
    expect(optionsOf(facet())).toEqual(["residency:eu", "risk:high"]);
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

/** The domain facet's input. */
const domainFacet = () => screen.getByRole("textbox", { name: "Domains" });

/** Picks ``domain`` in the list's domain facet. */
async function pickDomain(domain: string) {
  await userEvent.click(domainFacet());
  await userEvent.click(await screen.findByRole("option", { name: domain, hidden: true }));
}

describe("the Data Products list's domain facet", () => {
  beforeEach(() => {
    localStorage.clear();
    metaRead.unreadable = false;
  });

  it("offers the domain of every product listed, and no other", async () => {
    render(<DataProductsPage />, { initialEntries: ["/data-products"] });
    await waitFor(() => expect(listed()).toHaveLength(3));
    await userEvent.click(domainFacet());
    await screen.findByRole("option", { name: "care", hidden: true });
    expect(optionsOf(domainFacet())).toEqual(["care", "ops", "sales"]);
  });

  it("narrows the list to the products of the domains chosen", async () => {
    render(<DataProductsPage />, { initialEntries: ["/data-products"] });
    await waitFor(() => expect(listed()).toHaveLength(3));
    await pickDomain("care");
    expect(listed()).toEqual(["pet_health"]);
    await pickDomain("ops");
    expect(listed()).toEqual(["pet_health", "workforce_ops"]);
  });

  it("composes with the tag facet", async () => {
    render(<DataProductsPage />, { initialEntries: ["/data-products"] });
    await waitFor(() => expect(listed()).toHaveLength(3));
    await pick("risk:high");
    expect(listed()).toEqual(["eu_customers", "pet_health"]);
    await pickDomain("sales");
    expect(listed()).toEqual(["eu_customers"]);
  });

  it("composes with the text filter", async () => {
    render(<DataProductsPage />, { initialEntries: ["/data-products?search=pet"] });
    await waitFor(() => expect(listed()).toEqual(["pet_health"]));
    await pickDomain("ops");
    expect(listed()).toEqual([]);
    expect(screen.getByTestId("data-products-empty")).toHaveTextContent(
      "No data products match the filters",
    );
  });
});

describe("the Data Products list's empty text", () => {
  beforeEach(() => {
    localStorage.clear();
    metaRead.unreadable = false;
  });

  it("says nothing of products existing when the role cannot read them", async () => {
    metaRead.unreadable = true;
    render(<DataProductsPage />, { initialEntries: ["/data-products"] });
    await screen.findByTestId("data-products-unreadable");
    expect(screen.queryByTestId("data-products-empty")).toBeNull();
  });
});
