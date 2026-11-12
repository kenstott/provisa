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
import { useLocation, useNavigate } from "react-router-dom";

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

import { DataProductsPage, EXPANDED_STORAGE_KEY } from "../DataProductsPage";
import { storageKey as scopedKey } from "../../lib/browserStorage";

describe("the Data Products list's search", () => {
  it("lists the products matching every word of the search, or either side of or", async () => {
    render(<DataProductsPage />, { initialEntries: ["/data-products"] });
    await waitFor(() =>
      expect(screen.getByTestId("data-products-row-pet_health")).toBeInTheDocument(),
    );
    fireEvent.change(screen.getByTestId("filter-input"), { target: { value: "pet or invoices" } });
    expect(screen.getByTestId("data-products-row-pet_health")).toBeInTheDocument();
    expect(screen.queryByTestId("data-products-row-workforce_ops")).toBeNull();
  });
});

describe("the Data Products list's lifecycle", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it("shows each product's status", async () => {
    render(<DataProductsPage />, { initialEntries: ["/data-products"] });
    expect(await screen.findByTestId("data-products-status-pet_health")).toHaveTextContent("Live");
    expect(screen.getByTestId("data-products-status-workforce_ops")).toHaveTextContent("Draft");
  });

  it("narrows to the products with findings, To review", async () => {
    render(<DataProductsPage />, { initialEntries: ["/data-products"] });
    await screen.findByTestId("data-products-row-workforce_ops");
    fireEvent.click(screen.getByTestId("data-products-to-review-only"));
    expect(screen.getByTestId("data-products-row-pet_health")).toBeInTheDocument();
    expect(screen.queryByTestId("data-products-row-workforce_ops")).toBeNull();
  });
});

describe("Data Products deep link (REQ-1659)", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it("opens the product named by ?product= ahead of the remembered row", async () => {
    localStorage.setItem(scopedKey(EXPANDED_STORAGE_KEY), "pet_health");
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
    await waitFor(() =>
      expect(screen.getByTestId("data-products-row-pet_health")).toBeInTheDocument(),
    );
    fireEvent.click(screen.getByTestId("data-products-row-pet_health"));
    await waitFor(() => expect(screen.getByTestId("detail-panel")).toHaveTextContent("pet_health"));
    expect(localStorage.getItem(scopedKey(EXPANDED_STORAGE_KEY))).toBe("pet_health");
    fireEvent.click(screen.getByTestId("data-products-row-pet_health"));
    await waitFor(() => expect(screen.queryByTestId("detail-panel")).not.toBeInTheDocument());
    expect(localStorage.getItem(scopedKey(EXPANDED_STORAGE_KEY))).toBeNull();
  });
});

// The page's address as the router holds it, and the browser's Back.
function Where() {
  const location = useLocation();
  const navigate = useNavigate();
  return (
    <>
      <output data-testid="where">{location.search}</output>
      <button onClick={() => navigate(-1)}>history-back</button>
    </>
  );
}

describe("Data Products panel in the URL (REQ-2241) beside bulk selection (REQ-2240)", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it("opening a product writes ?product=, and Back closes it", async () => {
    render(
      <>
        <DataProductsPage />
        <Where />
      </>,
      { initialEntries: ["/data-products"] },
    );
    fireEvent.click(await screen.findByTestId("data-products-row-pet_health"));
    await waitFor(() =>
      expect(screen.getByTestId("where")).toHaveTextContent("?product=pet_health"),
    );
    expect(screen.getByTestId("detail-panel")).toHaveTextContent("pet_health");
    fireEvent.click(screen.getByText("history-back"));
    await waitFor(() => expect(screen.queryByTestId("detail-panel")).not.toBeInTheDocument());
    expect(screen.getByTestId("where")).toBeEmptyDOMElement();
  });

  it("ticking a row's checkbox selects it without opening its panel", async () => {
    render(
      <>
        <DataProductsPage />
        <Where />
      </>,
      { initialEntries: ["/data-products"] },
    );
    fireEvent.click(await screen.findByTestId("list-select-pet_health"));
    expect(screen.getByTestId("list-select-pet_health")).toBeChecked();
    expect(screen.getByTestId("data-products-bulk")).toBeInTheDocument();
    expect(screen.queryByTestId("detail-panel")).not.toBeInTheDocument();
    expect(screen.getByTestId("where")).toBeEmptyDOMElement();
  });
});
