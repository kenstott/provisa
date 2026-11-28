// Copyright (c) 2026 Kenneth Stott
// Canary: 3b8e1f6a-9d27-4c50-8a1e-62f4d0c7b915
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1659 (amended 2026-11-28 twice): the Data Products page has no domain filter of its own; it
 * uses the navbar Domains filter. On this page the filter and the list follow the active role as
 * one union: each domain where the role holds data product read or write brings in that domain and
 * all its products, whatever their status; each product offered (live) brings in its domain and
 * itself. A domain the role may only create tables in brings in nothing. The page's selection is
 * kept apart from the role's. A product's tags are edited from its row.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { fireEvent, render, screen, waitFor, within } from "../../test-utils/render";
import { offeredTagsFor } from "../../test-utils/offeredTags";
import type { Tag, TagTarget } from "../../types/admin";
import { storageKey as scopedKey } from "../../lib/browserStorage";
import {
  CHECKED_DOMAINS_KEY,
  KNOWN_DOMAINS_KEY,
  PRODUCT_DOMAIN_KEYS,
} from "../../lib/domainFilterKeys";

// steward: read-write on products in logistics. reader: read on products in finance. analyst: no
// product right; sales and hr are domains it may create tables in, and no product is in hr.
const role = (id: string, rights: string[], domains: string[]) => ({
  id,
  capabilities: rights,
  effectiveCapabilities: rights,
  domain_access: domains,
});
const STEWARD = role("steward", ["data_product_rw"], ["logistics"]);
const READER = role("reader", ["data_product_read"], ["finance"]);
const ANALYST = role("analyst", ["table_registration"], ["sales", "hr"]);
const ROLES = [STEWARD, READER, ANALYST];
// The active role set, switched by a test.
const auth: { selected: (typeof ROLES)[number][] } = { selected: [ANALYST] };

vi.mock("../../context/AuthContext", () => ({
  useAuth: () => ({
    displayName: "t",
    email: "t@example.com",
    devMode: true,
    authEnabled: false,
    capabilities: [] as string[],
    demonstrated: [] as string[],
    billing: null,
    activeOrgId: null,
    role: auth.selected[0],
    selectedRoles: auth.selected,
    availableRoles: ROLES,
    actingRoles: auth.selected.map((r) => r.id).join(","),
    userId: null,
    loading: false,
  }),
}));
vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  fetchSettings: () => Promise.resolve({ naming: { use_domains: true } }),
}));

// What the bar embeds is not under test: only its domain selector is.
vi.mock("../../context/subnavExtraSlot", () => ({
  useSubnavExtraSlot: () => ({ setNode: () => {} }),
}));
vi.mock("../../components/RoleSelector", () => ({ RoleSelector: () => null }));
vi.mock("../../components/OrgSwitcher", () => ({ OrgSwitcher: () => null }));
vi.mock("../../components/EnvSwitcher", () => ({ EnvSwitcher: () => null }));
vi.mock("../../components/UserProfileModal", () => ({ UserProfileModal: () => null }));
vi.mock("../../components/WhatsNewModal", () => ({ WhatsNewModal: () => null }));
vi.mock("../../components/CapabilityGate", () => ({ CapabilityGate: () => null }));
vi.mock("../../tour/useTour", () => ({
  useTour: () => ({
    startTour: () => {},
    canResume: false,
    coreSeen: false,
    status: "idle",
    available: false,
  }),
}));

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
vi.mock("../../hooks/useCapability", () => ({
  useCapability: (cap: string) => cap === "data_product_rw",
}));
vi.mock("../../api/actions", () => ({
  fetchActions: () => Promise.resolve({ functions: [] }),
  saveFunction: vi.fn(),
}));
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
vi.mock("../../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/glossary")>()),
  fetchRelatedGlossaryTerms: () => Promise.resolve([]),
}));
vi.mock("../data-products/DataProductDetailPanel", () => ({
  DataProductDetailPanel: ({ p }: { p: { id: string } }) => (
    <div data-testid="detail-panel">{p.id}</div>
  ),
}));

const product = (id: string, domainId: string, name: string, status: string) => ({
  id,
  domainId,
  name,
  status,
  owners: [],
  namedOwners: [],
  purpose: "",
  limitations: "",
  usage: "",
  customProperties: {},
});
const products = [
  product("eu_customers", "sales", "EU Customers", "live"),
  product("pet_health", "care", "Pet Health", "live"),
  product("workforce_ops", "logistics", "Workforce Ops", "draft"),
  product("old_ledger", "finance", "Old Ledger", "retired"),
];
const ALL_IDS = products.map((p) => p.id);

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
  // The domains the role may create tables in: hr is one, and no product is in it.
  useDomains: () => ({ domains: DOMAIN_ROWS, loading: false, refetch: vi.fn() }),
  useTables: () => ({ tables: none, loading: false, refetch: vi.fn() }),
  useSources: () => ({ sources: none, loading: false, refetch: vi.fn() }),
  useRelationships: () => ({ relationships: none, loading: false, refetch: vi.fn() }),
  useRoles: () => ({ roles: none, loading: false, refetch: vi.fn() }),
  useUpdateTable: () => ({ updateTable: vi.fn(), loading: false }),
}));
const DOMAIN_ROWS = [
  { id: "sales", description: "" },
  { id: "hr", description: "" },
];

const assignSpy = vi.hoisted(() => vi.fn(async () => ({ success: true, message: "ok" })));
const TAGS: Tag[] = [
  {
    id: "certified",
    description: "Certified product",
    appliesTo: ["product"],
    isSystem: false,
    derived: false,
    reasonPolicy: "hidden",
    expiresPolicy: "hidden",
    paramPolicy: "none",
    requiresCitation: false,
    required: false,
    allowedDomains: [],
    citations: [],
    needsReview: false,
    paramValues: [],
  },
];
vi.mock("../../hooks/useTagQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useTagQueries")>()),
  useTags: () => ({ tags: TAGS, loading: false }),
  useTagAssignments: () => ({
    tagAssignments: none,
    loading: false,
    error: undefined,
    refetch: vi.fn(),
  }),
  useAssignTag: () => ({ assignTag: assignSpy }),
  useUnassignTag: () => ({ unassignTag: vi.fn() }),
  useOfferedTags: (target: TagTarget) => offeredTagsFor(TAGS, target, new Set()),
}));

import { DomainFilterProvider } from "../../context/DomainFilterContext";
import { NavBar } from "../../components/NavBar";
import { DataProductsPage } from "../DataProductsPage";

function renderAt(path: string, page: React.ReactNode = <DataProductsPage />) {
  return render(
    <DomainFilterProvider>
      <NavBar />
      {page}
    </DomainFilterProvider>,
    { initialEntries: [path] },
  );
}

// /data/domains answers the asking role's domains; every other read goes where it went.
function stubRoleDomains() {
  const real = globalThis.fetch;
  vi.stubGlobal(
    "fetch",
    vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input) !== "/data/domains") return real(input, init);
      const asking = (init?.headers as Record<string, string>)["X-Provisa-Role"];
      const r = auth.selected.find((x) => x.id === asking);
      if (!r) throw new Error(`no role ${asking}`);
      return Promise.resolve(new Response(JSON.stringify(r.domain_access)));
    }),
  );
}

const listed = () =>
  ALL_IDS.filter((id) => screen.queryByTestId(`data-products-row-${id}`) !== null);

/** The ids the open navbar Domains menu lists, in order. */
async function menuDomains(): Promise<string[]> {
  await userEvent.click(screen.getByTestId("navbar-domain-trigger"));
  const label = await screen.findByText("Domains", { selector: ".mantine-Menu-label" });
  const menu = label.closest<HTMLElement>('[role="menu"]');
  if (!menu) throw new Error("the Domains label is not inside the domains menu");
  return Array.from(menu.querySelectorAll<HTMLElement>('[data-testid^="navbar-domain-item-"]'))
    .map((el) => (el.dataset.testid as string).replace("navbar-domain-item-", ""))
    .filter((id, i, all) => all.indexOf(id) === i);
}

/** Unchecks or checks ``domain`` in the open navbar Domains menu. */
function toggleInMenu(domain: string) {
  const item = screen
    .getAllByTestId(`navbar-domain-item-${domain}`)
    .find((el) => el instanceof HTMLInputElement);
  if (!item) throw new Error(`no checkbox for ${domain}`);
  fireEvent.click(item);
}

afterEach(() => {
  vi.unstubAllGlobals();
});

/** Waits for the navbar trigger to count ``n`` of ``n`` domains. */
async function domainCount(n: number) {
  await waitFor(() =>
    expect(screen.getByTestId("navbar-domain-trigger")).toHaveTextContent(`Domains (${n}/${n})`),
  );
}

describe("the Data Products page's domain filter", () => {
  beforeEach(() => {
    localStorage.clear();
    assignSpy.mockClear();
    auth.selected = [ANALYST];
    stubRoleDomains();
  });

  it("is the navbar's: the page has no domain filter of its own", async () => {
    renderAt("/data-products");
    await waitFor(() => expect(listed()).toHaveLength(2));
    expect(screen.queryByTestId("data-products-domain-facet")).toBeNull();
    expect(screen.queryByRole("textbox", { name: "Domains" })).toBeNull();
    expect(await screen.findByTestId("navbar-domain-trigger")).toBeInTheDocument();
  });

  it("shows a role with no product right only the offered products, and their domains", async () => {
    renderAt("/data-products");
    await domainCount(2);
    expect(listed()).toEqual(["eu_customers", "pet_health"]);
    // care is outside the role; hr, a domain it may only create tables in, brings in nothing.
    expect(await menuDomains()).toEqual(["care", "sales"]);
  });

  it("shows a read-write domain's every product, a draft included", async () => {
    auth.selected = [STEWARD];
    renderAt("/data-products");
    await domainCount(3);
    expect(listed()).toEqual(["eu_customers", "pet_health", "workforce_ops"]);
    expect(await menuDomains()).toEqual(["care", "logistics", "sales"]);
  });

  it("shows a read domain's every product, a retired one included", async () => {
    auth.selected = [READER];
    renderAt("/data-products");
    await domainCount(3);
    expect(listed()).toEqual(["eu_customers", "pet_health", "old_ledger"]);
    expect(await menuDomains()).toEqual(["care", "finance", "sales"]);
  });

  it("shows a role set the union of its roles", async () => {
    auth.selected = [STEWARD, READER, ANALYST];
    renderAt("/data-products");
    await domainCount(4);
    expect(listed()).toEqual(ALL_IDS);
    expect(await menuDomains()).toEqual(["care", "finance", "logistics", "sales"]);
  });

  it("brings in a product-right domain with no product in it", async () => {
    auth.selected = [role("curator", ["data_product_read"], ["hr"])];
    renderAt("/data-products");
    await domainCount(3);
    expect(listed()).toEqual(["eu_customers", "pet_health"]);
    expect(await menuDomains()).toEqual(["care", "hr", "sales"]);
  });

  it("switches the list and the domains with the role", async () => {
    auth.selected = [STEWARD];
    const view = renderAt("/data-products");
    await domainCount(3);
    expect(listed()).toEqual(["eu_customers", "pet_health", "workforce_ops"]);
    auth.selected = [ANALYST];
    view.rerender(
      <DomainFilterProvider>
        <NavBar />
        <DataProductsPage />
      </DomainFilterProvider>,
    );
    await domainCount(2);
    expect(listed()).toEqual(["eu_customers", "pet_health"]);
  });

  it("lists the role's domains on the other pages", async () => {
    renderAt("/tables", null);
    await domainCount(2);
    expect(await menuDomains()).toEqual(["hr", "sales"]);
  });

  it("narrows the list to the products of the domains checked", async () => {
    auth.selected = [STEWARD, READER];
    renderAt("/data-products");
    await domainCount(4);
    await menuDomains();
    toggleInMenu("care");
    await waitFor(() => expect(listed()).toEqual(["eu_customers", "workforce_ops", "old_ledger"]));
    toggleInMenu("logistics");
    await waitFor(() => expect(listed()).toEqual(["eu_customers", "old_ledger"]));
    toggleInMenu("care");
    await waitFor(() => expect(listed()).toEqual(["eu_customers", "pet_health", "old_ledger"]));
  });

  // The two lists hold different domains, so each keeps its own selection: a domain absent from
  // one list is neither dropped from nor added to the other's.
  it("keeps its selection apart from the role's", async () => {
    // The role's selection: sales unchecked, and a domain no product is in (hr) checked.
    localStorage.setItem(scopedKey(CHECKED_DOMAINS_KEY), JSON.stringify(["hr"]));
    localStorage.setItem(scopedKey(KNOWN_DOMAINS_KEY), JSON.stringify(["sales", "hr"]));
    renderAt("/data-products");
    await domainCount(2);
    // The role's unchecked sales does not narrow the products.
    expect(listed()).toEqual(["eu_customers", "pet_health"]);
    await menuDomains();
    toggleInMenu("care");
    await waitFor(() =>
      expect(
        JSON.parse(localStorage.getItem(scopedKey(PRODUCT_DOMAIN_KEYS.checked)) ?? "[]"),
      ).toEqual(["sales"]),
    );
    // The role's selection is as it was: hr kept, sales still unchecked.
    expect(JSON.parse(localStorage.getItem(scopedKey(CHECKED_DOMAINS_KEY)) ?? "[]")).toEqual([
      "hr",
    ]);
    expect(JSON.parse(localStorage.getItem(scopedKey(KNOWN_DOMAINS_KEY)) ?? "[]")).toEqual([
      "sales",
      "hr",
    ]);
  });
});

describe("a product's tags in the Data Products list", () => {
  beforeEach(() => {
    localStorage.clear();
    assignSpy.mockClear();
    auth.selected = [STEWARD];
    stubRoleDomains();
  });

  it("are edited from its row, beside its id, without opening it", async () => {
    renderAt("/data-products");
    await waitFor(() => expect(listed()).toHaveLength(3));
    const row = screen.getByTestId("data-products-row-pet_health");
    const control = within(row).getByTestId("tag-control-product-pet_health");
    expect(control.parentElement).toHaveTextContent("pet_health");
    fireEvent.click(within(control).getByTestId("tag-picker-toggle"));
    fireEvent.click(await screen.findByTestId("tag-option-certified"));
    fireEvent.click(await screen.findByTestId("tag-apply-certified"));
    await waitFor(() =>
      expect(assignSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          tagId: "certified",
          objectType: "product",
          productId: "pet_health",
        }),
        [],
      ),
    );
    // The click stayed in the control: the row did not open.
    expect(screen.queryByTestId("detail-panel")).toBeNull();
  });
});
