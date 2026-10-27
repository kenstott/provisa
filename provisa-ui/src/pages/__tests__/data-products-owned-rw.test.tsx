// Copyright (c) 2026 Kenneth Stott
// Canary: c1395c22-e090-4f5f-b04d-1d4711c3d894
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2155, REQ-2160: data_product_owned_rw edits the products whose owner role the caller holds,
// itself or through an ancestor; deleting stays data_product_rw's. The owner view follows the
// owner and team roles, the same way.

import { describe, it, expect, vi } from "vitest";
import { render, screen } from "../../test-utils/render";

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
// The caller acts as vet_lead, which inherits care_owner: it owns pet_health, not workforce_ops.
vi.mock("../../context/AuthContext", () => ({
  useAuth: () => ({
    role: { id: "vet_lead", capabilities: [] },
    selectedRoles: [{ id: "vet_lead", parents: ["care_owner"] }],
    availableRoles: [{ id: "vet_lead", parents: ["care_owner"] }],
  }),
}));
// Not data_product_rw: data_product_owned_rw alone.
vi.mock("../../hooks/useCapability", () => ({
  useCapability: (cap: string) => cap === "data_product_owned_rw",
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
  DataProductDetailPanel: (props: {
    p: { id: string };
    canEdit: boolean;
    canDelete: boolean;
    owner: boolean;
  }) => (
    <div
      data-testid={`panel-${props.p.id}`}
      data-edit={String(props.canEdit)}
      data-delete={String(props.canDelete)}
      data-owner={String(props.owner)}
    />
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
    ownerRole: "care_owner",
  },
  {
    id: "workforce_ops",
    domainId: "ops",
    name: "Workforce Ops",
    purpose: "Shelter staffing",
    limitations: "",
    usage: "",
    customProperties: {},
    ownerRole: "ops_owner",
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
  useTables: () => ({ tables: none, loading: false, refetch: vi.fn() }),
  useSources: () => ({ sources: none, loading: false, refetch: vi.fn() }),
  useRelationships: () => ({ relationships: none, loading: false, refetch: vi.fn() }),
  useRoles: () => ({ roles: none, loading: false, refetch: vi.fn() }),
  useUpdateTable: () => ({ updateTable: vi.fn(), loading: false }),
}));

import { DataProductsPage } from "../DataProductsPage";

describe("editing a product with data_product_owned_rw (REQ-2155)", () => {
  it("edits the product whose owner role it inherits, and deletes none", async () => {
    render(<DataProductsPage />, { initialEntries: ["/data-products?product=pet_health"] });
    const panel = await screen.findByTestId("panel-pet_health");
    expect(panel).toHaveAttribute("data-edit", "true");
    expect(panel).toHaveAttribute("data-delete", "false");
    expect(panel).toHaveAttribute("data-owner", "true");
  });

  it("edits no product whose owner role it does not hold", async () => {
    render(<DataProductsPage />, { initialEntries: ["/data-products?product=workforce_ops"] });
    const panel = await screen.findByTestId("panel-workforce_ops");
    expect(panel).toHaveAttribute("data-edit", "false");
    expect(panel).toHaveAttribute("data-owner", "false");
  });
});
