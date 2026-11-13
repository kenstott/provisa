// Copyright (c) 2026 Kenneth Stott
// Canary: 628d0979-e268-443f-9876-4e2f93f77e6d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Creating a data product: "+ New" opens an empty form whatever product was selected or edited
// before it, and Duplicate (the panel head, beside Edit) opens the create form filled from the
// selected product under a new id: its definition and its member tables, not its consumers,
// contracts, offers or bids (rule: components/list/__tests__/panelDuplicateGuard.test.ts).

import { describe, it, expect, vi, beforeEach } from "vitest";
import type { ReactNode } from "react";
import { render, screen, waitFor, fireEvent } from "../../test-utils/render";
import { expectHoldsNoneOf } from "../../test-utils/createForm";

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
vi.mock("../../hooks/useCapability", () => ({ useCapability: () => true }));
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
vi.mock("../../api/glossary", () => ({ fetchRelatedGlossaryTerms: () => Promise.resolve([]) }));
// The panel's parts are its own tests'; here it is its Edit and Duplicate, and the edit form the
// page hands it while the product is edited.
vi.mock("../data-products/DataProductDetailPanel", () => ({
  DataProductDetailPanel: ({
    p,
    onEdit,
    onDuplicate,
    editForm,
  }: {
    p: { id: string };
    onEdit: () => void;
    onDuplicate?: () => void;
    editForm?: ReactNode;
  }) =>
    editForm ?? (
      <div>
        <button data-testid={`edit-${p.id}`} onClick={() => onEdit()}>
          edit
        </button>
        {onDuplicate && (
          <button data-testid={`duplicate-${p.id}`} onClick={onDuplicate}>
            duplicate
          </button>
        )}
      </div>
    ),
}));

const products = [
  {
    id: "pet_health",
    domainId: "care",
    name: "Pet Health",
    ownerRole: "vet_lead",
    purpose: "Pet health records",
    limitations: "No billing data",
    usage: "Weekly review",
    version: "1.2",
    customProperties: { tier: "gold" },
  },
];
const none: never[] = [];
const saves = vi.hoisted(() => ({
  create: vi.fn<(input: Record<string, unknown>) => Promise<Record<string, unknown>>>(),
  update: vi.fn<(input: Record<string, unknown>) => Promise<Record<string, unknown>>>(),
  members:
    vi.fn<
      (id: string, tableIds: number[], commands: string[]) => Promise<Record<string, unknown>>
    >(),
}));
vi.mock("../../hooks/useDataProductMutations", () => ({
  useCreateDataProduct: () => ({ createDataProduct: saves.create, loading: false }),
  useUpdateDataProduct: () => ({ updateDataProduct: saves.update, loading: false }),
  useDeleteDataProduct: () => ({ deleteDataProduct: vi.fn(), loading: false }),
  useDeleteDataProducts: () => vi.fn(),
  useSetDataProductMembers: () => ({ setDataProductMembers: saves.members, loading: false }),
  useDataProductFindings: () => ({ findings: new Map(), refetch: vi.fn(), error: undefined }),
  useDataProductStatus: () => ({
    publish: vi.fn(),
    deprecate: vi.fn(),
    retire: vi.fn(),
    restore: vi.fn(),
  }),
}));
const TABLES = [
  {
    id: 1,
    domainId: "care",
    schemaName: "care",
    tableName: "pets",
    alias: null,
    dqContract: null,
    productIds: ["pet_health"],
    columns: [],
  },
];
vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useDomains: () => ({
    domains: [{ id: "care", description: "" }],
    loading: false,
    refetch: vi.fn(),
  }),
  useTables: () => ({ tables: TABLES, loading: false, refetch: vi.fn() }),
  useSources: () => ({ sources: none, loading: false, refetch: vi.fn() }),
  useRelationships: () => ({ relationships: none, loading: false, refetch: vi.fn() }),
  useRoles: () => ({ roles: none, loading: false, refetch: vi.fn() }),
  useUpdateTable: () => ({ updateTable: vi.fn(), loading: false }),
}));

import { DataProductsPage } from "../DataProductsPage";

const VALUES = ["pet_health", "Pet Health", "Pet health records", "No billing data", "1.2"];
const open = () =>
  render(<DataProductsPage />, { initialEntries: ["/data-products?product=pet_health"] });

describe("+ New data product opens an empty form", () => {
  beforeEach(() => localStorage.clear());

  it("holds none of the values of the product selected before it", async () => {
    open();
    await screen.findByTestId("edit-pet_health");
    fireEvent.click(await screen.findByTestId("data-products-new-button"));
    expectHoldsNoneOf(await screen.findByTestId("data-product-create-card"), VALUES);
  });

  it("holds none of them after the product was edited and the edit cancelled", async () => {
    open();
    fireEvent.click(await screen.findByTestId("edit-pet_health"));
    fireEvent.click(await screen.findByTestId("data-product-cancel-button"));
    fireEvent.click(await screen.findByTestId("data-products-new-button"));
    expectHoldsNoneOf(await screen.findByTestId("data-product-create-card"), VALUES);
  });
});

describe("Duplicate, in a data product's panel head", () => {
  beforeEach(() => {
    localStorage.clear();
    saves.create.mockReset();
    saves.update.mockReset();
    saves.members.mockReset();
  });

  it("opens the create form filled from the product, under a new id", async () => {
    open();
    fireEvent.click(await screen.findByTestId("duplicate-pet_health"));
    const card = await screen.findByTestId("data-product-create-card");
    expect(
      card.querySelector<HTMLInputElement>('[data-testid="data-product-id-input"]'),
    ).toHaveValue("pet_health-copy");
    expect(
      card.querySelector<HTMLInputElement>('[data-testid="data-product-name-input"]'),
    ).toHaveValue("Pet Health");
  });

  it("creates the copy as a create, with the definition and the member tables", async () => {
    saves.create.mockResolvedValueOnce({ success: true, message: "created" });
    saves.members.mockResolvedValueOnce({ success: true, message: "set" });
    open();
    fireEvent.click(await screen.findByTestId("duplicate-pet_health"));
    fireEvent.click(await screen.findByTestId("data-product-save-button"));
    await waitFor(() => expect(saves.create).toHaveBeenCalledTimes(1));
    expect(saves.update).not.toHaveBeenCalled();
    expect(saves.create.mock.calls[0][0]).toMatchObject({
      id: "pet_health-copy",
      domainId: "care",
      name: "Pet Health",
      purpose: "Pet health records",
      limitations: "No billing data",
      usage: "Weekly review",
      ownerRole: "vet_lead",
      customProperties: { tier: "gold" },
    });
    await waitFor(() => expect(saves.members).toHaveBeenCalledWith("pet_health-copy", [1], []));
  });

  it("opens + New empty after a duplicate was cancelled", async () => {
    open();
    fireEvent.click(await screen.findByTestId("duplicate-pet_health"));
    fireEvent.click(await screen.findByTestId("data-product-cancel-button"));
    fireEvent.click(await screen.findByTestId("data-products-new-button"));
    expectHoldsNoneOf(await screen.findByTestId("data-product-create-card"), [
      ...VALUES,
      "pet_health-copy",
    ]);
  });
});
